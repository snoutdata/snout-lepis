//! DDL fan-out: a schema change reaches every node that holds what it changes, atomically.
//!
//! `plan` decides, for one statement, where it goes and how:
//!
//! - **Transactional** DDL (CREATE/ALTER/DROP of tables, indexes, functions, types, GRANT on
//!   objects, …) runs inside one transaction on each target node, and the transactions commit
//!   together through two-phase commit (`twopc::Coordinator::run_everywhere`, or the session's
//!   own transaction when the client opened one). Either every node has the change or none does.
//! - **Per-node** DDL cannot run inside a transaction block at all (`CREATE INDEX CONCURRENTLY`,
//!   `VACUUM`, `ALTER SYSTEM`, …), so it cannot be atomic: it runs on each node on its own as a
//!   job (`start_job` / `run_job`), and `lepis.ddl_job_node` says where it has run.
//! - **Role** statements go to roles.rs (home first, then the verifier copied).
//!
//! Where: a table nobody has distributed is global and its DDL is the home node's alone; DDL on a
//! sharded or reference table goes to every node; objects with no table (schemas, functions,
//! types, extensions, sequences) go to every node, so a node is never missing what a distributed
//! table's defaults, checks or triggers call. A new table is global until it is distributed.
//!
//! Some DDL is refused because it would break what the catalog says (renaming a distributed
//! table, changing its shard key column) or L15 (a serial, identity or `nextval` default, or a
//! unique key without the shard key, on a sharded table).

use std::collections::HashMap;

use pg_query::NodeEnum;
use pg_query::protobuf::{
	AlterTableType, ConstrType, Constraint, Node, ObjectType, RangeVar, ReindexObjectType,
};

use crate::backend::{Backend, BackendError};
use crate::catalog::{Catalog, NodeId, RelationKind, RelationName, quote_ident};
use crate::roles::{self, RoleChange};
use crate::route::{NOT_ACROSS_NODES, Refusal};
use crate::twopc::{Participant, join_all};

/// The nodes a statement goes to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
	Home,
	/// Every node of the cluster.
	Everywhere,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DdlPlan {
	/// Not DDL this file handles; route as before.
	NotDdl,
	/// One transaction on each target, committed together.
	Transactional(Target),
	/// Cannot run in a transaction block: a job, node by node.
	PerNode(Target),
	/// A role statement: run it on home, then `roles::replicate`.
	Role(RoleChange),
	/// The statement names indexes; which table each is on decides the target. Run
	/// `INDEX_OWNERS_SQL` on home with these names and call `plan` again with the answers.
	NeedIndexOwners(Vec<RelationName>),
	Refuse(Refusal),
}

/// Index → its table, for `NeedIndexOwners`: `$1` is a text[] of `"schema"."index"`; the rows are
/// (index schema, index name, table schema, table name).
pub const INDEX_OWNERS_SQL: &str = "select ni.nspname, ci.relname, nt.nspname, ct.relname \
	from unnest($1::text[]) as x(name) \
	join pg_class ci on ci.oid = to_regclass(x.name) \
	join pg_namespace ni on ni.oid = ci.relnamespace \
	join pg_index i on i.indexrelid = ci.oid \
	join pg_class ct on ct.oid = i.indrelid \
	join pg_namespace nt on nt.oid = ct.relnamespace";

/// The text[] element `INDEX_OWNERS_SQL` takes for one index.
pub fn index_ref(r: &RelationName) -> String {
	format!("{}.{}", quote_ident(&r.schema), quote_ident(&r.table))
}

/// The nodes a target means: the home node, or every node a write may reach
/// (`twopc::writable_nodes`). A joining node, a physical standby among them, is never one: a
/// standby gets the change by replaying its source, and a node add creates what it needs.
pub fn nodes(target: Target, catalog: &Catalog) -> Vec<NodeId> {
	match target {
		Target::Home => catalog.home().map(|n| n.id).into_iter().collect(),
		Target::Everywhere => crate::twopc::writable_nodes(catalog),
	}
}

fn refuse(message: impl Into<String>, hint: impl Into<String>) -> DdlPlan {
	DdlPlan::Refuse(Refusal {
		code: NOT_ACROSS_NODES,
		message: message.into(),
		hint: hint.into(),
	})
}

/// The L15 sentence.
pub const L15_SEQUENCES: &str = "Sequences are not global: on a sharded table use uuidv7() (Postgres 18) or a bigint from a sequence striped by node, never serial, identity or nextval of a plain sequence.";
/// The other half of L15.
pub const L15_UNIQUE: &str = "A unique constraint or primary key on a sharded table must include its shard key, as on a partitioned table: only then can each node check it alone.";

/// Where and how one statement runs. `sql` is ONE statement; `tables` are the relations the
/// parser adapter found in it (`Facts::tables`, resolved through the search path); `index_owners`
/// answers a `NeedIndexOwners` from an earlier call (empty otherwise).
pub fn plan(
	sql: &str,
	tables: &[RelationName],
	catalog: &Catalog,
	index_owners: &HashMap<RelationName, RelationName>,
) -> DdlPlan {
	let Ok(parsed) = pg_query::parse(sql) else {
		return DdlPlan::NotDdl;
	};
	let Some(node) = parsed
		.protobuf
		.stmts
		.first()
		.and_then(|r| r.stmt.as_deref())
		.and_then(|n| n.node.as_ref())
	else {
		return DdlPlan::NotDdl;
	};
	if let Some(change) = roles::change_of(sql) {
		return DdlPlan::Role(change);
	}
	let p = Planner {
		tables,
		catalog,
		index_owners,
	};
	let d = p.statement(node);
	if catalog.nodes.len() <= 1 {
		// One node: nothing to fan out, and nothing a refusal protects.
		return match d {
			DdlPlan::Transactional(_) => DdlPlan::Transactional(Target::Home),
			DdlPlan::PerNode(_) => DdlPlan::PerNode(Target::Home),
			DdlPlan::Refuse(_) | DdlPlan::NeedIndexOwners(_) => {
				DdlPlan::Transactional(Target::Home)
			}
			other => other,
		};
	}
	d
}

struct Planner<'a> {
	tables: &'a [RelationName],
	catalog: &'a Catalog,
	index_owners: &'a HashMap<RelationName, RelationName>,
}

fn opt(n: &Option<Box<Node>>) -> Option<&NodeEnum> {
	n.as_deref().and_then(|n| n.node.as_ref())
}

fn concurrently(params: &[Node]) -> bool {
	params
		.iter()
		.any(|p| matches!(&p.node, Some(NodeEnum::DefElem(d)) if d.defname == "concurrently"))
}

impl Planner<'_> {
	fn kind(&self, r: &RelationName) -> Option<&RelationKind> {
		self.catalog.relations.get(r)
	}

	fn distributed(&self, r: &RelationName) -> bool {
		matches!(
			self.kind(r),
			Some(RelationKind::Sharded { .. } | RelationKind::Reference)
		)
	}

	/// The relation the adapter resolved for a RangeVar in the statement (matched by name).
	fn resolve(&self, rv: &RangeVar) -> Option<&RelationName> {
		self.tables.iter().find(|t| {
			t.table == rv.relname && (rv.schemaname.is_empty() || t.schema == rv.schemaname)
		})
	}

	/// The target for a statement about these tables: everywhere if any is distributed.
	fn by_tables(&self) -> Target {
		if self.tables.iter().any(|t| self.distributed(t)) {
			Target::Everywhere
		} else {
			Target::Home
		}
	}

	fn key_of<'b>(&'b self, r: &RelationName) -> Option<&'b str> {
		match self.kind(r) {
			Some(RelationKind::Sharded { key_column, .. }) => Some(key_column),
			_ => None,
		}
	}

	/// For index-named statements: the tables the indexes are on, or the names to look up.
	fn index_target(&self, indexes: Vec<RelationName>) -> Result<Target, DdlPlan> {
		let mut missing = Vec::new();
		let mut target = Target::Home;
		for i in indexes {
			match self.index_owners.get(&i) {
				Some(t) if self.distributed(t) => target = Target::Everywhere,
				Some(_) => {}
				None => missing.push(i),
			}
		}
		if missing.is_empty() {
			Ok(target)
		} else {
			Err(DdlPlan::NeedIndexOwners(missing))
		}
	}

	fn statement(&self, n: &NodeEnum) -> DdlPlan {
		use NodeEnum as N;
		let tx = DdlPlan::Transactional;
		match n {
			// A new table is global (home) until distributed, unless it is a partition or child
			// of a distributed one.
			N::CreateStmt(c) => {
				let parent = self.tables.iter().skip(1).any(|t| {
					self.distributed(t)
						&& c.inh_relations.iter().any(
							|r| matches!(&r.node, Some(N::RangeVar(rv)) if rv.relname == t.table),
						)
				});
				if parent {
					return refuse(
						"a partition or child of a distributed table cannot be created yet",
						"Create it as a table of its own and distribute it.",
					);
				}
				for e in c.table_elts.iter().chain(&c.constraints) {
					let fks: Vec<&Constraint> = match &e.node {
						Some(N::Constraint(k)) => vec![&**k],
						Some(N::ColumnDef(d)) => d
							.constraints
							.iter()
							.filter_map(|k| match &k.node {
								Some(N::Constraint(k)) => Some(&**k),
								_ => None,
							})
							.collect(),
						_ => Vec::new(),
					};
					for k in fks {
						if k.contype == ConstrType::ConstrForeign as i32
							&& let Some(pk) = &k.pktable
							&& let Some(t) = self.resolve(pk)
							&& matches!(self.kind(t), Some(RelationKind::Sharded { .. }))
						{
							return refuse(
								format!(
									"a foreign key from a new table to the sharded table {t} cannot be checked on one node"
								),
								"Create the table, distribute it in the same keyspace with the key in the foreign key, then add the foreign key.",
							);
						}
					}
				}
				tx(Target::Home)
			}
			N::CreateTableAsStmt(_) | N::ViewStmt(_) | N::SelectStmt(_) => {
				let made = match n {
					N::CreateTableAsStmt(c) => c.into.as_ref().and_then(|i| i.rel.as_ref()),
					N::ViewStmt(v) => v.view.as_ref(),
					N::SelectStmt(s) => s.into_clause.as_ref().and_then(|i| i.rel.as_ref()),
					_ => None,
				};
				let Some(made) = made.and_then(|r| self.resolve(r)) else {
					return DdlPlan::NotDdl;
				};
				// A view or table built from tables: from one node only if that node holds them all.
				let sources: Vec<&RelationName> =
					self.tables.iter().filter(|t| *t != made).collect();
				if let Some(t) = sources
					.iter()
					.find(|t| matches!(self.kind(t), Some(RelationKind::Sharded { .. })))
				{
					return refuse(
						format!(
							"a view or table built from the sharded table {t} is not supported yet"
						),
						"Query the sharded table directly; Lepis routes or scatters each query.",
					);
				}
				if !sources.is_empty() && sources.iter().all(|t| self.distributed(t)) {
					// Reference tables only: every node has every row.
					tx(Target::Everywhere)
				} else {
					tx(Target::Home)
				}
			}
			N::IndexStmt(i) => {
				let target = self.by_tables();
				if let Some(t) = i.relation.as_ref().and_then(|r| self.resolve(r))
					&& let Some(key) = self.key_of(t)
					&& (i.unique || i.primary)
					&& !i
						.index_params
						.iter()
						.any(|p| matches!(&p.node, Some(N::IndexElem(e)) if e.name == key))
				{
					return refuse(
						format!(
							"a unique index on the sharded table {t} must include its key {key}"
						),
						L15_UNIQUE,
					);
				}
				if i.concurrent {
					DdlPlan::PerNode(target)
				} else {
					tx(target)
				}
			}
			N::AlterTableStmt(a) => {
				if a.objtype == ObjectType::ObjectIndex as i32 {
					let idx = a
						.relation
						.as_ref()
						.and_then(|r| self.resolve(r))
						.cloned()
						.into_iter()
						.collect();
					return match self.index_target(idx) {
						Ok(t) => tx(t),
						Err(p) => p,
					};
				}
				let Some(t) = a.relation.as_ref().and_then(|r| self.resolve(r)) else {
					return tx(self.by_tables());
				};
				if let Some(key) = self.key_of(t) {
					for c in &a.cmds {
						if let Some(N::AlterTableCmd(cmd)) = &c.node
							&& let Some(r) = self.alter_cmd_refusal(t, key, cmd)
						{
							return r;
						}
					}
				}
				tx(self.by_tables())
			}
			N::RenameStmt(r) => {
				let t = r.relation.as_ref().and_then(|rv| self.resolve(rv));
				let kind = ObjectType::try_from(r.rename_type).ok();
				if kind == Some(ObjectType::ObjectIndex) {
					return match self.index_target(t.cloned().into_iter().collect()) {
						Ok(t) => tx(t),
						Err(p) => p,
					};
				}
				if let Some(t) = t
					&& self.distributed(t)
				{
					match kind {
						Some(ObjectType::ObjectTable) => {
							return refuse(
								format!(
									"the distributed table {t} cannot be renamed yet: the catalog names it"
								),
								"Create a view with the new name, or move it back to global, rename, and distribute again.",
							);
						}
						Some(ObjectType::ObjectColumn)
							if self.key_of(t) == Some(r.subname.as_str()) =>
						{
							return refuse(
								format!("{}, the shard key of {t}, cannot be renamed", r.subname),
								"The catalog names the key column; it stays as it is.",
							);
						}
						_ => {}
					}
				}
				// A function, type or schema has no table: every node has it.
				tx(if r.relation.is_some() {
					self.by_tables()
				} else {
					Target::Everywhere
				})
			}
			N::AlterObjectSchemaStmt(a) => {
				if let Some(t) = a.relation.as_ref().and_then(|r| self.resolve(r))
					&& self.distributed(t)
				{
					return refuse(
						format!(
							"the distributed table {t} cannot move schema yet: the catalog names it"
						),
						"Leave it in its schema.",
					);
				}
				tx(if a.relation.is_some() {
					self.by_tables()
				} else {
					Target::Everywhere
				})
			}
			N::DropStmt(d) => {
				let kind = ObjectType::try_from(d.remove_type).ok();
				let table_like =
					matches!(
						kind,
						Some(
							ObjectType::ObjectTable
								| ObjectType::ObjectView | ObjectType::ObjectMatview
								| ObjectType::ObjectForeignTable
								| ObjectType::ObjectRule | ObjectType::ObjectTrigger
								| ObjectType::ObjectPolicy
						)
					);
				let target = if kind == Some(ObjectType::ObjectIndex) {
					match self.index_target(self.tables.to_vec()) {
						Ok(t) => t,
						Err(p) => return p,
					}
				} else if table_like {
					self.by_tables()
				} else {
					// Sequences, functions, types, schemas, …: every node has them.
					Target::Everywhere
				};
				if d.concurrent {
					DdlPlan::PerNode(target)
				} else {
					tx(target)
				}
			}
			N::ReindexStmt(r) => {
				let kind = ReindexObjectType::try_from(r.kind).ok();
				let target = match kind {
					Some(ReindexObjectType::ReindexObjectIndex) => {
						match self.index_target(self.tables.to_vec()) {
							Ok(t) => t,
							Err(p) => return p,
						}
					}
					Some(ReindexObjectType::ReindexObjectTable) => self.by_tables(),
					_ => Target::Everywhere,
				};
				let whole = matches!(
					kind,
					Some(
						ReindexObjectType::ReindexObjectSchema
							| ReindexObjectType::ReindexObjectSystem
							| ReindexObjectType::ReindexObjectDatabase
					)
				);
				if whole || concurrently(&r.params) {
					DdlPlan::PerNode(target)
				} else {
					tx(target)
				}
			}
			N::VacuumStmt(_) | N::ClusterStmt(_) => DdlPlan::PerNode(if self.tables.is_empty() {
				Target::Everywhere
			} else {
				self.by_tables()
			}),
			N::RefreshMatViewStmt(r) => {
				let target = self.by_tables();
				if r.concurrent {
					DdlPlan::PerNode(target)
				} else {
					tx(target)
				}
			}
			N::AlterSystemStmt(_) | N::CheckPointStmt(_) => DdlPlan::PerNode(Target::Everywhere),
			// A publication is each node's own, and Realtime reads one per node: it is mirrored on
			// every writable node, so each publishes its own rows of the same tables.
			N::CreatePublicationStmt(p) => self.publication(&p.pubobjects),
			N::AlterPublicationStmt(p) => self.publication(&p.pubobjects),
			N::CreatedbStmt(_)
			| N::DropdbStmt(_)
			| N::AlterDatabaseStmt(_)
			| N::AlterDatabaseSetStmt(_)
			| N::CreateTableSpaceStmt(_)
			| N::DropTableSpaceStmt(_)
			| N::CreateSubscriptionStmt(_)
			| N::AlterSubscriptionStmt(_)
			| N::DropSubscriptionStmt(_) => refuse(
				"databases, tablespaces and subscriptions are each node's own, and Lepis does not manage them through the router",
				"Run it on the node directly.",
			),
			N::CreateSchemaStmt(s) if !s.schema_elts.is_empty() => refuse(
				"CREATE SCHEMA with the objects inside it is not supported on a sharded database",
				"Create the schema, then each object in it.",
			),
			// Trigger, rule, policy, comment, grant, truncate, statistics, ownership: where the table is.
			N::CreateTrigStmt(_)
			| N::RuleStmt(_)
			| N::CreatePolicyStmt(_)
			| N::AlterPolicyStmt(_)
			| N::TruncateStmt(_)
			| N::CreateStatsStmt(_)
			| N::AlterOwnerStmt(_)
			| N::CommentStmt(_)
			| N::SecLabelStmt(_)
			| N::GrantStmt(_)
			| N::LockStmt(_)
				if !self.tables.is_empty() =>
			{
				tx(self.by_tables())
			}
			N::CreateSeqStmt(_)
			| N::AlterSeqStmt(_)
			| N::CreateSchemaStmt(_)
			| N::CreateFunctionStmt(_)
			| N::AlterFunctionStmt(_)
			| N::CreateEventTrigStmt(_)
			| N::AlterEventTrigStmt(_)
			| N::CreateExtensionStmt(_)
			| N::AlterExtensionStmt(_)
			| N::AlterExtensionContentsStmt(_)
			| N::CreateDomainStmt(_)
			| N::AlterDomainStmt(_)
			| N::CompositeTypeStmt(_)
			| N::CreateEnumStmt(_)
			| N::CreateRangeStmt(_)
			| N::AlterEnumStmt(_)
			| N::AlterTypeStmt(_)
			| N::DefineStmt(_)
			| N::AlterOperatorStmt(_)
			| N::AlterDefaultPrivilegesStmt(_)
			| N::DropOwnedStmt(_)
			| N::ReassignOwnedStmt(_)
			| N::CreateCastStmt(_)
			| N::CreateConversionStmt(_)
			| N::CreateTransformStmt(_)
			| N::CreateOpClassStmt(_)
			| N::CreateOpFamilyStmt(_)
			| N::AlterOpFamilyStmt(_)
			| N::CreateAmStmt(_)
			| N::CreatePlangStmt(_)
			| N::AlterCollationStmt(_)
			| N::AlterTsdictionaryStmt(_)
			| N::AlterTsconfigurationStmt(_)
			| N::CreateFdwStmt(_)
			| N::AlterFdwStmt(_)
			| N::CreateForeignServerStmt(_)
			| N::AlterForeignServerStmt(_)
			| N::CreateUserMappingStmt(_)
			| N::AlterUserMappingStmt(_)
			| N::DropUserMappingStmt(_)
			| N::CreateTrigStmt(_)
			| N::RuleStmt(_)
			| N::CreatePolicyStmt(_)
			| N::AlterPolicyStmt(_)
			| N::TruncateStmt(_)
			| N::CreateStatsStmt(_)
			| N::AlterOwnerStmt(_)
			| N::CommentStmt(_)
			| N::SecLabelStmt(_)
			| N::GrantStmt(_) => tx(Target::Everywhere),
			_ => DdlPlan::NotDdl,
		}
	}

	/// CREATE / ALTER PUBLICATION: on every writable node when it names only distributed tables
	/// (or no table, or whole schemas, or FOR ALL TABLES: each node publishes what it holds), on
	/// home alone when it names only global ones, and refused when it mixes the two, since no
	/// one set of nodes has all of them.
	fn publication(&self, objects: &[Node]) -> DdlPlan {
		let mut named = Vec::new();
		for o in objects {
			if let Some(NodeEnum::PublicationObjSpec(s)) = &o.node
				&& let Some(rv) = s.pubtable.as_ref().and_then(|t| t.relation.as_ref())
			{
				named.push(self.resolve(rv).cloned().unwrap_or_else(|| RelationName {
					schema: if rv.schemaname.is_empty() {
						"public".into()
					} else {
						rv.schemaname.clone()
					},
					table: rv.relname.clone(),
				}));
			}
		}
		let distributed = named.iter().filter(|t| self.distributed(t)).count();
		match (distributed, named.len() - distributed) {
			(_, 0) => DdlPlan::Transactional(Target::Everywhere),
			(0, _) => DdlPlan::Transactional(Target::Home),
			_ => refuse(
				"a publication change that names both distributed and global tables",
				"Change the publication twice: once for the distributed tables (every node publishes its rows) and once for the global ones (home only).",
			),
		}
	}

	/// What an ALTER TABLE on a sharded table may not do (L15, and the key the catalog names).
	fn alter_cmd_refusal(
		&self,
		t: &RelationName,
		key: &str,
		cmd: &pg_query::protobuf::AlterTableCmd,
	) -> Option<DdlPlan> {
		let sub = AlterTableType::try_from(cmd.subtype).ok()?;
		match sub {
			AlterTableType::AtDropColumn | AlterTableType::AtAlterColumnType if cmd.name == key => {
				Some(refuse(
					format!("{key}, the shard key of {t}, cannot be dropped or change type"),
					"Every row's node is computed from it; create a new table and move the rows.",
				))
			}
			AlterTableType::AtAddIdentity | AlterTableType::AtSetIdentity => Some(refuse(
				format!("an identity column on the sharded table {t}"),
				L15_SEQUENCES,
			)),
			AlterTableType::AtColumnDefault => match opt(&cmd.def) {
				Some(d) if calls_nextval(d) => Some(refuse(
					format!("a nextval default on the sharded table {t}"),
					L15_SEQUENCES,
				)),
				_ => None,
			},
			AlterTableType::AtAddColumn => match opt(&cmd.def) {
				Some(NodeEnum::ColumnDef(d)) => {
					let serial = d.type_name.as_ref().is_some_and(|tn| {
						tn.names.last().is_some_and(|n| {
							matches!(&n.node, Some(NodeEnum::String(s)) if matches!(
								s.sval.as_str(),
								"serial" | "serial4" | "bigserial" | "serial8" | "smallserial" | "serial2"
							))
						})
					});
					let bad_default = d.constraints.iter().any(|k| match &k.node {
						Some(NodeEnum::Constraint(k)) => {
							k.contype == ConstrType::ConstrIdentity as i32
								|| (k.contype == ConstrType::ConstrDefault as i32
									&& opt(&k.raw_expr).is_some_and(calls_nextval))
						}
						_ => false,
					}) || d
						.raw_default
						.as_deref()
						.and_then(|n| n.node.as_ref())
						.is_some_and(calls_nextval);
					let unique_without_key = d.colname != key
						&& d.constraints.iter().any(|k| {
							matches!(&k.node, Some(NodeEnum::Constraint(k))
								if k.contype == ConstrType::ConstrPrimary as i32
									|| k.contype == ConstrType::ConstrUnique as i32)
						});
					if serial || bad_default {
						Some(refuse(
							format!(
								"{} on the sharded table {t} draws from a sequence",
								d.colname
							),
							L15_SEQUENCES,
						))
					} else if unique_without_key {
						Some(refuse(
							format!(
								"a unique {} on the sharded table {t} without its key {key}",
								d.colname
							),
							L15_UNIQUE,
						))
					} else {
						None
					}
				}
				_ => None,
			},
			AlterTableType::AtAddConstraint => match opt(&cmd.def) {
				Some(NodeEnum::Constraint(k))
					if [
						ConstrType::ConstrPrimary as i32,
						ConstrType::ConstrUnique as i32,
						ConstrType::ConstrExclusion as i32,
					]
					.contains(&k.contype) =>
				{
					let has_key = k
						.keys
						.iter()
						.any(|n| matches!(&n.node, Some(NodeEnum::String(s)) if s.sval == key))
						|| k.exclusions
							.iter()
							.any(|n| format!("{n:?}").contains(&format!("\"{key}\"")));
					// A constraint from an existing index (USING INDEX) was checked when the index was made.
					if has_key || !k.indexname.is_empty() {
						None
					} else {
						Some(refuse(
							format!(
								"a unique constraint on the sharded table {t} without its key {key}"
							),
							L15_UNIQUE,
						))
					}
				}
				_ => None,
			},
			_ => None,
		}
	}
}

/// Whether an expression calls `nextval` anywhere in it.
fn calls_nextval(n: &NodeEnum) -> bool {
	// The tree is deep and varied; its debug form names every function called.
	let s = format!("{n:?}");
	s.contains("sval: \"nextval\"")
}

// ---------------------------------------------------------------------------------------------
// Sequences (L15).

/// The stride of a sequence striped by node: room for this many nodes.
pub const STRIDE: i64 = 1024;

/// The statement that stripes a sequence on one node: from then on it yields `residue`,
/// `residue + STRIDE`, … above `floor` (the highest value any node has handed out), so no two
/// nodes ever hand out the same value. Each node gets its own residue (its node id is the
/// natural one).
pub fn stripe_sql(sequence: &RelationName, residue: i64, floor: i64) -> String {
	let residue = residue.rem_euclid(STRIDE);
	let base = (floor.max(0) / STRIDE + 1) * STRIDE;
	format!(
		"alter sequence {}.{} increment by {STRIDE} minvalue 1 no maxvalue restart with {}",
		quote_ident(&sequence.schema),
		quote_ident(&sequence.table),
		base + residue
	)
}

/// What `distribute_refusal` needs to know about a table's columns (`DISTRIBUTE_FACTS_SQL`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnFacts {
	pub name: String,
	/// The default expression, as `pg_get_expr` prints it.
	pub default: Option<String>,
	pub identity: bool,
	/// The increment of the sequence the column owns, if it owns one.
	pub increment: Option<i64>,
}

/// `$1` the table's regclass text; one row per column: name, default, is identity, increment
/// of its owned sequence.
pub const DISTRIBUTE_COLUMNS_SQL: &str = "select a.attname, pg_get_expr(d.adbin, d.adrelid), \
	a.attidentity <> '', \
	(select s.seqincrement from pg_sequence s \
		where s.seqrelid = pg_get_serial_sequence($1, a.attname)::regclass) \
	from pg_attribute a left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum \
	where a.attrelid = $1::regclass and a.attnum > 0 and not a.attisdropped order by a.attnum";

/// `$1` the table's regclass text; one row per unique/primary/exclusion index: its plain key
/// columns, comma-separated (an expression column shows as empty).
pub const DISTRIBUTE_UNIQUE_SQL: &str = "select (select string_agg(coalesce(col.attname, ''), ',' order by pos.n) \
		from generate_subscripts(ix.indkey, 1) as pos(n) \
		left join pg_attribute col on (col.attrelid, col.attnum) = (ix.indrelid, ix.indkey[pos.n])) \
	from pg_index ix \
	where ix.indrelid = $1::regclass and (ix.indisunique or ix.indisprimary \
		or exists (select from pg_constraint con where con.conindid = ix.indexrelid and con.contype = 'x'))";

/// L15 at distribute time: the refusal, or None when the table may be sharded by `key`.
pub fn distribute_refusal(
	table: &RelationName,
	key: &str,
	columns: &[ColumnFacts],
	unique: &[Vec<String>],
) -> Option<Refusal> {
	for c in columns {
		let from_sequence =
			c.identity || c.default.as_deref().is_some_and(|d| d.contains("nextval("));
		let striped = c.increment.is_some_and(|i| i >= STRIDE);
		if from_sequence && !striped {
			return Some(Refusal {
				code: NOT_ACROSS_NODES,
				message: format!(
					"{table} cannot be sharded: {} draws from a sequence, and sequences are not global",
					c.name
				),
				hint: L15_SEQUENCES.into(),
			});
		}
	}
	for u in unique {
		if !u.iter().any(|c| c == key) {
			return Some(Refusal {
				code: NOT_ACROSS_NODES,
				message: format!(
					"{table} cannot be sharded by {key}: its unique key ({}) does not include it",
					u.join(", ")
				),
				hint: L15_UNIQUE.into(),
			});
		}
	}
	None
}

/// Loads the striped sequences (`lepis.sequence`).
pub const LOAD_STRIPED_SQL: &str = "select schema_name, seq_name from lepis.sequence";

/// The sequences a statement calls `nextval` or `setval` on, by the literal it names them with
/// (as written: `'app.ids'`, `'ids'`).
pub fn sequence_calls(sql: &str) -> Vec<(String, String)> {
	// Postgres's own lexer: a call is `name ( 'literal'`, wherever the statement holds it (the
	// tree walk misses VALUES lists and other corners; the token stream has no corners).
	let Ok(scanned) = pg_query::scan(sql) else {
		return Vec::new();
	};
	let text: Vec<&str> = scanned
		.tokens
		.iter()
		.map(|t| sql.get(t.start as usize..t.end as usize).unwrap_or(""))
		.collect();
	let mut out = Vec::new();
	for (i, t) in text.iter().enumerate() {
		let name = t.trim_matches('"').to_ascii_lowercase();
		if (name != "nextval" && name != "setval") || text.get(i + 1) != Some(&"(") {
			continue;
		}
		let arg = text
			.get(i + 2)
			.filter(|a| a.len() >= 2 && a.starts_with('\'') && a.ends_with('\''))
			.map(|a| a[1..a.len() - 1].replace("''", "'"));
		out.push((name, arg.unwrap_or_default()));
	}
	out
}

/// L15 for a statement the router sends somewhere other than the home node alone: `nextval` on
/// a sequence that is not striped would hand out values another node also hands out, and
/// `setval` on one that is would break its stripe. `striped` answers for a sequence name as the
/// statement wrote it.
pub fn sequence_refusal(sql: &str, striped: impl Fn(&str) -> bool) -> Option<Refusal> {
	for (func, seq) in sequence_calls(sql) {
		let ok = match func.as_str() {
			"nextval" => !seq.is_empty() && striped(&seq),
			_ => false,
		};
		if !ok {
			return Some(Refusal {
				code: NOT_ACROSS_NODES,
				message: format!(
					"{func}({}) off the home node: sequences are not global",
					if seq.is_empty() { "…" } else { &seq }
				),
				hint: L15_SEQUENCES.into(),
			});
		}
	}
	None
}

// ---------------------------------------------------------------------------------------------
// Per-node jobs.

/// One node's part of a job.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobNode {
	pub node: NodeId,
	/// `pending`, `running`, `done` or `failed`.
	pub state: String,
	pub error: Option<String>,
}

/// Records a per-node job for `nodes` and returns its id. `log` is a service connection to the
/// home node.
pub async fn start_job(
	log: &mut Backend,
	sql: &str,
	nodes: &[NodeId],
) -> Result<i64, BackendError> {
	let ids = format!(
		"{{{}}}",
		nodes
			.iter()
			.map(|n| n.0.to_string())
			.collect::<Vec<_>>()
			.join(",")
	);
	let rows = log
		.query(
			"with j as (insert into lepis.ddl_job (sql) values ($1) returning id), \
			n as (insert into lepis.ddl_job_node (job_id, node_id) \
				select j.id, unnest($2::int[]) from j) \
			select id from j",
			&[sql, &ids],
		)
		.await?;
	rows.first()
		.and_then(|r| r.first().cloned().flatten())
		.and_then(|v| v.parse().ok())
		.ok_or_else(|| BackendError::Unsupported("the job was not recorded".into()))
}

/// Runs a job's statement on each participant (in parallel, each on its own, outside any
/// transaction), recording each node's state as it goes. Returns every node's final state; a
/// node that failed says why, and the others are not undone (they cannot be).
pub async fn run_job<P: Participant>(
	log: &mut Backend,
	job: i64,
	sql: &str,
	ps: &mut [P],
) -> Result<Vec<JobNode>, BackendError> {
	let id = job.to_string();
	log.query(
		"update lepis.ddl_job_node set state = 'running', started_at = now(), error = null \
		where job_id = $1::bigint and state <> 'done'",
		&[&id],
	)
	.await?;
	let results = join_all(
		ps.iter_mut()
			.map(|p| async move { p.execute(sql).await })
			.collect(),
	)
	.await;
	for (p, r) in ps.iter().zip(&results) {
		let (state, error) = match r {
			Ok(_) => ("done", String::new()),
			Err(e) => ("failed", e.to_string()),
		};
		log.query(
			"update lepis.ddl_job_node set state = $3, error = nullif($4, ''), finished_at = now() \
			where job_id = $1::bigint and node_id = $2::int",
			&[&id, &p.node().0.to_string(), state, &error],
		)
		.await?;
	}
	job_status(log, job).await
}

/// Every node's state for a job.
pub async fn job_status(log: &mut Backend, job: i64) -> Result<Vec<JobNode>, BackendError> {
	Ok(log
		.query(
			"select node_id, state, error from lepis.ddl_job_node where job_id = $1::bigint order by node_id",
			&[&job.to_string()],
		)
		.await?
		.into_iter()
		.map(|r| JobNode {
			node: NodeId(
				r.first()
					.cloned()
					.flatten()
					.and_then(|v| v.parse().ok())
					.unwrap_or(0),
			),
			state: r.get(1).cloned().flatten().unwrap_or_default(),
			error: r.get(2).cloned().flatten(),
		})
		.collect())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{Keyspace, Node, NodeState, Strategy};
	use crate::config::SslMode;
	use crate::hash::KeyType;

	fn rel(t: &str) -> RelationName {
		RelationName {
			schema: "app".into(),
			table: t.into(),
		}
	}

	fn catalog(nodes: i32) -> Catalog {
		let mut c = Catalog::default();
		for i in 1..=nodes {
			c.nodes.insert(
				NodeId(i),
				Node {
					id: NodeId(i),
					name: format!("n{i}"),
					host: "h".into(),
					port: 5432,
					dbname: "app".into(),
					sslmode: SslMode::Disable,
					home: i == 1,
					state: NodeState::Active,
					server_version_num: Some(180_000),
				},
			);
		}
		c.keyspaces.insert(
			"tenant".into(),
			Keyspace {
				name: "tenant".into(),
				strategy: Strategy::Hash,
				key_type: KeyType::Int8,
				seed: 1,
				ranges: Keyspace::even_ranges(
					nodes as usize,
					&c.nodes.keys().copied().collect::<Vec<_>>(),
				),
				pins: Default::default(),
			},
		);
		c.relations.insert(
			rel("orders"),
			RelationKind::Sharded {
				keyspace: "tenant".into(),
				key_column: "tenant_id".into(),
			},
		);
		c.relations
			.insert(rel("countries"), RelationKind::Reference);
		c.relations.insert(rel("plans"), RelationKind::Global);
		c
	}

	fn p(sql: &str, tables: &[&str]) -> DdlPlan {
		let t: Vec<RelationName> = tables.iter().map(|t| rel(t)).collect();
		plan(sql, &t, &catalog(3), &HashMap::new())
	}

	fn refused(d: &DdlPlan) -> bool {
		matches!(d, DdlPlan::Refuse(_))
	}

	#[test]
	fn where_ddl_goes() {
		use DdlPlan::*;
		use Target::*;
		assert_eq!(
			p("create table app.t (id int)", &["t"]),
			Transactional(Home)
		);
		assert_eq!(
			p("alter table app.plans add column x int", &["plans"]),
			Transactional(Home)
		);
		assert_eq!(
			p("alter table app.orders add column note text", &["orders"]),
			Transactional(Everywhere)
		);
		assert_eq!(
			p("create index on app.orders (note)", &["orders"]),
			Transactional(Everywhere)
		);
		assert_eq!(
			p(
				"create index concurrently on app.orders (note)",
				&["orders"]
			),
			PerNode(Everywhere)
		);
		assert_eq!(
			p("create index on app.plans (x)", &["plans"]),
			Transactional(Home)
		);
		assert_eq!(
			p(
				"create function f() returns int language sql as 'select 1'",
				&[]
			),
			Transactional(Everywhere)
		);
		assert_eq!(p("create schema s", &[]), Transactional(Everywhere));
		assert_eq!(
			p("create type mood as enum ('a')", &[]),
			Transactional(Everywhere)
		);
		assert_eq!(
			p("grant select on app.orders to r", &["orders"]),
			Transactional(Everywhere)
		);
		assert_eq!(
			p("grant select on app.plans to r", &["plans"]),
			Transactional(Home)
		);
		assert_eq!(
			p("grant usage on schema app to r", &[]),
			Transactional(Everywhere)
		);
		assert_eq!(
			p("truncate app.countries", &["countries"]),
			Transactional(Everywhere)
		);
		assert_eq!(
			p("drop table app.orders", &["orders"]),
			Transactional(Everywhere)
		);
		assert_eq!(p("drop function f()", &[]), Transactional(Everywhere));
		assert_eq!(p("vacuum", &[]), PerNode(Everywhere));
		assert_eq!(p("vacuum app.plans", &["plans"]), PerNode(Home));
		assert_eq!(p("analyze app.orders", &["orders"]), PerNode(Everywhere));
		assert_eq!(
			p("alter system set work_mem = '8MB'", &[]),
			PerNode(Everywhere)
		);
		assert_eq!(p("select 1", &[]), NotDdl);
		assert!(matches!(p("create role r", &[]), Role(_)));
		assert!(matches!(p("grant r to s", &[]), Role(_)));
		assert_eq!(
			p(
				"create view app.v as select * from app.countries",
				&["v", "countries"]
			),
			Transactional(Everywhere)
		);
		assert_eq!(
			p(
				"create view app.v as select * from app.plans",
				&["v", "plans"]
			),
			Transactional(Home)
		);
	}

	#[test]
	fn index_names_are_looked_up_on_home() {
		let c = catalog(3);
		let d = plan(
			"drop index app.orders_note",
			&[rel("orders_note")],
			&c,
			&HashMap::new(),
		);
		assert_eq!(d, DdlPlan::NeedIndexOwners(vec![rel("orders_note")]));
		let owners = HashMap::from([(rel("orders_note"), rel("orders"))]);
		assert_eq!(
			plan(
				"drop index app.orders_note",
				&[rel("orders_note")],
				&c,
				&owners
			),
			DdlPlan::Transactional(Target::Everywhere)
		);
		assert_eq!(
			plan(
				"drop index concurrently app.orders_note",
				&[rel("orders_note")],
				&c,
				&owners
			),
			DdlPlan::PerNode(Target::Everywhere)
		);
		let owners = HashMap::from([(rel("plans_x"), rel("plans"))]);
		assert_eq!(
			plan(
				"alter index app.plans_x rename to plans_y",
				&[rel("plans_x")],
				&c,
				&owners
			),
			DdlPlan::Transactional(Target::Home)
		);
	}

	#[test]
	fn what_would_break_the_catalog_or_l15_is_refused() {
		assert!(refused(&p(
			"alter table app.orders rename to o2",
			&["orders"]
		)));
		assert!(refused(&p(
			"alter table app.orders rename column tenant_id to t",
			&["orders"]
		)));
		assert!(!refused(&p(
			"alter table app.orders rename column note to n",
			&["orders"]
		)));
		assert!(refused(&p(
			"alter table app.orders drop column tenant_id",
			&["orders"]
		)));
		assert!(refused(&p(
			"alter table app.orders alter column tenant_id type text",
			&["orders"]
		)));
		assert!(refused(&p(
			"alter table app.orders add column id serial",
			&["orders"]
		)));
		assert!(refused(&p(
			"alter table app.orders add column id bigint generated always as identity",
			&["orders"]
		)));
		assert!(refused(&p(
			"alter table app.orders add column id bigint default nextval('s')",
			&["orders"]
		)));
		assert!(refused(&p(
			"alter table app.orders alter column x set default nextval('s')",
			&["orders"]
		)));
		assert!(refused(&p(
			"alter table app.orders add column code text unique",
			&["orders"]
		)));
		assert!(refused(&p(
			"alter table app.orders add primary key (id)",
			&["orders"]
		)));
		assert!(!refused(&p(
			"alter table app.orders add primary key (tenant_id, id)",
			&["orders"]
		)));
		assert!(refused(&p(
			"create unique index on app.orders (id)",
			&["orders"]
		)));
		assert!(!refused(&p(
			"create unique index on app.orders (tenant_id, id)",
			&["orders"]
		)));
		assert!(!refused(&p(
			"alter table app.plans add column id serial",
			&["plans"]
		)));
		assert!(refused(&p(
			"alter table app.orders set schema other",
			&["orders"]
		)));
		assert!(refused(&p(
			"create table app.t (id int references app.orders (tenant_id))",
			&["t", "orders"]
		)));
		assert!(!refused(&p(
			"create table app.t (code text references app.countries (code))",
			&["t", "countries"]
		)));
		assert!(refused(&p(
			"create view app.v as select * from app.orders",
			&["v", "orders"]
		)));
		assert!(refused(&p("create database x", &[])));
		// One node: nothing is refused, everything is home's.
		let one = catalog(1);
		assert_eq!(
			plan(
				"alter table app.orders rename to o2",
				&[rel("orders")],
				&one,
				&HashMap::new()
			),
			DdlPlan::Transactional(Target::Home)
		);
	}

	#[test]
	fn stripes_never_collide() {
		let seq = rel("ids");
		let sql = stripe_sql(&seq, 2, 5000);
		assert_eq!(
			sql,
			"alter sequence \"app\".\"ids\" increment by 1024 minvalue 1 no maxvalue restart with 5122"
		);
		// Every node's values are its residue mod the stride, all above the floor.
		let starts: Vec<i64> = (1..=8)
			.map(|r| {
				stripe_sql(&seq, r, 5000)
					.rsplit(' ')
					.next()
					.unwrap()
					.parse()
					.unwrap()
			})
			.collect();
		let residues: std::collections::HashSet<i64> = starts.iter().map(|s| s % STRIDE).collect();
		assert_eq!(residues.len(), 8);
		assert!(starts.iter().all(|s| *s > 5000));
	}

	#[test]
	fn distribute_checks_l15() {
		let t = rel("orders");
		let col = |n: &str, d: Option<&str>, identity: bool, inc: Option<i64>| ColumnFacts {
			name: n.into(),
			default: d.map(str::to_string),
			identity,
			increment: inc,
		};
		assert!(
			distribute_refusal(
				&t,
				"tenant_id",
				&[col(
					"id",
					Some("nextval('orders_id_seq'::regclass)"),
					false,
					Some(1)
				)],
				&[]
			)
			.is_some()
		);
		assert!(
			distribute_refusal(&t, "tenant_id", &[col("id", None, true, Some(1))], &[]).is_some()
		);
		assert!(
			distribute_refusal(
				&t,
				"tenant_id",
				&[col(
					"id",
					Some("nextval('orders_id_seq'::regclass)"),
					false,
					Some(1024)
				)],
				&[]
			)
			.is_none()
		);
		assert!(
			distribute_refusal(
				&t,
				"tenant_id",
				&[col("id", Some("uuidv7()"), false, None)],
				&[]
			)
			.is_none()
		);
		let pk = vec!["id".to_string()];
		assert!(distribute_refusal(&t, "tenant_id", &[], &[pk]).is_some());
		let pk = vec!["tenant_id".to_string(), "id".to_string()];
		assert!(distribute_refusal(&t, "tenant_id", &[], &[pk]).is_none());
	}

	#[test]
	fn publications_are_mirrored_where_their_tables_are() {
		use DdlPlan::*;
		use Target::*;
		assert_eq!(p("create publication rt", &[]), Transactional(Everywhere));
		assert_eq!(
			p("create publication rt for all tables", &[]),
			Transactional(Everywhere)
		);
		assert_eq!(
			p(
				"create publication rt for table app.orders, app.countries",
				&[]
			),
			Transactional(Everywhere)
		);
		assert_eq!(
			p("alter publication rt add table app.orders", &[]),
			Transactional(Everywhere)
		);
		assert_eq!(
			p("alter publication rt drop table app.orders", &[]),
			Transactional(Everywhere)
		);
		assert_eq!(
			p("alter publication rt add table app.plans", &[]),
			Transactional(Home)
		);
		assert!(refused(&p(
			"alter publication rt add table app.orders, app.plans",
			&[]
		)));
		assert_eq!(
			p("alter publication rt add tables in schema app", &[]),
			Transactional(Everywhere)
		);
		assert_eq!(p("drop publication rt", &[]), Transactional(Everywhere));
	}

	#[test]
	fn a_standby_is_never_a_target() {
		let mut c = catalog(4);
		c.nodes.get_mut(&NodeId(3)).unwrap().state = NodeState::Joining;
		c.nodes.get_mut(&NodeId(4)).unwrap().state = NodeState::Draining;
		assert_eq!(
			nodes(Target::Everywhere, &c),
			vec![NodeId(1), NodeId(2), NodeId(4)]
		);
		assert_eq!(nodes(Target::Home, &c), vec![NodeId(1)]);
		assert_eq!(
			crate::twopc::writable_nodes(&c),
			vec![NodeId(1), NodeId(2), NodeId(4)]
		);
		// Promoted: it is a target from the next catalog on.
		c.nodes.get_mut(&NodeId(3)).unwrap().state = NodeState::Active;
		assert_eq!(nodes(Target::Everywhere, &c).len(), 4);
	}

	#[test]
	fn nextval_off_home_needs_a_stripe() {
		let striped = |s: &str| s == "app.ids";
		assert!(
			sequence_refusal(
				"insert into app.orders values (nextval('app.ids'), 1)",
				striped
			)
			.is_none()
		);
		assert!(
			sequence_refusal(
				"insert into app.orders values (nextval('app.other'), 1)",
				striped
			)
			.is_some()
		);
		assert!(sequence_refusal("select setval('app.ids', 5)", striped).is_some());
		assert!(
			sequence_refusal(
				"insert into app.orders values (nextval('app.ids'::regclass), 1)",
				striped
			)
			.is_none()
		);
		assert!(sequence_refusal("select 1", striped).is_none());
		assert_eq!(
			sequence_calls("select nextval('a'), currval('b'), setval('c', 1)"),
			vec![
				("nextval".to_string(), "a".to_string()),
				("setval".to_string(), "c".to_string())
			]
		);
	}
}
