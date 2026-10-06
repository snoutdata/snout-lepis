//! Role and grant sync (L11): every node carries the same roles, with the SAME SCRAM verifier,
//! so the ClientKey a client proves to the router logs it into every node.
//!
//! The home node is the source of truth. A role statement (`CREATE/ALTER/DROP ROLE`, `GRANT
//! role TO …`, `ALTER ROLE … SET`) runs on the home node as the client, exactly as it would on
//! one Postgres; then `replicate` makes every other node match home for the roles it named. The
//! statement is never replayed as written: `CREATE ROLE x PASSWORD 'pw'` run on each node would
//! hash the password with a fresh salt on each, and the verifiers would differ. Instead the
//! role's attributes, its `rolpassword` verifier verbatim, its role-wide settings and its
//! memberships are read from home and written on each node (`plan`, a pure function).
//!
//! `reconcile` is the same thing for every role, which is how a node that missed a change (or a
//! new node) is brought in line. Only roles Lepis has synced before (`lepis.role`) are ever
//! dropped on a node for being gone from home, so a role a node has of its own is left alone;
//! the bootstrap superuser, predefined roles and Lepis's own service login are never touched.

use std::collections::{BTreeMap, HashSet};

use pg_query::NodeEnum;
use pg_query::protobuf::{ObjectType, RoleSpec, RoleSpecType};

use crate::backend::{Backend, BackendError};
use crate::catalog::{Catalog, NodeId, NodeState, quote_ident, quote_literal};
use crate::server::App;
use crate::twopc;

/// Which roles a change touched.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Scope {
	Names(Vec<String>),
	/// Every role (a statement that names a role only as CURRENT_USER, or `ALTER ROLE ALL`).
	All,
}

/// What a role statement changed, for `replicate`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoleChange {
	pub scope: Scope,
	/// `ALTER ROLE old RENAME TO new`: replayed as a rename, so what the role owns on each node
	/// stays its own.
	pub renames: Vec<(String, String)>,
}

fn spec_name(s: &RoleSpec) -> Result<Option<String>, ()> {
	match RoleSpecType::try_from(s.roletype) {
		Ok(RoleSpecType::RolespecCstring) => Ok(Some(s.rolename.clone())),
		Ok(RoleSpecType::RolespecPublic) => Ok(None),
		// CURRENT_USER and friends: the router may not know who that is after SET ROLE.
		_ => Err(()),
	}
}

/// The role change a statement makes, or None when it is not a role statement. Object
/// privileges (`GRANT SELECT ON …`) are DDL, not this (ddl.rs).
pub fn change_of(sql: &str) -> Option<RoleChange> {
	let parsed = pg_query::parse(sql).ok()?;
	let raw = parsed.protobuf.stmts.first()?;
	let node = raw.stmt.as_deref()?.node.as_ref()?;
	let mut names = Vec::new();
	let mut renames = Vec::new();
	let mut all = false;
	let mut spec = |s: Option<&RoleSpec>, names: &mut Vec<String>| match s.map(spec_name) {
		Some(Ok(Some(n))) => names.push(n),
		Some(Ok(None)) => {}
		Some(Err(())) | None => all = true,
	};
	match node {
		NodeEnum::CreateRoleStmt(c) => names.push(c.role.clone()),
		NodeEnum::AlterRoleStmt(a) => spec(a.role.as_ref(), &mut names),
		NodeEnum::AlterRoleSetStmt(a) => spec(a.role.as_ref(), &mut names),
		NodeEnum::DropRoleStmt(d) => {
			for r in &d.roles {
				if let Some(NodeEnum::RoleSpec(s)) = &r.node {
					spec(Some(s), &mut names);
				}
			}
		}
		NodeEnum::GrantRoleStmt(g) => {
			for r in &g.granted_roles {
				if let Some(NodeEnum::AccessPriv(p)) = &r.node {
					names.push(p.priv_name.clone());
				}
			}
			for r in &g.grantee_roles {
				if let Some(NodeEnum::RoleSpec(s)) = &r.node {
					spec(Some(s), &mut names);
				}
			}
		}
		NodeEnum::RenameStmt(r) if r.rename_type == ObjectType::ObjectRole as i32 => {
			names.push(r.subname.clone());
			names.push(r.newname.clone());
			renames.push((r.subname.clone(), r.newname.clone()));
		}
		_ => return None,
	}
	names.sort();
	names.dedup();
	Some(RoleChange {
		scope: if all { Scope::All } else { Scope::Names(names) },
		renames,
	})
}

/// The role attributes that are yes/no, in `pg_authid`'s column order, by their CREATE ROLE
/// keyword (`no` + the keyword is the other answer).
pub const FLAGS: [&str; 7] = [
	"superuser",
	"inherit",
	"createrole",
	"createdb",
	"login",
	"replication",
	"bypassrls",
];

/// One role as `pg_authid` holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoleRow {
	pub name: String,
	/// One answer per `FLAGS` entry.
	pub flags: [bool; 7],
	pub connlimit: i32,
	/// `infinity`, or a UTC timestamp; None for no limit.
	pub valid_until: Option<String>,
	/// The verifier, verbatim (`SCRAM-SHA-256$…`), or None.
	pub password: Option<String>,
	/// Role-wide settings (`ALTER ROLE x SET …`, all databases), `name=value` each.
	pub settings: Vec<String>,
}

/// One membership: `member` is a member of `role`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Membership {
	pub role: String,
	pub member: String,
	pub admin: bool,
	/// (INHERIT, SET), which every node has (Postgres 17 or later, L1).
	pub options: (bool, bool),
	pub grantor: Option<String>,
}

/// What makes a node match home.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RolePlan {
	/// Run in one transaction.
	pub apply: Vec<String>,
	/// `DROP ROLE`, each on its own: one that still owns objects on the node fails alone.
	pub drops: Vec<String>,
}

impl RolePlan {
	pub fn is_empty(&self) -> bool {
		self.apply.is_empty() && self.drops.is_empty()
	}
}

/// Everything CREATE/ALTER ROLE … WITH says about a role, as one clause; two roles agree when
/// their clauses do.
fn attributes(r: &RoleRow) -> String {
	let mut words: Vec<String> = FLAGS
		.iter()
		.zip(r.flags)
		.map(|(w, on)| if on { w.to_string() } else { format!("no{w}") })
		.collect();
	words.push(format!("connection limit {}", r.connlimit));
	words.push(format!(
		"valid until {}",
		quote_literal(r.valid_until.as_deref().unwrap_or("infinity"))
	));
	words.push(format!(
		"password {}",
		r.password
			.as_deref()
			.map_or("null".to_string(), quote_literal)
	));
	words.join(" ")
}

/// Settings whose value is a list: each element is its own literal, or the list would become
/// one element with commas in it.
const LIST_SETTINGS: [&str; 5] = [
	"search_path",
	"temp_tablespaces",
	"session_preload_libraries",
	"local_preload_libraries",
	"shared_preload_libraries",
];

/// Splits a list setting as Postgres stores it (`a, "B c"`) into its elements.
fn split_list(v: &str) -> Vec<String> {
	let mut out = Vec::new();
	let mut cur = String::new();
	let mut quoted = false;
	let mut chars = v.chars().peekable();
	while let Some(c) = chars.next() {
		match c {
			'"' if quoted && chars.peek() == Some(&'"') => {
				cur.push('"');
				chars.next();
			}
			'"' => quoted = !quoted,
			',' if !quoted => out.push(std::mem::take(&mut cur).trim().to_string()),
			c => cur.push(c),
		}
	}
	out.push(cur.trim().to_string());
	out.retain(|s| !s.is_empty());
	out
}

/// `ALTER ROLE r SET …` for one stored `name=value`; None for a name that is not a plain
/// setting name (nothing from a node's catalog is pasted into SQL unquoted).
fn setting_sql(role: &str, entry: &str) -> Option<String> {
	let (name, value) = entry.split_once('=')?;
	if name.is_empty()
		|| !name
			.chars()
			.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
	{
		return None;
	}
	let value = if LIST_SETTINGS.contains(&name) {
		split_list(value)
			.iter()
			.map(|e| quote_literal(e))
			.collect::<Vec<_>>()
			.join(", ")
	} else {
		quote_literal(value)
	};
	Some(format!(
		"alter role {} set {} = {value}",
		quote_ident(role),
		quote_ident(name)
	))
}

/// The statements that make a node's roles in `scope` match home's. `managed` is the set of
/// roles Lepis has synced before (only those are dropped for being gone from home).
pub fn plan(
	scope: &[String],
	home: &[RoleRow],
	node: &[RoleRow],
	home_m: &[Membership],
	node_m: &[Membership],
	managed: &HashSet<String>,
) -> RolePlan {
	let in_scope: HashSet<&str> = scope.iter().map(String::as_str).collect();
	let h: BTreeMap<&str, &RoleRow> = home.iter().map(|r| (r.name.as_str(), r)).collect();
	let n: BTreeMap<&str, &RoleRow> = node.iter().map(|r| (r.name.as_str(), r)).collect();
	let mut p = RolePlan::default();
	for name in scope {
		let ident = quote_ident(name);
		match (h.get(name.as_str()), n.get(name.as_str())) {
			(Some(want), None) => {
				p.apply
					.push(format!("create role {ident} with {}", attributes(want)));
				for s in &want.settings {
					p.apply.extend(setting_sql(name, s));
				}
			}
			(Some(want), Some(have)) => {
				if attributes(want) != attributes(have) {
					p.apply
						.push(format!("alter role {ident} with {}", attributes(want)));
				}
				let mut a = want.settings.clone();
				let mut b = have.settings.clone();
				a.sort();
				b.sort();
				if a != b {
					p.apply.push(format!("alter role {ident} reset all"));
					for s in &want.settings {
						p.apply.extend(setting_sql(name, s));
					}
				}
			}
			(None, Some(_)) if managed.contains(name) => {
				p.drops.push(format!("drop role {ident}"));
			}
			_ => {}
		}
	}

	// Memberships that involve a role in scope, one per (role, member).
	let relevant = |m: &&Membership| {
		in_scope.contains(m.role.as_str()) || in_scope.contains(m.member.as_str())
	};
	let mut want: BTreeMap<(&str, &str), &Membership> = BTreeMap::new();
	for m in home_m.iter().filter(relevant) {
		let e = want.entry((&m.role, &m.member)).or_insert(m);
		if m.admin && !e.admin {
			*e = m;
		}
	}
	let mut have: BTreeMap<(&str, &str), Vec<&Membership>> = BTreeMap::new();
	for m in node_m.iter().filter(relevant) {
		have.entry((&m.role, &m.member)).or_default().push(m);
	}
	let dropped: HashSet<&str> = scope
		.iter()
		.filter(|s| !h.contains_key(s.as_str()))
		.map(String::as_str)
		.collect();
	let grant = |m: &Membership| {
		let (i, set) = m.options;
		format!(
			"grant {} to {} with admin {}, inherit {i}, set {set}",
			quote_ident(&m.role),
			quote_ident(&m.member),
			m.admin
		)
	};
	for (key, w) in &want {
		match have.get(key) {
			None => p.apply.push(grant(w)),
			Some(rows) => {
				let admin = rows.iter().any(|r| r.admin);
				let first = rows[0];
				let options_differ = first.options != w.options;
				if admin != w.admin || options_differ {
					if admin && !w.admin {
						for r in rows {
							p.apply.push(format!(
								"revoke admin option for {} from {}{}",
								quote_ident(&r.role),
								quote_ident(&r.member),
								granted_by(r)
							));
						}
					}
					if w.admin || options_differ {
						p.apply.push(grant(w));
					}
				}
			}
		}
	}
	for (key, rows) in &have {
		// A role being dropped takes its memberships with it.
		if want.contains_key(key) || dropped.contains(key.0) || dropped.contains(key.1) {
			continue;
		}
		for r in rows {
			p.apply.push(format!(
				"revoke {} from {}{}",
				quote_ident(&r.role),
				quote_ident(&r.member),
				granted_by(r)
			));
		}
	}
	p
}

fn granted_by(m: &Membership) -> String {
	match &m.grantor {
		Some(g) => format!(" granted by {}", quote_ident(g)),
		None => String::new(),
	}
}

// ---------------------------------------------------------------------------------------------
// Reading roles.

/// `oid >= 16384` leaves out the bootstrap superuser and the predefined `pg_*` roles.
const ROLES_SQL: &str = "select a.rolname, a.rolsuper, a.rolinherit, a.rolcreaterole, \
	a.rolcreatedb, a.rolcanlogin, a.rolreplication, a.rolbypassrls, a.rolconnlimit, \
	case when a.rolvaliduntil is null then null \
	when a.rolvaliduntil = 'infinity' then 'infinity' \
	else to_char(a.rolvaliduntil at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') || '+00' end, \
	a.rolpassword, \
	coalesce((select array_to_string(s.setconfig, chr(31)) from pg_db_role_setting s \
		where s.setrole = a.oid and s.setdatabase = 0), '') \
	from pg_authid a where a.oid >= 16384 and a.rolname = any($1::text[])";

const MEMBERS_SQL: &str = "select r.rolname, m.rolname, am.admin_option, am.inherit_option, 	am.set_option, g.rolname 	from pg_auth_members am join pg_roles r on r.oid = am.roleid 	join pg_roles m on m.oid = am.member left join pg_roles g on g.oid = am.grantor 	where r.rolname = any($1::text[]) or m.rolname = any($1::text[])";

/// A text[] literal, every element quoted.
pub(crate) fn text_array(items: &[String]) -> String {
	format!(
		"{{{}}}",
		items
			.iter()
			.map(|s| format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")))
			.collect::<Vec<_>>()
			.join(",")
	)
}

fn flag(v: &Option<String>) -> bool {
	v.as_deref() == Some("t")
}

async fn read(
	b: &mut Backend,
	names: &[String],
) -> Result<(Vec<RoleRow>, Vec<Membership>), BackendError> {
	let arr = text_array(names);
	let roles = b
		.query(ROLES_SQL, &[&arr])
		.await?
		.into_iter()
		.map(|r| {
			let get = |i: usize| r.get(i).cloned().flatten();
			RoleRow {
				name: get(0).unwrap_or_default(),
				flags: std::array::from_fn(|i| flag(&get(i + 1))),
				connlimit: get(8).and_then(|v| v.parse().ok()).unwrap_or(-1),
				valid_until: get(9),
				password: get(10),
				settings: get(11)
					.unwrap_or_default()
					.split('\u{1f}')
					.filter(|s| !s.is_empty())
					.map(str::to_string)
					.collect(),
			}
		})
		.collect();
	let members = b
		.query(MEMBERS_SQL, &[&arr])
		.await?
		.into_iter()
		.map(|r| {
			let get = |i: usize| r.get(i).cloned().flatten();
			Membership {
				role: get(0).unwrap_or_default(),
				member: get(1).unwrap_or_default(),
				admin: flag(&get(2)),
				options: (flag(&get(3)), flag(&get(4))),
				grantor: get(5),
			}
		})
		.collect();
	Ok((roles, members))
}

async fn names_of(b: &mut Backend, sql: &str) -> Result<Vec<String>, BackendError> {
	Ok(b.query(sql, &[])
		.await?
		.into_iter()
		.filter_map(|r| r.into_iter().next().flatten())
		.collect())
}

// ---------------------------------------------------------------------------------------------
// Syncing.

/// What `replicate` did on one node: the statements it ran, or why it could not.
pub type NodeResult = (NodeId, Result<usize, String>);

/// Makes every node other than home match home for the roles `change` names. Run after the
/// statement has committed on home. A node that fails is reported and left for `reconcile`.
pub async fn replicate(
	app: &App,
	catalog: &Catalog,
	change: &RoleChange,
) -> Result<Vec<NodeResult>, String> {
	let mut home = twopc::home_service(app, "role sync")
		.await
		.map_err(|e| format!("home node: {e}"))?;
	let result = replicate_with(app, catalog, change, &mut home).await;
	home.close().await;
	result
}

/// What the Realtime service's role may do in the catalog on home (Phase 7): read the cluster's
/// shape, so it subscribes to every node's publication, and keep its own `lepis.router` row
/// (the heartbeat and acked epoch every router writes). SELECT on `lepis.router` too, which an
/// `INSERT … ON CONFLICT DO UPDATE` of its row needs.
pub fn realtime_grants(role: &str) -> String {
	let r = quote_ident(role);
	format!(
		"grant usage on schema lepis to {r};\n\
		grant select on lepis.cluster, lepis.node, lepis.relation to {r};\n\
		grant select, insert, update on lepis.router to {r}"
	)
}

/// Grants `realtime_grants(role)` on the home node, as Lepis's service login. The role must exist
/// (made like any role, then `replicate`d so it can log in to every node).
pub async fn grant_realtime(app: &App, role: &str) -> Result<(), String> {
	let mut home = twopc::home_service(app, "realtime grants")
		.await
		.map_err(|e| format!("home node: {e}"))?;
	let r = home
		.simple(&format!("begin;\n{};\ncommit", realtime_grants(role)))
		.await
		.map_err(|e| e.to_string());
	if r.is_err() {
		let _ = home.simple("rollback").await;
	}
	home.close().await;
	r.map(|_| ())
}

/// `replicate` for every role: brings each node in line with home.
pub async fn reconcile(app: &App, catalog: &Catalog) -> Result<Vec<NodeResult>, String> {
	replicate(
		app,
		catalog,
		&RoleChange {
			scope: Scope::All,
			renames: Vec::new(),
		},
	)
	.await
}

async fn replicate_with(
	app: &App,
	catalog: &Catalog,
	change: &RoleChange,
	home: &mut Backend,
) -> Result<Vec<NodeResult>, String> {
	let e = |e: BackendError| format!("home node: {e}");
	let service = app.config.service.user.as_str();
	let mut names: Vec<String> = match &change.scope {
		Scope::Names(n) => n.clone(),
		Scope::All => {
			let mut n = names_of(home, "select rolname from pg_authid where oid >= 16384")
				.await
				.map_err(e)?;
			n.extend(
				names_of(home, "select name from lepis.role")
					.await
					.map_err(e)?,
			);
			n
		}
	};
	names.retain(|n| n != service && !n.starts_with("pg_"));
	names.sort();
	names.dedup();
	let (home_roles, home_m) = read(home, &names).await.map_err(e)?;
	let managed: HashSet<String> = home
		.query(
			"select name from lepis.role where name = any($1::text[])",
			&[&text_array(&names)],
		)
		.await
		.map_err(e)?
		.into_iter()
		.filter_map(|r| r.into_iter().next().flatten())
		.collect();

	let mut results = Vec::new();
	let mut all_ok = true;
	let writable = twopc::writable_nodes(catalog);
	for node in catalog.nodes.values() {
		if node.home || node.state == NodeState::Removed {
			continue;
		}
		// A joining node (a physical standby until it is promoted) is skipped, and the roles it
		// may still need to lose stay in lepis.role: the leader reconciles it once it is active.
		if !writable.contains(&node.id) {
			all_ok = false;
			continue;
		}
		let r = sync_node(app, node, change, &names, &home_roles, &home_m, &managed).await;
		all_ok &= matches!(r, Ok(Some(_)));
		results.push((node.id, r.map(|n| n.unwrap_or(0))));
	}

	// Remember what is now synced; forget what is gone everywhere.
	let present: Vec<String> = home_roles.iter().map(|r| r.name.clone()).collect();
	let gone: Vec<String> = names
		.iter()
		.filter(|n| !present.contains(n))
		.cloned()
		.collect();
	home.query(
		"insert into lepis.role (name) select unnest($1::text[]) \
		on conflict (name) do update set synced_at = now()",
		&[&text_array(&present)],
	)
	.await
	.map_err(e)?;
	if all_ok && !gone.is_empty() {
		home.query(
			"delete from lepis.role where name = any($1::text[])",
			&[&text_array(&gone)],
		)
		.await
		.map_err(e)?;
	}
	Ok(results)
}

async fn sync_node(
	app: &App,
	node: &crate::catalog::Node,
	change: &RoleChange,
	names: &[String],
	home_roles: &[RoleRow],
	home_m: &[Membership],
	managed: &HashSet<String>,
) -> Result<Option<usize>, String> {
	let mut b = twopc::node_service(app, node, "role sync")
		.await
		.map_err(|e| e.to_string())?;
	// Still a standby though the catalog says otherwise: it takes no writes. None = skipped.
	if twopc::is_standby(&mut b).await.map_err(|e| e.to_string())? {
		b.close().await;
		return Ok(None);
	}
	let mut ran = 0;
	// Renames first, so the role keeps what it owns there.
	if !change.renames.is_empty() {
		let (have, _) = read(&mut b, names).await.map_err(|e| e.to_string())?;
		let has = |n: &str| have.iter().any(|r| r.name == n);
		for (old, new) in &change.renames {
			if has(old) && !has(new) {
				b.simple(&format!(
					"alter role {} rename to {}",
					quote_ident(old),
					quote_ident(new)
				))
				.await
				.map_err(|e| e.to_string())?;
				ran += 1;
			}
		}
	}
	let (have, have_m) = read(&mut b, names).await.map_err(|e| e.to_string())?;
	let p = plan(names, home_roles, &have, home_m, &have_m, managed);
	if !p.apply.is_empty() {
		let batch = format!("begin;\n{};\ncommit", p.apply.join(";\n"));
		if let Err(e) = b.simple(&batch).await {
			let _ = b.simple("rollback").await;
			return Err(e.to_string());
		}
		ran += p.apply.len();
	}
	let mut failed = Vec::new();
	for d in &p.drops {
		match b.simple(d).await {
			Ok(_) => ran += 1,
			Err(e) => failed.push(format!("{d}: {e}")),
		}
	}
	b.close().await;
	if failed.is_empty() {
		Ok(Some(ran))
	} else {
		Err(failed.join("; "))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn role(name: &str, password: &str) -> RoleRow {
		RoleRow {
			name: name.into(),
			flags: [false, true, false, false, true, false, false],
			connlimit: -1,
			valid_until: None,
			password: Some(password.into()),
			settings: Vec::new(),
		}
	}

	fn member(role: &str, member: &str, admin: bool) -> Membership {
		Membership {
			role: role.into(),
			member: member.into(),
			admin,
			options: (true, true),
			grantor: None,
		}
	}

	fn scope(n: &[&str]) -> Vec<String> {
		n.iter().map(|s| s.to_string()).collect()
	}

	#[test]
	fn realtime_reads_the_shape_and_writes_its_heartbeat() {
		let sql = realtime_grants("realtime_admin");
		assert_eq!(
			sql,
			"grant usage on schema lepis to \"realtime_admin\";\n\
			grant select on lepis.cluster, lepis.node, lepis.relation to \"realtime_admin\";\n\
			grant select, insert, update on lepis.router to \"realtime_admin\""
		);
		// A role name is an identifier, never SQL.
		assert!(realtime_grants("x\"; drop table t; --").contains("\"x\"\"; drop table t; --\""));
		// Every statement parses.
		assert_eq!(pg_query::parse(&sql).unwrap().protobuf.stmts.len(), 3);
	}

	#[test]
	fn statements_name_their_roles() {
		let names = |sql: &str| change_of(sql).unwrap().scope;
		assert_eq!(
			names("create role app login password 'x'"),
			Scope::Names(scope(&["app"]))
		);
		assert_eq!(
			names("create user app in role readers"),
			Scope::Names(scope(&["app"]))
		);
		assert_eq!(
			names("alter role \"Mixed\" password 'y'"),
			Scope::Names(scope(&["Mixed"]))
		);
		assert_eq!(
			names("drop role if exists a, b"),
			Scope::Names(scope(&["a", "b"]))
		);
		assert_eq!(
			names("grant readers, writers to app with admin option"),
			Scope::Names(scope(&["app", "readers", "writers"]))
		);
		assert_eq!(names("alter role current_user password 'z'"), Scope::All);
		assert_eq!(names("alter role all set work_mem = '4MB'"), Scope::All);
		let r = change_of("alter role a rename to b").unwrap();
		assert_eq!(r.renames, vec![("a".to_string(), "b".to_string())]);
		assert!(change_of("grant select on t to app").is_none());
		assert!(change_of("select 1").is_none());
	}

	#[test]
	fn a_new_role_gets_home_s_verifier() {
		let v = "SCRAM-SHA-256$4096:c2FsdA==$a:b";
		let p = plan(
			&scope(&["app"]),
			&[role("app", v)],
			&[],
			&[],
			&[],
			&HashSet::new(),
		);
		assert_eq!(p.apply.len(), 1);
		assert!(p.apply[0].starts_with("create role \"app\" with nosuperuser inherit"));
		assert!(p.apply[0].ends_with(&format!("password '{v}'")));
		assert!(p.drops.is_empty());
	}

	#[test]
	fn only_differences_are_written() {
		let same = plan(
			&scope(&["app"]),
			&[role("app", "v1")],
			&[role("app", "v1")],
			&[member("readers", "app", false)],
			&[member("readers", "app", false)],
			&HashSet::new(),
		);
		assert!(same.is_empty(), "{same:?}");
		let changed = plan(
			&scope(&["app"]),
			&[role("app", "v2")],
			&[role("app", "v1")],
			&[],
			&[member("readers", "app", false)],
			&HashSet::new(),
		);
		assert_eq!(changed.apply.len(), 2, "{changed:?}");
		assert!(changed.apply[0].starts_with("alter role \"app\" with"));
		assert_eq!(changed.apply[1], "revoke \"readers\" from \"app\"");
	}

	#[test]
	fn only_managed_roles_are_dropped() {
		let mine = plan(
			&scope(&["gone"]),
			&[],
			&[role("gone", "v")],
			&[],
			&[],
			&HashSet::from(["gone".to_string()]),
		);
		assert_eq!(mine.drops, vec!["drop role \"gone\"".to_string()]);
		let theirs = plan(
			&scope(&["local"]),
			&[],
			&[role("local", "v")],
			&[],
			&[],
			&HashSet::new(),
		);
		assert!(theirs.is_empty());
	}

	#[test]
	fn settings_are_quoted_and_lists_split() {
		assert_eq!(
			setting_sql("app", "statement_timeout=5s").unwrap(),
			"alter role \"app\" set \"statement_timeout\" = '5s'"
		);
		assert_eq!(
			setting_sql("app", "search_path=\"$user\", public, \"My \"\"S\"\"\"").unwrap(),
			"alter role \"app\" set \"search_path\" = '$user', 'public', 'My \"S\"'"
		);
		assert_eq!(
			setting_sql("app", "x=a'; drop table t; --").unwrap(),
			"alter role \"app\" set \"x\" = 'a''; drop table t; --'"
		);
		assert!(setting_sql("app", "bad name=1").is_none());
	}

	#[test]
	fn admin_option_is_added_and_taken_away() {
		let add = plan(
			&scope(&["app"]),
			&[role("app", "v")],
			&[role("app", "v")],
			&[member("readers", "app", true)],
			&[member("readers", "app", false)],
			&HashSet::new(),
		);
		assert_eq!(
			add.apply,
			vec!["grant \"readers\" to \"app\" with admin true, inherit true, set true".to_string()]
		);
		let take = plan(
			&scope(&["app"]),
			&[role("app", "v")],
			&[role("app", "v")],
			&[member("readers", "app", false)],
			&[member("readers", "app", true)],
			&HashSet::new(),
		);
		assert_eq!(
			take.apply,
			vec!["revoke admin option for \"readers\" from \"app\"".to_string()]
		);
	}
}
