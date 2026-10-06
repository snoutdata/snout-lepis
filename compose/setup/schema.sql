-- The sample application: a small multi-tenant shop. Loaded on EVERY node; distribute.sh then
-- keeps on each node only the rows it owns.
--
--   tenants, orders   sharded by tenant_id (bigint), in one keyspace: colocated, so a tenant's
--                     orders live on the same node as the tenant and joins on tenant_id stay local
--   events            sharded by device (uuid), a keyspace of its own
--   countries         a reference table: a full copy on every node, so joins with it are local
--   plans             a global table: on the home node only (home.sql)
--
-- Primary keys on sharded tables include the shard key, and no foreign key points from a node's
-- table to one that lives only on another node (L9, L15): the same rules Postgres partitioning has.

create table if not exists countries (
	code text primary key,
	name text not null
);

create table if not exists tenants (
	tenant_id bigint primary key,
	name text not null,
	country text not null references countries (code),
	-- plans lives on the home node only, so this is not a foreign key.
	plan_id int not null,
	created_at timestamptz not null default now()
);

create table if not exists orders (
	tenant_id bigint not null references tenants (tenant_id),
	order_id bigint not null,
	placed_at timestamptz not null,
	total_cents bigint not null,
	status text not null,
	primary key (tenant_id, order_id)
);

create table if not exists events (
	device uuid not null,
	seq bigint not null,
	at timestamptz not null,
	kind text not null,
	primary key (device, seq)
);

grant select, insert, update, delete on countries, tenants, orders, events to app;
