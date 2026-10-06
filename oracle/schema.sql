-- The oracle's schema (Phase 0): a small multi-tenant shop whose tables
-- cover every shard-key type and every table kind a cluster has. Loaded identically into the
-- reference Postgres and into the cluster; once Phase 1 exists, the cluster's copy is
-- distributed (the comments say how) and the reference stays one plain database.

drop schema if exists oracle cascade;
create schema oracle;
set search_path = oracle;

-- reference table (L: a full copy on every node)
create table countries (
	code text primary key,
	name text not null,
	region text not null
);

-- global table (home node only)
create table plans (
	id int primary key,
	name text not null,
	monthly_cents int not null
);

-- sharded by tenant_id (bigint), colocated with orders and items
create table tenants (
	tenant_id bigint primary key,
	name text not null,
	country text not null references countries (code),
	plan_id int not null references plans (id),
	created_at timestamptz not null
);

create table orders (
	tenant_id bigint not null references tenants (tenant_id),
	order_id bigint not null,
	placed_on date not null,
	placed_at timestamp not null,
	total_cents bigint not null,
	status text not null,
	ref uuid not null,
	primary key (tenant_id, order_id)
);

create table items (
	tenant_id bigint not null,
	order_id bigint not null,
	line int not null,
	sku text not null,
	qty smallint not null,
	price_cents int not null,
	attrs jsonb,
	blob bytea,
	primary key (tenant_id, order_id, line),
	foreign key (tenant_id, order_id) references orders (tenant_id, order_id)
);

-- sharded by a uuid key, its own keyspace
create table events (
	device uuid not null,
	seq bigint not null,
	at timestamptz not null,
	kind text not null,
	value double precision,
	primary key (device, seq)
);
