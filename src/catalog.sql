-- The Lepis catalog (L6): the whole cluster's shape, on the HOME node, in
-- schema `lepis`. Routers read it at start and again whenever the epoch moves; every change to
-- it bumps lepis.cluster.epoch in the same transaction and NOTIFY lepis_epoch.
--
-- Idempotent: run on every router start; a version row says what has been applied.

create schema if not exists lepis;

create table if not exists lepis.cluster (
	id int primary key default 1 check (id = 1),
	epoch bigint not null default 1,
	catalog_version int not null default 1,
	created_at timestamptz not null default now()
);
insert into lepis.cluster (id) values (1) on conflict do nothing;

create table if not exists lepis.node (
	id int primary key,
	name text not null unique,
	host text not null,
	port int not null check (port between 1 and 65535),
	dbname text not null,
	sslmode text not null check (sslmode in ('disable', 'require', 'verify-full')),
	kind text not null check (kind in ('home', 'data')),
	state text not null default 'joining' check (state in ('joining', 'active', 'draining', 'removed')),
	server_version_num int,
	labels jsonb not null default '{}'
);
create unique index if not exists node_one_home on lepis.node (kind) where kind = 'home';

create table if not exists lepis.keyspace (
	name text primary key,
	strategy text not null check (strategy in ('hash', 'range', 'list', 'schema')),
	key_type text not null,
	seed bigint not null
);

-- Ownership of the hash space. For a hash keyspace the ranges must cover every bigint exactly
-- once (lepis checks it on load and refuses a catalog that does not).
create table if not exists lepis.range (
	keyspace text not null references lepis.keyspace (name),
	lo bigint not null,
	hi bigint not null check (hi >= lo),
	node_id int not null references lepis.node (id),
	primary key (keyspace, lo)
);

-- A single key value given its own node (L12).
create table if not exists lepis.pin (
	keyspace text not null references lepis.keyspace (name),
	value text not null,
	node_id int not null references lepis.node (id),
	primary key (keyspace, value)
);

create table if not exists lepis.relation (
	schema_name text not null,
	table_name text not null,
	kind text not null check (kind in ('sharded', 'reference', 'global')),
	keyspace text references lepis.keyspace (name),
	key_column text,
	primary key (schema_name, table_name),
	check ((kind = 'sharded') = (keyspace is not null and key_column is not null))
);

create table if not exists lepis.router (
	id text primary key,
	epoch bigint not null,
	seen_at timestamptz not null default now()
);

-- Bumps the epoch and tells every router. Called at the end of any catalog change.
create or replace function lepis.bump() returns bigint language sql as $$
	update lepis.cluster set epoch = epoch + 1 where id = 1 returning epoch;
$$;
create or replace function lepis.notify_epoch() returns trigger language plpgsql as $$
begin
	perform pg_notify('lepis_epoch', (select epoch::text from lepis.cluster where id = 1));
	return null;
end $$;
drop trigger if exists epoch_moved on lepis.cluster;
create trigger epoch_moved after update of epoch on lepis.cluster
	for each statement execute function lepis.notify_epoch();
