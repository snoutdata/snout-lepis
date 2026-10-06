-- Phase 3: the two-phase-commit decision log and what goes with it.
-- Appended to catalog.sql; idempotent like it.

-- The cluster's own name in every prepared transaction's gid (twopc.rs). pg_prepared_xacts is
-- server-wide, so two clusters whose nodes share a Postgres server must never see each other's.
alter table lepis.cluster add column if not exists
	uid text not null default substr(md5(random()::text || clock_timestamp()::text), 1, 16);

-- The decision log: one row per two-phase transaction between its decision and its last
-- COMMIT PREPARED. A row can be written once (the primary key): the coordinator writes
-- 'commit', in-doubt recovery writes 'abort' for a prepared transaction that waited too long,
-- and whichever lands first is what happens. 'abort' rows are kept for a day so a coordinator
-- that stalled past the grace can never find the register empty again.
create table if not exists lepis.prepared (
	txid bigint primary key,
	decision text not null check (decision in ('commit', 'abort')),
	nodes int[] not null default '{}',
	decided_at timestamptz not null default now()
);

-- Roles Lepis keeps the same on every node (roles.rs). Only these are ever dropped on a node
-- for being gone from home: a role a node has of its own is left alone.
create table if not exists lepis.role (
	name text primary key,
	synced_at timestamptz not null default now()
);

-- DDL that cannot run inside a transaction block (CREATE INDEX CONCURRENTLY, VACUUM, ALTER
-- SYSTEM, …), run on each node on its own, with each node's state (ddl.rs).
create table if not exists lepis.ddl_job (
	id bigint generated always as identity primary key,
	sql text not null,
	created_at timestamptz not null default now()
);
create table if not exists lepis.ddl_job_node (
	job_id bigint not null references lepis.ddl_job (id) on delete cascade,
	node_id int not null references lepis.node (id),
	state text not null default 'pending' check (state in ('pending', 'running', 'done', 'failed')),
	error text,
	started_at timestamptz,
	finished_at timestamptz,
	primary key (job_id, node_id)
);

-- Sequences striped by node (L15): on every node `increment by stride`, each node starting at
-- its own residue, so values never collide across nodes. A nextval off the home node on a
-- sequence not listed here is refused (ddl.rs).
create table if not exists lepis.sequence (
	schema_name text not null,
	seq_name text not null,
	stride int not null check (stride > 0),
	primary key (schema_name, seq_name)
);
