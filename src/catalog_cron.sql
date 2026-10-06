-- Phase 7: pg_cron jobs that run on every node (cron.rs).
-- Appended to catalog.sql; idempotent like it.

-- A job is named here by its pg_cron `jobname`; a job not named runs on home alone. `scope =
-- 'home'` is the default made explicit, so a job can be unmarked without losing the row.
create table if not exists lepis.cron_job (
	jobname text primary key,
	scope text not null default 'every_node' check (scope in ('home', 'every_node')),
	marked_at timestamptz not null default now()
);
