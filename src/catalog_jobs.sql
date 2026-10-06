-- Phase 4: the operations engine's durable jobs and the routers' heartbeat.
-- Appended to catalog.sql; idempotent like it.

-- The choices L10 leaves to the user, read by every job unless the job's own arguments say
-- otherwise: max_write_pause_ms (2000), drain_timeout_ms (5000), ack_timeout_ms (2000),
-- copy_mb_per_s (50, the rate a plan estimates copy time with; chosen, not measured).
alter table lepis.cluster add column if not exists settings jsonb not null default '{}';

-- One management operation (L13). Its steps are written when it is created, so a job is the
-- same plan whichever router runs it and however often it is resumed.
create table if not exists lepis.job (
	id bigint generated always as identity primary key,
	op text not null,
	args jsonb not null,
	plan jsonb not null default '{}',
	state text not null default 'pending'
		check (state in ('pending', 'running', 'done', 'failed', 'cancelling', 'cancelled')),
	error text,
	runner text,
	created_at timestamptz not null default now(),
	updated_at timestamptz not null default now(),
	finished_at timestamptz
);

-- A step is idempotent: running it again after a crash finds what the first run left and
-- carries on. `detail` is its durable progress (the phase a move reached, the exact names of the
-- slots and publications it made, what it measured).
create table if not exists lepis.job_step (
	job_id bigint not null references lepis.job (id) on delete cascade,
	n int not null,
	kind text not null,
	args jsonb not null,
	state text not null default 'pending'
		check (state in ('pending', 'running', 'done', 'failed', 'skipped')),
	detail jsonb not null default '{}',
	started_at timestamptz,
	finished_at timestamptz,
	primary key (job_id, n)
);

-- lepis.router (catalog.sql) is each router's heartbeat and the epoch it has loaded, which is
-- its ack of a cutover. A router that did not ack in time is named here; the fences keep it from
-- writing where it should not (L7) until it catches up.
alter table lepis.router add column if not exists started_at timestamptz not null default now();
alter table lepis.router add column if not exists fenced_epoch bigint;

create or replace function lepis.notify_job() returns trigger language plpgsql as $$
begin
	perform pg_notify('lepis_job', new.id::text);
	return null;
end $$;
drop trigger if exists job_created on lepis.job;
create trigger job_created after insert on lepis.job
	for each row execute function lepis.notify_job();
