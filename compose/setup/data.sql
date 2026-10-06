-- The sample rows. Deterministic, and loaded identically on every node before distribute.sh
-- deletes what each node does not own: 1,000 tenants, 20,000 orders, 5,000 events from 50 devices.

insert into countries (code, name) values
	('CA', 'Canada'), ('DE', 'Germany'), ('JP', 'Japan'), ('BR', 'Brazil'), ('KE', 'Kenya')
on conflict do nothing;

insert into tenants (tenant_id, name, country, plan_id, created_at)
select t, 'tenant ' || t, (array['CA', 'DE', 'JP', 'BR', 'KE'])[1 + t % 5], 1 + t % 3,
	timestamptz '2026-01-01 00:00:00+00' + make_interval(hours => t::int)
from generate_series(1, 1000) t
on conflict do nothing;

insert into orders (tenant_id, order_id, placed_at, total_cents, status)
select t, o, timestamptz '2026-02-01 00:00:00+00' + make_interval(mins => (t * 20 + o)::int),
	(t * 7919 + o * 104729) % 50000 + 100,
	(array['placed', 'paid', 'shipped', 'returned'])[1 + (t + o) % 4]
from generate_series(1, 1000) t, generate_series(1, 20) o
on conflict do nothing;

insert into events (device, seq, at, kind)
select md5('device ' || d)::uuid, s, timestamptz '2026-03-01 00:00:00+00' + make_interval(secs => (d * 100 + s)::int),
	(array['boot', 'reading', 'alarm'])[1 + (d + s) % 3]
from generate_series(1, 50) d, generate_series(1, 100) s
on conflict do nothing;
