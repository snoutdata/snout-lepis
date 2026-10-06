-- The oracle's data: deterministic (no random()), so the reference and the cluster hold the
-- same rows byte for byte without copying one into the other.

set search_path = oracle;

insert into countries values
	('CA', 'Canada', 'Americas'), ('US', 'United States', 'Americas'), ('BR', 'Brazil', 'Americas'),
	('DE', 'Germany', 'Europe'), ('FR', 'France', 'Europe'), ('PT', 'Portugal', 'Europe'),
	('JP', 'Japan', 'Asia'), ('IN', 'India', 'Asia'), ('NG', 'Nigeria', 'Africa');

insert into plans values (1, 'free', 0), (2, 'plus', 1500), (3, 'pro', 4900), (4, 'team', 10000);

insert into tenants
select t,
	'tenant ' || t,
	(array['CA','US','BR','DE','FR','PT','JP','IN','NG'])[1 + (t * 7) % 9],
	1 + (t * 13) % 4,
	timestamptz '2024-01-01 00:00+00' + (t * 37) * interval '1 hour'
from generate_series(1, 500) t;

insert into orders
select t, o,
	date '2024-01-01' + ((t * 31 + o * 17) % 700),
	timestamp '2024-01-01' + ((t * 31 + o * 17) % 700) * interval '1 day' + (o * 97 % 86400) * interval '1 second',
	(t * 1009 + o * 313) % 50000 + 100,
	(array['new','paid','shipped','refunded'])[1 + (t + o) % 4],
	md5(t || '/' || o)::uuid
from generate_series(1, 500) t, generate_series(1, 1 + t % 40) o;

insert into items
select o.tenant_id, o.order_id, l,
	'SKU-' || ((o.tenant_id * 7 + l * 3) % 250),
	(1 + (o.order_id + l) % 5)::smallint,
	(100 + (o.tenant_id * l * 11) % 9000)::int,
	case when l % 3 = 0 then null else jsonb_build_object('color', (array['red','green','blue'])[1 + l % 3], 'size', l) end,
	case when l % 4 = 0 then decode(md5(o.ref::text), 'hex') else null end
from orders o, generate_series(1, 1 + (o.order_id % 4)::int) l;

insert into events
select md5('device ' || d)::uuid, s,
	timestamptz '2025-06-01 00:00+00' + (d * 1000 + s) * interval '1 minute',
	(array['boot','reading','alarm'])[1 + (d + s) % 3],
	case when (d + s) % 11 = 0 then null else ((d * 17 + s * 5) % 1000) / 10.0 end
from generate_series(1, 200) d, generate_series(1, 50) s;

analyze;
