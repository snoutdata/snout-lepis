-- The oracle's corpus. Each query starts with a header line:
--
--   -- name: <id> [ordered] [route=<what Phase 1+ must do>]
--
-- `ordered` compares rows in order (the query has a top-level ORDER BY that makes it total);
-- otherwise rows are compared as a multiset. `route` is what the router must decide once
-- tables are distributed: single, scatter, home, all, or refuse. Through Phase 0 every query
-- goes to the home node and only the answers are compared.
--
-- A query that is refused must be refused with an L9 error; a query that answers must answer
-- exactly what the reference Postgres answers.

-- name: one_tenant ordered route=single
select tenant_id, name, country, plan_id, created_at from oracle.tenants where tenant_id = 42;

-- name: one_tenant_orders ordered route=single
select order_id, placed_on, total_cents, status from oracle.orders where tenant_id = 7 order by order_id;

-- name: tenant_in_list ordered route=scatter
select tenant_id, count(*) from oracle.orders where tenant_id in (3, 99, 250, 401) group by tenant_id order by tenant_id;

-- name: colocated_join ordered route=single
select o.order_id, sum(i.qty * i.price_cents) as value
from oracle.orders o join oracle.items i using (tenant_id, order_id)
where o.tenant_id = 123 group by o.order_id order by o.order_id;

-- name: join_reference ordered route=single
select t.tenant_id, c.name, c.region from oracle.tenants t join oracle.countries c on c.code = t.country
where t.tenant_id = 77;

-- name: join_global ordered route=refuse
select t.tenant_id, p.name, p.monthly_cents from oracle.tenants t join oracle.plans p on p.id = t.plan_id
where t.tenant_id = 300;

-- name: count_all route=scatter
select count(*) from oracle.orders;

-- name: sum_min_max route=scatter
select sum(total_cents), min(total_cents), max(total_cents), min(placed_on), max(placed_at) from oracle.orders;

-- name: avg route=scatter
select round(avg(total_cents), 6), round(avg(qty)::numeric, 6) from oracle.orders join oracle.items using (tenant_id, order_id);

-- name: group_by_status ordered route=scatter
select status, count(*), sum(total_cents) from oracle.orders group by status order by status;

-- name: group_by_having ordered route=scatter
select tenant_id, count(*) n from oracle.orders group by tenant_id having count(*) > 35 order by tenant_id;

-- name: top_n ordered route=scatter
select tenant_id, order_id, total_cents from oracle.orders order by total_cents desc, tenant_id, order_id limit 20;

-- name: top_n_offset ordered route=scatter
select tenant_id, order_id from oracle.orders order by placed_at, tenant_id, order_id limit 10 offset 25;

-- name: distinct ordered route=scatter
select distinct status from oracle.orders order by 1;

-- name: count_distinct route=scatter
select count(distinct sku) from oracle.items;

-- name: reference_only ordered route=home
select region, count(*) from oracle.countries group by region order by region;

-- name: global_only ordered route=home
select * from oracle.plans order by id;

-- name: uuid_key ordered route=single
select seq, kind, value from oracle.events where device = '242df4cf-cef9-291f-2da4-ae33926355a5' order by seq;

-- name: uuid_key_range ordered route=single
select count(*), min(at), max(at) from oracle.events
where device = 'c13602be-7949-9645-9030-09f7798c999f'::uuid and at >= timestamptz '2025-06-07 00:00+00';

-- name: cte_single ordered route=single
with big as (select order_id, total_cents from oracle.orders where tenant_id = 64 and total_cents > 20000)
select count(*), coalesce(sum(total_cents), 0) from big;

-- name: subquery_in_key ordered route=single
select count(*) from oracle.items where tenant_id = 5 and order_id in (select order_id from oracle.orders where tenant_id = 5 and status = 'paid');

-- name: window_single ordered route=single
select order_id, total_cents, rank() over (order by total_cents desc, order_id) from oracle.orders where tenant_id = 200 order by order_id;

-- name: window_across ordered route=refuse
select tenant_id, order_id, rank() over (order by total_cents desc, tenant_id, order_id) r from oracle.orders order by r limit 5;

-- name: cross_shard_join_non_key ordered route=refuse
select a.tenant_id, b.tenant_id from oracle.orders a join oracle.orders b on a.ref = b.ref and a.tenant_id <> b.tenant_id limit 5;

-- name: jsonb ordered route=single
select line, attrs->>'color', (attrs->>'size')::int from oracle.items where tenant_id = 11 and attrs is not null order by order_id, line;

-- name: bytea ordered route=single
select order_id, line, encode(blob, 'hex') from oracle.items where tenant_id = 19 and blob is not null order by order_id, line;

-- name: nulls ordered route=scatter
select count(*) filter (where value is null), count(value) from oracle.events;

-- name: types_round_trip ordered route=single
select tenant_id, order_id, placed_on, placed_at, ref, total_cents::numeric / 100 from oracle.orders where tenant_id = 2 order by order_id;

-- name: dml_returning ordered route=single
begin;
update oracle.orders set status = 'paid' where tenant_id = 13 and order_id = 1 returning tenant_id, order_id, status;
rollback;

-- name: insert_returning ordered route=single
begin;
insert into oracle.orders (tenant_id, order_id, placed_on, placed_at, total_cents, status, ref)
values (499, 1000, date '2025-01-01', timestamp '2025-01-01 10:00', 999, 'new', md5('x')::uuid)
returning tenant_id, order_id, total_cents;
rollback;

-- name: settings_reach_the_node ordered route=home
select current_setting('application_name') <> '', current_user is not null;

-- Phase 2: reads across nodes. Each must answer exactly what one Postgres answers, or be
-- refused with an L9 error; the comments say what each one stresses.

-- name: scatter_plain route=scatter
select tenant_id, order_id, total_cents from oracle.orders where total_cents > 49500;

-- Text ordering is the node's collation, not byte order: upper case on every other row sorts
-- differently under en_US and under C.
-- name: order_by_collation ordered route=scatter
select tenant_id, case when tenant_id % 2 = 0 then upper(name) else name end as n
from oracle.tenants order by 2, tenant_id limit 25;

-- name: order_by_collate_c ordered route=scatter
select tenant_id from oracle.tenants
order by (case when tenant_id % 2 = 0 then upper(name) else name end) collate "C", tenant_id limit 25;

-- name: order_desc_nulls_last ordered route=scatter
select device, seq, value from oracle.events order by value desc nulls last, device, seq limit 30;

-- name: order_nulls_first ordered route=scatter
select device, seq, value from oracle.events order by value nulls first, device, seq limit 30 offset 5;

-- name: order_timestamptz ordered route=scatter
select device, seq, at from oracle.events order by at desc, device limit 12;

-- name: min_max_text ordered route=scatter
select min(sku), max(sku), min(name), max(name) from oracle.items join oracle.tenants using (tenant_id);

-- name: group_by_hidden_key route=scatter
select count(*), sum(qty) from oracle.items group by sku;

-- name: having_avg ordered route=scatter
select status, round(avg(total_cents), 2) from oracle.orders group by status having avg(total_cents) > 25000 order by status;

-- name: order_by_aggregate ordered route=scatter
select country, count(*) from oracle.tenants group by country order by count(*) desc, country limit 5;

-- name: avg_numeric_scale ordered route=scatter
select avg(total_cents::numeric / 100), sum(total_cents::numeric / 3) from oracle.orders;

-- name: count_distinct_grouped ordered route=scatter
select status, count(distinct placed_on), count(*) from oracle.orders group by status order by status;

-- name: sum_avg_distinct ordered route=scatter
select sum(distinct total_cents), avg(distinct qty), count(distinct sku) from oracle.orders join oracle.items using (tenant_id, order_id);

-- name: distinct_pairs route=scatter
select distinct status, placed_on >= date '2025-01-01' from oracle.orders;

-- name: distinct_order_limit ordered route=scatter
select distinct country from oracle.tenants order by country desc limit 4;

-- name: bool_aggregates ordered route=scatter
select bool_and(total_cents > 50), bool_or(status = 'zzz'), every(total_cents < 50100) from oracle.orders;

-- name: subquery_grouped_by_key ordered route=scatter
select avg(n), max(n), count(*) from (select tenant_id, count(*) n from oracle.orders group by tenant_id) s;

-- name: group_by_reference ordered route=scatter
select c.region, count(*), min(t.created_at) from oracle.tenants t join oracle.countries c on c.code = t.country
group by c.region order by c.region;

-- name: uuid_keyspace_scatter ordered route=scatter
select count(*), count(distinct device), max(seq) from oracle.events where kind = 'alarm';

-- name: group_by_position_and_expression ordered route=scatter
select date_trunc('month', placed_at), count(*) from oracle.orders group by 1 order by 1 desc limit 6;

-- name: empty_aggregate ordered route=scatter
select count(*), sum(total_cents), avg(total_cents), min(status) from oracle.orders where total_cents < 0;

-- name: empty_groups ordered route=scatter
select status, count(*) from oracle.orders where total_cents < 0 group by status;

-- name: float_sum route=refuse
select sum(value) from oracle.events;

-- name: string_agg route=refuse
select string_agg(name, ',') from oracle.tenants;

-- name: subquery_aggregate route=refuse
select count(*) from oracle.orders where total_cents > (select avg(total_cents) from oracle.orders);

-- name: subquery_limit route=refuse
select count(*) from (select tenant_id from oracle.orders order by total_cents limit 7) s;

-- name: distinct_on route=refuse
select distinct on (status) status, tenant_id from oracle.orders order by status, total_cents;

-- name: union_across route=refuse
select tenant_id from oracle.orders union select tenant_id from oracle.items;
