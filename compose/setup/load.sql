-- The write load for "Split while a load runs" (../README.md): pgbench runs this through the
-- router, each transaction one new order for a random tenant. Every row it writes is marked
-- status = 'load', so afterwards the rows can be counted on the nodes and set against what
-- pgbench reports it committed.
\set tenant random(1, 1000)
\set id random(1000000, 9000000000000000)
insert into orders (tenant_id, order_id, placed_at, total_cents, status)
	values (:tenant, :id, now(), 100, 'load');
