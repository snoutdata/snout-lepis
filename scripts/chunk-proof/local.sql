-- Proof, node-local semantics of the shell + chunk tables (PG 18).
-- Node owns chunks 0 and 1 of 4 (seed 0). Every check raises on failure.
\set ON_ERROR_STOP 1
create function expect(sql text, code text) returns void language plpgsql as $$
begin
	execute sql;
	raise exception 'expected % from: %', code, sql using errcode = 'LP001';
exception when others then
	if sqlstate <> code then raise; end if;
end $$;
create function check_(ok boolean, what text) returns void language plpgsql as $$
begin if ok is not true then raise exception 'FAILED: %', what; end if; end $$;
create table ref (code int primary key, label text);
insert into ref values (1, 'one'), (2, 'two');
create table cust (id int8 primary key, name text not null, code int,
	constraint lepis_shell_insert check (false) no inherit);
create table cust_c000 (primary key (id), foreign key (code) references ref (code),
	constraint lepis_owns check (hashint8extended(id, 0) between -9223372036854775808 and -4611686018427387905)) inherits (cust);
create table cust_c001 (primary key (id), foreign key (code) references ref (code),
	constraint lepis_owns check (hashint8extended(id, 0) between -4611686018427387904 and -1)) inherits (cust);
create table ord (cust_id int8 not null, n int not null, amt int, primary key (cust_id, n),
	constraint lepis_shell_insert check (false) no inherit);
create table ord_c000 (primary key (cust_id, n), foreign key (cust_id) references cust_c000 (id),
	constraint lepis_owns check (hashint8extended(cust_id, 0) between -9223372036854775808 and -4611686018427387905)) inherits (ord);
create table ord_c001 (primary key (cust_id, n), foreign key (cust_id) references cust_c001 (id),
	constraint lepis_owns check (hashint8extended(cust_id, 0) between -4611686018427387904 and -1)) inherits (ord);
create table keys as select g as id,
	case when hashint8extended(g, 0) < -4611686018427387904 then 0 when hashint8extended(g, 0) < 0 then 1 else 9 end as c
	from generate_series(1, 2000) g;
insert into cust_c000 select id, 'n' || id, 1 from keys where c = 0;
insert into cust_c001 select id, 'n' || id, 2 from keys where c = 1;
insert into ord_c000 select id, 1, 10 from keys where c = 0;
insert into ord_c001 select id, 1, 10 from keys where c = 1;
create table pick as select (select id from keys where c = 0 order by id limit 1) a0,
	(select id from keys where c = 1 order by id limit 1) a1,
	(select id from keys where c = 9 order by id limit 1) other;

select check_((select count(*) from cust) = (select count(*) from keys where c in (0, 1)), 'shell reads every local chunk');
select check_((select count(*) from only cust) = 0, 'the shell holds no row');
-- a node-local INSERT into the shell is an error, never a no-op; ON CONFLICT and RETURNING too
select expect('insert into cust values (' || other || ', ''x'', 1)', '23514') from pick;
select expect('insert into cust values (' || other || ', ''x'', 1) returning *', '23514') from pick;
select expect('insert into cust values (' || a0 || ', ''x'', 1) on conflict do nothing', '23514') from pick;
select expect('insert into cust values (' || a0 || ', ''x'', 1) on conflict (id) do update set name = ''y''', '23514') from pick;
do $$ begin insert into cust values (-5, 'x', 1);
exception when check_violation then
	perform check_(sqlerrm like '%lepis_shell_insert%', 'the error names lepis_shell_insert');
end $$;
-- UPDATE / DELETE through the shell reach the chunks, RETURNING included
create table got as with u as (update cust set name = 'u' where id in (select a0 from pick union all select a1 from pick)
	returning tableoid::regclass::text t) select t from u;
select check_((select count(*) from got) = 2 and (select count(distinct t) from got) = 2, 'update returning through the shell, both chunks');
create table gone as with d as (delete from ord where cust_id = (select a1 from pick) returning cust_id) select * from d;
select check_((select count(*) from gone) = 1, 'delete returning through the shell');
-- a shard-key change across chunks fails on the target fence; the same key in two chunks is impossible
select expect('update cust_c000 set id = ' || other || ' where id = ' || a0, '23514') from pick;
select expect('insert into cust_c001 values (' || a0 || ', ''dup'', 1)', '23514') from pick;
-- ON CONFLICT routed to the chunk
insert into cust_c000 select a0, 'oc', 1 from pick on conflict (id) do update set name = excluded.name;
select check_((select name from cust where id = (select a0 from pick)) = 'oc', 'on conflict on the chunk');
-- FKs: chunk to chunk (same chunk number) and chunk to a reference table hold; an FK to the shell fails loudly
do $$ declare k int8; begin
	select g into k from generate_series(100000, 200000) g where hashint8extended(g, 0) < -4611686018427387904 limit 1;
	perform expect(format('insert into ord_c000 values (%s, 2, 1)', k), '23503');
	perform expect(format('insert into cust_c000 values (%s, ''z'', 99)', k), '23503');
end $$;
create table other (id int primary key, cust_id int8 references cust (id));
select expect('insert into other values (1, ' || a0 || ')', '23503') from pick;
-- row triggers on every chunk fire once per row through the shell; a statement trigger fires on
-- the table the statement names, once
create table trig_log (id int8, tg text);
create function tf() returns trigger language plpgsql as $$ begin insert into trig_log values (new.id, tg_table_name); return new; end $$;
create trigger t_row before update on cust_c000 for each row execute function tf();
create trigger t_row before update on cust_c001 for each row execute function tf();
create table stmt_log (tg text);
create function sf() returns trigger language plpgsql as $$ begin insert into stmt_log values (tg_table_name); return null; end $$;
create trigger t_stmt after update on cust for each statement execute function sf();
create trigger t_stmt after update on cust_c000 for each statement execute function sf();
create trigger t_stmt after update on cust_c001 for each statement execute function sf();
update cust set name = 'tr' where id in (select id from keys where c in (0, 1) order by id limit 10);
select check_((select count(*) from trig_log) = 10 and (select count(distinct id) from trig_log) = 10, 'row triggers once per row');
select check_((select count(*) from stmt_log) = 1 and (select tg from stmt_log) = 'cust', 'statement trigger once, on the named table');
-- views and functions on the shell
create view cv as select c.id, r.label from cust c join ref r using (code);
create function cnt() returns bigint language sql stable as 'select count(*) from cust';
select check_((select count(*) from cv) = (select count(*) from cust) and cnt() = (select count(*) from cust), 'views and functions');
-- DDL on the shell reaches the chunks for columns and inherited CHECKs, not for indexes or triggers
alter table cust add column note text default 'n';
select check_((select count(*) from cust_c001 where note = 'n') = (select count(*) from cust_c001), 'add column recurses');
\echo LOCAL OK
