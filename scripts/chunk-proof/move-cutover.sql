-- The source's half of a chunk move's cutover (L10): the chunk leaves the shell, is renamed out
-- of reach and fenced, in one transaction. A stale router's DELETE, UPDATE or INSERT then fails.
\set ON_ERROR_STOP 1
create function expect(sql text, code text) returns void language plpgsql as $$
begin
	execute sql;
	raise exception 'expected % from: %', code, sql using errcode = 'LP001';
exception when others then
	if sqlstate <> code then raise; end if;
end $$;
begin;
lock table acct_c000b in access exclusive mode;
alter table acct_c000b no inherit acct;
alter table acct_c000b rename to lepis_moved_acct_c000b;
alter table lepis_moved_acct_c000b drop constraint lepis_owns, add constraint lepis_owns check (false) not valid;
commit;
select expect('delete from acct_c000b', '42P01');
select expect('update acct_c000b set v = 0', '42P01');
select expect('insert into lepis_moved_acct_c000b values (-1, 0)', '23514');
