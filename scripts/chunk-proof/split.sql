create table acct_c000a (like acct, primary key (id), constraint lepis_owns check (hashint8extended(id, 0) between -9223372036854775808 and -6917529027641081857));
create table acct_c000b (like acct, primary key (id), constraint lepis_owns check (hashint8extended(id, 0) between -6917529027641081856 and -4611686018427387905));
create unlogged table split_log (seq bigserial, id int8);
create function split_cap() returns trigger language plpgsql as $$ begin
  if tg_op <> 'INSERT' then insert into split_log (id) values (old.id); end if;
  if tg_op <> 'DELETE' then insert into split_log (id) values (new.id); end if;
  return null; end $$;
create trigger lepis_split after insert or update or delete on acct_c000 for each row execute function split_cap();
select pg_sleep(1);
insert into acct_c000a select * from only acct_c000 where hashint8extended(id,0) <= -6917529027641081857;
insert into acct_c000b select * from only acct_c000 where hashint8extended(id,0) >= -6917529027641081856;
create function split_apply(ids int8[]) returns void language plpgsql as $$ begin
  delete from acct_c000a where id = any(ids);
  delete from acct_c000b where id = any(ids);
  insert into acct_c000a select * from only acct_c000 where id = any(ids) and hashint8extended(id,0) <= -6917529027641081857;
  insert into acct_c000b select * from only acct_c000 where id = any(ids) and hashint8extended(id,0) >= -6917529027641081856;
end $$;
create function split_round() returns int language plpgsql as $$
declare ids int8[]; begin
  with b as (delete from split_log where seq in (select seq from split_log order by seq limit 20000) returning id)
  select array_agg(distinct id) into ids from b;
  if ids is null then return 0; end if;
  perform split_apply(ids);
  return cardinality(ids);
end $$;
select split_round() r1; select split_round() r2; select split_round() r3;
begin;
set local lock_timeout = '5s';
lock table acct_c000 in access exclusive mode;
select split_round() final;
select split_round() final2;
alter table acct_c000 no inherit acct;
alter table acct_c000 rename to acct_c000_old;
alter table acct_c000_old drop constraint lepis_owns, add constraint lepis_owns check (false) not valid;
alter table acct_c000a inherit acct;
alter table acct_c000b inherit acct;
update chunkmap set hi = -6917529027641081857, tab = 'acct_c000a' where tab = 'acct_c000';
insert into chunkmap values (-6917529027641081856, -4611686018427387905, 'acct_c000b');
commit;
select pg_sleep(1);
drop table acct_c000_old;
drop table split_log;
