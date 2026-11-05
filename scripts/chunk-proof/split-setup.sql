create table acct (id int8 primary key, v int not null default 0, constraint lepis_shell_insert check (false) no inherit);
create table acct_c000 (primary key (id), constraint lepis_owns check (hashint8extended(id, 0) between -9223372036854775808 and -4611686018427387905)) inherits (acct);
create table chunkmap (lo int8, hi int8, tab text);
insert into chunkmap values (-9223372036854775808, -4611686018427387905, 'acct_c000');
create table pool (i int primary key, id int8);
insert into pool select row_number() over (), g from generate_series(1, 200000) g where hashint8extended(g,0) < -4611686018427387904;
insert into acct_c000 select id, 0 from pool;
create sequence s start 1000000;
create table upd_log (id int8);
create table ins_log (id int8);
create table retries (n int);
-- the "router": read the map, write the chunk, retry on a fence or a dropped chunk
create function w(r int) returns void language plpgsql as $$
declare t text; k int8; nid int8; n int;
begin
  select id into k from pool where i = 1 + r % (select count(*) from pool);
  loop
    nid := nextval('s');
    exit when hashint8extended(nid, 0) < -4611686018427387904;
  end loop;
  for attempt in 1..50 loop
    begin
      select tab into t from chunkmap where hashint8extended(k,0) between lo and hi;
      execute format('update %I set v = v + 1 where id = $1', t) using k;
      get diagnostics n = row_count;
      if n <> 1 then raise exception 'lost row %', k; end if;
      select tab into t from chunkmap where hashint8extended(nid,0) between lo and hi;
      execute format('insert into %I (id, v) values ($1, 0)', t) using nid;
      insert into upd_log values (k);
      insert into ins_log values (nid);
      return;
    exception when check_violation or undefined_table then
      insert into retries values (1);
      perform pg_sleep(0.01);
    end;
  end loop;
  raise exception 'gave up';
end $$;
