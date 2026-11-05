create table acct (id int8 primary key, v int not null default 0, constraint lepis_shell_insert check (false) no inherit);
create table acct_c000b (like acct, primary key (id), constraint lepis_owns check (hashint8extended(id, 0) between -6917529027641081856 and -4611686018427387905));
create subscription lm connection 'host=:SOURCE user=postgres password=x' publication lm;
