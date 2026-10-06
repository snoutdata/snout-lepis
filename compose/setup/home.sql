-- What only the home node has: the global table.

create table if not exists plans (
	id int primary key,
	name text not null,
	monthly_cents int not null
);
insert into plans values (1, 'free', 0), (2, 'pro', 2500), (3, 'team', 10000)
on conflict do nothing;
grant select, insert, update, delete on plans to app;
