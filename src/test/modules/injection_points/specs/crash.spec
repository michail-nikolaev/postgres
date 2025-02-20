setup
{
 create table D1 (id int primary key not null);
 create table D2 (id int primary key not null);
 insert into D1 values (1);
 insert into D2 values (1);

 CREATE EXTENSION injection_points;
}

teardown
{
 DROP TABLE D1, D2;
 DROP EXTENSION injection_points;
}

session s1
setup		{ BEGIN ISOLATION LEVEL SERIALIZABLE;}
step wx1	{ update D1 set id = id + 1; }
step c1		{ COMMIT; }

session s2
setup		{
	BEGIN ISOLATION LEVEL SERIALIZABLE;
}
step rxwy2	{ update D2 set id = (select id+1 from D1); }
step c2		{ COMMIT; }

session s3
setup		{ BEGIN ISOLATION LEVEL SERIALIZABLE; }
step ry3	{ select id from D2; }
step c3		{ COMMIT; }

session s4
setup	{
	SELECT injection_points_set_local();
}
step s4_attach_locally	{ SELECT injection_points_attach('invalidate_catalog_snapshot_end', 'wait'); }

permutation
	s4_attach_locally
	wx1
	rxwy2
	c1
	ry3
	c2
	c3