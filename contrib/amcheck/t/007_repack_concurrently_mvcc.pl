
# Copyright (c) 2021-2025, PostgreSQL Global Development Group

# Test REPACK CONCURRENTLY with concurrent modifications
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

my $node;

#
# Test set-up
#
$node = PostgreSQL::Test::Cluster->new('CIC_test');
$node->init;
$node->append_conf('postgresql.conf',
	'lock_timeout = ' . (1000 * $PostgreSQL::Test::Utils::timeout_default));
$node->append_conf(
	'postgresql.conf', qq(
wal_level = logical
));
$node->start;
$node->safe_psql('postgres', q(CREATE TABLE tbl1(i int PRIMARY KEY, j int)));
$node->safe_psql('postgres', q(CREATE TABLE tbl2(i int PRIMARY KEY, j int)));


# Insert 100 rows into tbl1
$node->safe_psql('postgres', q(
    INSERT INTO tbl1 SELECT i, i % 100 FROM generate_series(1,100) i
));

# Insert 100 rows into tbl2
$node->safe_psql('postgres', q(
    INSERT INTO tbl2 SELECT i, i % 100 FROM generate_series(1,100) i
));


# Insert 100 rows into tbl1
$node->safe_psql('postgres', q(
	CREATE OR REPLACE FUNCTION log_raise(i int, j1 int, j2 int) RETURNS VOID AS $$
	BEGIN
	  RAISE NOTICE 'ERROR i=% j1=% j2=%', i, j1, j2;
	END;$$ LANGUAGE plpgsql;
));

$node->safe_psql('postgres', q(CREATE UNLOGGED SEQUENCE in_row_rebuild START 1 INCREMENT 1;));
$node->safe_psql('postgres', q(SELECT nextval('in_row_rebuild');));


$node->pgbench(
'--no-vacuum --client=10 --jobs=4 --exit-on-abort --transactions=2500',
0,
[qr{actually processed}],
[qr{^$}],
'concurrent operations with REINDEX/CREATE INDEX CONCURRENTLY',
{
	'concurrent_ops' => q(
		SELECT pg_try_advisory_lock(42)::integer AS gotlock \gset
		\if :gotlock
			SELECT nextval('in_row_rebuild') AS last_value \gset
			\if :last_value = 2
				REPACK (CONCURRENTLY) tbl1 USING INDEX tbl1_pkey;
				\sleep 10 ms
				REPACK (CONCURRENTLY) tbl2 USING INDEX tbl2_pkey;
				\sleep 10 ms
			\endif
			SELECT pg_advisory_unlock(42);
		\else
			\set num random(1, 100)
			BEGIN;
			UPDATE tbl1 SET j = j + 1 WHERE i = :num;
			\sleep 1 ms
			UPDATE tbl1 SET j = j + 2 WHERE i = :num;
			\sleep 1 ms
			UPDATE tbl1 SET j = j + 3 WHERE i = :num;
			\sleep 1 ms
			UPDATE tbl1 SET j = j + 4 WHERE i = :num;
			\sleep 1 ms

			UPDATE tbl2 SET j = j + 1 WHERE i = :num;
			\sleep 1 ms
			UPDATE tbl2 SET j = j + 2 WHERE i = :num;
			\sleep 1 ms
			UPDATE tbl2 SET j = j + 3 WHERE i = :num;
			\sleep 1 ms
			UPDATE tbl2 SET j = j + 4 WHERE i = :num;

			COMMIT;
			SELECT setval('in_row_rebuild', 1);

			BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
			SELECT COALESCE(SUM(j), 0) AS t1 FROM tbl1 WHERE i = :num \gset p_
			\sleep 10 ms
			SELECT COALESCE(SUM(j), 0) AS t2 FROM tbl2 WHERE i = :num \gset p_
			\if :p_t1 != :p_t2
				COMMIT;
				SELECT log_raise(tbl1.i, tbl1.j, tbl2.j) FROM tbl1 LEFT OUTER JOIN tbl2 ON tbl1.i = tbl2.i WHERE tbl1.j != tbl2.j;
				\sleep 10 ms
				SELECT log_raise(tbl1.i, tbl1.j, tbl2.j) FROM tbl1 LEFT OUTER JOIN tbl2 ON tbl1.i = tbl2.i WHERE tbl1.j != tbl2.j;
				SELECT (:p_t1 + :p_t2) / 0;
			\endif

			COMMIT;
		\endif
	)
});

$node->stop;
done_testing();
