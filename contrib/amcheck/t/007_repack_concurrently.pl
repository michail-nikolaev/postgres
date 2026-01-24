
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
max_worker_processes = 32
));

my $n=1000;
my $no_hot = int(rand(2));

$node->start;
$node->safe_psql('postgres', q(CREATE TABLE tbl(i int PRIMARY KEY, j int)));

if ($no_hot)
{
	$node->safe_psql('postgres', q(CREATE INDEX test_idx ON tbl(j);));
}
else
{
	$node->safe_psql('postgres', q(CREATE INDEX test_idx ON tbl(i);));
}


# Load amcheck
$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));

# Insert $n rows into tbl
$node->safe_psql('postgres', qq(
	INSERT INTO tbl SELECT i, i FROM generate_series(1,$n) i
));

my $sum = $node->safe_psql('postgres', q(
	SELECT SUM(j) AS sum FROM tbl
));


$node->pgbench(
'--no-vacuum --client=15 --jobs=4 --exit-on-abort --transactions=5000',
0,
[qr{actually processed}],
[qr{^$}],
'concurrent operations with REPACK CONCURRENTLY',
{
	'concurrent_ops' => qq(
		SELECT pg_try_advisory_lock(42)::integer AS gotlock \\gset
		\\if :gotlock
			REPACK (CONCURRENTLY) tbl USING INDEX tbl_pkey;
			SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
			SELECT bt_index_parent_check('test_idx', heapallindexed => true);
			\\sleep 10 ms

			REPACK (CONCURRENTLY) tbl USING INDEX test_idx;
			SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
			SELECT bt_index_parent_check('test_idx', heapallindexed => true);
			\\sleep 10 ms

			REPACK (CONCURRENTLY) tbl;
			SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
			SELECT bt_index_parent_check('test_idx', heapallindexed => true);
			\\sleep 10 ms

			SELECT pg_advisory_unlock(42);
		\\else
			\\set num_a random(1, $n)
			\\set num_b random(1, $n)
			\\set diff random(1, 10000)
			BEGIN;
			UPDATE tbl SET j = j + :diff WHERE i = :num_a;
			\\sleep 1 ms
			UPDATE tbl SET j = j - :diff WHERE i = :num_b;
			\\sleep 1 ms
			COMMIT;

			BEGIN
			--TRANSACTION ISOLATION LEVEL REPEATABLE READ
			;
			SELECT 1;
			\\sleep 1 ms
			SELECT COALESCE(SUM(j), 0) AS sum FROM tbl \\gset p_
			\\if :p_sum != $sum
				COMMIT;
				SELECT (:p_sum) / 0;
			\\endif

			COMMIT;
		\\endif
	)
});

$node->stop;
done_testing();
