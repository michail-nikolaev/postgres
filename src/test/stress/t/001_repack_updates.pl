# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Stress test for REPACK CONCURRENTLY with concurrent modifications.
#
# Concurrent clients apply balanced pairs of updates (one +diff, one
# -diff within a single transaction), so the sum over the val column is
# invariant at every transaction boundary.  While that is going on, one
# client repeatedly runs REPACK (CONCURRENTLY) in its various forms and
# verifies the indexes with amcheck; reader clients verify that the sum
# never changes.  Any SQL error or broken invariant aborts pgbench,
# failing the test.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled REPACK CONCURRENTLY stress test');

my $node;

#
# Test set-up
#
my $nrows = 10_000 * $stressval;
my $duration = 6 * $stressval;
my $no_hot = int(rand(2));

$node = stress_init_node('repack_updates');
$node->safe_psql('postgres', q(CREATE TABLE tbl(id int PRIMARY KEY, val int)));

if ($no_hot)
{
	$node->safe_psql('postgres', q(CREATE INDEX test_idx ON tbl(val);));
}
else
{
	$node->safe_psql('postgres', q(CREATE INDEX test_idx ON tbl(id);));
}

# Load amcheck
$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));

# Insert $nrows rows into tbl
$node->safe_psql(
	'postgres', qq(
	INSERT INTO tbl SELECT g, g FROM generate_series(1, $nrows) g
));

my $sum = $node->safe_psql(
	'postgres', q(
	SELECT SUM(val) AS sum FROM tbl
));

$node->pgbench(
	"--no-vacuum --client=30 --jobs=4 --exit-on-abort -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'REPACK CONCURRENTLY with concurrent balanced updates',
	{
		'concurrent_ops' => stress_ddl_gate(
			# Each round repacks by each index in turn, checking both
			# indexes after every one; this is a fixed sequence, not a
			# random pick.
			ddl => [
				[
					'REPACK (CONCURRENTLY) tbl USING INDEX tbl_pkey;',
					"SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);",
					"SELECT bt_index_parent_check('test_idx', heapallindexed => true);",
					'\sleep 10 ms',
					'REPACK (CONCURRENTLY) tbl USING INDEX test_idx;',
					"SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);",
					"SELECT bt_index_parent_check('test_idx', heapallindexed => true);",
					'\sleep 10 ms',
					'REPACK (CONCURRENTLY) tbl;',
					"SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);",
					"SELECT bt_index_parent_check('test_idx', heapallindexed => true);",
				],
			],
			else => stress_workload(
				mutations => [
					qq(\\set num_a random(1, $nrows)
					\\set num_b random(1, $nrows)
					\\set diff random(1, 10000)
					BEGIN;
					UPDATE tbl SET val = val + :diff WHERE id = :num_a;
					\\sleep 1 ms
					UPDATE tbl SET val = val - :diff WHERE id = :num_b;
					\\sleep 1 ms
					COMMIT;),
				],
				checks => [
					qq(BEGIN
					--TRANSACTION ISOLATION LEVEL REPEATABLE READ
					;
					SELECT 1;
					\\sleep 1 ms
					SELECT stress_assert(COALESCE(SUM(val), 0) = $sum,
						format('sum is %s, not $sum', COALESCE(SUM(val), 0)))
						FROM tbl;
					COMMIT;),
				],
			),
		),
	});

$node->stop;

done_testing();
