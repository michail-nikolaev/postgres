# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for CONCURRENTLY commands on partitioned tables.
#
# CREATE INDEX CONCURRENTLY cannot be run on a partitioned table
# directly; the documented way to build one without blocking writes is
# to create the index on ONLY the parent (which leaves it invalid),
# build a matching index concurrently on every partition, and attach
# them one by one, at which point the parent index becomes valid.  One
# client repeatedly does exactly that, and also runs REINDEX TABLE
# CONCURRENTLY on the parent (which recurses into the partitions),
# REINDEX INDEX CONCURRENTLY on a partition's own index, and REPACK
# (CONCURRENTLY) on individual partitions.
#
# Writer clients apply balanced pairs of updates through the parent,
# routed across all partitions, so the sum over the val column is
# invariant; reader clients verify it through the parent and through
# individual partitions.  Any SQL error or broken invariant aborts
# pgbench, failing the test.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled partitioned table stress test');

my $duration = 6 * $stressval;
my $nrows = 10_000;
my $bound = $nrows / 4;

my $node;

#
# Test set-up
#
$node = stress_init_node('partitioned');
$node->safe_psql(
	'postgres', qq(
	CREATE EXTENSION amcheck;
	CREATE TABLE p(id int, val int, PRIMARY KEY(id)) PARTITION BY RANGE (id);
	CREATE TABLE p1 PARTITION OF p FOR VALUES FROM (1) TO (@{[ $bound + 1 ]});
	CREATE TABLE p2 PARTITION OF p
		FOR VALUES FROM (@{[ $bound + 1 ]}) TO (@{[ 2 * $bound + 1 ]});
	CREATE TABLE p3 PARTITION OF p
		FOR VALUES FROM (@{[ 2 * $bound + 1 ]}) TO (@{[ 3 * $bound + 1 ]});
	CREATE TABLE p4 PARTITION OF p
		FOR VALUES FROM (@{[ 3 * $bound + 1 ]}) TO (@{[ $nrows + 1 ]});
	INSERT INTO p SELECT g, g FROM generate_series(1, $nrows) g;
));

my $sum = $node->safe_psql('postgres', q(SELECT SUM(val) AS sum FROM p));

$node->pgbench(
	"--no-vacuum --client=30 --jobs=4 --exit-on-abort -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'CONCURRENTLY commands on a partitioned table',
	{
		'concurrent_ops' => stress_ddl_gate(
			ddl => [
				[
					'-- The documented way to build a partitioned index',
					'-- without blocking writes.',
					'CREATE INDEX p_val_idx ON ONLY p (val);',
					'CREATE INDEX CONCURRENTLY p1_val_idx ON p1(val);',
					'ALTER INDEX p_val_idx ATTACH PARTITION p1_val_idx;',
					'CREATE INDEX CONCURRENTLY p2_val_idx ON p2(val);',
					'ALTER INDEX p_val_idx ATTACH PARTITION p2_val_idx;',
					'CREATE INDEX CONCURRENTLY p3_val_idx ON p3(val);',
					'ALTER INDEX p_val_idx ATTACH PARTITION p3_val_idx;',
					'CREATE INDEX CONCURRENTLY p4_val_idx ON p4(val);',
					'ALTER INDEX p_val_idx ATTACH PARTITION p4_val_idx;',
					'-- Once every partition has one, the parent index is valid.',
					'SELECT stress_assert(indisvalid,'
					  . q{ 'parent index still invalid after every partition was attached')}
					  . q{ FROM pg_index WHERE indexrelid = 'p_val_idx'::regclass;},
					"SELECT bt_index_parent_check('p1_val_idx', heapallindexed => true);",
					'DROP INDEX p_val_idx;',
				],
				'REINDEX TABLE CONCURRENTLY p;',
				'REINDEX INDEX CONCURRENTLY p1_pkey;',
				[ '\set part random(1, 4)', 'REPACK (CONCURRENTLY) p:part;' ],
			],
			post =>
			  "SELECT bt_index_check('p1_pkey', heapallindexed => true, checkunique => true);",
			else => stress_workload(
				mutations => [
					qq(\\set num_a random(1, $nrows)
					\\set num_b random(1, $nrows)
					\\set diff random(1, 10000)
					BEGIN;
					UPDATE p SET val = val + :diff WHERE id = :num_a;
					\\sleep 1 ms
					UPDATE p SET val = val - :diff WHERE id = :num_b;
					\\sleep 1 ms
					COMMIT;),
				],
				checks => [
					qq(-- Through the parent: all partitions together.
					SELECT stress_assert(COALESCE(SUM(val), 0) = $sum,
						format('sum through parent is %s, not $sum',
							COALESCE(SUM(val), 0))) FROM p;),
					qq(-- Every row must still be reachable through the parent.
					SELECT stress_assert(COUNT(*) = $nrows,
						format('%s rows through parent, not $nrows', COUNT(*)))
						FROM p;),
				],
			),
		),
	});

is( $node->safe_psql('postgres', q(SELECT SUM(val) FROM p)),
	$sum, 'sum invariant holds after partitioned DDL churn');

is( $node->safe_psql('postgres', q(SELECT COUNT(*) FROM p)),
	"$nrows", 'no rows lost after partitioned DDL churn');

$node->safe_psql(
	'postgres', q(
	SELECT bt_index_parent_check('p1_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('p2_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('p3_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('p4_pkey', heapallindexed => true);
));

$node->stop;

done_testing();
