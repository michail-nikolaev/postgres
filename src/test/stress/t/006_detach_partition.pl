# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for ALTER TABLE ... DETACH PARTITION ... CONCURRENTLY with
# concurrent modifications and queries.
#
# Writer clients apply sum-neutral updates through the partitioned
# parent, with both affected rows always in the same partition; hence
# the sum over each partition's val column is invariant, whether the
# partition is currently attached or not (while it is detached, the
# update through the parent simply finds no rows).  One client
# repeatedly detaches a partition concurrently, verifies its index with
# amcheck while it is detached, and re-attaches it (DETACH CONCURRENTLY
# left behind a CHECK constraint matching the partition bound, so the
# re-attach does not need a validation scan).  Reader clients verify the
# per-partition invariants and also query through the parent, which must
# never fail while the partition descriptor changes under them.  Any SQL
# error or broken invariant aborts pgbench, failing the test.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled DETACH PARTITION CONCURRENTLY stress test');

my $nrows = 10_000;
my $halfway = $nrows / 2 + 1;
my $upper = $nrows + 1;
my $duration = 6 * $stressval;

my $node;

#
# Test set-up
#
$node = stress_init_node('detach_partition');
$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));
$node->safe_psql(
	'postgres', qq(
	CREATE TABLE ptab(id int PRIMARY KEY, val int) PARTITION BY RANGE (id);
	CREATE TABLE ptab_p1 PARTITION OF ptab
		FOR VALUES FROM (1) TO ($halfway);
	CREATE TABLE ptab_p2 PARTITION OF ptab
		FOR VALUES FROM ($halfway) TO ($upper);
	INSERT INTO ptab SELECT g, g FROM generate_series(1, $nrows) g;
));

my $sum1 = $node->safe_psql('postgres',
	q(SELECT SUM(val) AS sum FROM ptab_p1));
my $sum2 = $node->safe_psql('postgres',
	q(SELECT SUM(val) AS sum FROM ptab_p2));

$node->pgbench(
	"--no-vacuum --client=30 --jobs=4 --exit-on-abort -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'DETACH PARTITION CONCURRENTLY with concurrent balanced updates',
	{
		'concurrent_ops' => stress_ddl_gate(
			# A fixed detach/re-attach cycle, not a random pick.
			ddl => [
				[
					'ALTER TABLE ptab DETACH PARTITION ptab_p2 CONCURRENTLY;',
					'\sleep 10 ms',
					"SELECT bt_index_parent_check('ptab_p2_pkey', heapallindexed => true);",
					"ALTER TABLE ptab ATTACH PARTITION ptab_p2 FOR VALUES FROM ($halfway) TO ($upper);",
					"SELECT bt_index_parent_check('ptab_p1_pkey', heapallindexed => true);",
				],
			],
			# Both rows of an update always land in the same partition, so
			# each partition's sum is invariant on its own; pick which one
			# this transaction works on.
			else => stress_variant_switch(
				var => 'part',
				variants => [
					qq(\\set num_a random(1, $nrows / 2)
					\\set num_b random(1, $nrows / 2)),
					qq(\\set num_a random($halfway, $nrows)
					\\set num_b random($halfway, $nrows)),
				],
			  )
			  . qq(
			\\set diff random(1, 10000)
			UPDATE ptab SET val = val
				+ CASE WHEN id = :num_a THEN (:diff) ELSE 0 END
				+ CASE WHEN id = :num_b THEN -(:diff) ELSE 0 END
				WHERE id IN (:num_a, :num_b);
			\\sleep 1 ms

			SELECT COUNT(*) FROM ptab;
			SELECT stress_assert(COALESCE(SUM(val), 0) = $sum1,
				format('ptab_p1 sum is %s, not $sum1', COALESCE(SUM(val), 0)))
				FROM ptab_p1;
			SELECT stress_assert(COALESCE(SUM(val), 0) = $sum2,
				format('ptab_p2 sum is %s, not $sum2', COALESCE(SUM(val), 0)))
				FROM ptab_p2;)
		),
	});

$node->stop;

done_testing();
