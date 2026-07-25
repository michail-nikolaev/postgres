# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for REFRESH MATERIALIZED VIEW CONCURRENTLY with concurrent
# modifications of the underlying table.
#
# Concurrent clients apply balanced pairs of updates (one +diff, one
# -diff within a single transaction) to the base table, so the sum over
# its val column is invariant at every transaction boundary.  Any
# snapshot the refresh may use therefore sees the same sum, and so the
# materialized view must always report it too, both while the refresh is
# applying its diff and after it.  One client repeatedly refreshes the
# materialized view and verifies its unique index with amcheck; reader
# clients verify the invariant.  Any SQL error or broken invariant
# aborts pgbench, failing the test.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled REFRESH MATERIALIZED VIEW CONCURRENTLY stress test');

my $nrows = 10_000;
my $duration = 6 * $stressval;

my $node;

#
# Test set-up
#
$node = stress_init_node('refresh_matview');
$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));
$node->safe_psql('postgres', q(CREATE TABLE base(id int PRIMARY KEY, val int)));
$node->safe_psql(
	'postgres', qq(
	INSERT INTO base SELECT g, g FROM generate_series(1, $nrows) g
));
$node->safe_psql('postgres',
	q(CREATE MATERIALIZED VIEW mv AS SELECT id, val FROM base));
# REFRESH MATERIALIZED VIEW CONCURRENTLY requires a unique index.
$node->safe_psql('postgres', q(CREATE UNIQUE INDEX mv_id_idx ON mv(id)));

my $sum = $node->safe_psql(
	'postgres', q(
	SELECT SUM(val) AS sum FROM base
));

$node->pgbench(
	"--no-vacuum --client=30 --jobs=4 --exit-on-abort -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'REFRESH MATERIALIZED VIEW CONCURRENTLY with concurrent balanced updates',
	{
		'concurrent_ops' => stress_ddl_gate(
			ddl => ['REFRESH MATERIALIZED VIEW CONCURRENTLY mv;'],
			post =>
			  "SELECT bt_index_check('mv_id_idx', heapallindexed => true, checkunique => true);",
			else => stress_workload(
				mutations => [
					qq(\\set num_a random(1, $nrows)
					\\set num_b random(1, $nrows)
					\\set diff random(1, 10000)
					BEGIN;
					UPDATE base SET val = val + :diff WHERE id = :num_a;
					\\sleep 1 ms
					UPDATE base SET val = val - :diff WHERE id = :num_b;
					\\sleep 1 ms
					COMMIT;),
				],
				checks => [
					qq(BEGIN;
					SELECT 1;
					\\sleep 1 ms
					SELECT stress_assert(COALESCE(SUM(val), 0) = $sum,
						format('matview sum is %s, not $sum',
							COALESCE(SUM(val), 0))) FROM mv;
					COMMIT;),
				],
			),
		),
	});

$node->stop;

done_testing();
