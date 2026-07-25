# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for replication slot creation and removal, concurrently
# with data modifications, logical decoding and REPACK (CONCURRENTLY).
#
# The node runs with the default wal_level = replica, so creating and
# dropping logical replication slots (including the transient one used
# internally by REPACK (CONCURRENTLY)) toggles the dynamic activation
# and deactivation of logical decoding; see effective_wal_level.
#
# One client cycles a logical slot through
# create/consume/peek/copy/drop, another repeatedly repacks the table
# concurrently and verifies its index with amcheck, a third one creates
# and drops a physical slot, and the remaining clients apply balanced
# updates and verify the sum invariant, as in 001_repack_updates.pl.
# Any SQL error or broken invariant aborts pgbench, failing the test.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled replication slot stress test');

my $nrows = 10_000;
my $duration = 6 * $stressval;

my $node;

#
# Test set-up
#
$node = stress_init_node('replication_slots',
	init => { allows_streaming => 1 },
	extra_conf => [ 'max_connections = 50' ]);
$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));
$node->safe_psql('postgres', q(CREATE TABLE tbl(id int PRIMARY KEY, val int)));
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
	'replication slot creation/removal with concurrent activity',
	{
		# Three gates on three different locks, so that at most one client
		# at a time does each of: cycle a logical slot, repack the table,
		# cycle a physical slot.  Everyone else writes and checks.
		'concurrent_ops' => stress_ddl_gate(
			lock => 42,
			var => 'gotlock_slots',
			ddl => [
				[
					"SELECT 'created' FROM pg_create_logical_replication_slot('stress_slot', 'test_decoding');",
					"SELECT COUNT(*) FROM pg_logical_slot_get_changes('stress_slot', NULL, NULL);",
					'\sleep 10 ms',
					"SELECT COUNT(*) FROM pg_logical_slot_peek_changes('stress_slot', NULL, NULL);",
					"SELECT 'copied' FROM pg_copy_logical_replication_slot('stress_slot', 'stress_slot_copy');",
					"SELECT COUNT(*) FROM pg_logical_slot_get_changes('stress_slot', NULL, NULL);",
					"SELECT pg_drop_replication_slot('stress_slot_copy');",
					"SELECT pg_drop_replication_slot('stress_slot');",
				],
			],
			else => stress_ddl_gate(
				indent => "\t\t\t",
				lock => 43,
				var => 'gotlock_repack',
				ddl => ['REPACK (CONCURRENTLY) tbl;'],
				post =>
				  "SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);",
				else => stress_ddl_gate(
					indent => "\t\t\t\t",
					lock => 44,
					var => 'gotlock_phys',
					ddl => [
						[
							"SELECT 'created' FROM pg_create_physical_replication_slot('stress_phys_slot', true);",
							'\sleep 10 ms',
							"SELECT pg_drop_replication_slot('stress_phys_slot');",
						],
					],
					else => stress_workload(
						indent => "\t\t\t\t\t",
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
							qq(BEGIN;
							SELECT 1;
							\\sleep 1 ms
							SELECT stress_assert(COALESCE(SUM(val), 0) = $sum,
								format('sum is %s, not $sum', COALESCE(SUM(val), 0)))
								FROM tbl;
							COMMIT;),
						],
					),
				),
			),
		),
	});

$node->stop;

done_testing();
