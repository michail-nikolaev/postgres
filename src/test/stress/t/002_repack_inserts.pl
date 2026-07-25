# Copyright (c) 2021-2026, PostgreSQL Global Development Group

# Stress test for REPACK CONCURRENTLY with concurrent insertions.
#
# Insertions are serialized with an advisory lock, so the values
# assigned by the sequence are committed in monotonically increasing
# order.  Therefore, at any later snapshot, the number of rows with
# val <= j must be exactly j once the row with val = j is known to be
# committed.  Reader clients verify that invariant while one client
# repeatedly runs REPACK (CONCURRENTLY) and verifies the indexes with
# amcheck.  Any SQL error or broken invariant aborts pgbench, failing
# the test.
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
my $duration = 6 * $stressval;
my $no_hot = int(rand(2));

$node = stress_init_node('repack_inserts');
$node->safe_psql('postgres',
	q(CREATE TABLE tbl(id SERIAL PRIMARY KEY, val int)));
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

$node->safe_psql('postgres',
	q(CREATE UNLOGGED SEQUENCE last_j START 1 INCREMENT 1;));

$node->pgbench(
	"--no-vacuum --client=30 --jobs=4 --exit-on-abort -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'REPACK CONCURRENTLY with concurrent insertions',
	{
		'concurrent_ops' => stress_ddl_gate(
			# A fixed sequence repacking by each index in turn, not a
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
			# The insert is serialized so that the values committed by the
			# sequence are strictly increasing; the check then reads them
			# back under its own snapshot.
			else => qq(
			SELECT pg_advisory_lock(43);
				BEGIN;
				INSERT INTO tbl(val) VALUES (nextval('last_j')) RETURNING val AS j \\gset p_
				COMMIT;
			SELECT pg_advisory_unlock(43);
			\\sleep 1 ms

			BEGIN
			--TRANSACTION ISOLATION LEVEL REPEATABLE READ
			;
			SELECT 1;
			\\sleep 1 ms
			SELECT stress_assert(COUNT(*) = :p_j,
				format('%s rows have val <= %s, expected %s', COUNT(*), :p_j, :p_j))
				FROM tbl WHERE val <= :p_j;
			COMMIT;)
		),
	});

$node->stop;
done_testing();
