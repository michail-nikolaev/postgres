# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for CONCURRENTLY commands racing with two-phase commit.
#
# Writer clients apply balanced pairs of updates through PREPARE
# TRANSACTION followed by COMMIT PREPARED (or, sometimes, ROLLBACK
# PREPARED) -- either way the transaction is internally balanced, so
# the sum over the val column is invariant at every commit, and
# prepared-but-unresolved transactions are invisible.  Meanwhile one
# client rotates through REPACK (CONCURRENTLY), DROP/CREATE INDEX
# CONCURRENTLY, REINDEX INDEX CONCURRENTLY and REINDEX TABLE
# CONCURRENTLY, all of which must wait for or otherwise cope with
# prepared transactions (including REPACK's logical decoding of them).
# Reader clients verify the invariant; any SQL error or broken
# invariant aborts pgbench, failing the test.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled two-phase commit stress test');

my $duration = 6 * $stressval;
my $nrows = 10_000;

my $node;

#
# Test set-up
#
$node = stress_init_node('twophase',
	extra_conf => [ 'max_prepared_transactions = 64' ]);
$node->safe_psql(
	'postgres', qq(
	CREATE EXTENSION amcheck;
	CREATE TABLE tbl(id int PRIMARY KEY, val int);
	CREATE INDEX tbl_val_idx ON tbl(val);
	INSERT INTO tbl SELECT g, g FROM generate_series(1, $nrows) g;
));

my $sum = $node->safe_psql('postgres',
	q(SELECT SUM(val) AS sum FROM tbl));

$node->pgbench(
	"--no-vacuum --client=30 --jobs=4 --exit-on-abort -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'CONCURRENTLY commands with concurrent two-phase transactions',
	{
		'concurrent_ops' => stress_ddl_gate(
			indent => "\t\t",
			ddl => [
				'REPACK (CONCURRENTLY) tbl;',
				[
					'DROP INDEX CONCURRENTLY tbl_val_idx;',
					'CREATE INDEX CONCURRENTLY tbl_val_idx ON tbl(val);',
				],
				'REINDEX INDEX CONCURRENTLY tbl_val_idx;',
				'REINDEX TABLE CONCURRENTLY tbl;',
			],
			post =>
				"SELECT bt_index_check('tbl_pkey', heapallindexed => true, checkunique => true);",
		) . qq(
			\\set num_a random(1, $nrows)
			\\set num_b random(1, $nrows)
			\\set diff random(1, 10000)
			\\set use_rollback random(0, 9)
			BEGIN;
			UPDATE tbl SET val = val + :diff WHERE id = :num_a;
			\\sleep 1 ms
			UPDATE tbl SET val = val - :diff WHERE id = :num_b;
			PREPARE TRANSACTION 'p:client_id';
			\\sleep 1 ms
			\\if :use_rollback = 0
				ROLLBACK PREPARED 'p:client_id';
			\\else
				COMMIT PREPARED 'p:client_id';
			\\endif

			BEGIN;
			SELECT 1;
			\\sleep 1 ms
			SELECT stress_assert(COALESCE(SUM(val), 0) = $sum,
				format('sum is %s, not $sum', COALESCE(SUM(val), 0))) FROM tbl;
			COMMIT;
		\\endif
	)
	});

# pgbench may have been cut off between PREPARE TRANSACTION and its
# resolution; commit the leftovers (they are internally balanced, so
# this cannot break the invariant).
my $gids = $node->safe_psql('postgres',
	q(SELECT gid FROM pg_prepared_xacts));
foreach my $gid (split /\n/, $gids)
{
	$node->safe_psql('postgres', qq(COMMIT PREPARED '$gid'));
}

is( $node->safe_psql('postgres', q(SELECT SUM(val) FROM tbl)),
	$sum, 'sum invariant holds after two-phase churn');

$node->safe_psql(
	'postgres', q(
	SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('tbl_val_idx', heapallindexed => true);
));

$node->stop;

done_testing();
