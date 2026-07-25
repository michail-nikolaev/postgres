# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for VACUUM running concurrently with CONCURRENTLY
# commands and DML.
#
# Writer clients apply balanced pairs of updates (so the sum over the
# val column is invariant at every commit); the val column is indexed
# and the updates are not HOT, so they leave dead tuples for VACUUM to
# reclaim.  Aggressive autovacuum is enabled, and one client
# additionally rotates through manual VACUUM, VACUUM (FREEZE) and
# VACUUM (ANALYZE), while another rotates through DROP/CREATE INDEX
# CONCURRENTLY, REINDEX INDEX CONCURRENTLY, REINDEX TABLE CONCURRENTLY
# and REPACK (CONCURRENTLY): VACUUM's index cleanup and freezing must
# coexist with the concurrent index rebuilds and tuple movement.
#
# Some reader clients hold a REPEATABLE READ snapshot open across two
# reads: VACUUM must retain whatever those snapshots still need.
# REPACK (CONCURRENTLY) is not MVCC-safe yet, so such a snapshot may
# find the table empty if it spans the swap; that much is tolerated,
# but a non-empty view must be complete and correct, which is what the
# readers check.  Any SQL error or broken invariant aborts pgbench,
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
	'skipping disabled VACUUM stress test');

my $duration = 6 * $stressval;
my $nrows = 10_000;

my $node;

#
# Test set-up
#
$node = stress_init_node('vacuum',
	extra_conf => [ 'autovacuum_naptime = 1s', 'autovacuum_vacuum_scale_factor = 0.0', 'autovacuum_vacuum_threshold = 100', 'autovacuum_vacuum_insert_scale_factor = 0.0', 'autovacuum_vacuum_insert_threshold = 100' ]);
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
	'VACUUM with concurrent CONCURRENTLY commands and DML',
	{
		'concurrent_ops' => stress_ddl_gate(
			indent => "\t\t",
			ddl => [
				[
					'DROP INDEX CONCURRENTLY tbl_val_idx;',
					'CREATE INDEX CONCURRENTLY tbl_val_idx ON tbl(val);',
				],
				'REINDEX INDEX CONCURRENTLY tbl_val_idx;',
				'REINDEX TABLE CONCURRENTLY tbl;',
				'REPACK (CONCURRENTLY) tbl;',
			],
			post =>
				"SELECT bt_index_check('tbl_pkey', heapallindexed => true, checkunique => true);",
		) . "\n" . stress_ddl_gate(
			# A second gate, on its own lock: one client at a time also
			# runs a manual VACUUM alongside the DDL and the writers.
			indent => "\t\t\t",
			lock => 43,
			var => 'gotvac',
			ddl => [
				'VACUUM tbl;', 'VACUUM (FREEZE) tbl;', 'VACUUM (ANALYZE) tbl;',
			],
		) . qq(
				\\set num_a random(1, $nrows)
				\\set num_b random(1, $nrows)
				\\set diff random(1, 10000)
				\\set use_rr random(0, 4)
				\\if :use_rr = 0
					BEGIN ISOLATION LEVEL REPEATABLE READ;
					SELECT COUNT(*) AS cnt, COALESCE(SUM(val), 0) AS sum
						FROM tbl \\gset a_
					\\sleep 5 ms
					SELECT COUNT(*) AS cnt, COALESCE(SUM(val), 0) AS sum
						FROM tbl \\gset b_
					COMMIT;
					-- REPACK (CONCURRENTLY) is not MVCC-safe yet: a
					-- snapshot spanning its swap may find the table
					-- empty.  That is tolerated here, but nothing else
					-- is: a non-empty view must be complete and correct,
					-- and the two reads must agree.
					\\if :a_cnt = 0
						-- Logged rather than ignored, so that it stays
						-- visible whether this tolerance is being
						-- exercised, and how often.
						SELECT 'repack: empty view tolerated' AS marker;
					\\endif
					SELECT stress_assert(:a_cnt = 0
							OR (:a_cnt = $nrows AND :a_sum = $sum),
						format('RR read A: rows=%s sum=%s', :a_cnt, :a_sum));
					SELECT stress_assert(:b_cnt = 0
							OR (:b_cnt = $nrows AND :b_sum = $sum),
						format('RR read B: rows=%s sum=%s', :b_cnt, :b_sum));
				\\else
					BEGIN;
					UPDATE tbl SET val = val + :diff WHERE id = :num_a;
					\\sleep 1 ms
					UPDATE tbl SET val = val - :diff WHERE id = :num_b;
					\\sleep 1 ms
					COMMIT;

					SELECT stress_assert(COALESCE(SUM(val), 0) = $sum,
						format('sum is %s, not $sum', COALESCE(SUM(val), 0)))
						FROM tbl;
				\\endif
			\\endif
		\\endif
	)
	});

$node->safe_psql(
	'postgres', q(
	SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('tbl_val_idx', heapallindexed => true);
));

$node->stop;

done_testing();
