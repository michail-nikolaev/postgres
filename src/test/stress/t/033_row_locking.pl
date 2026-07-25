# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for row locks and EvalPlanQual rechecks across
# CONCURRENTLY commands.
#
# A row lock lives in the tuple it is taken on: SELECT ... FOR UPDATE
# marks the tuple's xmax, and a later locker or updater of the same row
# has to follow the update chain to find the row's current version.
# REPACK (CONCURRENTLY) moves every row to a new relfilenode while such
# locks are held, and REINDEX CONCURRENTLY replaces the index the
# lookups go through, so both have to leave the locks in force.
#
# Writer clients apply balanced pairs of updates (one +diff, one -diff
# in the same transaction), taking the rows they are about to change
# with FOR UPDATE, FOR NO KEY UPDATE or FOR UPDATE SKIP LOCKED first, so
# the sum over the val column is invariant at every commit.  Part of the
# traffic is aimed at a deliberately small set of hot rows, so that
# updates constantly land on rows another transaction has just changed,
# which is what drives the EvalPlanQual rechecks: the recheck re-reads
# the updated row and re-evaluates the qual against it, and its result
# is what ends up in the table.
#
# Reader clients check the sum, and also hold FOR UPDATE locks on a
# range of rows across a pause and then re-read that range in a fresh
# snapshot: nothing may have changed under a held lock, so the two reads
# must agree even though the table was repacked in between.
#
# Every lock is taken in ascending id order, so the clients cannot
# deadlock among themselves; a deadlock report would abort pgbench, as
# would any other SQL error or broken invariant.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled row locking stress test');

my $duration = 6 * $stressval;
my $nrows = 10_000;

# The hot set is small enough that concurrent writers keep colliding on
# it, which is what makes EvalPlanQual rechecks routine rather than rare.
my $nhot = 50;

my $node;

#
# Test set-up
#
$node = stress_init_node('row_locking');
$node->safe_psql(
	'postgres', qq(
	CREATE EXTENSION amcheck;
	CREATE TABLE tbl(id int PRIMARY KEY, val int);
	CREATE INDEX tbl_val_idx ON tbl(val);
	INSERT INTO tbl SELECT g, g FROM generate_series(1, $nrows) g;
));

my $sum = $node->safe_psql('postgres', q(SELECT SUM(val) FROM tbl));

$node->pgbench(
	"--no-vacuum --client=30 --jobs=4 --exit-on-abort -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'row locks and EvalPlanQual across CONCURRENTLY commands',
	{
		'concurrent_ops' => stress_ddl_gate(
			ddl => [
				'REPACK (CONCURRENTLY) tbl;',
				'REPACK (CONCURRENTLY) tbl USING INDEX tbl_val_idx;',
				'REINDEX INDEX CONCURRENTLY tbl_val_idx;',
				'REINDEX TABLE CONCURRENTLY tbl;',
				[
					'DROP INDEX CONCURRENTLY tbl_val_idx;',
					'CREATE INDEX CONCURRENTLY tbl_val_idx ON tbl(val);',
				],
			],
			post =>
			  "SELECT bt_index_check('tbl_pkey', heapallindexed => true, checkunique => true);",
			else => stress_workload(
				mutations => [
					# Lock both rows in one statement, in ascending id
					# order, then change them; the lock is held across a
					# pause, so a CONCURRENTLY command routinely runs
					# while it is in force.
					qq(\\set num_a random(1, $nrows)
					\\set num_b random(1, $nrows)
					\\set lo least(:num_a, :num_b)
					\\set hi greatest(:num_a, :num_b)
					\\set diff random(1, 10000)
					BEGIN;
					SELECT val FROM tbl WHERE id IN (:lo, :hi)
						ORDER BY id FOR UPDATE;
					\\sleep 5 ms
					UPDATE tbl SET val = val + :diff WHERE id = :lo;
					UPDATE tbl SET val = val - :diff WHERE id = :hi;
					COMMIT;),

					# The same against the hot set, with the weaker FOR NO
					# KEY UPDATE, which is the lock strength an ordinary
					# non-key update takes and which several clients can
					# queue on at once.
					qq(\\set num_a random(1, $nhot)
					\\set num_b random(1, $nhot)
					\\set lo least(:num_a, :num_b)
					\\set hi greatest(:num_a, :num_b)
					\\set diff random(1, 100)
					BEGIN;
					SELECT val FROM tbl WHERE id IN (:lo, :hi)
						ORDER BY id FOR NO KEY UPDATE;
					\\sleep 2 ms
					UPDATE tbl SET val = val + :diff WHERE id = :lo;
					UPDATE tbl SET val = val - :diff WHERE id = :hi;
					COMMIT;),

					# Unlocked updates of hot rows, which is where the
					# EvalPlanQual rechecks happen: by the time the update
					# reaches the row, another transaction has usually
					# changed and committed it, so the qual is re-evaluated
					# against that newer version.  The qual is always true,
					# so the update must go through either way and the pair
					# stays balanced.
					qq(\\set num_a random(1, $nhot)
					\\set num_b random(1, $nhot)
					\\set lo least(:num_a, :num_b)
					\\set hi greatest(:num_a, :num_b)
					\\set diff random(1, 100)
					BEGIN;
					UPDATE tbl SET val = val + :diff
						WHERE id = :lo AND val > -2147483000;
					UPDATE tbl SET val = val - :diff
						WHERE id = :hi AND val > -2147483000;
					COMMIT;),

					# SKIP LOCKED takes whatever row of the range is free
					# at that instant.  There may be none, hence the
					# aggregate rather than a bare row; the pair of updates
					# nets to zero on the row it does get.
					qq(\\set lo random(1, $nrows - 9)
					\\set diff random(1, 10000)
					BEGIN;
					SELECT COALESCE(MIN(id), 0) AS id FROM
						(SELECT id FROM tbl WHERE id BETWEEN :lo AND :lo + 9
							ORDER BY id FOR UPDATE SKIP LOCKED) s \\gset skipped_
					\\if :skipped_id > 0
						UPDATE tbl SET val = val + :diff WHERE id = :skipped_id;
						\\sleep 2 ms
						UPDATE tbl SET val = val - :diff WHERE id = :skipped_id;
					\\endif
					COMMIT;),
				],
				checks => [
					# Nothing may change under a held row lock.  The second
					# read runs in a fresh snapshot (this transaction is
					# READ COMMITTED), so it would see any concurrent commit
					# touching the range -- and there must be none, however
					# the table was rewritten in between.
					qq(\\set lo random(1, $nrows - 4)
					BEGIN;
					SELECT COUNT(*) AS cnt, COALESCE(SUM(val), 0) AS sum FROM
						(SELECT val FROM tbl WHERE id BETWEEN :lo AND :lo + 4
							ORDER BY id FOR UPDATE) s \\gset locked_
					\\sleep 20 ms
					SELECT COUNT(*) AS cnt, COALESCE(SUM(val), 0) AS sum
						FROM tbl WHERE id BETWEEN :lo AND :lo + 4 \\gset reread_
					COMMIT;
					-- REPACK (CONCURRENTLY) is not MVCC-safe yet: a read
					-- that spans its swap may find the table empty.  That
					-- much is tolerated; anything else must match what was
					-- locked.
					\\if :reread_cnt = 0
						SELECT 'repack: empty view tolerated' AS marker;
					\\endif
					SELECT stress_assert(:reread_cnt = 0
							OR (:locked_cnt = :reread_cnt
								AND :locked_sum = :reread_sum),
						format('rows %s..%s changed under a held lock: locked (%s rows, sum %s), re-read (%s rows, sum %s)',
							:lo, :lo + 4, :locked_cnt, :locked_sum,
							:reread_cnt, :reread_sum));),

					qq(SELECT stress_assert(cnt = 0 OR (cnt = $nrows AND sum = $sum),
						format('rows=%s sum=%s (want 0, or $nrows rows sum $sum)',
							cnt, sum))
					FROM (SELECT COUNT(*) AS cnt, COALESCE(SUM(val), 0) AS sum
						FROM tbl) x;),
				],
			),
		),
	});

is($node->safe_psql('postgres', q(SELECT SUM(val) FROM tbl)),
	$sum, 'sum invariant holds after row-locking churn');
is($node->safe_psql('postgres', q(SELECT COUNT(*) FROM tbl)),
	$nrows, 'no rows lost');

$node->safe_psql(
	'postgres', q(
	SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('tbl_val_idx', heapallindexed => true);
));

$node->stop;

done_testing();
