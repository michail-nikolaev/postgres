# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for subtransactions racing with CONCURRENTLY commands.
#
# Subtransactions complicate every mechanism these commands rely on.  A
# backend caches only PGPROC_MAX_CACHED_SUBXIDS (64) subtransaction ids
# in shared memory; past that its snapshot entry is marked overflowed,
# and visibility checks for it have to go to pg_subtrans instead.  The
# CONCURRENTLY commands wait for concurrent transactions using exactly
# those snapshots, and REPACK (CONCURRENTLY) decodes the table's own
# changes, so it has to reassemble each subtransaction into its parent
# and throw away the ones that aborted.
#
# Writer clients therefore apply their balanced pairs of updates (one
# +diff, one -diff, so the sum over the val column is invariant at every
# commit) in several shapes:
#
# - with savepoints, including ones that are rolled back after making a
#   change that must not survive,
# - from a PL/pgSQL function whose loop body is an exception block --
#   that is a subtransaction per iteration, and with more than 64
#   iterations the transaction's subxid cache overflows, all but the
#   last iteration being rolled back, and
# - as a whole transaction that does all that and then rolls back, which
#   must leave nothing at all behind.
#
# Meanwhile one client rotates through REPACK (CONCURRENTLY), REINDEX
# CONCURRENTLY and DROP/CREATE INDEX CONCURRENTLY.  Readers check the
# sum; any SQL error or broken invariant aborts pgbench, failing the
# test.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled subtransaction stress test');

my $duration = 6 * $stressval;
my $nrows = 10_000;

# Comfortably more than PGPROC_MAX_CACHED_SUBXIDS, so that a transaction
# calling the function below shows up as overflowed in other backends'
# snapshots.
my $nsubxacts = 80;

my $node;

#
# Test set-up
#
$node = stress_init_node('subtransactions');
$node->safe_psql(
	'postgres', qq(
	CREATE EXTENSION amcheck;
	CREATE TABLE tbl(id int PRIMARY KEY, val int);
	CREATE INDEX tbl_val_idx ON tbl(val);
	INSERT INTO tbl SELECT g, g FROM generate_series(1, $nrows) g;
));

# Each iteration of the loop is a subtransaction, because a PL/pgSQL
# block with an EXCEPTION clause is one.  All but the last iteration
# raise, and so are rolled back; the net effect of the call is a single
# balanced pair of updates.  The ids are taken in ascending order by the
# caller, so concurrent callers cannot deadlock.
$node->safe_psql(
	'postgres', q(
	CREATE FUNCTION subxact_churn(lo int, hi int, diff int, n int)
	RETURNS void LANGUAGE plpgsql AS $$
	DECLARE
		i int;
	BEGIN
		FOR i IN 1 .. n LOOP
			BEGIN
				UPDATE tbl SET val = val + diff WHERE id = lo;
				UPDATE tbl SET val = val - diff WHERE id = hi;
				IF i < n THEN
					RAISE EXCEPTION 'discarding subtransaction %', i;
				END IF;
			EXCEPTION WHEN raise_exception THEN
				NULL;
			END;
		END LOOP;
	END;
	$$;
));

my $sum = $node->safe_psql('postgres', q(SELECT SUM(val) FROM tbl));

$node->pgbench(
	"--no-vacuum --client=30 --jobs=4 --exit-on-abort -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'subtransactions with concurrent CONCURRENTLY commands',
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
					# Savepoints by hand: a change that is rolled back, the
					# same change again for real, and a large one at the end
					# that must not survive either.
					qq(\\set num_a random(1, $nrows)
					\\set num_b random(1, $nrows)
					\\set lo least(:num_a, :num_b)
					\\set hi greatest(:num_a, :num_b)
					\\set diff random(1, 10000)
					BEGIN;
					SAVEPOINT sp1;
					UPDATE tbl SET val = val + :diff WHERE id = :lo;
					ROLLBACK TO SAVEPOINT sp1;
					UPDATE tbl SET val = val + :diff WHERE id = :lo;
					SAVEPOINT sp2;
					UPDATE tbl SET val = val - :diff WHERE id = :hi;
					RELEASE SAVEPOINT sp2;
					SAVEPOINT sp3;
					UPDATE tbl SET val = val + 1000000 WHERE id = :lo;
					\\sleep 2 ms
					ROLLBACK TO SAVEPOINT sp3;
					COMMIT;),

					# More than 64 subtransactions in one transaction, so
					# the subxid cache overflows while a CONCURRENTLY
					# command is waiting on it.
					qq(\\set num_a random(1, $nrows)
					\\set num_b random(1, $nrows)
					\\set lo least(:num_a, :num_b)
					\\set hi greatest(:num_a, :num_b)
					\\set diff random(1, 10000)
					SELECT subxact_churn(:lo, :hi, :diff, $nsubxacts);),

					# The same, then thrown away entirely: neither the
					# aborted subtransactions nor the surviving one may
					# leave a trace.
					qq(\\set num_a random(1, $nrows)
					\\set num_b random(1, $nrows)
					\\set lo least(:num_a, :num_b)
					\\set hi greatest(:num_a, :num_b)
					\\set diff random(1, 10000)
					BEGIN;
					SELECT subxact_churn(:lo, :hi, :diff, $nsubxacts);
					SAVEPOINT sp1;
					UPDATE tbl SET val = val - 1000000 WHERE id = :lo;
					\\sleep 2 ms
					ROLLBACK;),
				],
				checks => [
					qq(-- REPACK (CONCURRENTLY) is not MVCC-safe yet, so a
					-- read spanning its swap may find the table empty;
					-- that is tolerated, nothing else is.
					SELECT stress_assert(cnt = 0 OR (cnt = $nrows AND sum = $sum),
						format('rows=%s sum=%s (want 0, or $nrows rows sum $sum)',
							cnt, sum))
					FROM (SELECT COUNT(*) AS cnt, COALESCE(SUM(val), 0) AS sum
						FROM tbl) x;),

					# A reader that is itself deep in a subtransaction, and
					# whose own uncommitted change is visible to it and to
					# nobody else.
					qq(\\set num_a random(1, $nrows)
					\\set diff random(1, 10000)
					BEGIN;
					SAVEPOINT sp1;
					UPDATE tbl SET val = val + :diff WHERE id = :num_a;
					SELECT COUNT(*) AS cnt, COALESCE(SUM(val), 0) AS sum
						FROM tbl \\gset own_
					ROLLBACK TO SAVEPOINT sp1;
					SELECT COUNT(*) AS cnt, COALESCE(SUM(val), 0) AS sum
						FROM tbl \\gset undone_
					COMMIT;
					SELECT stress_assert(:own_cnt = 0
							OR :own_sum = $sum + :diff,
						format('own uncommitted change: rows=%s sum=%s',
							:own_cnt, :own_sum));
					SELECT stress_assert(:undone_cnt = 0
							OR :undone_sum = $sum,
						format('after rollback to savepoint: rows=%s sum=%s',
							:undone_cnt, :undone_sum));),
				],
			),
		),
	});

is($node->safe_psql('postgres', q(SELECT SUM(val) FROM tbl)),
	$sum, 'sum invariant holds after subtransaction churn');
is($node->safe_psql('postgres', q(SELECT COUNT(*) FROM tbl)),
	$nrows, 'no rows lost');

$node->safe_psql(
	'postgres', q(
	SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('tbl_val_idx', heapallindexed => true);
));

$node->stop;

done_testing();
