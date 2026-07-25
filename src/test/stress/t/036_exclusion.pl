# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for exclusion constraints across CONCURRENTLY commands.
#
# An exclusion constraint is enforced at run time rather than by the
# index itself: after inserting the index tuple, the executor scans the
# constraint's index for a conflicting row, and waits on the inserting
# transaction of any row it finds that is still in progress.  It is
# therefore much more exposed to an index being replaced underneath it
# than a plain unique index is -- the scan has to find rows that were
# inserted through whichever index was current when the other
# transaction ran.
#
# Writer clients keep one row per slot: they delete a slot and insert it
# again in the same transaction, apply balanced pairs of updates so the
# sum over the val column stays put, and constantly try to insert
# duplicate slots, which must always be rejected.  The rejection is what
# is really under test: a duplicate that gets in means the constraint
# stopped being enforced while its index was being rebuilt, and both the
# insert's own check and the readers' distinct-slot count would then
# notice it.
#
# REPACK (CONCURRENTLY) does handle such a table: it gives the transient
# copy its own copies of the constraints, precisely so that the executor
# keeps enforcing them while the rows are being moved -- so the rotation
# leans on it, and the duplicate inserts above are what checks that the
# enforcement really did survive.  The constraint's own index is the one
# thing REINDEX will not rebuild concurrently: REINDEX INDEX
# CONCURRENTLY on it is an error, and REINDEX TABLE CONCURRENTLY warns
# and skips it.  Both are pinned at the end of this file, so that they
# are noticed if they ever change.
#
# The rotation therefore drives REPACK (CONCURRENTLY), DROP/CREATE INDEX
# CONCURRENTLY on a secondary index, REINDEX TABLE CONCURRENTLY (which
# rebuilds the other indexes and skips this one), and the blocking
# rebuilds -- REINDEX and REPACK without CONCURRENTLY -- which take the
# table away under AccessExclusiveLock and hand it back with every index
# replaced.  Any unexpected SQL error or broken invariant aborts
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
	'skipping disabled exclusion constraint stress test');

my $duration = 6 * $stressval;

# Slots up to $nslots hold the value the writers move around; the ones
# above it are deleted and re-inserted, and carry val = 0 so that they
# do not enter into the sum.  Keeping the two apart matters: an update
# of a row another client is deleting would match nothing, which would
# break the invariant with no bug involved.
my $nslots = 2000;
my $nchurn = 200;
my $ntotal = $nslots + $nchurn;

my $node;

#
# Test set-up
#
$node = stress_init_node('exclusion');
$node->safe_psql(
	'postgres', qq(
	CREATE EXTENSION amcheck;
	-- The constraint is written over a range so that it needs nothing
	-- but the built-in GiST opclasses, and so that the index is built
	-- from an expression rather than a plain column.
	CREATE TABLE tbl(id serial PRIMARY KEY, slot int NOT NULL, val int,
		CONSTRAINT tbl_slot_excl
			EXCLUDE USING gist (int4range(slot, slot + 1) WITH &&));
	CREATE INDEX tbl_val_idx ON tbl(val);
	INSERT INTO tbl(slot, val) SELECT g, g FROM generate_series(1, $nslots) g;
	INSERT INTO tbl(slot, val)
		SELECT g, 0 FROM generate_series(@{[ $nslots + 1 ]}, $ntotal) g;
));

# An insert that hits the exclusion constraint is an expected outcome
# here, not a failure, so it is caught and reported back as a value the
# workload can assert on.
$node->safe_psql(
	'postgres', q(
	CREATE FUNCTION try_insert(p_slot int, p_val int) RETURNS boolean
	LANGUAGE plpgsql AS $$
	BEGIN
		INSERT INTO tbl(slot, val) VALUES (p_slot, p_val);
		RETURN true;
	EXCEPTION WHEN exclusion_violation THEN
		RETURN false;
	END;
	$$;
));

my $sum = $node->safe_psql('postgres', q(SELECT SUM(val) FROM tbl));

$node->pgbench(
	"--no-vacuum --client=30 --jobs=4 --exit-on-abort -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'exclusion constraints across CONCURRENTLY commands',
	{
		'concurrent_ops' => stress_ddl_gate(
			ddl => [
				'REPACK (CONCURRENTLY) tbl;',
				[
					'DROP INDEX CONCURRENTLY tbl_val_idx;',
					'CREATE INDEX CONCURRENTLY tbl_val_idx ON tbl(val);',
				],
				[
					# Rebuilds the other indexes and skips the constraint's
					# own, which it announces with a WARNING; that would go
					# to pgbench's stderr, where the test insists on
					# silence, so keep it quiet here.
					'SET client_min_messages = error;',
					'REINDEX TABLE CONCURRENTLY tbl;',
					'RESET client_min_messages;',
				],
				'REINDEX INDEX tbl_slot_excl;',
				'REINDEX TABLE tbl;',
				'REPACK tbl;',
			],
			post =>
			  "SELECT bt_index_check('tbl_pkey', heapallindexed => true, checkunique => true);",
			else => stress_workload(
				mutations => [
					# Every slot in the stable range is occupied, so an
					# insert into one of them must be rejected -- however
					# the constraint's index is being rebuilt at the time.
					qq(\\set slot random(1, $nslots)
					SELECT stress_assert(NOT try_insert(:slot, 0),
						format('duplicate slot %s was accepted', :slot));),

					# Balanced pair of updates, in slot order so that
					# concurrent writers cannot deadlock.
					qq(\\set slot_a random(1, $nslots)
					\\set slot_b random(1, $nslots)
					\\set lo least(:slot_a, :slot_b)
					\\set hi greatest(:slot_a, :slot_b)
					\\set diff random(1, 10000)
					BEGIN;
					UPDATE tbl SET val = val + :diff WHERE slot = :lo;
					\\sleep 1 ms
					UPDATE tbl SET val = val - :diff WHERE slot = :hi;
					COMMIT;),

					# Free a slot and take it again in the same
					# transaction: the row is gone only for as long as the
					# transaction runs, and the insert has to re-check the
					# constraint against whatever index is current.  Another
					# client may have got there first, in which case the
					# insert is simply rejected and the slot stays occupied.
					qq(\\set slot random(@{[ $nslots + 1 ]}, $ntotal)
					BEGIN;
					DELETE FROM tbl WHERE slot = :slot;
					\\sleep 1 ms
					SELECT try_insert(:slot, 0);
					COMMIT;),
				],
				checks => [
					qq(-- One row per slot, and the sum where the writers
					-- left it.  REPACK (CONCURRENTLY) is not MVCC-safe
					-- yet, so an empty read is tolerated; nothing else is.
					SELECT stress_assert(cnt = 0
							OR (cnt = $ntotal AND slots = $ntotal AND sum = $sum),
						format('rows=%s distinct slots=%s sum=%s (want $ntotal, $ntotal, $sum)',
							cnt, slots, sum))
					FROM (SELECT COUNT(*) AS cnt, COUNT(DISTINCT slot) AS slots,
							COALESCE(SUM(val), 0) AS sum FROM tbl) x;),
				],
			),
		),
	});

is($node->safe_psql('postgres', q(SELECT COUNT(*) FROM tbl)),
	$ntotal, 'one row per slot survived');
is( $node->safe_psql('postgres', q(SELECT COUNT(DISTINCT slot) FROM tbl)),
	$ntotal, 'no duplicate slot got past the exclusion constraint');
is($node->safe_psql('postgres', q(SELECT SUM(val) FROM tbl)),
	$sum, 'sum invariant holds after exclusion constraint churn');

# The constraint must still be enforced, and still backed by a working
# index, after all the rebuilds.
my ($ret, $out, $err) =
  $node->psql('postgres', q(INSERT INTO tbl(slot, val) VALUES (1, 0)));
isnt($ret, 0, 'exclusion constraint still rejects a duplicate slot');
like($err, qr/conflicting key value violates exclusion constraint/,
	'and rejects it as an exclusion violation');

# Pin the limitation the rotation above had to work around: an
# exclusion constraint's index is the one index REINDEX will not rebuild
# concurrently.  Asked for it directly, it refuses; asked for the whole
# table, it says so and rebuilds the rest.
($ret, $out, $err) =
  $node->psql('postgres', q(REINDEX INDEX CONCURRENTLY tbl_slot_excl));
isnt($ret, 0,
	'REINDEX INDEX CONCURRENTLY still refuses an exclusion constraint index');
like(
	$err,
	qr/concurrent index creation for exclusion constraints is not supported/,
	'and says why');

($ret, $out, $err) = $node->psql('postgres', q(REINDEX TABLE CONCURRENTLY tbl));
is($ret, 0, 'REINDEX TABLE CONCURRENTLY still rebuilds the rest of them');
like(
	$err,
	qr/cannot reindex exclusion constraint index .* concurrently, skipping/,
	'and skips that one with a warning');

$node->safe_psql(
	'postgres', q(
	SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('tbl_val_idx', heapallindexed => true);
));

$node->stop;

done_testing();
