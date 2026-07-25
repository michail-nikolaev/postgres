# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for cursors held open across CONCURRENTLY commands.
#
# A cursor keeps the snapshot it was opened with, and a WITH HOLD
# cursor keeps its materialized result across the commit that created
# it.  Either way, whatever the cursor reads must stay readable and
# consistent while the indexes it may be scanning are dropped, rebuilt
# or reindexed underneath it.
#
# Both kinds of cursor are driven from PL/pgSQL so that the whole
# scan-with-a-pause-in-the-middle happens inside a single call: the
# routines add up the val column across the pause and raise an
# exception if the total does not match the invariant that the writer
# clients maintain, which aborts pgbench.
#
# REPACK (CONCURRENTLY) is part of the rotation even though it is
# currently known not to be MVCC-safe.  What it may do to a snapshot
# that spans its swap is make the table look empty; what it must never
# do is show a partial or otherwise wrong view.  The routines below
# therefore tolerate reading no rows at all, but hold anything
# non-empty to the full invariant.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled cursor stress test');

my $duration = 6 * $stressval;

# Modest row count: each cursor call walks the whole table row by row.
my $nrows = 2000;

my $node;

#
# Test set-up
#
$node = stress_init_node('cursors');
$node->safe_psql(
	'postgres', qq(
	CREATE EXTENSION amcheck;
	CREATE TABLE tbl(id int PRIMARY KEY, val int);
	CREATE INDEX tbl_val_idx ON tbl(val);
	INSERT INTO tbl SELECT g, g FROM generate_series(1, $nrows) g;
));

my $sum = $node->safe_psql('postgres',
	q(SELECT SUM(val) AS sum FROM tbl));

# Walk a plain cursor, pausing in the middle so that concurrent DDL
# lands while the cursor is open.  The snapshot is taken when the
# cursor is opened, so the total must match the invariant.
$node->safe_psql(
	'postgres', q(
	CREATE FUNCTION cursor_check(expected bigint, expected_rows bigint)
	RETURNS void
	LANGUAGE plpgsql AS $$
	DECLARE
		c CURSOR FOR SELECT val FROM tbl ORDER BY id;
		v int;
		s bigint := 0;
		n bigint := 0;
	BEGIN
		OPEN c;
		LOOP
			FETCH c INTO v;
			EXIT WHEN NOT FOUND;
			s := s + v;
			n := n + 1;
			IF n = 100 THEN
				PERFORM pg_sleep(0.01);
			END IF;
		END LOOP;
		CLOSE c;
		-- REPACK (CONCURRENTLY) is not MVCC-safe yet, so a snapshot
		-- spanning its swap may find no rows at all; anything else must
		-- be complete and correct.  Log it, so that it stays visible
		-- whether this tolerance is actually being exercised.
		IF n = 0 THEN
			RAISE LOG 'repack: empty view tolerated by cursor_check';
			RETURN;
		END IF;
		IF n <> expected_rows OR s <> expected THEN
			RAISE EXCEPTION 'cursor saw % rows summing to %, expected % rows summing to %',
				n, s, expected_rows, expected;
		END IF;
	END; $$;
));

# Same, but with a WITH HOLD cursor: it is materialized by the COMMIT
# and then read after concurrent DDL has had a chance to run.
$node->safe_psql(
	'postgres', q(
	CREATE PROCEDURE hold_cursor_check(expected bigint, expected_rows bigint)
	LANGUAGE plpgsql AS $$
	DECLARE
		r record;
		s bigint := 0;
		n bigint := 0;
	BEGIN
		EXECUTE 'CLOSE ALL';
		EXECUTE 'DECLARE hc CURSOR WITH HOLD FOR SELECT val FROM tbl';
		COMMIT;
		PERFORM pg_sleep(0.01);
		FOR r IN EXECUTE 'FETCH ALL FROM hc' LOOP
			s := s + r.val;
			n := n + 1;
		END LOOP;
		EXECUTE 'CLOSE hc';
		-- See cursor_check() for why an empty result is tolerated.
		IF n = 0 THEN
			RAISE LOG 'repack: empty view tolerated by hold_cursor_check';
			RETURN;
		END IF;
		IF n <> expected_rows OR s <> expected THEN
			RAISE EXCEPTION 'held cursor saw % rows summing to %, expected % rows summing to %',
				n, s, expected_rows, expected;
		END IF;
	END; $$;
));

$node->pgbench(
	"--no-vacuum --client=20 --jobs=4 --exit-on-abort -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'cursors held across CONCURRENTLY commands',
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
			else => stress_workload(
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
					# These plpgsql routines walk a cursor with a pause in the
					# middle and raise their own descriptive error on mismatch.
					qq(SELECT cursor_check($sum, $nrows);),
					qq(CALL hold_cursor_check($sum, $nrows);),
				],
			),
		),
	});

is( $node->safe_psql('postgres', q(SELECT SUM(val) FROM tbl)),
	$sum, 'sum invariant holds after cursor churn');

# No cursor may have been left behind.
is( $node->safe_psql('postgres', q(SELECT COUNT(*) FROM pg_cursors)),
	'0', 'no cursors left open');

$node->safe_psql(
	'postgres', q(
	SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('tbl_val_idx', heapallindexed => true);
));

$node->stop;

done_testing();
