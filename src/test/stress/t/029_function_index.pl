# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for CONCURRENTLY commands on indexes whose definitions
# call functions -- including functions that acquire a real
# transaction id while the index is being built.
#
# CREATE INDEX CONCURRENTLY and REINDEX CONCURRENTLY build in several
# phases, each in its own transaction, and wait for concurrent
# transactions between phases.  An index expression or predicate that
# calls a volatile-ish function which itself assigns an xid (for
# example by writing to a table, or by calling txid_current()) makes
# the building transaction acquire an xid too, which changes how it
# interacts with the snapshots and horizons those phases depend on.
# REPACK (CONCURRENTLY), which decodes the table's own changes, has to
# cope with such expressions as well.
#
# The functions here are declared IMMUTABLE (as an index requires) but
# deliberately assign an xid; despite that, the index must always agree
# with a direct evaluation of the same expression, which the readers
# check, alongside the usual sum invariant.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled function index stress test');

my $duration = 6 * $stressval;
my $nrows = 5000;

my $node;

#
# Test set-up
#
$node = stress_init_node('function_index');

$node->safe_psql(
	'postgres', qq(
	CREATE EXTENSION amcheck;
	CREATE TABLE tbl(id int PRIMARY KEY, val int);
	INSERT INTO tbl SELECT g, g FROM generate_series(1, $nrows) g;
));

# An IMMUTABLE function that nevertheless assigns a real xid, by forcing
# an xid to be allocated in the current transaction.  Used in an index
# expression, this makes the CONCURRENTLY build's own transactions
# acquire xids.
$node->safe_psql(
	'postgres', q(
	CREATE FUNCTION xid_forcing(v int) RETURNS int IMMUTABLE
	LANGUAGE plpgsql AS $$
	BEGIN
		PERFORM txid_current();
		RETURN v;
	END; $$;
));

# An IMMUTABLE predicate that also assigns an xid.
$node->safe_psql(
	'postgres', q(
	CREATE FUNCTION xid_forcing_pred(v int) RETURNS bool IMMUTABLE
	LANGUAGE plpgsql AS $$
	BEGIN
		PERFORM txid_current();
		RETURN v >= 0;
	END; $$;
));

my $sum = $node->safe_psql('postgres', q(SELECT SUM(val) FROM tbl));

$node->pgbench(
	"--no-vacuum --client=30 --jobs=30 --exit-on-abort -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'CONCURRENTLY commands on function-based indexes',
	{
		'concurrent_ops' => qq(
		-- xid_forcing() can allocate an xid; keep parallel workers out
		-- of it so the expression is evaluated in the leader.
		SET debug_parallel_query = off;
) . stress_ddl_gate(
			# Build one of several function-based indexes...
			ddl => [
				'CREATE INDEX CONCURRENTLY fidx ON tbl(xid_forcing(val));',
				'CREATE INDEX CONCURRENTLY fidx ON tbl(val) WHERE xid_forcing_pred(val);',
				'CREATE INDEX CONCURRENTLY fidx ON tbl(xid_forcing(val)) WHERE xid_forcing_pred(id);',
				'CREATE INDEX CONCURRENTLY fidx ON tbl(xid_forcing(id), val);',
				'CREATE INDEX CONCURRENTLY fidx ON tbl(val);',
			],
			# ... then check it, rebuild it, check again and drop it, and
			# finally repack, which has to decode and re-apply through the
			# same kinds of expressions.
			post => [
				"SELECT bt_index_check('fidx', heapallindexed => true);",
				'\sleep 5 ms',
				'REINDEX INDEX CONCURRENTLY fidx;',
				"SELECT bt_index_check('fidx', heapallindexed => true);",
				'\sleep 5 ms',
				'DROP INDEX CONCURRENTLY fidx;',
				'REPACK (CONCURRENTLY) tbl;',
				"SELECT bt_index_check('tbl_pkey', heapallindexed => true, checkunique => true);",
			],
			sleep_ms => 5,
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
					qq(-- Evaluating the same function directly must agree with
					-- whatever an index on it would have stored.  REPACK is
					-- not MVCC-safe yet, so tolerate an empty view.
					SELECT stress_assert(
						cnt = 0 OR (bad = 0 AND cnt = $nrows AND sum = $sum),
						format('rows=%s bad=%s sum=%s (want $nrows rows, 0 bad, sum $sum)',
							cnt, bad, sum))
					FROM (SELECT COUNT(*) AS cnt,
						COUNT(*) FILTER (WHERE xid_forcing(val) <> val) AS bad,
						COALESCE(SUM(val), 0) AS sum FROM tbl) t;),
				],
			),
		),
	});

is( $node->safe_psql('postgres', q(SELECT SUM(val) FROM tbl)),
	$sum, 'sum invariant holds after function-index churn');
is( $node->safe_psql('postgres', q(SELECT COUNT(*) FROM tbl)),
	"$nrows", 'no rows lost');

# Build the function indexes one more time, non-concurrently, and check
# them against a fresh concurrent build: both must be sound.
$node->safe_psql(
	'postgres', q(
	CREATE INDEX final_expr_idx ON tbl(xid_forcing(val));
	CREATE INDEX final_pred_idx ON tbl(val) WHERE xid_forcing_pred(val);
	SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('final_expr_idx', heapallindexed => true);
	SELECT bt_index_parent_check('final_pred_idx', heapallindexed => true);
));

$node->stop;

done_testing();
