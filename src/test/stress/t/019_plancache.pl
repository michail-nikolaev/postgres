# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for cached plans across CONCURRENTLY commands.
#
# Cached plans reference the indexes they were planned against, so
# dropping, rebuilding or repacking those indexes has to invalidate
# them.  This test keeps plans cached in three different ways at once:
#
# - pgbench runs in --protocol=prepared mode, so each client's
#   statements are prepared once per connection and re-executed
#   afterwards,
# - plan_cache_mode = force_generic_plan makes the server keep the
#   generic plans rather than re-planning per execution, and
# - the workload also goes through a PL/pgSQL function, whose plans are
#   cached in its own plan cache across calls.
#
# The writers keep the sum over the val column invariant, and every
# read -- direct or through the function -- must agree with it, so a
# stale plan reading a dropped or half-built index shows up as a wrong
# answer or an error, either of which aborts pgbench.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled plan cache stress test');

my $duration = 6 * $stressval;
my $nrows = 10_000;

my $node;

#
# Test set-up
#
$node = stress_init_node('plancache',
	extra_conf => [ 'plan_cache_mode = force_generic_plan', 'log_lock_waits = on' ]);
$node->safe_psql(
	'postgres', qq(
	CREATE EXTENSION amcheck;
	CREATE TABLE tbl(id int PRIMARY KEY, val int);
	CREATE INDEX tbl_val_idx ON tbl(val);
	INSERT INTO tbl SELECT g, g FROM generate_series(1, $nrows) g;
));

# A PL/pgSQL function caches the plans of the statements it runs.
$node->safe_psql(
	'postgres', q(
	CREATE FUNCTION lookup_sum() RETURNS bigint
	LANGUAGE plpgsql AS $$
	DECLARE
		s bigint;
	BEGIN
		SELECT COALESCE(SUM(val), 0) INTO s FROM tbl;
		RETURN s;
	END; $$;
));

$node->safe_psql(
	'postgres', q(
	CREATE FUNCTION lookup_one(key int) RETURNS int
	LANGUAGE plpgsql AS $$
	DECLARE
		v int;
	BEGIN
		SELECT val INTO v FROM tbl WHERE id = key;
		RETURN v;
	END; $$;
));

my $sum = $node->safe_psql('postgres',
	q(SELECT SUM(val) AS sum FROM tbl));

# --protocol=prepared keeps the statements prepared per connection.
#
# Note that each client needs its own thread here.  pgbench prepares a
# statement synchronously the first time a client reaches it, which
# blocks the whole thread; if that PREPARE queues behind the
# AccessExclusiveLock that a concurrent REPACK or REINDEX is waiting
# for, any sibling client sharing the thread stops too -- including one
# sitting in an open transaction, whose locks then keep the DDL waiting
# until it times out.  That is a pgbench-side artifact, not a server
# problem, and one thread per client avoids it.
$node->pgbench(
	"--no-vacuum --client=20 --jobs=20 --exit-on-abort --protocol=prepared -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'cached plans across CONCURRENTLY commands',
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
					qq(-- Read through the prepared statement plan.
					SELECT stress_assert(COALESCE(SUM(val), 0) = $sum,
						format('sum is %s, not $sum', COALESCE(SUM(val), 0)))
						FROM tbl;),
					qq(-- Read through the function's own plan cache, and do an
					-- index scan (on the index being rebuilt) through another.
					\\set num_a random(1, $nrows)
					SELECT stress_assert(lookup_sum() = $sum,
						format('lookup_sum is %s, not $sum', lookup_sum()));
					SELECT stress_assert(lookup_one(:num_a) IS NOT NULL,
						'lookup_one found no row for an id that always exists');),
				],
			),
		),
	});

is( $node->safe_psql('postgres', q(SELECT SUM(val) FROM tbl)),
	$sum, 'sum invariant holds after plan cache churn');

is( $node->safe_psql('postgres', q(SELECT lookup_sum())),
	$sum, 'function plan cache still returns the right answer');

$node->safe_psql(
	'postgres', q(
	SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('tbl_val_idx', heapallindexed => true);
));

$node->stop;

done_testing();
