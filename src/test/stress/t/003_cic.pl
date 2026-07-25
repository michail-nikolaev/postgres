# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for CREATE INDEX CONCURRENTLY, REINDEX (INDEX|TABLE)
# CONCURRENTLY and DROP INDEX CONCURRENTLY on btree indexes, running
# concurrently with upserts.
#
# The indexes are built with various options (plain, expression and
# partial ones, with both stable and volatile-ish predicates), with and
# without parallel workers, and under both read-committed and
# repeatable-read default isolation levels.  Every index built is
# verified with amcheck; any SQL error aborts pgbench, failing the test.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled CREATE INDEX CONCURRENTLY stress test');

Test::More->builder->todo_start('filesystem bug')
  if PostgreSQL::Test::Utils::has_wal_read_bug;

# This file runs two pgbench phases, so give each one half of the
# calibrated total duration.
my $duration = 3 * $stressval;
my $max_sleep_ms = 10;
my $no_hot = int(rand(2));
my $pgbench_options =
  "--no-vacuum --client=30 --jobs=4 --exit-on-abort -T $duration";

my $node;

#
# Test set-up
#
$node = stress_init_node('cic_stress',
	extra_conf => [ 'maintenance_work_mem = 32MB', 'shared_buffers = 32MB' ]);
$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));
$node->safe_psql('postgres', q(CREATE UNLOGGED TABLE tbl(i int primary key,
								c1 money default 0, c2 money default 0,
								c3 money default 0, updated_at timestamp,
								ia int4[], p point)));

if ($no_hot)
{
	$node->safe_psql('postgres',
		q(CREATE INDEX CONCURRENTLY idx ON tbl(i, updated_at);));
}

# The in_row_rebuild sequence is used to skip index rebuilds while no
# concurrent modifications are happening: writers reset it, the DDL
# client increments it and stops rebuilding after a few unaccompanied
# iterations.
$node->safe_psql('postgres',
	q(CREATE UNLOGGED SEQUENCE in_row_rebuild START 1 INCREMENT 1;));
$node->safe_psql('postgres', q(SELECT nextval('in_row_rebuild');));

# Create helper functions for predicate tests
$node->safe_psql(
	'postgres', q(
	CREATE FUNCTION predicate_stable() RETURNS bool IMMUTABLE
	LANGUAGE plpgsql AS $$
	BEGIN
		EXECUTE 'SELECT txid_current()';
		RETURN true;
	END; $$;
));

$node->safe_psql(
	'postgres', q(
	CREATE FUNCTION predicate_const(integer) RETURNS bool IMMUTABLE
	LANGUAGE plpgsql AS $$
	BEGIN
		RETURN MOD($1, 2) = 0;
	END; $$;
));

# Run CIC/RIC in different options concurrently with upserts
$node->pgbench(
	$pgbench_options,
	0,
	[qr{actually processed}],
	[qr{^$}],
	'concurrent operations with REINDEX/CREATE INDEX CONCURRENTLY',
	{
		'concurrent_ops' => qq(
		SET debug_parallel_query = off; -- this is because predicate_stable implementation
) . stress_ddl_gate(
			var => 'gotlock',
			sleep_ms => 0,
			ddl => [
				[
					"SELECT nextval('in_row_rebuild') AS last_value \\gset",
					'\set parallels random(0, 4)',
					'\set use_rr random(0, 9)',
					'\set reindex_table random(0, 4)',
					'\if :last_value < 3',
					"\tALTER TABLE tbl SET (parallel_workers=:parallels);",
					"\t\\if :use_rr = 0",
					"\t\tSET default_transaction_isolation = 'repeatable read';",
					"\t\\endif",
					split(
						/\n/,
						stress_variant_switch(
							var => 'variant',
							indent => "\t",
							variants => [
								'CREATE INDEX CONCURRENTLY new_idx ON tbl(i, updated_at);',
								'CREATE INDEX CONCURRENTLY new_idx ON tbl(i, updated_at) WHERE predicate_stable();',
								'CREATE INDEX CONCURRENTLY new_idx ON tbl(i, updated_at) WHERE MOD(i, 2) = 0;',
								'CREATE INDEX CONCURRENTLY new_idx ON tbl(i, updated_at) WHERE predicate_const(i);',
								'CREATE INDEX CONCURRENTLY new_idx ON tbl(predicate_const(i));',
								'CREATE INDEX CONCURRENTLY new_idx ON tbl(i, predicate_const(i), updated_at) WHERE predicate_const(i);',
							])),
					"\t\\set sleep_ms random(0, $max_sleep_ms)",
					"\t\\sleep :sleep_ms ms",
					"\tSELECT bt_index_check('new_idx', heapallindexed => true, checkunique => true);",
					"\t\\if :reindex_table = 0",
					"\t\tREINDEX TABLE CONCURRENTLY tbl;",
					"\t\tSELECT bt_index_check('tbl_pkey', heapallindexed => true, checkunique => true);",
					"\t\\else",
					"\t\tREINDEX INDEX CONCURRENTLY new_idx;",
					"\t\\endif",
					"\t\\set sleep_ms random(0, $max_sleep_ms)",
					"\t\\sleep :sleep_ms ms",
					"\tSELECT bt_index_check('new_idx', heapallindexed => true, checkunique => true);",
					"\tDROP INDEX CONCURRENTLY new_idx;",
					"\tRESET default_transaction_isolation;",
					'\endif',
				],
			],
			else => qq(
				\\set num random(1000, 100000)
				BEGIN;
				INSERT INTO tbl VALUES(floor(random()*:num),0,0,0,now(),ARRAY[floor(random()*100)::int],point(random(),random()))
					ON CONFLICT(i) DO UPDATE SET updated_at = now(), ia = ARRAY[floor(random()*100)::int], p = point(random(),random());
				INSERT INTO tbl VALUES(floor(random()*:num),0,0,0,now(),ARRAY[floor(random()*100)::int],point(random(),random()))
					ON CONFLICT(i) DO UPDATE SET updated_at = now(), ia = ARRAY[floor(random()*100)::int], p = point(random(),random());
				INSERT INTO tbl VALUES(floor(random()*:num),0,0,0,now(),ARRAY[floor(random()*100)::int],point(random(),random()))
					ON CONFLICT(i) DO UPDATE SET updated_at = now(), ia = ARRAY[floor(random()*100)::int], p = point(random(),random());
				INSERT INTO tbl VALUES(floor(random()*:num),0,0,0,now(),ARRAY[floor(random()*100)::int],point(random(),random()))
					ON CONFLICT(i) DO UPDATE SET updated_at = now(), ia = ARRAY[floor(random()*100)::int], p = point(random(),random());
				INSERT INTO tbl VALUES(floor(random()*:num),0,0,0,now(),ARRAY[floor(random()*100)::int],point(random(),random()))
					ON CONFLICT(i) DO UPDATE SET updated_at = now(), ia = ARRAY[floor(random()*100)::int], p = point(random(),random());
				SELECT setval('in_row_rebuild', 1);
				COMMIT;)
		),
	});

$node->safe_psql('postgres', q(TRUNCATE TABLE tbl;));

# Run CIC/RIC for unique index concurrently with upserts
$node->pgbench(
	$pgbench_options,
	0,
	[qr{actually processed}],
	[qr{^$}],
	'concurrent operations with REINDEX/CREATE INDEX CONCURRENTLY for unique BTREE',
	{
		# The in_row_rebuild gadget keeps the rebuild cycle from running
		# when no writer has touched the table lately, so the whole cycle
		# sits inside that guard.
		'concurrent_ops_unique_idx' => stress_ddl_gate(
			var => 'gotlock',
			sleep_ms => 0,
			ddl => [
				[
					"SELECT nextval('in_row_rebuild') AS last_value \\gset",
					'\set parallels random(0, 4)',
					'\set use_rr random(0, 9)',
					'\if :last_value < 3',
					"\tALTER TABLE tbl SET (parallel_workers=:parallels);",
					"\t\\if :use_rr = 0",
					"\t\tSET default_transaction_isolation = 'repeatable read';",
					"\t\\endif",
					"\tCREATE UNIQUE INDEX CONCURRENTLY new_idx ON tbl(i);",
					"\t\\set sleep_ms random(0, $max_sleep_ms)",
					"\t\\sleep :sleep_ms ms",
					"\tSELECT bt_index_check('new_idx', heapallindexed => true, checkunique => true);",
					"\tREINDEX INDEX CONCURRENTLY new_idx;",
					"\t\\set sleep_ms random(0, $max_sleep_ms)",
					"\t\\sleep :sleep_ms ms",
					"\tSELECT bt_index_check('new_idx', heapallindexed => true, checkunique => true);",
					"\tDROP INDEX CONCURRENTLY new_idx;",
					"\tRESET default_transaction_isolation;",
					'\endif',
				],
			],
			else => qq(
			\\set num random(1, power(10, random(1, 5)))
			INSERT INTO tbl VALUES(floor(random()*:num),0,0,0,now(),ARRAY[floor(random()*100)::int],point(random(),random()))
				ON CONFLICT(i) DO UPDATE SET updated_at = now(), ia = ARRAY[floor(random()*100)::int], p = point(random(),random());
			SELECT setval('in_row_rebuild', 1);)
		),
	});

$node->stop;
done_testing();
