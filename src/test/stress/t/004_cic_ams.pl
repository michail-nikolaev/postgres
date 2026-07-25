# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for CREATE INDEX CONCURRENTLY, REINDEX INDEX CONCURRENTLY
# and DROP INDEX CONCURRENTLY on non-btree access methods (GIN, GIST,
# BRIN, HASH, SPGIST), running concurrently with upserts.
#
# GIN indexes are verified with amcheck; for the other access methods
# the point is to exercise the concurrent build/rebuild/drop paths, with
# any SQL error aborting pgbench and failing the test.
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
my $pgbench_options =
  "--no-vacuum --client=30 --jobs=4 --exit-on-abort -T $duration";

my $node;

#
# Test set-up
#
$node = stress_init_node('cic_stress_ams',
	extra_conf => [ 'maintenance_work_mem = 32MB', 'shared_buffers = 32MB' ]);
$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));
$node->safe_psql('postgres', q(CREATE UNLOGGED TABLE tbl(i int primary key,
								c1 money default 0, c2 money default 0,
								c3 money default 0, updated_at timestamp,
								ia int4[], p point)));

# See 003_cic.pl for the purpose of this sequence.
$node->safe_psql('postgres',
	q(CREATE UNLOGGED SEQUENCE in_row_rebuild START 1 INCREMENT 1;));
$node->safe_psql('postgres', q(SELECT nextval('in_row_rebuild');));

# Run CIC/RIC for GIN with upserts
$node->pgbench(
	$pgbench_options,
	0,
	[qr{actually processed}],
	[qr{^$}],
	'concurrent operations with REINDEX/CREATE INDEX CONCURRENTLY for GIN',
	{
		# See 003_cic.pl for the in_row_rebuild guard.
		'concurrent_ops_gin_idx' => stress_ddl_gate(
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
					"\tCREATE INDEX CONCURRENTLY new_idx ON tbl USING GIN (ia);",
					"\t\\set sleep_ms random(0, $max_sleep_ms)",
					"\t\\sleep :sleep_ms ms",
					"\tSELECT gin_index_check('new_idx');",
					"\tREINDEX INDEX CONCURRENTLY new_idx;",
					"\t\\set sleep_ms random(0, $max_sleep_ms)",
					"\t\\sleep :sleep_ms ms",
					"\tSELECT gin_index_check('new_idx');",
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

$node->safe_psql('postgres', q(TRUNCATE TABLE tbl;));

# Run CIC/RIC for GIST/BRIN/HASH/SPGIST index concurrently with upserts
$node->pgbench(
	$pgbench_options,
	0,
	[qr{actually processed}],
	[qr{^$}],
	'concurrent operations with REINDEX/CREATE INDEX CONCURRENTLY for GIST/BRIN/HASH/SPGIST',
	{
		# See 003_cic.pl for the in_row_rebuild guard.  The index built
		# each round is one of the non-btree access methods.
		'concurrent_ops_other_idx' => stress_ddl_gate(
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
					split(
						/\n/,
						stress_variant_switch(
							var => 'variant',
							indent => "\t",
							variants => [
								'CREATE INDEX CONCURRENTLY new_idx ON tbl USING GIST (p);',
								'CREATE INDEX CONCURRENTLY new_idx ON tbl USING BRIN (updated_at);',
								'CREATE INDEX CONCURRENTLY new_idx ON tbl USING HASH (updated_at);',
								'CREATE INDEX CONCURRENTLY new_idx ON tbl USING SPGIST (p);',
							])),
					"\t\\set sleep_ms random(0, $max_sleep_ms)",
					"\t\\sleep :sleep_ms ms",
					"\tREINDEX INDEX CONCURRENTLY new_idx;",
					"\t\\set sleep_ms random(0, $max_sleep_ms)",
					"\t\\sleep :sleep_ms ms",
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
