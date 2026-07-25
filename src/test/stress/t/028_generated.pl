# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for CONCURRENTLY commands on tables with generated and
# identity columns.
#
# REPACK (CONCURRENTLY) rebuilds a table by decoding its own changes
# and re-applying them, which has to reproduce stored generated columns
# and identity columns exactly; a past bug lost stored generated column
# values during REPACK.  Here writers update the base columns that feed
# a stored generated column and an index built on it, while the DDL
# rotation repacks and reindexes the table.
#
# The generated column is defined so that it always equals a fixed
# function of the base columns, so every row must satisfy that relation
# no matter how the table has been rewritten; readers verify it, and
# also verify the invariant kept by the writers.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled generated column stress test');

my $duration = 6 * $stressval;
my $nrows = 10_000;

my $node;

#
# Test set-up
#
$node = stress_init_node('generated');

# gen = a + b always; total = a + b invariant across rows via the
# balanced updates below.  The identity column exercises the identity
# path through REPACK's decode/apply.
$node->safe_psql(
	'postgres', qq(
	CREATE EXTENSION amcheck;
	CREATE TABLE tbl(
		id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		a int,
		b int,
		gen int GENERATED ALWAYS AS (a + b) STORED);
	CREATE INDEX tbl_gen_idx ON tbl(gen);
	INSERT INTO tbl(a, b) SELECT g, 0 FROM generate_series(1, $nrows) g;
));

my $sum = $node->safe_psql('postgres', q(SELECT SUM(a + b) FROM tbl));

$node->pgbench(
	"--no-vacuum --client=30 --jobs=30 --exit-on-abort -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'CONCURRENTLY commands with generated and identity columns',
	{
		'concurrent_ops' => stress_ddl_gate(
			indent => "\t\t",
			ddl => [
				'REPACK (CONCURRENTLY) tbl;',
				'REINDEX TABLE CONCURRENTLY tbl;',
				[
					'DROP INDEX CONCURRENTLY tbl_gen_idx;',
					'CREATE INDEX CONCURRENTLY tbl_gen_idx ON tbl(gen);',
				],
			],
			post =>
			  "SELECT bt_index_check('tbl_pkey', heapallindexed => true, checkunique => true);",
			else => stress_workload(
				mutations => [
					qq(-- Move value between two rows via the base columns; the
					-- generated column follows automatically.
					\\set id_a random(1, $nrows)
					\\set id_b random(1, $nrows)
					\\set diff random(1, 10000)
					BEGIN;
					UPDATE tbl SET a = a + :diff WHERE id = :id_a;
					\\sleep 1 ms
					UPDATE tbl SET b = b - :diff WHERE id = :id_b;
					\\sleep 1 ms
					COMMIT;),
				],
				checks => [
					# The stored generated column must always equal a + b, and
					# the total a + b is invariant.  REPACK is not MVCC-safe
					# yet, so tolerate an empty view; anything non-empty must
					# be complete and correct.
					qq(SELECT stress_assert(
						cnt = 0 OR (bad = 0 AND cnt = $nrows AND sum = $sum),
						format('rows=%s bad=%s sum=%s (want $nrows rows, 0 bad, sum $sum)',
							cnt, bad, sum))
					FROM (SELECT COUNT(*) AS cnt,
						COUNT(*) FILTER (WHERE gen <> a + b) AS bad,
						COALESCE(SUM(gen), 0) AS sum FROM tbl) t;),
				],
			),
		),
	});

is( $node->safe_psql('postgres',
		q(SELECT COUNT(*) FROM tbl WHERE gen <> a + b)),
	'0', 'every stored generated value matches its base columns');

is( $node->safe_psql('postgres', q(SELECT SUM(a + b) FROM tbl)),
	$sum, 'sum invariant holds after generated-column churn');

is( $node->safe_psql('postgres', q(SELECT COUNT(*) FROM tbl)),
	"$nrows", 'no rows lost');

$node->safe_psql(
	'postgres', q(
	SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('tbl_gen_idx', heapallindexed => true);
));

$node->stop;

done_testing();
