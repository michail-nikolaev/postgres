# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for table-rewriting ALTER TABLE against CONCURRENTLY
# commands.
#
# A rewriting ALTER TABLE (for example SET DATA TYPE on an indexed
# column, or ADD COLUMN with a volatile default) takes an
# AccessExclusiveLock and gives the table a brand new relfilenode,
# rebuilding every index in the process.
# The CONCURRENTLY commands take weaker locks and cache relfilenodes
# across their phases, so an ALTER TABLE rewrite landing in the middle
# of one is a sharp test of how they cope with the table changing shape
# underneath them.
#
# The two kinds of command cannot actually overlap, because ALTER
# TABLE's AccessExclusiveLock conflicts with everything, but they do
# hand the table back and forth: a REINDEX or REPACK that was waiting
# resumes against a freshly rewritten table.  Writers keep the sum over
# the val column invariant throughout; readers verify it.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled ALTER TABLE stress test');

my $duration = 6 * $stressval;
my $nrows = 5000;

my $node;

#
# Test set-up
#
$node = stress_init_node('alter_table',
	extra_conf => [ 'max_connections = 50' ]);

$node->safe_psql(
	'postgres', qq(
	CREATE EXTENSION amcheck;
	CREATE TABLE tbl(id int PRIMARY KEY, val int);
	CREATE INDEX tbl_val_idx ON tbl(val);
	INSERT INTO tbl SELECT g, g FROM generate_series(1, $nrows) g;
));

my $sum = $node->safe_psql('postgres', q(SELECT SUM(val) FROM tbl));

$node->pgbench(
	"--no-vacuum --client=30 --jobs=30 --exit-on-abort -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'table-rewriting ALTER TABLE against CONCURRENTLY commands',
	{
		'concurrent_ops' => stress_ddl_gate(
			indent => "\t\t",
			ddl => [
				'REPACK (CONCURRENTLY) tbl;',
				'REINDEX TABLE CONCURRENTLY tbl;',
				[
					'DROP INDEX CONCURRENTLY tbl_val_idx;',
					'CREATE INDEX CONCURRENTLY tbl_val_idx ON tbl(val);',
				],
				[
					'-- Rewrites the table and every index; new relfilenode.',
					'ALTER TABLE tbl ALTER COLUMN val TYPE bigint;',
					'ALTER TABLE tbl ALTER COLUMN val TYPE int;',
				],
				[
					'-- A volatile default forces a full rewrite; dropping the',
					'-- column afterwards leaves the (id, val) shape unchanged.',
					'ALTER TABLE tbl ADD COLUMN tmp float DEFAULT random();',
					'ALTER TABLE tbl DROP COLUMN tmp;',
				],
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
					# REPACK is not MVCC-safe yet, so tolerate an empty view;
					# anything non-empty must be complete and correct.
					qq(SELECT stress_assert(
						cnt = 0 OR (cnt = $nrows AND sum = $sum),
						format('rows=%s sum=%s (want 0, or $nrows rows summing to $sum)',
							cnt, sum))
					FROM (SELECT COUNT(*) AS cnt, COALESCE(SUM(val), 0) AS sum
						FROM tbl) t;),
				],
			),
		),
	});

is( $node->safe_psql('postgres', q(SELECT SUM(val) FROM tbl)),
	$sum, 'sum invariant holds after ALTER TABLE churn');
is( $node->safe_psql('postgres', q(SELECT COUNT(*) FROM tbl)),
	"$nrows", 'no rows lost');

$node->safe_psql(
	'postgres', q(
	SELECT bt_index_parent_check('tbl_pkey', heapallindexed => true);
	SELECT bt_index_parent_check('tbl_val_idx', heapallindexed => true);
));

$node->stop;

done_testing();
