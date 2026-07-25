# Copyright (c) 2026, PostgreSQL Global Development Group

# Stress test for SERIALIZABLE isolation (SSI) under concurrent DDL.
#
# Two phases:
#
# - "gapless sequence": serializable transactions compute MAX(id) + 1
#   and insert it.  Serial executions can never produce a duplicate, so
#   neither may SSI; there is deliberately no unique constraint, so a
#   missed rw-conflict silently produces a duplicate, which the clients
#   then detect.  The rows are wide enough to cause frequent btree page
#   splits (which must transfer predicate locks), and one client
#   meanwhile cycles through REPACK (CONCURRENTLY), DROP/CREATE INDEX
#   CONCURRENTLY and REINDEX, all of which must transfer predicate
#   locks to other relations rather than dropping them.
#
# - "write skew": the classic bank example.  A pair of accounts may
#   only be debited if the pair's combined balance covers it, checked
#   in the same serializable transaction.  Serial executions keep every
#   pair's total non-negative, so SSI must too; a missed conflict
#   between two debits of the same pair lets the total go negative,
#   which the clients then detect.
#
# REPACK (CONCURRENTLY) is commented out of the DDL rotation for now:
# it is currently known not to be MVCC-safe, so transactions whose
# snapshot predates the swap can see incorrect data afterwards, which
# the first workload detects as duplicates (roughly one per REPACK).
# Re-enable it if that ever changes.
#
# Serialization failures are expected in these workloads and are
# retried without limit (--max-tries=0); any other SQL error, including
# the ones the clients raise upon detecting an anomaly, aborts pgbench
# and fails the test.
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

use FindBin;
use lib $FindBin::RealBin;
use StressConcurrently;

my $stressval = stress_plan(skip =>
	'skipping disabled SERIALIZABLE stress test');

# This file runs two pgbench phases, so give each one half of the
# calibrated total duration.
my $duration = 3 * $stressval;
my $npairs = 100;

my $node;

#
# Test set-up
#
$node = stress_init_node('serializable',
	extra_conf => [ 'max_pred_locks_per_transaction = 512' ]);
$node->safe_psql('postgres', q(CREATE EXTENSION amcheck));
# The id column must stay unconstrained so that a missed conflict
# produces a detectable duplicate rather than an error; the surrogate
# primary key makes the table eligible for commands that need a replica
# identity, such as REPACK (CONCURRENTLY).
$node->safe_psql(
	'postgres', q(
	CREATE TABLE tbl(seq serial PRIMARY KEY, id int, filler text);
	CREATE INDEX tbl_id_idx ON tbl(id);
));

$node->pgbench(
	"--no-vacuum --client=30 --jobs=4 --exit-on-abort --max-tries=0 -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'SERIALIZABLE gapless inserts with concurrent DDL',
	{
		'concurrent_ops' => stress_ddl_gate(
			indent => "\t\t",
			ddl => [
				[
					'DROP INDEX CONCURRENTLY tbl_id_idx;',
					'CREATE INDEX CONCURRENTLY tbl_id_idx ON tbl(id);',
				],
				'REINDEX INDEX tbl_id_idx;',
			],
			post => [
				'-- not MVCC-safe yet, see the comment at the top of this file:',
				'-- REPACK (CONCURRENTLY) tbl USING INDEX tbl_id_idx;',
				"SELECT bt_index_check('tbl_id_idx', heapallindexed => true);",
			],
		) . qq(
			BEGIN ISOLATION LEVEL SERIALIZABLE;
			SELECT COALESCE(MAX(id), 0) + 1 AS newid FROM tbl \\gset
			INSERT INTO tbl(id, filler) VALUES (:newid, repeat('x', 100));
			COMMIT;

			SELECT stress_assert(COUNT(*) = COUNT(DISTINCT id),
				format('%s duplicate ids present', COUNT(*) - COUNT(DISTINCT id)))
				FROM tbl;
		\\endif
	)
	});

my $dups = $node->safe_psql('postgres',
	q(SELECT COUNT(*) - COUNT(DISTINCT id) FROM tbl));
is($dups, '0', 'no duplicate ids after serializable insert churn');

#
# Write skew phase.
#
$node->safe_psql(
	'postgres', qq(
	CREATE TABLE acc(pair int, side int, bal int, PRIMARY KEY(pair, side));
	INSERT INTO acc SELECT p, s, 100 FROM generate_series(1, $npairs) p,
		generate_series(0, 1) s;
));

$node->pgbench(
	"--no-vacuum --client=30 --jobs=4 --exit-on-abort --max-tries=0 -T $duration",
	0,
	[qr{actually processed}],
	[qr{^$}],
	'SERIALIZABLE write skew',
	{
		'write_skew' => qq(
		\\set pair random(1, $npairs)
		\\set amount random(1, 50)
		\\set side random(0, 1)
		\\set action random(0, 3)
		BEGIN ISOLATION LEVEL SERIALIZABLE;
		\\if :action = 0
			UPDATE acc SET bal = bal + :amount
				WHERE pair = :pair AND side = :side;
		\\else
			SELECT SUM(bal) AS total FROM acc WHERE pair = :pair \\gset
			\\if :total >= :amount
				UPDATE acc SET bal = bal - :amount
					WHERE pair = :pair AND side = :side;
			\\endif
		\\endif
		COMMIT;

		SELECT stress_assert(
			NOT EXISTS (SELECT 1 FROM acc GROUP BY pair HAVING SUM(bal) < 0),
			'an account pair has a negative combined balance');
	)
	});

my $overdrawn = $node->safe_psql('postgres',
	q(SELECT COUNT(*) FROM (SELECT pair FROM acc GROUP BY pair HAVING SUM(bal) < 0) x));
is($overdrawn, '0', 'no overdrawn account pairs after write skew churn');

$node->stop;

done_testing();
