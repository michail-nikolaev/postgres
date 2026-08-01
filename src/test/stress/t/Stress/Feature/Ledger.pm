
# Copyright (c) 2026, PostgreSQL Global Development Group

# A column whose sum never moves: balanced pairs of updates, and
# the checks that hold them to it.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::Ledger;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';
use Stress::MVCC qw(mvcc_or_empty);

# A column whose sum never moves, because every writer applies a
# balanced pair of updates.  Several dimensions need an invariant
# that is a constant rather than a relation between sums, and this is
# it.  It is a fast default, so adding it rewrites nothing.
schema ledger => {
		setup => q(
			ALTER TABLE pgbench_accounts
				ADD COLUMN ledger int NOT NULL DEFAULT 0;
		),
		indexes => [ {
			table => 'pgbench_accounts',
			name => 'pgb_ledger_idx',
			am => 'btree',
			defn => 'ON pgbench_accounts(ledger)',
		} ],
};

# A balanced pair of updates on the ledger: one +diff, one -diff in
# the same transaction, in id order so that concurrent writers cannot
# deadlock.  The sum is therefore the same at every commit.
load balanced_pair => {
		weight => 3,
		requires => { schema => ['ledger'] },
		checks => ['ledger_sum'],
		script => q(
			\set a random(1, :naccounts)
			\set b random(1, :naccounts)
			\set lo least(:a, :b)
			\set hi greatest(:a, :b)
			\set diff random(1, 10000)
			BEGIN;
			UPDATE pgbench_accounts SET ledger = ledger + :diff WHERE aid = :lo;
			\sleep 1 ms
			UPDATE pgbench_accounts SET ledger = ledger - :diff WHERE aid = :hi;
			COMMIT;
		),
};

# The ledger's sum never moves.
check ledger_sum => {
		weight => 1,
		requires => { schema => ['ledger'] },
		script => sub {
			my ($ctx) = @_;
			my $tol = mvcc_or_empty($ctx, 'cnt');
			return qq(
			SELECT stress_assert(${tol}sum = 0,
				format('ledger has %s rows summing to %s', cnt, sum))
			FROM (SELECT COUNT(*) AS cnt, COALESCE(SUM(ledger), 0) AS sum
				FROM pgbench_accounts) x;
			);
		},
		final => sub {
			my ($node, $ctx) = @_;
			Test::More::is(
				$node->safe_psql(
					'postgres', 'SELECT SUM(ledger) FROM pgbench_accounts'),
				'0', 'the balanced pairs still sum to zero');
		},
};

# Nothing may change under a held row lock: the second read runs in a
# fresh snapshot, so it would see any concurrent commit.
check row_lock_durability => {
		weight => 1,
		auto => 1,
		# Joins wherever both halves are present: the column it sums
		# and the load whose held locks it is checking under.
		requires => { schema => ['ledger'], load => ['row_lock'] },
		# Takes row locks, so it cannot run on a standby.
		writes => 1,
		script => sub {
			my ($ctx) = @_;
			my $tol = mvcc_or_empty($ctx, ':reread_cnt::bigint');
			return qq(
			\\set lo random(1, :naccounts - 4)
			BEGIN;
			SELECT COUNT(*) AS cnt, COALESCE(SUM(ledger), 0) AS sum FROM
				(SELECT ledger FROM pgbench_accounts
					WHERE aid BETWEEN :lo AND :lo + 4
					ORDER BY aid FOR UPDATE) s \\gset locked_
			\\sleep 20 ms
			SELECT COUNT(*) AS cnt, COALESCE(SUM(ledger), 0) AS sum
				FROM pgbench_accounts WHERE aid BETWEEN :lo AND :lo + 4 \\gset reread_
			COMMIT;
			SELECT stress_assert($tol(:locked_cnt::bigint = :reread_cnt::bigint
						AND :locked_sum::bigint = :reread_sum::bigint),
				format('rows changed under a held lock: locked (%s rows, sum %s), re-read (%s rows, sum %s)',
					:locked_cnt::bigint, :locked_sum::bigint,
					:reread_cnt::bigint, :reread_sum::bigint));
			);
		},
};

1;
