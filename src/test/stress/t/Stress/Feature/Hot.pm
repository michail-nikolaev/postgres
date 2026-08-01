
# Copyright (c) 2026, PostgreSQL Global Development Group

# HOT chains: room left on every page, updates no index covers,
# and the small-table repacks that race pruning.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::Hot;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

# Room left on every page for the next version of the rows already
# there.  pgbench fills its pages to the brim, which pushes updates
# onto other pages and cuts the HOT chains short; leaving half the
# page free keeps the chains on the page, where they can be pruned.
# The setting applies to pages written from here on, so the effect
# arrives as the load rewrites the table rather than at once.
schema low_fillfactor => {
		# A partitioned table holds no rows and takes no storage
		# parameters, so this cannot be applied to pgbench_accounts once
		# the partitioning decorator has replaced it with a parent.  Which
		# of the two runs first decides whether it errors, so the pair is
		# declared incompatible rather than left to the order they happen
		# to be listed in.  Found by a soak combination that named both.
		conflicts => { schema => ['partitioned'] },
		setup => q(
			ALTER TABLE pgbench_accounts SET (fillfactor = 50);
		),
};

# Nothing but updates to one column, spread evenly over the table.
# Where no index covers that column the new version stays on the
# page and the old one becomes prunable, so this produces HOT chains
# and, through them, opportunistic pruning on pages all over the
# relation -- which is what a concurrent build has to survive.  It
# does not move money between tables, so a scenario using this one
# has no balance invariant to check.
load hot_churn => {
		weight => 1,
		# Moves abalance without the matching teller, branch and history
		# rows, so the four-way total no longer holds.
		conflicts => { checks => ['balances'] },
		script => q(
			\set aid random(1, :naccounts)
			\set delta random(-5000, 5000)
			UPDATE pgbench_accounts SET abalance = abalance + :delta
				WHERE aid = :aid;
		),
};

# REPACK aimed only at the tables small enough that the whole copy
# runs in the time it takes to set one CLOG bit.
#
# REPACK (CONCURRENTLY) builds its snapshot with
# SnapBuildInitialSnapshot(), from the decoding snapshot builder, and
# copies the old heap under it -- so it is one of the two places in
# the server where a snapshot derived from decoded COMMIT records is
# used for ordinary MVCC visibility checks.  A commit that has been
# decoded but not yet recorded in CLOG is absent from such a snapshot
# and absent from CLOG at once, which reads as aborted: the copy
# drops the row and, worse, hint-bits the old heap to say so
# permanently.  See the "Race conditions in logical decoding" thread.
#
# Every REPACK is one exposure, so what matters is how many of them
# fit in a run and how little the copy has to scan between building
# the snapshot and reaching the row a commit is racing.  pgbench's
# accounts and history are far too big for either.
ddl repack_hot_small => {
		variants => sub {
			my ($ctx) = @_;
			return map {
				{ table => $_, stmts => ["REPACK (CONCURRENTLY) $_;"] }
			}
			  grep { $_ eq 'pgbench_branches' || $_ eq 'pgbench_tellers' }
			  @{ $ctx->{tables} };
		},
};

1;
