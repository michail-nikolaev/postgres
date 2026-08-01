
# Copyright (c) 2026, PostgreSQL Global Development Group

# Transactions prepared and resolved later, which the
# CONCURRENTLY commands have to wait out.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::Twophase;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

# The same, through PREPARE TRANSACTION and COMMIT PREPARED (or,
# sometimes, ROLLBACK PREPARED): either way the transaction is
# internally balanced, and the CONCURRENTLY commands have to cope
# with transactions that are prepared but not yet resolved.
load twophase => {
		weight => 1,
		requires => { schema => ['ledger'] },
		checks => ['ledger_sum'],
		conf => ['max_prepared_transactions = 100'],
		# The transaction identifier has to be a literal, and the only
		# way to make one per client is to put the variable inside the
		# quotes.  pgbench substitutes there too, so under the extended
		# and prepared protocols this arrives as a bind parameter while
		# the server sees 'stress_$1' as ordinary text and wants none.
		simple_protocol_only => 1,
		script => q(
			\set a random(1, :naccounts)
			\set b random(1, :naccounts)
			\set lo least(:a, :b)
			\set hi greatest(:a, :b)
			\set diff random(1, 10000)
			\set abort random(0, 4)
			BEGIN;
			UPDATE pgbench_accounts SET ledger = ledger + :diff WHERE aid = :lo;
			UPDATE pgbench_accounts SET ledger = ledger - :diff WHERE aid = :hi;
			PREPARE TRANSACTION 'stress_:client_id';
			\sleep 2 ms
			\if :abort = 0
				ROLLBACK PREPARED 'stress_:client_id';
			\else
				COMMIT PREPARED 'stress_:client_id';
			\endif
		),
};

1;
