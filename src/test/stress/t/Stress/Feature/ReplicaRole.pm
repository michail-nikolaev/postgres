
# Copyright (c) 2026, PostgreSQL Global Development Group

# Row level security and replica-mode triggers, the paths the
# apply worker takes through the executor.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::ReplicaRole;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

# Row level security, forced so that the owner is subject to it too,
# and a trigger that fires only when the session is in replica mode.
# Both change the path an ordinary update takes through the executor,
# and replica mode is the one logical replication's apply worker uses,
# so it is worth driving against a table being rebuilt.
schema replica_role => {
		setup => q(
			ALTER TABLE pgbench_accounts
				ADD COLUMN rr_touched int NOT NULL DEFAULT 0;

			CREATE FUNCTION pgb_rr_trigger() RETURNS trigger
			LANGUAGE plpgsql AS $$
			BEGIN
				NEW.rr_touched := OLD.rr_touched + 1;
				RETURN NEW;
			END;
			$$;
			CREATE TRIGGER pgb_rr_trg BEFORE UPDATE ON pgbench_accounts
				FOR EACH ROW EXECUTE FUNCTION pgb_rr_trigger();
			-- Fires only when session_replication_role is 'replica', so
			-- the ordinary workload never sees it.
			ALTER TABLE pgbench_accounts ENABLE REPLICA TRIGGER pgb_rr_trg;

			ALTER TABLE pgbench_accounts ENABLE ROW LEVEL SECURITY;
			ALTER TABLE pgbench_accounts FORCE ROW LEVEL SECURITY;
			CREATE POLICY pgb_rr_policy ON pgbench_accounts
				USING (true) WITH CHECK (true);
		),
};

# The same balanced pair, applied in the mode logical replication's
# apply worker runs in: ordinary triggers do not fire, replica ones
# do, and row level security is still enforced.
load replica_role_apply => {
		weight => 2,
		requires => { schema => [ 'replica_role', 'ledger' ] },
		script => q(
			\set a random(1, :naccounts)
			\set b random(1, :naccounts)
			\set lo least(:a, :b)
			\set hi greatest(:a, :b)
			\set diff random(1, 10000)
			BEGIN;
			SET LOCAL session_replication_role = replica;
			UPDATE pgbench_accounts SET ledger = ledger + :diff WHERE aid = :lo;
			UPDATE pgbench_accounts SET ledger = ledger - :diff WHERE aid = :hi;
			COMMIT;
		),
};

1;
