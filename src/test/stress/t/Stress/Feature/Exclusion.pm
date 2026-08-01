
# Copyright (c) 2026, PostgreSQL Global Development Group

# An exclusion constraint over a range expression, raced by slot
# claims while its index is rebuilt.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::Exclusion;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

# Rows in the slot band.  Small enough to stay cheap, large enough that
# an index over one has more than a single page.
my $NSLOTS = 2_000;

# At most one row per slot, kept that way by an exclusion constraint.
# The constraint is written over a range so that it needs nothing but
# the built-in GiST opclasses, and so that its index is built from an
# expression.  It is partial because the rows the load has not
# claimed have no slot, and an unbounded range would overlap
# everything.
schema exclusion_slot => {
		setup => qq(
			ALTER TABLE pgbench_accounts ADD COLUMN slot int;
			ALTER TABLE pgbench_accounts ADD CONSTRAINT pgb_slot_excl
				EXCLUDE USING gist (int4range(slot, slot + 1) WITH &&)
				WHERE (slot IS NOT NULL);
			CREATE FUNCTION pgb_try_slot(p_aid int, p_slot int)
			RETURNS boolean LANGUAGE plpgsql AS \$\$
			BEGIN
				UPDATE pgbench_accounts SET slot = p_slot WHERE aid = p_aid;
				RETURN true;
			EXCEPTION WHEN exclusion_violation THEN
				RETURN false;
			END;
			\$\$;
		),
		context => sub { return { nslots => $NSLOTS, has_exclusion => 1 } },
};

# Duplicate slots, which must always be rejected, and a slot freed
# and taken again in one transaction, which must end up occupied
# exactly once.
load exclusion_churn => {
		weight => 2,
		requires => { schema => ['exclusion_slot'] },
		script => q(
			\set slot random(1, :nslots)
			\set aid random(1, :naccounts)
			\set mode random(0, 1)
			\if :mode = 0
				-- Claiming a slot that is taken has to be refused, and
				-- the constraint's index is being rebuilt while it is
				-- asked to decide that.
				SELECT pgb_try_slot(:aid, :slot);
			\else
				BEGIN;
				UPDATE pgbench_accounts SET slot = NULL WHERE slot = :slot;
				\sleep 1 ms
				SELECT pgb_try_slot(:aid, :slot);
				COMMIT;
			\endif
		),
};

# One row per slot, and never two.
check distinct_slots => {
		weight => 1,
		requires => { schema => ['exclusion_slot'] },
		script => q(
			-- How many rows hold a slot rises and falls as the load
			-- claims and releases them; what may never happen is two
			-- rows holding the same one.
			SELECT stress_assert(cnt = slots,
				format('%s rows hold only %s distinct slots', cnt, slots))
			FROM (SELECT COUNT(*) AS cnt, COUNT(DISTINCT slot) AS slots
				FROM pgbench_accounts WHERE slot IS NOT NULL) x;
		),
		final => sub {
			my ($node, $ctx) = @_;
			Test::More::is(
				$node->safe_psql('postgres',
					'SELECT COUNT(*) - COUNT(DISTINCT slot) FROM pgbench_accounts WHERE slot IS NOT NULL'),
				'0', 'no duplicate slot got past the exclusion constraint');
		},
};

1;
