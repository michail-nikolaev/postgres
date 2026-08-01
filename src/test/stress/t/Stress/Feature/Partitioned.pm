
# Copyright (c) 2026, PostgreSQL Global Development Group

# pgbench_accounts partitioned in place, with an overflow
# partition for the detach commands to take away.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::Partitioned;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

# pgbench_accounts, turned into a partitioned table by the recipe for
# partitioning a table that already has rows in it: rename it, build
# a partitioned parent over it, and attach it as the partition
# holding everything that was there.
#
# Alongside it goes an overflow partition covering every account
# number above the ones pgbench created.  That is what the detach
# commands are aimed at, and the reason they can run at all: an
# account row the workload never touches contributes nothing to any
# of the four sums, so hiding the whole partition leaves the balance
# where it was.  Detaching a partition that held real accounts would
# break it permanently -- the account update would quietly match no
# row while the teller, branch and history rows still moved.
schema partitioned => {
		# An exclusion constraint on a partitioned table has to contain
		# the partition key, and this one does not.  A foreign key would
		# survive the rename pointing at the partition rather than the
		# parent, which is not the shape worth testing.  The subscription
		# environment builds an index of its own on the subscriber and
		# drops it concurrently, which a partitioned index refuses.
		conflicts => {
			schema => [ 'exclusion_slot', 'fk_child' ],
			topology => ['subscription'],
		},
		setup => q(
			ALTER TABLE pgbench_accounts RENAME TO pgbench_accounts_main;
			-- And its primary key with it.  Renaming a table leaves the
			-- constraint holding the old name, so the partitioned parent
			-- built below would have to settle for a generated one --
			-- and anything naming pgbench_accounts_pkey, such as an
			-- upsert inferring its arbiter from the constraint, would be
			-- pointed at the wrong table.
			ALTER TABLE pgbench_accounts_main
				RENAME CONSTRAINT pgbench_accounts_pkey
				TO pgbench_accounts_main_pkey;
			CREATE TABLE pgbench_accounts
				(LIKE pgbench_accounts_main INCLUDING ALL)
				PARTITION BY RANGE (aid);
			DO $$
			DECLARE
				top bigint;
			BEGIN
				SELECT COALESCE(MAX(aid), 0) INTO top FROM pgbench_accounts_main;
				EXECUTE format(
					'ALTER TABLE pgbench_accounts ATTACH PARTITION '
					|| 'pgbench_accounts_main FOR VALUES FROM (MINVALUE) TO (%s)',
					top + 1);
				EXECUTE format(
					'CREATE TABLE pgbench_accounts_over PARTITION OF '
					|| 'pgbench_accounts FOR VALUES FROM (%s) TO (MAXVALUE)',
					top + 1);
			END $$;
		),
		# The parent is where the DML goes; the partitions are what the
		# CONCURRENTLY commands can be pointed at, since most of them
		# refuse a partitioned table.
		tables => [qw(pgbench_accounts_main pgbench_accounts_over)],
		untables => ['pgbench_accounts'],
};

# The overflow partition, itself partitioned.  A child index then has
# a grandparent as well as a parent, which is what the arbiter and
# descriptor code walks when it asks who an index belongs to.
schema partitioned_2_levels => {
		requires => { schema => ['partitioned'] },
		setup => q(
			ALTER TABLE pgbench_accounts DETACH PARTITION pgbench_accounts_over;
			DROP TABLE pgbench_accounts_over;
			DO $$
			DECLARE
				top bigint;
			BEGIN
				SELECT COALESCE(MAX(aid), 0) INTO top FROM pgbench_accounts_main;
				EXECUTE format(
					'CREATE TABLE pgbench_accounts_over PARTITION OF '
					|| 'pgbench_accounts FOR VALUES FROM (%s) TO (MAXVALUE) '
					|| 'PARTITION BY HASH (aid)',
					top + 1);
			END $$;
			CREATE TABLE pgbench_accounts_over_0 PARTITION OF
				pgbench_accounts_over FOR VALUES WITH (MODULUS 2, REMAINDER 0);
			CREATE TABLE pgbench_accounts_over_1 PARTITION OF
				pgbench_accounts_over FOR VALUES WITH (MODULUS 2, REMAINDER 1);
		),
		tables => [qw(pgbench_accounts_over_0 pgbench_accounts_over_1)],
		untables => ['pgbench_accounts_over'],
};

# Traffic in the overflow partition, so that the partition the detach
# commands take away is not an empty one.  Every row carries a zero
# balance, so however many of them exist, and whether or not their
# partition is currently attached, the four sums are unmoved.
load overflow_churn => {
		weight => 3,
		requires => { schema => ['partitioned'] },
		script => q(
			\set scratch random(:naccounts + 1, :naccounts + 5000)
			\set mode random(0, 1)
			\if :mode = 0
				SELECT pgb_scratch_insert(:scratch);
			\else
				DELETE FROM pgbench_accounts WHERE aid = :scratch;
			\endif
		),
};

# Detach and re-attach the overflow partition of pgbench_accounts.
# This is the detach running against the table the whole workload is
# on, rather than one standing to the side of it.
ddl detach_overflow_partition => {
		requires => { schema => ['partitioned'] },
		# Soak invents load mixes that hold locks on the parent for long
		# stretches -- prepared transactions, held cursors, row locks --
		# and DETACH ... CONCURRENTLY waits for all of them.  Under those
		# it can sit out the whole lock timeout, which is the feature
		# behaving as documented rather than anything worth reporting, so
		# keep it to the scenario whose workload is known to leave gaps.
		catalogue_only => 1,
		variants => sub {
			my ($ctx) = @_;
			my $from = $ctx->{naccounts} + 1;
			return ({
				table => 'pgbench_accounts_over',
				stmts => [
					# Skipped when a previous turn could not get the
					# partition back on, so the cycle heals itself
					# instead of failing on "is not a partition".
					"SELECT COUNT(*) > 0 AS attached FROM pg_inherits "
					  . "WHERE inhrelid = 'pgbench_accounts_over'::regclass "
					  . '\\gset',
					'\if :attached',
					'ALTER TABLE pgbench_accounts DETACH PARTITION '
					  . 'pgbench_accounts_over CONCURRENTLY;',
					'\endif',
					'\sleep 10 ms',
					"SELECT pgb_ddl_bounded('ALTER TABLE pgbench_accounts "
					  . 'ATTACH PARTITION pgbench_accounts_over '
					  . "FOR VALUES FROM ($from) TO (MAXVALUE)');"
				]
			});
		},
};

# The same one level down, where the partition being detached has a
# grandparent.
ddl detach_subpartition => {
		requires => { schema => ['partitioned_2_levels'] },
		catalogue_only => 1,
		variants => sub {
			my ($ctx) = @_;
			my @v;
			for my $i (0 .. 1)
			{
				push @v,
				  {
					table => "pgbench_accounts_over_$i",
					stmts => [
						"SELECT COUNT(*) > 0 AS attached FROM pg_inherits "
						  . "WHERE inhrelid = "
						  . "'pgbench_accounts_over_$i'::regclass \\gset",
						'\if :attached',
						'ALTER TABLE pgbench_accounts_over DETACH PARTITION '
						  . "pgbench_accounts_over_$i CONCURRENTLY;",
						'\endif',
						'\sleep 10 ms',
						"SELECT pgb_ddl_bounded('ALTER TABLE "
						  . 'pgbench_accounts_over ATTACH PARTITION '
						  . "pgbench_accounts_over_$i "
						  . "FOR VALUES WITH (MODULUS 2, REMAINDER $i)');"
					]
				  };
			}
			return @v;
		},
};

1;
