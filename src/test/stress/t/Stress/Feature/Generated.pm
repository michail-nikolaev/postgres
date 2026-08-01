
# Copyright (c) 2026, PostgreSQL Global Development Group

# Generated columns on the table the rotation rewrites, and the
# checks that the rewrite computed and carried them correctly.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::Generated;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

use Stress::MVCC qw(mvcc_or_empty);

# How each column of pgbench_accounts is defined: whether it is stored,
# virtual or neither, and the expression behind it.  REPACK swaps the
# table's storage and has to bring the pg_attrdef entries across, and
# nothing about the workload would notice if it brought across the wrong
# ones, so compare the definitions themselves before and after.
my $GEN_DEFS_QUERY = q(
	SELECT a.attname || ' ' || a.attgenerated::text || ' '
			|| COALESCE(pg_get_expr(d.adbin, d.adrelid), '-')
		FROM pg_attribute a
		LEFT JOIN pg_attrdef d
			ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		WHERE a.attrelid = 'pgbench_accounts'::regclass AND a.attnum > 0
			AND NOT a.attisdropped
		ORDER BY a.attnum);

# Generated columns on pgbench_accounts, computed from the balance
# every TPC-B transaction moves.  Putting them on the table the
# rotation already works hardest against is the point: a side table
# with a load of its own would exercise the same code with far less
# going on around it.
schema generated => {
		# A stored generated column pins the type of the column it reads,
		# so the rewriting ALTER cannot run against this schema.
		conflicts => { ddl => ['alter_table_rewrite'] },
		setup => q(
			ALTER TABLE pgbench_accounts
				-- Stored: rewritten on every insert and update, which is
				-- what the replay of concurrent changes has to reproduce
				-- against the transient table.
				ADD COLUMN gen int
					GENERATED ALWAYS AS (abalance * 2 + 1) STORED,
				-- Stored, and out of line for a thousandth of the rows:
				-- the replay has to compute the value and then toast it,
				-- without making every row wide.
				ADD COLUMN gen_txt text GENERATED ALWAYS AS (
					CASE WHEN aid % 1000 = 0
						THEN repeat(md5(abalance::text), 100)
						ELSE md5(abalance::text) END) STORED,
				-- Virtual: computed on read, never stored, so the replay
				-- must leave it alone.  It cannot be indexed, made unique
				-- or referenced, so payload is all it can be.
				ADD COLUMN gen_v int GENERATED ALWAYS AS (abalance + 1) VIRTUAL,
				-- An ordinary default, which lives in pg_attrdef beside
				-- the generation expressions and travels with them.
				ADD COLUMN note text DEFAULT 'note';
		),
		indexes => [ {
			table => 'pgbench_accounts',
			name => 'pgb_gen_idx',
			am => 'btree',
			defn => 'ON pgbench_accounts(gen)',
		} ],
		context => sub {
			my ($node) = @_;
			return {
				# What the column definitions looked like before anything
				# rewrote the table, so the run can be held to them.
				gen_defs => $node->safe_psql('postgres', $GEN_DEFS_QUERY),
			};
		},
};

# Updates of the column a stored generated column is computed from.
# The fix for stored generated columns under REPACK was about tuples
# "concurrently updated or inserted", so this does both, and deletes
# what it inserts so the table does not grow without bound.  Inserted
# rows use ids above the ones the setup created, which keeps them out
# of the way of the updates.
load generated_update => {
		weight => 2,
		requires => { schema => ['generated'] },
		checks => [ 'generated_matches', 'generated_defs_intact' ],
		script => q(
			\set scratch random(:naccounts + 1, :naccounts + 5000)
			\set mode random(0, 1)
			-- The rows inserted here carry a zero balance and sit above
			-- the range every other load works in, so adding and removing
			-- them leaves all four sums where they were.  The updates
			-- that drive the generated columns are the workload's own:
			-- every TPC-B transaction moves a balance.
			\if :mode = 0
				SELECT pgb_scratch_insert(:scratch);
			\else
				DELETE FROM pgbench_accounts WHERE aid = :scratch;
			\endif
		),
};

# Each stored generated column is a fixed function of its base column,
# however the table has been rewritten.  The wide one is checked as
# well as the narrow one, because computing the value and storing it
# out of line are separate things for the replay to get wrong.
#
# The virtual column is deliberately not compared against its own
# expression: it is computed on read, so that comparison can only ever
# hold.  What matters about it is that the table still has it and that
# REPACK did not choke on it, which generated_defs_intact covers.
check generated_matches => {
		weight => 1,
		requires => { schema => ['generated'] },
		script => sub {
			my ($ctx) = @_;
			my $tol = mvcc_or_empty($ctx, 'cnt');
			return qq(
			SELECT stress_assert(${tol}bad = 0,
				format('%s rows whose generated column does not match', bad))
			FROM (SELECT COUNT(*) FILTER (WHERE gen <> abalance * 2 + 1
						OR gen_txt <> CASE WHEN aid % 1000 = 0
							THEN repeat(md5(abalance::text), 100)
							ELSE md5(abalance::text) END) AS bad,
				COUNT(*) AS cnt FROM pgbench_accounts) x;
			);
		},
		final => sub {
			my ($node, $ctx) = @_;
			Test::More::is(
				$node->safe_psql(
					'postgres', q(
					SELECT COUNT(*) FROM pgbench_accounts
						WHERE gen <> abalance * 2 + 1
							OR gen_txt <> CASE WHEN aid % 1000 = 0
								THEN repeat(md5(abalance::text), 100)
								ELSE md5(abalance::text) END
							OR gen_v <> abalance + 1
							OR note <> 'note')),
				'0', 'every generated value matches its base column');
		},
};

# A rewrite has to bring the generation expressions across intact.
# Nothing the workload can observe would notice if it brought across
# the wrong ones -- the values would simply be computed from a
# different expression and agree with themselves -- so compare the
# definitions against what they were before the run.
check generated_defs_intact => {
		auto => 1,
		requires => { schema => ['generated'] },
		final => sub {
			my ($node, $ctx) = @_;
			Test::More::is(
				$node->safe_psql('postgres', $GEN_DEFS_QUERY),
				$ctx->{gen_defs},
				'the generated column definitions are unchanged');
		},
};

1;
