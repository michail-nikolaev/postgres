
# Copyright (c) 2026, PostgreSQL Global Development Group

# Checks that apply to any scenario: index integrity, the
# visibility map, slot hygiene, invalid-index cleanup.
#
# See Stress::Registry for what each declaration means.

package Stress::Cross::GenericChecks;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

use Stress::Cross::DDLCore qw(_unpartitioned_indexes);
use Stress::Util qw(_retry_on_deadlock);

# Every index the scenario built must still be a valid index.
check amcheck => {
		auto => 1,
		final => sub {
			my ($node, $ctx) = @_;

			# Primary keys are not among the declared indexes -- they
			# arrive with the table -- but reindex_pkey_concurrently and
			# reindex_table_concurrently both rebuild them, so they need
			# checking too.  Resolved through the catalog rather than by
			# name, so that a table without one, or one whose constraint a
			# decorator renamed, is simply skipped.
			foreach my $table (@{ $ctx->{tables} })
			{
				$node->safe_psql(
					'postgres', qq(
					SELECT bt_index_parent_check(i.indexrelid,
												 heapallindexed => true)
					FROM pg_index i
					WHERE i.indrelid = to_regclass('$table')
					  AND i.indisprimary AND i.indisvalid));
			}

			# amcheck wants a real index, not a partitioned one, and an
			# index on a table a decorator has partitioned is the latter.
			foreach my $idx (_unpartitioned_indexes($ctx))
			{
				# amcheck covers btree and GIN; the other access methods
				# are exercised by being built and rebuilt rather than
				# verified afterwards.
				if ($idx->{am} eq 'btree')
				{
					$node->safe_psql('postgres',
						"SELECT bt_index_parent_check('$idx->{name}', heapallindexed => true)"
					);
				}
				elsif ($idx->{am} eq 'gin')
				{
					$node->safe_psql('postgres',
						"SELECT gin_index_check('$idx->{name}')");
				}
			}
			Test::More::pass('indexes pass amcheck');
		},
};

# The visibility map must describe the table it ended up with.  Every
# relation the rotation could have rewritten gets checked, which is
# also how this avoids being handed a partitioned table: those are
# not in the rotation's list, and pg_visibility refuses them.
check visibility_map => {
		auto => 1,
		final => sub {
			my ($node, $ctx) = @_;
			$node->safe_psql('postgres',
				'CREATE EXTENSION IF NOT EXISTS pg_visibility');
			foreach my $table (@{ $ctx->{tables} })
			{
				my $bad = $node->safe_psql(
					'postgres', qq(
					SELECT (SELECT COUNT(*) FROM pg_check_visible('$table'))
						+ (SELECT COUNT(*) FROM pg_check_frozen('$table'))));
				Test::More::is($bad, '0',
					"the visibility map matches the heap for $table");
			}
		},
};

# A cancelled or completed REPACK must not leave its transient slot
# behind, and logical decoding must have been switched off again.
check no_slot_leak => {
		auto => 1,
		final => sub {
			my ($node, $ctx) = @_;
			# A subscription owns a slot on the publisher for as long as
			# it exists, and that slot has no row here to recognize it by,
			# so compare against what was there before the workload
			# started: REPACK's transient slot is what must be gone.
			Test::More::is(
				$node->safe_psql('postgres', $ctx->{slot_query}),
				$ctx->{baseline_slots},
				'no replication slot leaked');
		},
};

# REPACK CONCURRENTLY turns logical decoding on for as long as it
# needs to decode, and must turn it back off.  Leaving it on costs
# every writer the extra WAL for no reason, and no invariant in the
# suite can see it, so ask directly.  The checkpointer does the
# lowering, so it does not happen the instant the command ends.
check decoding_disabled => {
		auto => 1,
		# With wal_level = logical there is nothing to switch back to.
		requires => { env => ['wal_replica'] },
		final => sub {
			my ($node, $ctx) = @_;
			Test::More::ok(
				$node->poll_query_until(
					'postgres',
					q(SELECT current_setting('effective_wal_level') = 'replica')
				),
				'logical decoding was switched back off');
		},
};

# A cancelled build leaves an invalid index behind: that is
# documented, and the cancellation environment expects it.  What is
# not acceptable is one that cannot then be dropped, which is how a
# half-finished DROP INDEX CONCURRENTLY used to strand an index for
# good.  Indexes that belong to a constraint are skipped, since DROP
# INDEX is not how those come out.
check invalid_indexes_droppable => {
		auto => 1,
		final => sub {
			my ($node, $ctx) = @_;
			my @left = grep { $_ ne '' } split /\n/,
			  $node->safe_psql(
				'postgres', q(
				SELECT i.indexrelid::regclass::text
				FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
				WHERE NOT i.indisvalid
					AND c.relnamespace = 'public'::regnamespace
					AND NOT EXISTS (SELECT 1 FROM pg_constraint con
									WHERE con.conindid = i.indexrelid)));

			foreach my $idx (@left)
			{
				# A REPACK worker can still be finishing as the workload
				# ends, so this drop can still lose a deadlock.  Losing
				# one says nothing about whether the index is droppable,
				# which is the whole question here.
				# IF EXISTS because dropping one of these can take
				# another with it: a transient index built for a
				# partitioned index goes away with its parent, and the
				# list was taken before any of them were dropped.
				my $ok =
				  eval { _retry_on_deadlock($node, "DROP INDEX IF EXISTS $idx"); 1 };
				Test::More::ok($ok, "invalid index $idx could be dropped")
				  or Test::More::diag($@);
			}
			Test::More::pass(
				'no invalid index was left behind that could not be dropped');
		},
};

1;
