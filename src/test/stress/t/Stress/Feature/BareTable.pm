
# Copyright (c) 2026, PostgreSQL Global Development Group

# A table with no indexes and an event trigger that needs a
# snapshot, for the early returns.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::BareTable;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

# A table with no indexes at all, and an event trigger whose
# function needs a snapshot.  REINDEX has nothing to do on such a
# table and returns early, and an event trigger firing afterwards is
# what notices if that early return left the snapshot stack wrong.
# Nothing else here has either piece: every table carries at least a
# primary key, and there are no event triggers.
schema bare_table_event_trigger => {
		setup => q(
			CREATE TABLE pgb_bare(a int);
			INSERT INTO pgb_bare SELECT g FROM generate_series(1, 100) g;
			CREATE FUNCTION pgb_evt() RETURNS event_trigger
			LANGUAGE plpgsql AS $$
			DECLARE n bigint;
			BEGIN
				-- Reading a catalog is the point: it needs a snapshot.
				SELECT count(*) INTO n FROM pg_class;
			END $$;
			CREATE EVENT TRIGGER pgb_evt_trg ON ddl_command_end
				EXECUTE FUNCTION pgb_evt();
		),
		# Reached through reindex_schema_concurrently, which walks every
		# table in the schema including this one.  Not in the rotation
		# itself: it has no primary key for reindex_pkey_concurrently.
		tables => [],
};

# REINDEX on a table that has no indexes.  There is nothing to
# rebuild, so the command returns early -- and that early return is
# the one place the snapshot stack can be left wrong, which the
# event trigger firing afterwards is there to notice.  It cannot go
# through reindex_table_concurrently, because that walks the
# rotation's tables and this one has no primary key for the rest of
# the rotation to work with.
ddl reindex_bare_table => {
		requires => { schema => ['bare_table_event_trigger'] },
		variants => sub {
			return (
				{
					table => 'pgb_bare',
					stmts => [
						# "has no indexes that can be reindexed
						# concurrently" is a NOTICE, and the run insists
						# on a silent stderr.
						'SET client_min_messages = warning;',
						'REINDEX TABLE CONCURRENTLY pgb_bare;',
						'RESET client_min_messages;'
					]
				});
		},
};

# The statistics functions must refuse an invalid index rather than
# reading it.  A failed concurrent build leaves one behind, and its
# storage may be torn or half-written, so pgstatindex reading it is
# how a "can\'t happen" corruption error gets reported for something
# that is merely incomplete.  The invalid index is made here rather
# than waited for: a unique build over duplicate values fails and
# leaves exactly one.
check pgstat_rejects_invalid_index => {
		# Makes its invalid index on pgb_bare, which that schema supplies.
		requires => { schema => ['bare_table_event_trigger'] },
		final => sub {
			my ($node, $ctx) = @_;

			$node->safe_psql('postgres',
				'CREATE EXTENSION IF NOT EXISTS pgstattuple');
			$node->psql(
				'postgres',
				'INSERT INTO pgb_bare VALUES (1), (1);'
				  . 'CREATE UNIQUE INDEX CONCURRENTLY pgb_bare_uniq'
				  . ' ON pgb_bare(a);',
				on_error_stop => 0);

			my $invalid = $node->safe_psql(
				'postgres', q(
				SELECT COUNT(*) FROM pg_index i
				WHERE i.indexrelid = to_regclass('pgb_bare_uniq')
				  AND NOT i.indisvalid));
			Test::More::is($invalid, '1',
				'the failed unique build left an invalid index');

			my (undef, undef, $err) =
			  $node->psql('postgres', "SELECT pgstatindex('pgb_bare_uniq')",
				on_error_stop => 0);
			Test::More::like($err, qr/is not valid/,
				'pgstatindex refuses an invalid index');

			$node->safe_psql('postgres',
				'DROP INDEX IF EXISTS pgb_bare_uniq');
		},
};

1;
