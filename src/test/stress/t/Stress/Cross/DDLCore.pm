
# Copyright (c) 2026, PostgreSQL Global Development Group

# The DDL rotation everything shares: the CONCURRENTLY commands
# aimed at whatever tables and indexes the scenario has.
#
# See Stress::Registry for what each declaration means.

package Stress::Cross::DDLCore;

use strict;
use warnings FATAL => 'all';

use Exporter 'import';
use Test::More;
use Stress::Registry ':declare';

our @EXPORT_OK = qw(_verify_stmts _unpartitioned_indexes);

# Verify a table's indexes immediately after a command rebuilt them,
# rather than only once the run is over.
#
# Checking at the end says an index was corrupted somewhere in six
# seconds of commands.  Checking here says which command did it -- and
# catches damage that a later rebuild would have repaired before any
# final check could see it, which is the case that matters: a rotation
# that repacks the same table every few hundred milliseconds is also a
# rotation that keeps overwriting the evidence.
#
# This uses bt_index_check rather than bt_index_parent_check, which the
# final check still does.  Both fingerprint the index and look for heap
# tuples missing from it -- the class of damage these commands cause --
# but bt_index_check takes AccessShareLock where the parent check takes
# ShareLock.  Inside a running workload that difference is the whole
# point: the parent check would stop every writer for the duration of a
# heap scan, several times a second, and a stress test that spends its
# time blocked is not stressing anything.
sub _verify_stmts
{
	my ($ctx, $table) = @_;

	# Driven off pg_index rather than naming the index directly, so that
	# an index which is not there to check produces no rows instead of an
	# error: to_regclass gives NULL for one a concurrent DROP has taken
	# away, and indisvalid excludes the invalid leftovers a cancelled
	# build is documented to leave, which amcheck refuses outright.
	return map {
		"SELECT bt_index_check(i.indexrelid, heapallindexed => true) "
		  . "FROM pg_index i WHERE i.indexrelid = to_regclass('$_->{name}') "
		  . 'AND i.indisvalid;'
	}
	  grep { $_->{am} eq 'btree' && $_->{table} eq $table }
	  _unpartitioned_indexes($ctx);
}

# Indexes the concurrent index commands can be aimed at.  Neither CREATE
# INDEX CONCURRENTLY nor REINDEX INDEX CONCURRENTLY accepts a partitioned
# index, and a decorator that partitions one of the tables takes it out
# of the rotation's table list, so an index still naming it is one on a
# partitioned parent.
sub _unpartitioned_indexes
{
	my ($ctx) = @_;
	my %ok = map { $_ => 1 } @{ $ctx->{tables} };
	return grep { $ok{ $_->{table} } } @{ $ctx->{indexes} };
}

ddl repack_concurrently => {
		variants => sub {
			my ($ctx) = @_;
			return map {
				{
					table => $_,
					stmts =>
					  [ "REPACK (CONCURRENTLY) $_;", _verify_stmts($ctx, $_) ]
				}
			} @{ $ctx->{tables} };
		},
};

ddl repack_using_index => {
		variants => sub {
			my ($ctx) = @_;
			return map {
				{
					table => $_->{table},
					stmts => [
						"REPACK (CONCURRENTLY) $_->{table} USING INDEX $_->{name};",
						_verify_stmts($ctx, $_->{table})
					]
				}
			} grep { $_->{am} eq 'btree' && !$_->{partial} }
			  _unpartitioned_indexes($ctx);
		},
};

ddl reindex_table_concurrently => {
		variants => sub {
			my ($ctx) = @_;
			# REINDEX will not rebuild an exclusion constraint's index
			# concurrently and says so with a WARNING.  That is expected
			# here -- it still rebuilds every other index of the table --
			# but it would land on pgbench's stderr, where the run insists
			# on silence, so keep it quiet where such a constraint exists.
			my @quiet =
			  $ctx->{has_exclusion}
			  ? ('SET client_min_messages = error;')
			  : ();
			my @restore =
			  $ctx->{has_exclusion} ? ('RESET client_min_messages;') : ();
			return map {
				{
					table => $_,
					stmts => [
						@quiet, "REINDEX TABLE CONCURRENTLY $_;",
						@restore, _verify_stmts($ctx, $_)
					]
				}
			} @{ $ctx->{tables} };
		},
};

ddl reindex_index_concurrently => {
		variants => sub {
			my ($ctx) = @_;
			return map {
				{
					table => $_->{table},
					stmts => [
						"REINDEX INDEX CONCURRENTLY $_->{name};",
						_verify_stmts($ctx, $_->{table})
					]
				}
			} _unpartitioned_indexes($ctx);
		},
};

# The primary keys, rebuilt on their own.
#
# reindex_index_concurrently walks the indexes a scenario declared,
# and a primary key is never one of those -- it arrives with the
# table.  So the only thing that used to rebuild one was
# reindex_table_concurrently, which rebuilds every index of the table
# and therefore swaps the primary key at a fraction of the rate.
# That matters because the primary key is what a foreign key
# constraint resolves through and what a replica identity defaults
# to, so the races around those are races against this swap.
ddl reindex_pkey_concurrently => {
		variants => sub {
			my ($ctx) = @_;
			return map {
				{
					table => $_,
					stmts => [
						"REINDEX INDEX CONCURRENTLY ${_}_pkey;",
						# Checked in the run rather than only at the end,
						# so that a rebuild which loses rows is reported
						# next to the rebuild that lost them.
						"SELECT bt_index_check(i.indexrelid, "
						  . "heapallindexed => true) FROM pg_index i "
						  . "WHERE i.indrelid = to_regclass('$_') "
						  . 'AND i.indisprimary AND i.indisvalid;'
					]
				}
			} @{ $ctx->{tables} };
		},
};

ddl drop_create_index => {
		variants => sub {
			my ($ctx) = @_;
			return map {
				{
					table => $_->{table},
					stmts => [
						"DROP INDEX CONCURRENTLY $_->{name};",
						"CREATE INDEX CONCURRENTLY $_->{name} $_->{defn};",
						_verify_stmts($ctx, $_->{table})
					]
				}
			} _unpartitioned_indexes($ctx);
		},
};

# A constraint added unvalidated and then validated.  Neither takes
# an exclusive lock, so both run against a live workload, and
# VALIDATE scans the whole table while the rotation rewrites it.
ddl add_validate_constraint => {
		# Kept out of the combinations soak invents.  ADD and DROP
		# CONSTRAINT need AccessExclusiveLock, which queues ahead of every
		# writer, and at scale 1 the pgbench_branches row is contended
		# enough that the queue does not drain: the writers wait out their
		# own lock_timeout and the run reports a starvation that says
		# nothing about the server.  REGRESSIONS records the class.  The
		# hand-written scenario that uses it is tuned for it.
		catalogue_only => 1,
		variants => sub {
			my ($ctx) = @_;
			return map {
				my $t = $_;
				{
					table => $t,
					stmts => [
						# The drop is normally a no-op and would say so on
						# stderr, where the run insists on silence.
						'SET client_min_messages = warning;',
						"ALTER TABLE $t DROP CONSTRAINT IF EXISTS ${t}_stress_chk;",
						'RESET client_min_messages;',
						# ADD and DROP CONSTRAINT need
						# AccessExclusiveLock; VALIDATE does not, and is
						# the part worth running against a live workload
						# anyway.
						"SELECT pgb_ddl_bounded('ALTER TABLE $t ADD "
						  . "CONSTRAINT ${t}_stress_chk CHECK (true) NOT VALID');",
						"ALTER TABLE $t VALIDATE CONSTRAINT ${t}_stress_chk;",
						"SELECT pgb_ddl_bounded('ALTER TABLE $t DROP "
						  . "CONSTRAINT ${t}_stress_chk');"
					]
				}
			} @{ $ctx->{tables} };
		},
};

# One command rebuilding every index in the schema, which sequences
# its locks differently from the per-index and per-table forms.
ddl reindex_schema_concurrently => {
		solo => 1,
		variants => sub {
			return (
				{
					table => 'public',
					stmts => ['REINDEX SCHEMA CONCURRENTLY public;']
				});
		},
};

# VACUUM's index cleanup and freezing have to coexist with the
# concurrent rebuilds and the tuple movement.
ddl vacuum => {
		variants => sub {
			my ($ctx) = @_;
			return map {
				{ table => $_, stmts => ["VACUUM $_;"] },
				  { table => $_, stmts => ["VACUUM (FREEZE) $_;"] },
				  { table => $_, stmts => ["VACUUM (ANALYZE) $_;"] }
			} @{ $ctx->{tables} };
		},
};

# A rewriting ALTER TABLE takes AccessExclusiveLock and gives the
# table a new relfilenode with every index rebuilt, so a REINDEX or
# REPACK that was waiting resumes against a table of a new shape.
ddl alter_table_rewrite => {
		# Adding and dropping a column changes the shape of a published
		# table, and the subscriber does not follow: apply then fails on
		# every change until the column is gone again, and the
		# subscription never catches up.
		conflicts => { topology => ['subscription'] },
		# Changing a column's type changes the result type of everything
		# selecting it, and a client holding a cached statement across
		# that gets "cached plan must not change result type" -- correct
		# of the server, and something a real client re-prepares over.
		# pgbench does not, so keep this to the protocol that sends the
		# text every time.
		simple_protocol_only => 1,
		variants => sub {
			my ($ctx) = @_;
			return (
				{
					table => 'pgbench_accounts',
					stmts => [
						'ALTER TABLE pgbench_accounts ALTER COLUMN abalance TYPE bigint;',
						'ALTER TABLE pgbench_accounts ALTER COLUMN abalance TYPE int;'
					]
				},
				{
					table => 'pgbench_tellers',
					stmts => [
						'ALTER TABLE pgbench_tellers ADD COLUMN pad text DEFAULT random()::text;',
						'ALTER TABLE pgbench_tellers DROP COLUMN pad;'
					]
				});
		},
};

1;
