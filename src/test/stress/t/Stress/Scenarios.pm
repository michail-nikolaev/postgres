
# Copyright (c) 2026, PostgreSQL Global Development Group

=pod

=head1 NAME

Stress::Scenarios - the catalogue of stress scenarios

=head1 DESCRIPTION

A scenario names the plugins it is built from.  Each one has a stub
under t/ so that meson and prove see a stable test name, run it in
parallel with the others and keep its logs apart; the stub is a few
lines and everything that describes the scenario lives here.

Fields:

  schema           schema plugins; the first is the loader, the rest
                   decorate it
  pgbench_scale    scale factor for the base load (default 1)
  indexes          indexes to build before the run
  load             workload scripts, mixed by their weights
  ddl              the commands the DDL client rotates through
  ddl_concurrency  how many may be in flight: 1, N, or 'none'
  checks           invariants, checked during and/or after the run
  env              which cluster to run against
  conf             extra postgresql.conf lines for this scenario
  pgbench_args     extra pgbench arguments
  clients          pgbench clients
  duration         seconds at stressval 1 (default 6)
  tags             'ci' for the default run; everything runs in soak mode

See PORTING for which of the old hand-written tests each one covers.

=cut

package Stress::Scenarios;

use strict;
use warnings FATAL => 'all';

use Exporter 'import';

our @EXPORT_OK = qw(%SCENARIOS);

# The rotation almost every scenario uses.
my @STANDARD_DDL = (
	'repack_concurrently', 'repack_using_index',
	'reindex_table_concurrently', 'reindex_index_concurrently',
	'drop_create_index');

our %SCENARIOS = (
	# The smallest scale there is: one branch row, so every transaction
	# in the mix collides on it, and the DDL runs against tables that are
	# being modified as fast as the lock manager allows.
	repack_dml_s1 => {
		schema => ['pgbench'],
		pgbench_scale => 1,
		indexes => [ 'btree_abalance', 'btree_history_delta' ],
		load => [ 'tpcb_like', 'row_lock' ],
		ddl => [@STANDARD_DDL],
		ddl_concurrency => 1,
		checks => [ 'balances', 'index_vs_seq', 'amcheck', 'no_slot_leak' ],
		env => 'standalone',
		clients => 30,
		tags => ['ci'],
	},

	# The same, but with indexes large enough to be multi-level, so a
	# concurrent build has real work to do and page splits are routine.
	repack_dml_s50 => {
		schema => ['pgbench'],
		pgbench_scale => 50,
		indexes => [ 'btree_abalance', 'partial_abalance', 'expr_abalance' ],
		load => ['tpcb_like'],
		ddl => [@STANDARD_DDL],
		ddl_concurrency => 1,
		checks => [ 'balances', 'index_vs_seq', 'amcheck' ],
		env => 'standalone',
		clients => 30,
		tags => ['ci'],
	},

	# Every access method's index built, rebuilt and dropped concurrently
	# while the columns they cover are rewritten.
	access_methods => {
		schema => [ 'pgbench', 'am_columns' ],
		load => [ 'tpcb_like', 'am_churn' ],
		# REPACK orders a table by an index, which only btree can do, so
		# the index-ordered variant is left out here.
		ddl => [ 'repack_concurrently', 'reindex_table_concurrently',
			'reindex_index_concurrently', 'drop_create_index' ],
		ddl_concurrency => 1,
		checks => [ 'balances', 'amcheck' ],
		env => 'standalone',
		clients => 20,
		tags => ['ci'],
	},

	# Row locks held across the DDL, savepoints and subxid-cache
	# overflow, and cursors held open across it.
	locks_and_subxacts => {
		schema => [ 'pgbench', 'ledger' ],
		load => [ 'tpcb_like', 'row_lock', 'subxact_churn', 'cursor_hold' ],
		ddl => [@STANDARD_DDL],
		ddl_concurrency => 1,
		checks =>
		  [ 'balances', 'ledger_sum', 'row_lock_durability', 'amcheck' ],
		env => 'standalone',
		clients => 30,
		tags => ['ci'],
	},

	# Unique keys under upserts and MERGE, and inserts whose commit order
	# is pinned by an advisory lock.
	unique_and_gapless => {
		schema => [ 'pgbench', 'upsert_keys', 'gapless' ],
		load => [ 'tpcb_like', 'upsert_merge', 'serial_insert' ],
		ddl => [@STANDARD_DDL],
		ddl_concurrency => 1,
		checks => [ 'distinct_keys', 'gapless_count', 'amcheck' ],
		env => 'standalone',
		clients => 30,
		tags => ['ci'],
	},

	# Out-of-line values, stored generated columns and an exclusion
	# constraint: three things REPACK has to reproduce exactly when it
	# re-applies what it decoded.
	rewrite_fidelity => {
		schema => [ 'pgbench', 'toast', 'generated', 'exclusion_slot' ],
		load => [ 'tpcb_like', 'toast_rewrite', 'generated_update',
			'exclusion_churn' ],
		# REINDEX will not rebuild an exclusion constraint's index
		# concurrently, so that one is left out of the rotation here; the
		# blocking rebuild covers it instead.
		ddl => [ 'repack_concurrently', 'reindex_table_concurrently',
			'drop_create_index' ],
		ddl_concurrency => 1,
		checks => [ 'toast_md5', 'generated_matches', 'distinct_slots',
			'amcheck' ],
		env => 'standalone',
		clients => 20,
		tags => ['ci'],
	},

	# Foreign key checks, which resolve the parent through the index
	# behind its primary key, while that index is rebuilt underneath
	# them.
	foreign_keys => {
		schema => [ 'pgbench', 'fk_child' ],
		load => [ 'tpcb_like', 'fk_churn' ],
		ddl => [@STANDARD_DDL],
		ddl_concurrency => 1,
		checks => [ 'balances', 'no_orphans', 'amcheck' ],
		env => 'standalone',
		clients => 20,
		tags => ['ci'],
	},

	# Partitions attached and detached concurrently, and a partitioned
	# index built the documented way, while DML routes through the
	# parent.
	partitions => {
		schema => [ 'pgbench', 'partitioned' ],
		load => [ 'tpcb_like', 'partition_dml' ],
		ddl => [
			'repack_concurrently', 'reindex_table_concurrently',
			'detach_partition_concurrently', 'partitionwise_index_build'
		],
		ddl_concurrency => 1,
		checks => [ 'partition_sum', 'balances' ],
		env => 'standalone',
		clients => 20,
		tags => ['ci'],
	},

	# VACUUM alongside the rebuilds, with index-only scans reading
	# through the visibility map it maintains.
	vacuum_and_ios => {
		schema => ['pgbench'],
		indexes => [ 'covering_aid', 'btree_abalance' ],
		load => ['tpcb_like'],
		ddl => [ @STANDARD_DDL, 'vacuum' ],
		ddl_concurrency => 1,
		checks =>
		  [ 'balances', 'ios_vs_seq', 'visibility_map', 'amcheck' ],
		env => 'autovacuum',
		clients => 20,
		tags => ['ci'],
	},

	# Bulk loads, two-phase commit and a rewriting ALTER TABLE, which
	# hands the table back with every index replaced.
	bulk_and_rewrite => {
		schema => [ 'pgbench', 'ledger' ],
		load => [ 'tpcb_like', 'bulk_copy', 'twophase' ],
		ddl => [ @STANDARD_DDL, 'alter_table_rewrite' ],
		ddl_concurrency => 1,
		checks => [ 'balances', 'ledger_sum', 'amcheck' ],
		env => 'standalone',
		clients => 20,
		tags => ['ci'],
	},

	# Cached plans held across the DDL that must invalidate them: the
	# prepared protocol, generic plans and a PL/pgSQL function's own
	# plan cache at once.  The materialized view refresh runs here too,
	# since it is another thing whose plan and contents must keep up.
	plancache_and_matview => {
		schema => [ 'pgbench', 'ledger', 'matview' ],
		load => [ 'tpcb_like', 'plancache' ],
		ddl => [ @STANDARD_DDL, 'refresh_matview_concurrently' ],
		ddl_concurrency => 1,
		checks => [ 'ledger_sum', 'matview_matches', 'amcheck' ],
		env => 'standalone',
		conf => ['plan_cache_mode = force_generic_plan'],
		pgbench_args => '--protocol=prepared',
		clients => 20,
		tags => ['ci'],
	},

	# Several CONCURRENTLY commands in flight at once on different
	# tables, each taking a transient slot of its own and switching
	# logical decoding on and off.  wal_level = replica, so those
	# switches are real.
	overlapping_ddl => {
		schema => [ 'pgbench', 'ledger' ],
		load => [ 'tpcb_like', 'balanced_pair' ],
		ddl => [@STANDARD_DDL],
		ddl_concurrency => 4,
		checks => [ 'balances', 'ledger_sum', 'amcheck', 'no_slot_leak' ],
		env => 'wal_replica',
		clients => 30,
		tags => ['ci'],
	},

	# Killed and restarted while the commands are in flight, so their
	# cleanup happens through crash recovery.
	crash_recovery => {
		schema => [ 'pgbench', 'ledger' ],
		indexes => ['btree_abalance'],
		load => [ 'tpcb_like', 'balanced_pair' ],
		ddl => [@STANDARD_DDL],
		ddl_concurrency => 1,
		checks => [ 'balances', 'ledger_sum', 'amcheck', 'no_slot_leak' ],
		env => 'crash_loop',
		clients => 20,
		tags => ['ci'],
	},

	# The commands cancelled partway, so that their own cleanup paths run
	# rather than recovery's.
	cancellation => {
		schema => [ 'pgbench', 'ledger' ],
		indexes => ['btree_abalance'],
		load => [ 'tpcb_like', 'balanced_pair' ],
		ddl => [@STANDARD_DDL],
		ddl_concurrency => 1,
		checks => [ 'balances', 'ledger_sum', 'amcheck', 'no_slot_leak' ],
		env => 'cancellation',
		clients => 20,
		tags => ['ci'],
	},

	# A hot standby replaying the rebuilds while it serves the checks,
	# then taking over.
	standby => {
		schema => ['pgbench'],
		indexes => ['btree_abalance'],
		load => ['tpcb_like'],
		ddl => [@STANDARD_DDL],
		ddl_concurrency => 1,
		checks => [ 'balances', 'amcheck' ],
		env => 'standby',
		clients => 20,
		tags => ['ci'],
	},

	# Logical replication applying the workload while the published
	# tables are rebuilt under the decoding.
	subscription => {
		schema => ['pgbench'],
		indexes => ['btree_abalance'],
		load => ['tpcb_like'],
		ddl => [@STANDARD_DDL],
		ddl_concurrency => 1,
		checks => [ 'balances', 'amcheck' ],
		env => 'subscription',
		clients => 20,
		tags => ['ci'],
	},

	# The same commands with a lock table small enough to run out, which
	# they are heavy users of.
	lock_exhaustion => {
		schema => [ 'pgbench', 'ledger' ],
		indexes => [ 'btree_abalance', 'partial_abalance', 'expr_abalance',
			'covering_aid' ],
		load => [ 'tpcb_like', 'balanced_pair' ],
		ddl => [@STANDARD_DDL],
		ddl_concurrency => 1,
		checks => [ 'ledger_sum', 'amcheck' ],
		env => 'lock_exhaustion',
		clients => 20,
		tags => ['ci'],
	},
);

1;
