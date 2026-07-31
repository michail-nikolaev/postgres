
# Copyright (c) 2026, PostgreSQL Global Development Group

=pod

=head1 NAME

Stress::Plugins - the pieces a stress scenario is built from

=head1 DESCRIPTION

Everything a scenario is made of lives in one of the registries here.  A
scenario names entries from each; C<Stress::Run> assembles them into a
node, a set of pgbench scripts and a set of final checks.

Each registry is a hash of name => definition.  A definition may declare

  requires => { schema => [...] }   entries it cannot work without
  conflicts => { ... }              entries it must not be combined with

which C<Stress::Run::stress_run()> validates before anything is created,
so an impossible combination fails at once and says why, rather than
halfway through a run.

The registries are:

  %SCHEMA    what tables exist: one loader plus any decorators
  %INDEXES   what is built on them
  %LOAD      what changes the data, preserving some invariant
  %DDL       what runs concurrently with that
  %CHECK     what must hold regardless
  %ENVS      what the cluster looks like

A C<script> in a load or a check may be a string, or a sub called with
the scenario context when the values it needs are only known once the
schema exists.

=cut

package Stress::Plugins;

use strict;
use warnings FATAL => 'all';

use Exporter 'import';
use IPC::Run;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

our @EXPORT_OK =
  qw(%SCHEMA %INDEXES %LOAD %DDL %CHECK %ENVS %CHAOS %CHAOS_POINTS %MODIFIERS
  stress_repack_tolerated stress_rollback_prepared);

=pod

=head2 %CHAOS

A chaos profile widens the windows a race has to be lost in, without
changing anything the server decides.

Most of the windows this suite hunts are microseconds wide.  Repetition
does not find those: a scenario can run for hours and never place two
operations within a microsecond of each other.  What does find them is
making the window milliseconds wide for a small fraction of the
operations that reach it -- which is what jitter attached to an
injection point does, and what the probabilistic form of
debug_discard_caches does for cache-flush hazards.

C<points> maps an injection point to [ probability, min_us, max_us ].
C<discard_probability> is the chance of a forced cache flush at each
opportunity.  A profile is declared by a scenario as C<chaos>, is
orthogonal to the environment, and does nothing at all on a build
without injection points.

Two rules hold for every profile here, and both are load-bearing.
Sleeps only ever delay; a failure seen under chaos is a failure that
exists without it, which is what makes a chaos run worth reporting.  And
no sleep may approach lock_timeout: a lock request that dawdles at the
head of a queue stalls every writer behind it, which ends the run in a
cascade that tests nothing.

=cut

=pod

=head2 %MODIFIERS

A modifier is a set of GUCs at values that change how the server does
its work without changing what the work produces.  Where a chaos profile
widens a window, a modifier moves the whole run onto a different code
path: everything spills instead of fitting in memory, the planner picks a
different node, WAL is really flushed, a vacuum freezes at once.

The invariants have to hold under every one of them.  That is the whole
claim, and it is what makes them cheap coverage: a modifier costs no new
scenario, no new check, and no new plugin combination, and every scenario
in the catalogue can be run under each.

Two rules, both learned the hard way in this file.  A modifier may not
change results -- so nothing here touches isolation level, or a GUC that
decides what a query returns rather than how it gets there.  And a
modifier must not silently disable the thing a scenario exists to test:
turning off enable_indexscan would leave the index-only scan scenario
comparing two sequential scans and passing for the wrong reason, which is
why those checks now pin the GUCs they need and assert the plan they got.

=cut

our %MODIFIERS = (
	# The default: whatever the environment already set.
	none => {},

	# Durability actually engaged.  A test cluster runs with fsync off, so
	# every scenario in this suite has always been testing the code path
	# where the WAL writes never wait for the disk.  full_page_writes is
	# pinned rather than changed -- its boot value is already on -- so that
	# this stays the durable end of the axis whatever the default becomes;
	# the variation in the other direction is no_full_page_writes.
	durable => {
		# Slow enough that the lock timeout has to be scaled with it.
		slow => 1,
		# And not combinable with the cancellation environment.  That
		# environment needs its writers to be numerous and quick, so
		# that there is something in flight to interrupt; on a server
		# this slow a DDL command's AccessExclusiveLock request sits at
		# the head of the queue and the writers behind it wait out their
		# own timeout instead.  Capping the clients was tried and is
		# worse: it leaves the environment with nothing to cancel, and
		# its own check then fails.
		conflicts => { env => ['cancellation'] },
		conf => [
			'fsync = on',
			'full_page_writes = on',
			'wal_log_hints = on',
			'wal_compression = on',
			'synchronous_commit = on',
		],
	},

	# Nothing fits in memory.  Sorts spill, hash aggregates spill, hash
	# joins batch, and the index builds the rotation runs use their
	# external path rather than an in-memory one -- which is a different
	# tuplesort code path being driven against a live workload.
	spill => {
		# Slow enough that the lock timeout has to be scaled with it.
		slow => 1,
		# And not combinable with the cancellation environment.  That
		# environment needs its writers to be numerous and quick, so
		# that there is something in flight to interrupt; on a server
		# this slow a DDL command's AccessExclusiveLock request sits at
		# the head of the queue and the writers behind it wait out their
		# own timeout instead.  Capping the clients was tried and is
		# worse: it leaves the environment with nothing to cancel, and
		# its own check then fails.
		conflicts => { env => ['cancellation'] },
		conf => [
			'work_mem = 64kB',
			'maintenance_work_mem = 1MB',
			'hash_mem_multiplier = 1.0',
			'temp_buffers = 800kB',
		],
	},

	# The planner denied its usual answers.  None of these changes a
	# result; each moves the executor onto another node.  enable_indexscan
	# is deliberately absent: it gates index-only scans too, and two
	# scenarios depend on getting one.
	other_plans => {
		conf => [
			'enable_seqscan = off',
			'enable_hashagg = off',
			'enable_hashjoin = off',
			'enable_material = off',
			'enable_memoize = off',
			'enable_incremental_sort = off',
			'enable_partition_pruning = off',
		],
	},

	# Parallelism wherever it can be had.  The rotation's index builds and
	# the checks' aggregates are what pick it up.
	parallel => {
		# Not with the expr_xid index.  That index's expression assigns a
		# transaction id every time it is evaluated -- deliberately, to
		# make a concurrent build keep up with one -- and amcheck has to
		# evaluate it to fingerprint the index.  With parallelism forced
		# even onto a one-row catalog scan, that evaluation happens while
		# the leader is in parallel mode, where assigning an XID is an
		# error.  The expression is declared IMMUTABLE and is not, which is
		# the suite's own doing, so the pair is declared incompatible
		# rather than blamed on the server.
		conflicts => { indexes => ['expr_xid'] },
		conf => [
			'min_parallel_table_scan_size = 0',
			'min_parallel_index_scan_size = 0',
			'parallel_setup_cost = 0',
			'parallel_tuple_cost = 0',
			'max_parallel_workers_per_gather = 4',
			'max_parallel_maintenance_workers = 4',
		],
	},

	# Freezing and pruning as eagerly as possible, which is what decides
	# whether a concurrent build's horizon is respected.
	eager_vacuum => {
		conf => [
			'vacuum_freeze_min_age = 0',
			'vacuum_freeze_table_age = 0',
			'vacuum_multixact_freeze_min_age = 0',
			'vacuum_cost_delay = 0',
			'autovacuum_naptime = 1s',
			'autovacuum_vacuum_threshold = 1',
			'autovacuum_vacuum_scale_factor = 0.0',
		],
	},

	# Buffers evicted and checkpoints taken constantly, so a rewrite's
	# pages are written, evicted and read back rather than staying
	# resident.
	buffer_churn => {
		# One megabyte is a hundred and twenty-eight buffers, and a backend
		# pins several at a time, so this does not survive a scenario with
		# hundreds of clients: the workload dies with "no unpinned buffers
		# available", which is the modifier being wrong for the scenario
		# rather than anything about the server.  Found by a soak that gave
		# it to decoding_startup_race, which runs three hundred.
		max_clients => 50,
		# Slow enough that the lock timeout has to be scaled with it.
		slow => 1,
		# And not combinable with the cancellation environment.  That
		# environment needs its writers to be numerous and quick, so
		# that there is something in flight to interrupt; on a server
		# this slow a DDL command's AccessExclusiveLock request sits at
		# the head of the queue and the writers behind it wait out their
		# own timeout instead.  Capping the clients was tried and is
		# worse: it leaves the environment with nothing to cancel, and
		# its own check then fails.
		conflicts => { env => ['cancellation'] },
		conf => [
			'shared_buffers = 1MB',
			'bgwriter_delay = 10ms',
			'bgwriter_lru_maxpages = 1000',
			'checkpoint_timeout = 30s',
			'max_wal_size = 48MB',
			'checkpoint_completion_target = 0.1',
		],
	},

	# The node-tree self-checks.  Every parse and plan tree is copied,
	# and written out and read back, and compared -- so a node type whose
	# copy, out or read function is wrong, or which carries a field that
	# does not survive the round trip, is caught at the point it is built
	# rather than by whatever misbehaves later.
	#
	# A different detector from the rest of this file: chaos and the cache
	# discard find state that goes stale, this finds state that was never
	# written correctly.  The scenarios here build plans continuously --
	# cached plans, generic plans, partition pruning, expression indexes --
	# so there is plenty for it to check.
	node_tests => {
		# Roughly a factor of two, which is enough to matter.
		slow => 1,
		conf => [
			'debug_copy_parse_plan_trees = on',
			'debug_write_read_parse_plan_trees = on',
			'debug_raw_expression_coverage_test = on',
		],
	},

	# Data checksums off.  initdb in this tree turns them on, so every
	# scenario has been reading and writing pages with a checksum computed
	# and verified on each one; without them that whole path is skipped.
	# It is an initdb decision rather than a GUC, which is why a modifier
	# can carry init options.
	no_checksums => {
		init => { no_data_checksums => 1 },
	},

	# Full page writes off.  The boot value is on and nothing here changed
	# it, so the path this suite has never taken is the one where a page
	# modified after a checkpoint is not written to WAL in full -- which
	# is a different WAL record stream, and a different amount of it.
	no_full_page_writes => {
		conf => [ 'full_page_writes = off', ],
	},

	# Asynchronous IO turned off altogether.  io_method defaults to
	# 'worker' in this tree, so the synchronous path is the one no
	# scenario exercises unless it asks -- the reverse of what one might
	# expect.  Every one of these commands rewrites a relation and reads
	# it back, so which machinery those reads go through is a dimension in
	# its own right.
	aio_sync => {
		conf => [ 'io_method = sync', ],
	},

	# The worker method leaned on: more workers than the default, kept
	# alive, and a deeper queue, so the reads really are handed off rather
	# than completed inline.
	aio_worker_busy => {
		conf => [
			'io_method = worker',
			'io_min_workers = 4',
			'io_max_workers = 8',
			'io_max_concurrency = 128',
			'io_worker_idle_timeout = 1s',
			'io_worker_launch_interval = 1ms',
		],
	},

	# io_uring, which completes in the issuing backend rather than through
	# a worker and is a wholly separate implementation.  Gated on the
	# build: io_method is a postmaster GUC, so an unsupported value cannot
	# be caught after startup the way conf_optional handles the rest -- the
	# server simply refuses to start.
	aio_uring => {
		requires_build => '#define USE_LIBURING 1',
		conf => [
			'io_method = io_uring',
			'io_max_concurrency = 128',
			'io_combine_limit = 16',
		],
	},

	# Reads issued through different machinery, and toast compressed by
	# the other algorithm.
	#
	# The compression setting is optional because it depends on how the
	# server was built: an unsupported value in postgresql.conf stops it
	# from starting, so anything build-dependent goes here and is applied
	# afterwards, where the refusal can be caught.
	io_variants => {
		conf => [
			'effective_io_concurrency = 16',
			'maintenance_io_concurrency = 16',
			'io_combine_limit = 8',
		],
		conf_optional => [ "default_toast_compression = 'lz4'", ],
	},

	# JIT for everything it can be used for.  Entirely optional: a build
	# without LLVM accepts jit = on and ignores it, and the cost settings
	# are meaningless there, so nothing is lost by trying.
	jit_always => {
		conf_optional => [
			'jit = on',
			'jit_above_cost = 0',
			'jit_inline_above_cost = 0',
			'jit_optimize_above_cost = 0',
		],
	},
);

our %CHAOS_POINTS = (
	# The commit window: between the flush that lets a decoder see a
	# commit and the CLOG update that lets an ordinary snapshot see it,
	# and between that update and the transaction leaving the procarray.
	# Reached by every commit, so the probability stays low; the sleeps
	# can be long, since what has to be outlasted is a whole REPACK
	# startup.
	'commit-before-clog-update' => { max_p => 0.15, max_us => 60_000 },
	'xact-end-before-procarray-clear' => { max_p => 0.15, max_us => 60_000 },

	# Catalog staleness: a lock held over a descriptor not yet built, an
	# invalidation not yet absorbed, a relcache entry half made.
	'relation-open-after-lock' => { max_p => 0.02, max_us => 10_000 },
	'accept-invalidation-messages' => { max_p => 0.02, max_us => 10_000 },
	'relcache-build-catalogs-read' => { max_p => 0.1, max_us => 20_000 },
	'catcache-list-miss-systable-scan-started' =>
	  { max_p => 0.1, max_us => 20_000 },
	'typecache-before-rel-type-cache-insert' =>
	  { max_p => 0.1, max_us => 20_000 },
	'inplace-before-pin' => { max_p => 0.2, max_us => 20_000 },
	'transaction-end-process-inval' => { max_p => 0.05, max_us => 10_000 },
	'invalidate-catalog-snapshot-end' => { max_p => 0.02, max_us => 10_000 },

	# A snapshot taken and not yet read with, an index list held over the
	# opening of its members.  The second is the planner's window on a
	# standby, where replay does not wait for the reader's lock.
	'transaction-snapshot-taken' => { max_p => 0.02, max_us => 10_000 },
	'relation-index-list-built' => { max_p => 0.05, max_us => 20_000 },

	# The phase changes of a concurrent build, each of which decides
	# something on the strength of a wait that has just finished.
	'wait-for-lockers-done' => { max_p => 1.0, max_us => 30_000 },
	'define-index-before-set-valid' => { max_p => 1.0, max_us => 30_000 },
	'reindex-conc-index-built' => { max_p => 1.0, max_us => 30_000 },
	'reindex-conc-index-safe' => { max_p => 1.0, max_us => 30_000 },
	'reindex-conc-index-not-safe' => { max_p => 1.0, max_us => 30_000 },
	'reindex-relation-concurrently-before-swap' =>
	  { max_p => 1.0, max_us => 30_000 },
	'reindex-relation-concurrently-before-set-dead' =>
	  { max_p => 1.0, max_us => 30_000 },
	'repack-concurrently-before-lock' => { max_p => 1.0, max_us => 30_000 },
	'detach-partition-before-finalize' => { max_p => 1.0, max_us => 30_000 },

	# Speculative insertion and the checks around it, which is where an
	# arbiter index that two transactions disagree about does its damage.
	'exec-insert-before-insert-speculative' =>
	  { max_p => 0.2, max_us => 10_000 },
	'check-exclusion-or-unique-constraint-no-conflict' =>
	  { max_p => 0.2, max_us => 10_000 },

	# Partition routing, where the ancestors of a partition are read and
	# then relied on.
	'exec-init-partition-before-open' => { max_p => 0.2, max_us => 10_000 },
	'exec-init-partition-after-get-partition-ancestors' =>
	  { max_p => 0.2, max_us => 10_000 },

	# Index page splits and deletions left incomplete, which is what a
	# scan or an amcheck run has to cope with meeting.
	'nbtree-leave-leaf-split-incomplete' => { max_p => 1.0, max_us => 20_000 },
	'nbtree-leave-internal-split-incomplete' =>
	  { max_p => 1.0, max_us => 20_000 },
	'nbtree-finish-incomplete-split' => { max_p => 1.0, max_us => 20_000 },
	'nbtree-leave-page-half-dead' => { max_p => 1.0, max_us => 20_000 },
	'nbtree-finish-half-dead-page-vacuum' =>
	  { max_p => 1.0, max_us => 20_000 },
	'gin-leave-leaf-split-incomplete' => { max_p => 1.0, max_us => 20_000 },
	'gin-leave-internal-split-incomplete' =>
	  { max_p => 1.0, max_us => 20_000 },
	'gin-finish-incomplete-split' => { max_p => 1.0, max_us => 20_000 },

	# A backward scan meeting a page that split, moved or was deleted
	# under it.  These are reached often, so they stay rare -- but this
	# is the one place where an ordinary read is walking the same pages a
	# concurrent build is rearranging, which is the class amcheck is the
	# detector for.
	'nbtree-walk-left' => { max_p => 0.05, max_us => 10_000 },
	'nbtree-walk-left-step-right' => { max_p => 0.5, max_us => 10_000 },
	'nbtree-walk-left-deleted' => { max_p => 0.5, max_us => 10_000 },
	'nbtree-walk-left-restart' => { max_p => 0.5, max_us => 10_000 },
	'nbtree-endpoint-empty' => { max_p => 1.0, max_us => 10_000 },
	'nbtree-first-empty' => { max_p => 1.0, max_us => 10_000 },

	# Multixact creation, which the row-locking loads reach whenever two
	# sessions lock the same row.
	'multixact-create-from-members' => { max_p => 0.2, max_us => 10_000 },

	# Decoding, reached by the subscription environment and by every
	# REPACK (CONCURRENTLY).
	'logical-decoding-activation' => { max_p => 1.0, max_us => 20_000 },
	'logical-replication-slot-advance-segment' =>
	  { max_p => 0.5, max_us => 10_000 },

	# Autovacuum starting up beside a build that is already running.
	'autovacuum-worker-start' => { max_p => 1.0, max_us => 20_000 },
	'autovacuum-start-parallel-vacuum' => { max_p => 1.0, max_us => 20_000 },

	# The choices a vacuum makes about cleanup and truncation, each
	# reached once per vacuum.
	'vacuum-index-cleanup-auto' => { max_p => 1.0, max_us => 20_000 },
	'vacuum-index-cleanup-enabled' => { max_p => 1.0, max_us => 20_000 },
	'vacuum-index-cleanup-disabled' => { max_p => 1.0, max_us => 20_000 },
	'vacuum-truncate-auto' => { max_p => 1.0, max_us => 20_000 },
	'vacuum-truncate-enabled' => { max_p => 1.0, max_us => 20_000 },
	'vacuum-truncate-disabled' => { max_p => 1.0, max_us => 20_000 },

	# Checkpoint timing against a build that has to survive one, and the
	# delay-checkpoint window a commit passes through.
	'commit-after-delay-checkpoint' => { max_p => 0.05, max_us => 10_000 },
	'checkpoint-before-old-wal-removal' => { max_p => 1.0, max_us => 20_000 },
	'create-checkpoint-initial' => { max_p => 1.0, max_us => 20_000 },
	'create-checkpoint-run' => { max_p => 1.0, max_us => 20_000 },

	# The horizon a vacuum decided on, before anything is removed on the
	# strength of it.
	'vacuum-cutoffs-computed' => { max_p => 1.0, max_us => 30_000 },

	# The online data checksum worker.  It walks every page of every
	# relation, and the relations are being rewritten underneath it by the
	# rotation, so the gap between counting a relation's blocks and
	# processing them is the one that matters.  Reached only by a scenario
	# that drives the transitions.
	'datachecksums-before-page' => { max_p => 0.25, max_us => 30_000 },
	'datachecksums-after-page' => { max_p => 0.25, max_us => 30_000 },
	'datachecksumsworker-startup-delay' => { max_p => 1.0, max_us => 20_000 },
	'datachecksumsworker-launcher-delay' => { max_p => 1.0, max_us => 20_000 },
	'datachecksums-enable-checksums-delay' =>
	  { max_p => 1.0, max_us => 20_000 },

	# The lock queue itself.  Short and rare, always: a request that
	# dawdles at the head of a queue stalls every writer behind it, and
	# lock_timeout is what ends the run.
	'lock-before-acquire' => { max_p => 0.0005, max_us => 2000 },
);

our %CHAOS = (
	# Named so a scenario can say it wants none.
	off => {},

	# Catalog and snapshot staleness, plus the build phase changes.  Wide
	# enough to shake the ordinary races, small enough to leave
	# throughput recognisable.
	light => {
		points => {
			'relation-open-after-lock' => [ 0.002, 100, 3000 ],
			'accept-invalidation-messages' => [ 0.001, 100, 3000 ],
			'transaction-snapshot-taken' => [ 0.002, 100, 3000 ],
			'relation-index-list-built' => [ 0.01, 200, 5000 ],
			'wait-for-lockers-done' => [ 0.5, 500, 5000 ],
			'define-index-before-set-valid' => [ 0.5, 500, 5000 ],
		},
		discard_probability => 0.001,
	},

	# Everything at once, including the commit window and the lock queue.
	# Throughput drops noticeably; this is for a hunt rather than for the
	# catalogue.
	heavy => {
		points => {
			'commit-before-clog-update' => [ 0.02, 1000, 20000 ],
			'xact-end-before-procarray-clear' => [ 0.02, 1000, 20000 ],
			'relation-open-after-lock' => [ 0.01, 500, 8000 ],
			'accept-invalidation-messages' => [ 0.01, 500, 8000 ],
			'relcache-build-catalogs-read' => [ 0.05, 500, 8000 ],
			'transaction-snapshot-taken' => [ 0.01, 500, 8000 ],
			'relation-index-list-built' => [ 0.05, 500, 8000 ],
			'wait-for-lockers-done' => [ 0.8, 1000, 15000 ],
			'define-index-before-set-valid' => [ 0.8, 1000, 15000 ],
			'reindex-relation-concurrently-before-swap' =>
			  [ 0.8, 1000, 15000 ],
			'exec-insert-before-insert-speculative' => [ 0.05, 500, 5000 ],
			# Short, and rare: see the rule about lock_timeout above.
			'lock-before-acquire' => [ 0.0002, 100, 1000 ],
		},
		discard_probability => 0.005,
	},

	# Aimed at the gap between a relcache entry being built from the
	# catalogs and the check for an invalidation absorbed while that was
	# happening -- and at how long a backend may then go on using the
	# stale entry before absorbing anything.
	relcache_probe => {
		points => {
			'relcache-build-catalogs-read' => [ 0.2, 2000, 20000 ],
			'accept-invalidation-messages' => [ 0.05, 2000, 20000 ],
		},
	},

	# Aimed at the online checksum worker: the gap between counting a
	# relation's blocks and writing each of them, which is when the
	# rotation can swap the relfilenode out from under it.
	checksums => {
		points => {
			'datachecksums-before-page' => [ 0.01, 500, 5000 ],
			'datachecksums-after-page' => [ 0.01, 500, 5000 ],
			'datachecksumsworker-startup-delay' => [ 1.0, 1000, 15000 ],
		},
	},

	# The checksum worker held open as wide as the caps allow.  A quarter
	# of its pages wait up to thirty milliseconds before being read and
	# again after being dirtied, so a relation is in the middle of being
	# checksummed for most of the time the worker is on it -- which is the
	# state a concurrent rewrite has to be safe against.
	checksums_heavy => {
		points => {
			'datachecksums-before-page' => [ 0.25, 5000, 30000 ],
			'datachecksums-after-page' => [ 0.25, 5000, 30000 ],
			'datachecksumsworker-startup-delay' => [ 1.0, 5000, 20000 ],
			'datachecksumsworker-launcher-delay' => [ 1.0, 5000, 20000 ],
			'relation-open-after-lock' => [ 0.01, 500, 8000 ],
			'transaction-snapshot-taken' => [ 0.01, 500, 8000 ],
		},
		discard_probability => 0.002,
	},

	# Only the probabilistic cache discard, and nothing else.  Used to
	# tell a crash that needs a forced catalog flush from one that needs a
	# widened window: they are different findings.
	discard_only => {
		discard_probability => 0.002,
	},

	# Aimed at one window: the gap between the flush that lets a decoder
	# see a commit and the CLOG update that lets an ordinary snapshot see
	# it.  Sized from the measurements in REGRESSIONS -- REPACK needs the
	# stall to outlast a 6-15ms gap, and nature almost never provides one.
	decoding => {
		points => { 'commit-before-clog-update' => [ 0.125, 20000, 60000 ] },
	},
);

=pod

=head2 The REPACK tolerance

REPACK (CONCURRENTLY) is not MVCC-safe yet: a snapshot that spans its
relfilenode swap can find the table empty.  Every check that reads a
relation the rotation may repack has to allow for that, and nothing
else: an empty read is tolerated, a partial or otherwise wrong one is
not.

C<stress_repack_tolerated($count_expr)> returns the SQL condition that
expresses it, so the caveat lives in one place.  Setting
C<stress_strict_mvcc=1> in PG_TEST_EXTRA turns the tolerance off, which
is how to find out whether REPACK has become MVCC-safe; when it has,
this function and its callers are what has to be removed.

=cut

sub stress_repack_tolerated
{
	my ($count_expr) = @_;
	return '' if ($ENV{PG_TEST_EXTRA} // '') =~ /\bstress_strict_mvcc=1\b/;
	return "$count_expr = 0 OR ";
}

# Extra nodes need names of their own, and soak mode builds many of them
# in one test.
my $node_seq = 0;

# Rows in the tables the decorators add.  Small enough to stay cheap,
# large enough that an index over one has more than a single page.
my $NROWS = 10_000;
my $NKEYS = 2_000;
my $NSLOTS = 2_000;

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

=pod

=head2 %SCHEMA

The first entry a scenario names is the loader; the rest are
decorators.  A decorator adds its own table -- and with it its own
invariant -- on top of the pgbench schema, so that a scenario is always
some specialized dimension running alongside an ordinary workload.

  init      loader only: how the base schema is created
  setup     SQL applied after the schema exists
  tables    tables the DDL rotation and checks may target
  indexes   indexes created with the decorator, in %INDEXES form
  context   sub($node) returning values the scripts need to be told

=cut

=pod

=head2 stress_rollback_prepared($node)

Roll back every prepared transaction, and say how many there were.

A prepared transaction outlives the session that made it and keeps every
lock it took, which is the whole point of one -- and a nuisance
afterwards.  The two-phase load leaves them behind whenever the run stops
mid-transaction, and a crash brings them back with recovery, after which
anything that needs a conflicting lock waits for a transaction that will
never be resolved.  An invalid index cannot be dropped while one holds
AccessExclusiveLock on it, and the wait ends in the lock timeout rather
than in an answer.

Called where the workload is over, or over for this cycle, so there is
nothing left to commit.  A scenario wanting to assert something about
prepared transactions surviving has to do it before this runs.

=cut

sub stress_rollback_prepared
{
	my ($node) = @_;

	# Built as statements rather than as gids, so that quoting is the
	# server's problem.
	my @rollbacks = grep { $_ ne '' } split /\n/,
	  $node->safe_psql('postgres',
		q(SELECT format('ROLLBACK PREPARED %L', gid) FROM pg_prepared_xacts));

	$node->safe_psql('postgres', $_) for @rollbacks;

	return scalar @rollbacks;
}

our %SCHEMA = (
	pgbench => {
		init => 'pgbench',
		# REPACK (CONCURRENTLY) decodes the table's own changes, so every
		# table it may touch needs a replica identity; pgbench_history
		# ships without one.
		setup => q(
			ALTER TABLE pgbench_history ADD COLUMN hid bigserial PRIMARY KEY;

			-- Rows above the account numbers pgbench created, carrying a
			-- zero balance: nothing else reads or writes them, so adding
			-- and removing them moves none of the four sums.  When the
			-- partition they belong in has been detached there is nowhere
			-- to put them, which is not what any load using this is
			-- looking for.
			-- An index expression that assigns a transaction id, which
			-- the build has to cope with: a concurrent build runs in
			-- several transactions and reasons about snapshots and the
			-- horizon, and an expression that takes an xid of its own
			-- changes how those interact.  It claims to be immutable and
			-- is not, which is exactly the point.
			-- Declared over bigint, and the index casts to it, so that
			-- the rewriting ALTER can widen the column underneath the
			-- expression without the index losing its function.
			CREATE FUNCTION pgb_xid_expr(v bigint) RETURNS bigint
			LANGUAGE plpgsql IMMUTABLE AS $$
			BEGIN
				PERFORM pg_current_xact_id();
				RETURN v;
			END;
			$$;

			CREATE FUNCTION pgb_scratch_insert(p_aid int) RETURNS boolean
			LANGUAGE plpgsql AS $$
			BEGIN
				INSERT INTO pgbench_accounts(aid, bid, abalance)
					VALUES (p_aid, 1, 0) ON CONFLICT (aid) DO NOTHING;
				RETURN true;
			EXCEPTION WHEN check_violation THEN
				RETURN false;
			END;
			$$;


			-- Run a command that needs a lock the workload conflicts
			-- with, without parking the request at the head of the lock
			-- queue.  An unbounded ALTER TABLE that wants
			-- AccessExclusiveLock -- attaching a partition, adding or
			-- dropping a constraint -- has every writer stack up behind
			-- it until the lock timeout fires, which is minutes of
			-- nothing and then a failed run.  Retrying in short slices
			-- lets the queue drain between attempts, so the worst stall
			-- is half a second.
			-- A command needing a lock that conflicts with the workload,
			-- retried in slices so that a failed attempt lets the queue
			-- drain instead of parking a request at its head.
			--
			-- The duty cycle is what matters, not the number of attempts.
			-- A request waiting 500ms blocks every writer behind it for
			-- 500ms, so pausing only 10ms between attempts leaves writers
			-- about a fiftieth of the time and they wait out their own
			-- lock_timeout having achieved nothing.  Found by a soak
			-- combination running this against thirty clients: the DDL
			-- succeeded and the writers were the ones that failed.  A
			-- shorter hold and a longer drain gives them most of the time
			-- and costs only that the command may need more attempts.
			CREATE FUNCTION pgb_ddl_bounded(cmd text) RETURNS boolean
			LANGUAGE plpgsql AS $fn$
			DECLARE i int;
			BEGIN
				FOR i IN 1..120 LOOP
					BEGIN
						SET LOCAL lock_timeout = '200ms';
						EXECUTE cmd;
						RETURN true;
					EXCEPTION WHEN lock_not_available THEN
						PERFORM pg_sleep(0.1);
					END;
				END LOOP;
				RETURN false;
			END $fn$;
		),
		tables => [
			qw(pgbench_accounts pgbench_tellers pgbench_branches pgbench_history)
		],
	},

	# A column whose sum never moves, because every writer applies a
	# balanced pair of updates.  Several dimensions need an invariant
	# that is a constant rather than a relation between sums, and this is
	# it.  It is a fast default, so adding it rewrites nothing.
	ledger => {
		setup => q(
			ALTER TABLE pgbench_accounts
				ADD COLUMN ledger int NOT NULL DEFAULT 0;
		),
		indexes => [ {
			table => 'pgbench_accounts',
			name => 'pgb_ledger_idx',
			am => 'btree',
			defn => 'ON pgbench_accounts(ledger)',
		} ],
	},

	# Inserts serialized behind an advisory lock, so the values a
	# sequence hands out are committed in increasing order.  At any later
	# snapshot the rows carrying one must then be an unbroken prefix, so
	# their count is the largest value handed out.
	#
	# They go into pgbench_history, which is append-only and is one of
	# the relations the rotation repacks.
	gapless => {
		setup => q(
			ALTER TABLE pgbench_history ADD COLUMN gval bigint;
			CREATE SEQUENCE pgb_gapless_val;
		),
	},

	# Room left on every page for the next version of the rows already
	# there.  pgbench fills its pages to the brim, which pushes updates
	# onto other pages and cuts the HOT chains short; leaving half the
	# page free keeps the chains on the page, where they can be pruned.
	# The setting applies to pages written from here on, so the effect
	# arrives as the load rewrites the table rather than at once.
	low_fillfactor => {
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
	},

	# A table with no indexes at all, and an event trigger whose
	# function needs a snapshot.  REINDEX has nothing to do on such a
	# table and returns early, and an event trigger firing afterwards is
	# what notices if that early return left the snapshot stack wrong.
	# Nothing else here has either piece: every table carries at least a
	# primary key, and there are no event triggers.
	bare_table_event_trigger => {
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
	},

	# A deferrable primary key.  REPACK has to locate a tuple by an
	# identity, and a deferrable key is not usable for that: under
	# deferral the index can hold duplicates until commit, so a tuple
	# being modified may not be findable by key.  Nothing else here
	# declares one.
	deferrable_pk => {
		setup => q(
			CREATE TABLE pgb_defer(id int NOT NULL, rid int NOT NULL, val int);
			ALTER TABLE pgb_defer ADD CONSTRAINT pgb_defer_pkey
				PRIMARY KEY (id) DEFERRABLE;
			-- Deliberately no replica identity: the deferrable key is
			-- the only identity this table has, which is the case REPACK
			-- has to decline.  Designating another index would defeat
			-- the test, because the broken code prefers a replica
			-- identity too and only falls back to the primary key.
			INSERT INTO pgb_defer
				SELECT g, g, 0 FROM generate_series(1, 2000) g;
		),
		# Deliberately not in the rotation: with the fix in place REPACK
		# refuses this table by design, and a refusal arriving in the DDL
		# client would be indistinguishable from a failure.  The check
		# asserts the refusal instead.
		tables => [],
	},

	# Hash partitioning, which detaches differently from range.  The
	# substitute constraint DETACH CONCURRENTLY used to leave behind
	# names the parent's OID inside satisfies_hash_partition(), so it is
	# only observable on a hash partition -- the equivalent constraint on
	# a range partition is harmless and indistinguishable from the
	# partition bound.
	partitioned_hash => {
		setup => q(
			CREATE TABLE pgb_hash(id int PRIMARY KEY, val int)
				PARTITION BY HASH (id);
			CREATE TABLE pgb_hash_0 PARTITION OF pgb_hash
				FOR VALUES WITH (MODULUS 4, REMAINDER 0);
			CREATE TABLE pgb_hash_1 PARTITION OF pgb_hash
				FOR VALUES WITH (MODULUS 4, REMAINDER 1);
			CREATE TABLE pgb_hash_2 PARTITION OF pgb_hash
				FOR VALUES WITH (MODULUS 4, REMAINDER 2);
			CREATE TABLE pgb_hash_3 PARTITION OF pgb_hash
				FOR VALUES WITH (MODULUS 4, REMAINDER 3);
			INSERT INTO pgb_hash SELECT g, 0 FROM generate_series(1, 4000) g;
		),
		tables => [qw(pgb_hash_0 pgb_hash_1 pgb_hash_2 pgb_hash_3)],
	},

	# A unique index that treats nulls as equal.  The executor compares
	# ii_NullsNotDistinct when it decides whether one index can stand in
	# for another as an arbiter, and nothing here ever set it.
	nulls_not_distinct => {
		setup => q(
			CREATE TABLE pgb_nnd(id serial PRIMARY KEY, k int, v int);
			INSERT INTO pgb_nnd(k, v) SELECT g, 0 FROM generate_series(1, 500) g;
			INSERT INTO pgb_nnd(k, v) VALUES (NULL, 0);
			CREATE UNIQUE INDEX pgb_nnd_k ON pgb_nnd(k) NULLS NOT DISTINCT;
		),
		tables => ['pgb_nnd'],
	},

	# Rows that are only ever upserted, never deleted, so every upsert
	# and every MERGE takes its "matched" path.  The arbiter is
	# pgbench_accounts' own primary key, which the rotation rebuilds
	# underneath the speculative insertions.
	upsert_keys => {
		setup => q(
			ALTER TABLE pgbench_accounts ADD COLUMN ukey int DEFAULT 0;

			-- A second unique index over the same column as the primary
			-- key, so that ON CONFLICT (aid) infers two arbiters rather
			-- than one.  Every arbiter in this suite used to be a
			-- primary key, which meant the executor's arbiter list was
			-- always a single entry and the code that matches several of
			-- them onto a partition, dedupes them by parent and counts
			-- the ones being rebuilt was never given anything to do.
			-- reindex_table_concurrently and the repacks rebuild this
			-- one along with the rest.
			CREATE UNIQUE INDEX pgb_aid_uniq ON pgbench_accounts(aid);
		),
	},

	# Wide values that go out of line, stored with an md5 of themselves
	# so that a torn or stale TOAST fetch is visible as a mismatch.  The
	# columns start out null, so only the rows the load has reached are
	# wide and the table does not balloon.
	toast => {
		setup => q(
			ALTER TABLE pgbench_accounts
				ADD COLUMN payload text,
				ADD COLUMN h text;
			-- Left as EXTENDED, the default: the value should go out of
			-- line AND be compressed there.  A rewrite that reassembles
			-- such a datum has to preserve the compression flag in its
			-- header, and EXTERNAL storage -- which never compresses --
			-- would put that path out of reach.
		),
	},

	# Generated columns on pgbench_accounts, computed from the balance
	# every TPC-B transaction moves.  Putting them on the table the
	# rotation already works hardest against is the point: a side table
	# with a load of its own would exercise the same code with far less
	# going on around it.
	generated => {
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
	},

	# A self-referencing foreign key, so that every repoint fires a
	# referential integrity check that resolves the parent through
	# pgbench_accounts' own primary key -- while the rotation rebuilds
	# that index underneath it.
	# A parent and a child of their own, rather than a column of
	# pgbench_accounts referencing itself.
	#
	# This is the one dimension where reusing the pgbench schema was
	# tried and measured worse, and the reason is the size of the parent.
	# What the referential integrity race needs is the parent's primary
	# key being swapped underneath a check, and REINDEX INDEX
	# CONCURRENTLY on a thousand rows finishes in about a millisecond
	# where the same command on pgbench_accounts takes hundreds.  The
	# rate of the thing being raced is what decides whether the race is
	# ever seen, so the parent is kept small and its primary key is
	# rebuilt on its own rather than as part of the whole table.
	# A parent and a child of their own, rather than a column of the
	# pgbench schema referencing another part of it.
	#
	# This is the one dimension where reusing pgbench's tables was tried
	# and measured worse, and it is worth saying why, because the obvious
	# reading is wrong.  What the referential integrity race needs is the
	# parent's primary key swapped underneath a check, so the parent has
	# to be small enough that REINDEX INDEX CONCURRENTLY on it finishes
	# quickly.  pgbench_tellers is small -- ten rows -- but it is also the
	# table every TPC-B transaction updates, so a concurrent rebuild of
	# its primary key spends its time waiting for lockers and the swap
	# rate collapses.  Referencing it reproduced nothing in eight runs at
	# stressval 4; referencing pgbench_accounts, which is quiet enough but
	# has a hundred thousand rows, reproduced nothing in fifty-odd.  A
	# thousand-row parent that nothing writes reproduces it in two runs of
	# eight.  Small and quiet, not just small.
	fk_child => {
		setup => q(
			CREATE TABLE pgb_parent(id int PRIMARY KEY, val int);
			CREATE TABLE pgb_child(cid bigserial PRIMARY KEY,
				pid int NOT NULL REFERENCES pgb_parent(id), val int);
			INSERT INTO pgb_parent SELECT g, g FROM generate_series(1, 1000) g;
			INSERT INTO pgb_child(pid, val)
				SELECT g, g FROM generate_series(1, 1000) g;
		),
		tables => [ 'pgb_parent', 'pgb_child' ],
		indexes => [ {
			table => 'pgb_child',
			name => 'pgb_child_pid_idx',
			am => 'btree',
			defn => 'ON pgb_child(pid)',
		} ],
		context => sub { return { nparents => 1000 } },
	},

	# At most one row per slot, kept that way by an exclusion constraint.
	# The constraint is written over a range so that it needs nothing but
	# the built-in GiST opclasses, and so that its index is built from an
	# expression.  It is partial because the rows the load has not
	# claimed have no slot, and an unbounded range would overlap
	# everything.
	exclusion_slot => {
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
	},

	# Columns the non-btree access methods have opclasses for, so that
	# CREATE INDEX CONCURRENTLY and its rebuilds can be driven against
	# every AM rather than btree alone -- and against the table the
	# workload is hammering, rather than one standing still.
	am_columns => {
		setup => q(
			ALTER TABLE pgbench_accounts
				ADD COLUMN tags text[] DEFAULT ARRAY['tag'],
				ADD COLUMN p point DEFAULT point(0, 0),
				ADD COLUMN n int DEFAULT 0,
				ADD COLUMN ip inet DEFAULT '10.0.0.1';
		),
		indexes => [
			{
				table => 'pgbench_accounts',
				name => 'pgb_am_gin_idx',
				am => 'gin',
				defn => 'ON pgbench_accounts USING gin (tags)',
			},
			{
				table => 'pgbench_accounts',
				name => 'pgb_am_gist_idx',
				am => 'gist',
				defn => 'ON pgbench_accounts USING gist (p)',
			},
			{
				table => 'pgbench_accounts',
				name => 'pgb_am_brin_idx',
				am => 'brin',
				defn => 'ON pgbench_accounts USING brin (n)',
			},
			{
				table => 'pgbench_accounts',
				name => 'pgb_am_hash_idx',
				am => 'hash',
				defn => 'ON pgbench_accounts USING hash (n)',
			},
			{
				table => 'pgbench_accounts',
				name => 'pgb_am_spgist_idx',
				am => 'spgist',
				defn => 'ON pgbench_accounts USING spgist (ip)',
			},
		],
	},

	# A small table nothing writes, with an index of its own.
	#
	# This exists to raise the rate of one thing: an index being dropped
	# and recreated.  On a few hundred rows that cycle takes about a
	# millisecond, where the same commands against pgbench_accounts take
	# hundreds, so a standby replaying them sees the catalog entry come
	# and go far more often -- which is what a reader planning against a
	# stale index list has to collide with.
	quiet_index => {
		setup => q(
			CREATE TABLE pgb_quiet(id int PRIMARY KEY, val int);
			INSERT INTO pgb_quiet SELECT g, g FROM generate_series(1, 500) g;
		),
		tables => ['pgb_quiet'],
		indexes => [ {
			table => 'pgb_quiet',
			name => 'pgb_quiet_val_idx',
			am => 'btree',
			defn => 'ON pgb_quiet(val)',
		} ],
	},

	# A materialized view over the ledger column, so REFRESH ...
	# CONCURRENTLY has something whose contents can be predicted:
	# whatever snapshot the refresh used, the ledger summed to zero at
	# that instant, so the bucket sums it recorded do too.
	#
	# Bucketing keeps the view small enough to refresh repeatedly while
	# still giving the refresh a diff of a thousand rows to work out.
	matview => {
		requires => { schema => ['ledger'] },
		setup => q(
			CREATE MATERIALIZED VIEW pgb_mv AS
				SELECT aid % 1000 AS bucket, SUM(ledger) AS s
					FROM pgbench_accounts GROUP BY 1;
			CREATE UNIQUE INDEX pgb_mv_bucket_idx ON pgb_mv(bucket);
		),
	},

	# Row level security, forced so that the owner is subject to it too,
	# and a trigger that fires only when the session is in replica mode.
	# Both change the path an ordinary update takes through the executor,
	# and replica mode is the one logical replication's apply worker uses,
	# so it is worth driving against a table being rebuilt.
	replica_role => {
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
	},

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
	partitioned => {
		# An exclusion constraint on a partitioned table has to contain
		# the partition key, and this one does not.  A foreign key would
		# survive the rename pointing at the partition rather than the
		# parent, which is not the shape worth testing.  The subscription
		# environment builds an index of its own on the subscriber and
		# drops it concurrently, which a partitioned index refuses.
		conflicts => {
			schema => [ 'exclusion_slot', 'fk_child' ],
			env => ['subscription'],
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
	},

	# The overflow partition, itself partitioned.  A child index then has
	# a grandparent as well as a parent, which is what the arbiter and
	# descriptor code walks when it asks who an index belongs to.
	partitioned_2_levels => {
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
	},

	# A partitioned table of its own, for the dimensions that need to
	# detach a partition holding rows the invariant counts.
	partitioned_side => {
		setup => qq(
			CREATE TABLE pgb_part(id int NOT NULL, val int) PARTITION BY RANGE (id);
			CREATE TABLE pgb_part_1 PARTITION OF pgb_part
				FOR VALUES FROM (1) TO (2501);
			CREATE TABLE pgb_part_2 PARTITION OF pgb_part
				FOR VALUES FROM (2501) TO (5001);
			CREATE TABLE pgb_part_3 PARTITION OF pgb_part
				FOR VALUES FROM (5001) TO (7501);
			CREATE TABLE pgb_part_4 PARTITION OF pgb_part
				FOR VALUES FROM (7501) TO ($NROWS + 1);
			ALTER TABLE pgb_part ADD PRIMARY KEY (id);
			-- See upsert_keys: two inferable arbiters rather than one,
			-- here on the partitioned table, so the per-partition
			-- mapping has to match both.
			CREATE UNIQUE INDEX pgb_part_id_uniq ON pgb_part(id);
			-- The first sixteen ids are the contention band: they carry
			-- no value, so partition_upsert_contend can delete and
			-- re-insert them without moving the sum the checks watch.
			INSERT INTO pgb_part
				SELECT g, CASE WHEN g <= 16 THEN 0 ELSE g END
				FROM generate_series(1, $NROWS) g;

			-- An upsert routed through the parent has nowhere to put the
			-- row while the partition covering it is detached, which the
			-- rotation does deliberately.  That is not what this load is
			-- looking for, so swallow it and let the arbiter-index work
			-- happen on every other attempt.
			CREATE FUNCTION pgb_part_upsert(p_id int) RETURNS boolean
			LANGUAGE plpgsql AS \$\$
			BEGIN
				INSERT INTO pgb_part VALUES (p_id, 0)
					ON CONFLICT (id) DO UPDATE SET val = pgb_part.val;
				RETURN true;
			EXCEPTION WHEN check_violation THEN
				RETURN false;
			END;
			\$\$;
		),
		# The parent is where the DML goes; the partitions are what the
		# CONCURRENTLY commands can be pointed at, since most of them
		# refuse a partitioned table.
		tables => [qw(pgb_part_1 pgb_part_2 pgb_part_3 pgb_part_4)],
		context => sub {
			my ($node) = @_;
			return {
				part_rows => $NROWS,
				part_sum =>
				  $node->safe_psql('postgres', 'SELECT SUM(val) FROM pgb_part'),
			};
		},
	},

	# A table shaped for a bitmap heap scan to skip fetching pages.
	#
	# The skip_fetch optimization lets a bitmap heap scan that needs no
	# columns -- count(*) with an indexable qual and nothing else -- avoid
	# reading a page at all when the visibility map says every tuple on it
	# is visible, taking the tuple count from the bitmap instead.  The
	# bitmap was built earlier, though, and a vacuum running since can
	# have removed the dead TIDs in it and marked those pages
	# all-visible.  The scan then counts TIDs that are no longer there.
	#
	# Wide rows at fillfactor 10 put roughly one row on a page, so a few
	# thousand rows make a few thousand pages: enough that a scan takes
	# long enough for a vacuum to get ahead of it, which is what the race
	# needs.  Autovacuum is off so the vacuum that matters is the one the
	# rotation runs.
	bitmap_skip_fetch => {
		setup => q(
			CREATE TABLE pgb_bmskip (b int NOT NULL, pad char(1024) DEFAULT '')
				WITH (autovacuum_enabled = false, fillfactor = 10);
			INSERT INTO pgb_bmskip(b) SELECT g FROM generate_series(1, 4000) g;
			CREATE INDEX pgb_bmskip_b_idx ON pgb_bmskip(b);
			VACUUM (ANALYZE) pgb_bmskip;

			-- The same count taken two ways under one snapshot: once by a
			-- bitmap heap scan that is allowed to skip fetching pages, and
			-- once by a sequential scan that cannot.  They can only
			-- disagree if the bitmap scan counted TIDs that are not there.
			--
			-- The caller must be in a repeatable read transaction, or the
			-- two counts are taken under different snapshots.
			CREATE FUNCTION pgb_bmskip_check() RETURNS boolean
			LANGUAGE plpgsql AS $fn$
			DECLARE
				bitmap_count bigint;
				seq_count bigint;
				plan text;
			BEGIN
				-- No qual and no columns on the scan node is what makes
				-- the optimization applicable; the index condition does
				-- all the work.
				PERFORM set_config('enable_seqscan', 'off', true);
				PERFORM set_config('enable_indexscan', 'off', true);
				PERFORM set_config('enable_indexonlyscan', 'off', true);
				PERFORM set_config('enable_bitmapscan', 'on', true);
				-- Assert the shape rather than assume it: a bitmap heap
				-- scan is the whole point, and a modifier could take it
				-- away without anything noticing.
				EXECUTE 'EXPLAIN (COSTS OFF, FORMAT JSON) '
					'SELECT count(*) FROM pgb_bmskip WHERE b >= 0' INTO plan;
				IF plan NOT LIKE '%%Bitmap Heap Scan%%' THEN
					RAISE EXCEPTION 'not a bitmap heap scan: %', plan;
				END IF;

				SELECT count(*) INTO bitmap_count
					FROM pgb_bmskip WHERE b >= 0;

				PERFORM set_config('enable_seqscan', 'on', true);
				PERFORM set_config('enable_bitmapscan', 'off', true);
				SELECT count(*) INTO seq_count FROM pgb_bmskip;

				IF bitmap_count <> seq_count THEN
					RAISE EXCEPTION
						'bitmap heap scan counted % rows, the table has % under the same snapshot',
						bitmap_count, seq_count;
				END IF;
				RETURN true;
			END $fn$;
		),
		# Deliberately not in the rotation.  This table has no primary key
		# and no replica identity -- it is shaped for a bitmap heap scan,
		# not for being rewritten -- so REPACK (CONCURRENTLY) refuses it
		# and reindex_pkey_concurrently names an index that does not
		# exist.  The scenario's own DDL entries name it explicitly.
		# Found by soak, which combined it with the standard rotation.
		tables => [],
	},

	# Two indexes either of which can serve as the replica identity, so
	# that the identity can be moved between them while a command that
	# depends on it is running.
	#
	# REPACK (CONCURRENTLY) picks the identity index once, in
	# check_concurrent_repack_requirements(), and then uses it to replay
	# the changes that happen while it copies.  It runs in several
	# transactions, so ALTER TABLE ... REPLICA IDENTITY can commit in
	# between -- and the index it picked may by then be neither the
	# identity nor, if it was dropped, present at all.  That is the same
	# shape as the apply worker holding a stale identity across a REINDEX,
	# which is a fix carried earlier in this branch.
	movable_identity => {
		# The identity moves between indexes of pgbench_accounts, which
		# the partitioning decorator replaces.
		conflicts => { schema => ['partitioned'] },
		setup => q(
			CREATE UNIQUE INDEX pgb_ident_a ON pgbench_accounts(aid);
			CREATE UNIQUE INDEX pgb_ident_b ON pgbench_accounts(aid)
				INCLUDE (bid);
			ALTER TABLE pgbench_accounts REPLICA IDENTITY USING INDEX pgb_ident_a;

			-- Which identity has been seen, so the run can prove the
			-- identity really moved.  ALTER TABLE ... REPLICA IDENTITY
			-- needs AccessExclusiveLock and goes through the bounded
			-- helper, which gives up rather than waiting; a run where it
			-- always gave up would pass while testing nothing.
			CREATE TABLE pgb_ident_seen(idx name PRIMARY KEY);

			CREATE FUNCTION pgb_note_identity() RETURNS void
			LANGUAGE sql AS $$
				INSERT INTO pgb_ident_seen
					SELECT c.relname FROM pg_index i
					JOIN pg_class c ON c.oid = i.indexrelid
					WHERE i.indrelid = 'pgbench_accounts'::regclass
					  AND i.indisreplident
				ON CONFLICT DO NOTHING;
			$$;
		),
	},

	# The helper that drives the data checksum transitions.
	#
	# Enabling checksums is not a switch: a background worker walks every
	# page of every relation, reads it, computes a checksum and writes it
	# back, and the cluster reports "inprogress-on" until it finishes.
	# That is a whole-database rewrite running against the rotation's own
	# rewrites, which nothing else in this suite reaches.
	#
	# The helper flips to whichever state the cluster is not in, so every
	# turn does real work rather than finding itself already there, and
	# waits for the worker before returning so two transitions are never
	# in flight at once.
	data_checksum_helper => {
		setup => q(
			-- Needed by no_checksum_failures to pull every page through
			-- shared buffers.  Without it that check reads nothing and
			-- passes for the wrong reason, which is how it was written the
			-- first time.
			CREATE EXTENSION IF NOT EXISTS pg_prewarm;

			-- An unlogged relation, which the checksum worker handles
			-- differently: it dirties the pages without logging them,
			-- because a crash resets the relation from its init fork --
			-- and the init fork is logged, precisely so a standby does not
			-- inherit a stale one that fails verification after promotion.
			CREATE UNLOGGED TABLE pgb_unlogged(id int PRIMARY KEY, pad text);
			INSERT INTO pgb_unlogged
				SELECT g, repeat(md5(g::text), 8)
				FROM generate_series(1, 2000) g;
			CREATE FUNCTION pgb_flip_data_checksums() RETURNS text
			LANGUAGE plpgsql AS $fn$
			DECLARE
				state text;
				target text;
			BEGIN
				SELECT current_setting('data_checksums') INTO state;

				-- Mid-transition: let it finish rather than stack another.
				IF state NOT IN ('on', 'off') THEN
					RETURN state;
				END IF;

				IF state = 'on' THEN
					PERFORM pg_disable_data_checksums();
					target := 'off';
				ELSE
					-- cost_delay 0 and a high cost_limit: the point is to
					-- finish inside the run, not to be gentle about it.
					PERFORM pg_enable_data_checksums(0, 10000);
					target := 'on';
				END IF;

				-- Wait for the worker, bounded: a run that ends mid-flip
				-- leaves the cluster in an inprogress state, which the
				-- next turn would then decline to touch.
				FOR i IN 1..600 LOOP
					EXIT WHEN current_setting('data_checksums') = target;
					PERFORM pg_sleep(0.05);
				END LOOP;

				RETURN current_setting('data_checksums');
			END $fn$;
		),
		tables => [],
	},

	# Two tables shaped for an index-only scan to race a vacuum, one per
	# access method that has the problem.
	#
	# GiST and SP-GiST index scans do not interlock with their own index
	# vacuum in a way that guarantees an index-only scan returns only TIDs
	# that were still valid when it returned them.  A scan that has queued
	# TIDs and let go of the page can have them removed underneath it, and
	# the pages they were on marked all-visible -- after which the scan
	# returns them from the index without ever looking at the heap, which
	# is a row that was deleted before the scan's snapshot began.
	#
	# The shape is from the report: wide rows and a fillfactor of 10, so
	# that a handful of rows fill a page and the pages the deleted rows
	# were on can go all-visible on their own.  Autovacuum is off for the
	# same reason it is in the isolation test -- the vacuum that matters
	# is the one the rotation runs, at a point in the workload rather
	# than whenever the daemon happens to wake up.
	ios_vacuum_race => {
		setup => q(
			CREATE TABLE pgb_ios_gist (
				a point NOT NULL, b int NOT NULL, pad char(1024) DEFAULT '')
				WITH (autovacuum_enabled = false, fillfactor = 10);
			CREATE TABLE pgb_ios_spgist (
				a point NOT NULL, b int NOT NULL, pad char(1024) DEFAULT '')
				WITH (autovacuum_enabled = false, fillfactor = 10);

			INSERT INTO pgb_ios_gist(a, b)
				SELECT point(g, g), g FROM generate_series(1, 200) g;
			INSERT INTO pgb_ios_spgist(a, b)
				SELECT point(g, g), g FROM generate_series(1, 200) g;

			CREATE INDEX pgb_ios_gist_a_idx ON pgb_ios_gist USING gist (a);
			CREATE INDEX pgb_ios_spgist_a_idx ON pgb_ios_spgist USING spgist (a);

			VACUUM (ANALYZE) pgb_ios_gist;
			VACUUM (ANALYZE) pgb_ios_spgist;

			CREATE SEQUENCE pgb_ios_seq;

			-- An index-only scan held open across a vacuum, checked
			-- against the heap under the same snapshot.
			--
			-- The comparison is between the values themselves rather than
			-- a count of them: every row version gets a y coordinate of
			-- its own from the sequence, so a value the scan returns that
			-- the heap does not have is a row that was deleted before
			-- this snapshot began -- revived because its TID was queued
			-- before a vacuum removed it and marked its page all-visible.
			-- Counting alone cannot tell that from a row still present,
			-- since a deleted row and its replacement share everything a
			-- count can see.
			--
			-- The caller must be in a repeatable read transaction, or the
			-- two sides are read under different snapshots and differ for
			-- honest reasons.
			CREATE FUNCTION pgb_ios_cursor_check(p_table text, p_sleep float8)
			RETURNS boolean LANGUAGE plpgsql AS $fn$
			DECLARE
				c refcursor;
				pt point;
				ios text[] := '{}';
				heap text[];
				extra text[];
				plan text;
			BEGIN
				-- The cursor has to be an index-only scan for any of this
				-- to mean anything.
				-- enable_indexscan has to be pinned as well: it gates
				-- index-only scan paths too, so a server-level modifier
				-- that turned it off would leave this comparing a
				-- sequential scan against a sequential scan and passing
				-- for the wrong reason.
				PERFORM set_config('enable_seqscan', 'off', true);
				PERFORM set_config('enable_bitmapscan', 'off', true);
				PERFORM set_config('enable_indexscan', 'on', true);
				PERFORM set_config('enable_indexonlyscan', 'on', true);

				-- Assert the shape rather than assume it.  If this is not
				-- an index-only scan the comparison below is vacuous.
				-- FORMAT JSON, so the whole plan arrives as one row: a
				-- plain EXPLAIN INTO takes only the first line, which for
				-- this query happens to be the scan and would have made
				-- the test pass for the wrong reason.
				EXECUTE format(
					'EXPLAIN (COSTS OFF, FORMAT JSON) SELECT a FROM %I '
					'ORDER BY a <-> point ''(0,0)''', p_table) INTO plan;
				IF plan NOT LIKE '%%Index Only Scan%%' THEN
					RAISE EXCEPTION
						'not an index-only scan on %: %', p_table, plan;
				END IF;

				OPEN c NO SCROLL FOR EXECUTE format(
					'SELECT a FROM %I ORDER BY a <-> point ''(0,0)''',
					p_table);

				-- The first fetch is what makes the scan read a page and
				-- queue what is on it; the rest come back from that queue.
				FETCH c INTO pt;
				IF NOT FOUND THEN
					CLOSE c;
					RETURN true;
				END IF;
				ios := array_append(ios, pt::text);

				-- The window a vacuum has to land in.
				PERFORM pg_sleep(p_sleep);

				LOOP
					FETCH c INTO pt;
					EXIT WHEN NOT FOUND;
					ios := array_append(ios, pt::text);
				END LOOP;
				CLOSE c;

				-- The same values, under the same snapshot, from the heap.
				PERFORM set_config('enable_seqscan', 'on', true);
				PERFORM set_config('enable_indexscan', 'off', true);
				PERFORM set_config('enable_indexonlyscan', 'off', true);
				EXECUTE format(
					'SELECT coalesce(array_agg(a::text), ''{}'') FROM %I',
					p_table) INTO heap;

				SELECT coalesce(array_agg(v), '{}') INTO extra
					FROM (SELECT unnest(ios) EXCEPT ALL
						  SELECT unnest(heap)) s(v);

				IF cardinality(extra) > 0 THEN
					RAISE EXCEPTION
						'index-only scan on % returned % rows the heap does not have, out of %: %',
						p_table, cardinality(extra), cardinality(ios),
						extra[1:5];
				END IF;
				RETURN true;
			END $fn$;
		),
		# Not in the rotation, for the same reason as pgb_bmskip: no
		# primary key and no replica identity, so REPACK refuses them and
		# the primary-key reindex has nothing to name.  vacuum_ios_tables
		# and the scenario's reindex entry name them directly.
		tables => [],
	},

	# Triggers on the table the rotation rebuilds, and the tables they
	# write into.  Two paths arrive with them that nothing else here
	# has.  The rows of pgb_audit are written from inside a trigger,
	# after the statement rather than as part of it, so its indexes are
	# fed by a path no other scenario drives.  And the queries the
	# trigger bodies run come from plans plpgsql caches and reuses, so a
	# rebuild that swaps or drops an index has to reach a plan made
	# before it and force a replan -- every other statement in this
	# suite is parsed afresh and can only ever see the current catalog.
	trigger_audit => {
		# The trigger is created on pgbench_accounts by name, and the
		# partitioning decorator renames that table out from under it.
		# Which of the two runs first decides whether the trigger ends up
		# on the parent or on the partition holding the old rows, and the
		# DDL that names it later would only find it in one of those.
		conflicts => { schema => ['partitioned'] },
		setup => q(
			CREATE TABLE pgb_audit(
				id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
				aid int NOT NULL,
				delta int NOT NULL);

			-- The counter rows the log trigger upserts into, keyed by the
			-- pgbench client rather than by anything the workload picks,
			-- so two sessions never want the same row.  What is under
			-- test is the arbiter index; a shared row would only add
			-- waiting, and an update order for a deadlock to form in.
			CREATE TABLE pgb_calls(
				client int PRIMARY KEY,
				hits bigint NOT NULL);

			CREATE TABLE pgb_call_log(client int NOT NULL);

			CREATE FUNCTION pgb_audit_trigger() RETURNS trigger
			LANGUAGE plpgsql AS $$
			BEGIN
				INSERT INTO pgb_audit(aid, delta)
					VALUES (NEW.aid, NEW.abalance - OLD.abalance);
				RETURN NULL;
			END $$;

			CREATE TRIGGER pgb_audit_trg AFTER UPDATE ON pgbench_accounts
				FOR EACH ROW
				WHEN (OLD.abalance IS DISTINCT FROM NEW.abalance)
				EXECUTE FUNCTION pgb_audit_trigger();

			-- Deferred to commit, so its lookup runs after the statement
			-- that queued it and after everything else the transaction
			-- did: a window a rebuild can land in the middle of.  The row
			-- it looks for is the transaction's own, so it is visible
			-- however the planner reaches it, and not finding it means
			-- the index it was reached through has lost it.
			CREATE FUNCTION pgb_audit_defer_trigger() RETURNS trigger
			LANGUAGE plpgsql AS $$
			DECLARE n bigint;
			BEGIN
				SELECT count(*) INTO n FROM pgb_audit WHERE id = NEW.id;
				IF n <> 1 THEN
					RAISE EXCEPTION
						'audit row % not found at commit, count %', NEW.id, n;
				END IF;
				RETURN NULL;
			END $$;

			CREATE CONSTRAINT TRIGGER pgb_audit_defer_trg
				AFTER INSERT ON pgb_audit
				DEFERRABLE INITIALLY DEFERRED
				FOR EACH ROW EXECUTE FUNCTION pgb_audit_defer_trigger();

			-- An upsert that has to infer its arbiter every time, from a
			-- plan that was cached the first time this session fired the
			-- trigger and is reused for the rest of the run.
			CREATE FUNCTION pgb_calls_trigger() RETURNS trigger
			LANGUAGE plpgsql AS $$
			BEGIN
				INSERT INTO pgb_calls(client, hits) VALUES (NEW.client, 1)
					ON CONFLICT (client) DO UPDATE
					SET hits = pgb_calls.hits + 1;
				RETURN NULL;
			END $$;

			CREATE TRIGGER pgb_calls_trg AFTER INSERT ON pgb_call_log
				FOR EACH ROW EXECUTE FUNCTION pgb_calls_trigger();

			-- What the trigger DDL creates and drops.  It returns NEW
			-- rather than NULL: a BEFORE ROW trigger returning NULL
			-- skips the row, which would quietly discard the workload's
			-- updates and every invariant with them.
			CREATE FUNCTION pgb_noop_trigger() RETURNS trigger
			LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
		),
		# pgb_call_log stays out of the rotation: it has no identity, so
		# REPACK and the primary key rebuilds would have nothing to work
		# with.  It is append-only anyway, like pgbench_history.
		tables => [ 'pgb_audit', 'pgb_calls' ],
	},
);

=pod

=head2 %INDEXES

C<defn> is everything after the index name, so that both the blocking
and the concurrent form of the build can be generated from it;
C<table> is what the index is on, and C<am> decides which amcheck
function, if any, can verify it.

=cut

our %INDEXES = (
	btree_abalance => {
		table => 'pgbench_accounts',
		name => 'pgb_abalance_idx',
		am => 'btree',
		defn => 'ON pgbench_accounts(abalance)',
	},
	# An index on a column nothing updates.  That is what makes the
	# updates around it HOT updates: an update is HOT only when no index
	# covers any column it changes, and every other index here is on
	# abalance, which is exactly what the load moves.  A scenario that
	# wants HOT chains declares this one and no abalance index.
	btree_bid => {
		table => 'pgbench_accounts',
		name => 'pgb_bid_idx',
		am => 'btree',
		defn => 'ON pgbench_accounts(bid)',
	},
	btree_history_delta => {
		table => 'pgbench_history',
		name => 'pgb_history_delta_idx',
		am => 'btree',
		defn => 'ON pgbench_history(delta)',
	},
	partial_abalance => {
		table => 'pgbench_accounts',
		name => 'pgb_abalance_partial_idx',
		am => 'btree',
		# REPACK and CLUSTER refuse to order a table by a partial index.
		partial => 1,
		defn => 'ON pgbench_accounts(abalance) WHERE abalance > 0',
	},
	expr_abalance => {
		table => 'pgbench_accounts',
		name => 'pgb_abalance_expr_idx',
		am => 'btree',
		defn => 'ON pgbench_accounts(abs(abalance), aid)',
	},
	covering_aid => {
		table => 'pgbench_accounts',
		name => 'pgb_aid_covering_idx',
		am => 'btree',
		defn => 'ON pgbench_accounts(aid) INCLUDE (abalance)',
	},
	# An index over an expression that takes a transaction id of its own
	# every time it is evaluated -- during the build, and during every
	# insert and update the build has to keep up with.
	expr_xid => {
		table => 'pgbench_accounts',
		name => 'pgb_abalance_xid_idx',
		am => 'btree',
		defn => 'ON pgbench_accounts(pgb_xid_expr(abalance::bigint))',
	},
	# A predicate long enough that the pg_index row holding it goes out
	# of line.  CREATE, REINDEX and DROP INDEX CONCURRENTLY update
	# pg_index from transactions of their own, and reading a toasted
	# column needs an active snapshot that those transactions have not
	# always had.
	toasted_predicate => {
		table => 'pgbench_accounts',
		name => 'pgb_abalance_toasted_idx',
		am => 'btree',
		# CLUSTER and REPACK refuse to order a table by a partial index.
		partial => 1,
		defn => 'ON pgbench_accounts(abalance) WHERE '
		  . join(' AND ', map { "aid <> -$_" } 1 .. 60),
	},
	# An index fed entirely from inside a trigger: every row of pgb_audit
	# arrives from the after-update trigger on pgbench_accounts rather
	# than from a statement pgbench sent, so the rows and the index
	# entries are made in a part of the executor the rest of this suite
	# never reaches while a build is running.  amcheck's heapallindexed
	# is what has to agree at the end.
	btree_audit_aid => {
		# The only index here whose table is not one of pgbench's, so the
		# only one that is not there for every scenario.
		requires => { schema => ['trigger_audit'] },
		table => 'pgb_audit',
		name => 'pgb_audit_aid_idx',
		am => 'btree',
		defn => 'ON pgb_audit(aid)',
	},
);

=pod

=head2 %LOAD

A load is a pgbench script that changes data while preserving the
invariant its scenario checks.  C<weight> is its share of the
transaction mix, and C<setup> is any SQL it needs beforehand.

=cut

our %LOAD = (
	# The standard TPC-B-like transaction: one delta applied to an
	# account, a teller and a branch, and recorded in history.  Every
	# completed transaction moves all four sums by the same amount, which
	# is what the 'balances' check relies on.
	tpcb_like => {
		weight => 4,
		script => q(
			\set aid random(1, :naccounts)
			\set bid random(1, :nbranches)
			\set tid random(1, :ntellers)
			\set delta random(-5000, 5000)
			BEGIN;
			UPDATE pgbench_accounts SET abalance = abalance + :delta
				WHERE aid = :aid;
			SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
			UPDATE pgbench_tellers SET tbalance = tbalance + :delta
				WHERE tid = :tid;
			UPDATE pgbench_branches SET bbalance = bbalance + :delta
				WHERE bid = :bid;
			INSERT INTO pgbench_history (tid, bid, aid, delta, mtime)
				VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
			COMMIT;
		),
	},

	# Upserts that really do insert.  upsert_merge only ever meets rows
	# that already exist, so it always takes the matched path and never
	# reaches speculative insertion -- and speculative insertion is where
	# an arbiter index that two transactions disagree about does its
	# damage.  Here the keys live in a narrow band above the ones pgbench
	# created, and a quarter of the work deletes them again, so a key is
	# forever going missing and being raced for by several clients at
	# once.  The rows carry no balance, so the four-way total is
	# untouched no matter how many of them exist.
	upsert_contend => {
		weight => 3,
		requires => { schema => ['upsert_keys'] },
		script => q(
			\set k random(:naccounts + 1, :naccounts + 16)
			\set v random(1, 100000)
			\set mode random(0, 3)
			\if :mode = 0
				DELETE FROM pgbench_accounts WHERE aid = :k;
			\elif :mode = 1
				-- Naming the constraint rather than the attribute
				-- resolves the arbiter a different way, and during a
				-- rebuild several indexes answer to the constraint's
				-- definition at once.
				INSERT INTO pgbench_accounts(aid, bid, abalance, ukey)
					VALUES (:k, 1, 0, :v)
					ON CONFLICT ON CONSTRAINT pgbench_accounts_pkey
					DO UPDATE SET ukey = EXCLUDED.ukey;
			\else
				INSERT INTO pgbench_accounts(aid, bid, abalance, ukey)
					VALUES (:k, 1, 0, :v)
					ON CONFLICT (aid) DO UPDATE SET ukey = EXCLUDED.ukey;
			\endif
		),
	},

	# The partitioned counterpart of upsert_contend: several clients
	# racing to insert the same absent key through the parent, so the
	# arbiter indexes have to be mapped onto a partition while one of
	# them is being rebuilt.  partition_upsert never reaches that path,
	# because the row it upserts always exists already.  Confined to the
	# contention band, whose rows carry no value, so the sum the checks
	# watch does not move -- and partition_sum only asserts when every
	# row is present anyway.
	partition_upsert_contend => {
		weight => 3,
		requires => { schema => ['partitioned_side'] },
		script => q(
			\set id random(1, 16)
			\set mode random(0, 3)
			\if :mode = 0
				-- No routing needed: with the partition detached this
				-- prunes to nothing rather than failing.
				DELETE FROM pgb_part WHERE id = :id;
			\else
				-- Through the same wrapper partition_upsert uses, which
				-- swallows the check violation an upsert gets while the
				-- partition covering the row is detached.
				SELECT pgb_part_upsert(:id);
			\endif
		),
	},

	# Swapping two primary keys, which is what a deferrable key exists
	# for: neither UPDATE is unique on its own and only the pair is, so
	# the index carries a duplicate until commit.  Each client swaps
	# only within its own residue class mod 64, so clients never contend
	# for a key and the transient duplicate is always this client's own;
	# any client count up to 64 works.  The keys are taken in ascending
	# order, so there is no deadlock either.
	deferred_key_swap => {
		weight => 2,
		requires => { schema => ['deferrable_pk'] },
		script => q(
			\set i1 random(0, 30)
			\set i2 random(0, 30)
			\set x :i1 * 64 + (:client_id % 64) + 1
			\set y :i2 * 64 + (:client_id % 64) + 1
			\set lo least(:x, :y)
			\set hi greatest(:x, :y)
			BEGIN;
			SET CONSTRAINTS pgb_defer_pkey DEFERRED;
			UPDATE pgb_defer SET id = -1 - :client_id WHERE id = :lo;
			UPDATE pgb_defer SET id = :lo WHERE id = :hi;
			UPDATE pgb_defer SET id = :hi WHERE id = -1 - :client_id;
			COMMIT;
		),
	},

	# Nothing but inserts into an append-only table.  This exists to be
	# raced by a high rate of CREATE INDEX CONCURRENTLY on a small
	# relation, which is the shape contrib/amcheck's 002_cic uses and
	# the shape the relcache build race needs: a backend has to absorb
	# an invalidation while building the descriptor for the new index,
	# and the chance of that scales with how often an index is built.
	history_insert => {
		weight => 1,
		# It writes deltas into pgbench_history without the account,
		# teller and branch rows that would balance them, and the
		# invariant sums all four.
		conflicts => { checks => ['balances'] },
		script => q(
			\set aid random(1, :naccounts)
			\set delta random(-5000, 5000)
			INSERT INTO pgbench_history (tid, bid, aid, delta, mtime)
				VALUES (1, 1, :aid, :delta, CURRENT_TIMESTAMP);
		),
	},

	# Writes routed through the hash-partitioned parent, so a detach of
	# one of its partitions has something to race.
	hash_dml => {
		weight => 2,
		requires => { schema => ['partitioned_hash'] },
		script => q(
			\set id random(1, 4000)
			\set d random(1, 100)
			-- Pruning sends this to one partition; while that partition
			-- is detached it matches nothing, which is not an error.
			UPDATE pgb_hash SET val = val + :d WHERE id = :id;
		),
	},

	# Upserts whose arbiter is the nulls-not-distinct index, including
	# the null key, which conflicts with itself under that index.
	nnd_upsert => {
		weight => 2,
		requires => { schema => ['nulls_not_distinct'] },
		script => q(
			\set k random(1, 600)
			\set usenull random(0, 9)
			\if :usenull = 0
				INSERT INTO pgb_nnd(k, v) VALUES (NULL, 1)
					ON CONFLICT (k) DO UPDATE SET v = pgb_nnd.v + 1;
			\else
				INSERT INTO pgb_nnd(k, v) VALUES (:k, 1)
					ON CONFLICT (k) DO UPDATE SET v = pgb_nnd.v + 1;
			\endif
		),
	},

	# Nothing but updates to one column, spread evenly over the table.
	# Where no index covers that column the new version stays on the
	# page and the old one becomes prunable, so this produces HOT chains
	# and, through them, opportunistic pruning on pages all over the
	# relation -- which is what a concurrent build has to survive.  It
	# does not move money between tables, so a scenario using this one
	# has no balance invariant to check.
	hot_churn => {
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
	},

	# The same movement, but with the rows taken with FOR UPDATE first
	# and the lock held across a pause, so a CONCURRENTLY command
	# routinely runs while a row lock is in force.  The rows are locked
	# in a fixed order across the three tables, so concurrent clients
	# cannot deadlock.
	row_lock => {
		weight => 1,
		# This holds row locks across a pause, which is the point of it.
		# Against a lock table small enough to run out it stops being a
		# test of anything: a rebuild queues for its exclusive lock behind
		# the held ones, every later statement queues behind the rebuild,
		# and the whole thing sits there until lock_timeout fires.  That
		# is fair queuing working as designed, and it produced two runs
		# that spent three minutes proving nothing.  The other
		# environments have room for both.
		conflicts => { env => ['lock_exhaustion'] },
		script => q(
			\set aid random(1, :naccounts)
			\set bid random(1, :nbranches)
			\set tid random(1, :ntellers)
			\set delta random(-5000, 5000)
			BEGIN;
			SELECT abalance FROM pgbench_accounts WHERE aid = :aid FOR UPDATE;
			SELECT tbalance FROM pgbench_tellers WHERE tid = :tid FOR UPDATE;
			SELECT bbalance FROM pgbench_branches WHERE bid = :bid FOR UPDATE;
			\sleep 5 ms
			UPDATE pgbench_accounts SET abalance = abalance + :delta
				WHERE aid = :aid;
			UPDATE pgbench_tellers SET tbalance = tbalance + :delta
				WHERE tid = :tid;
			UPDATE pgbench_branches SET bbalance = bbalance + :delta
				WHERE bid = :bid;
			INSERT INTO pgbench_history (tid, bid, aid, delta, mtime)
				VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
			COMMIT;
		),
	},

	# A balanced pair of updates on the ledger: one +diff, one -diff in
	# the same transaction, in id order so that concurrent writers cannot
	# deadlock.  The sum is therefore the same at every commit.
	balanced_pair => {
		weight => 3,
		requires => { schema => ['ledger'] },
		script => q(
			\set a random(1, :naccounts)
			\set b random(1, :naccounts)
			\set lo least(:a, :b)
			\set hi greatest(:a, :b)
			\set diff random(1, 10000)
			BEGIN;
			UPDATE pgbench_accounts SET ledger = ledger + :diff WHERE aid = :lo;
			\sleep 1 ms
			UPDATE pgbench_accounts SET ledger = ledger - :diff WHERE aid = :hi;
			COMMIT;
		),
	},

	# The same, through PREPARE TRANSACTION and COMMIT PREPARED (or,
	# sometimes, ROLLBACK PREPARED): either way the transaction is
	# internally balanced, and the CONCURRENTLY commands have to cope
	# with transactions that are prepared but not yet resolved.
	twophase => {
		weight => 1,
		requires => { schema => ['ledger'] },
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
	},

	# Batches rather than single rows: every batch holds as many +1 rows
	# as -1 rows, so the sum is zero at every commit no matter which
	# batches happen to be present.  This drives the multi-insert and
	# COPY paths and the bulk index insertions they cause.
	# Bulk insertion into pgbench_history, which is append-only, is one of
	# the relations the rotation repacks, and whose delta column the
	# balance check adds up.  Every batch holds as many +1 rows as -1
	# rows, so the sum is untouched however many batches are present, and
	# the rows are marked with teller 0, which nothing else writes, so a
	# batch can be removed again without disturbing anything.
	bulk_copy => {
		weight => 1,
		# pgbench has no way to feed COPY from its own script, so the
		# batch lives in a file the server reads.
		files => {
			'copy_batch.txt' => join('',
				map { "0\t1\t1\t" . ($_ % 2 == 0 ? 1 : -1) . "\n" } (1 .. 200)),
		},
		script => sub {
			my ($ctx) = @_;
			my $copyfile = $ctx->{files}->{'copy_batch.txt'};
			return qq(
			\\set mode random(0, 2)
			\\if :mode = 0
				BEGIN;
				INSERT INTO pgbench_history(tid, bid, aid, delta, mtime)
					SELECT 0, 1, 1, CASE WHEN g % 2 = 0 THEN 1 ELSE -1 END,
						CURRENT_TIMESTAMP
					FROM generate_series(1, 200) g;
				COMMIT;
			\\elif :mode = 1
				COPY pgbench_history(tid, bid, aid, delta) FROM '$copyfile';
			\\else
				DELETE FROM pgbench_history WHERE tid = 0;
			\\endif
			);
		},
	},

	# Every key exists and none is ever deleted, so an upsert and a MERGE
	# both take their "matched" path.  A rebuild that missed a row being
	# upserted at that moment would put a second row under the same key.
	#
	# The two ON CONFLICT forms are both here because they reach the
	# arbiter index by different routes: inferring from the column list,
	# and naming the constraint.  While REINDEX CONCURRENTLY swaps a
	# constraint's index, more than one index matches the constraint, and
	# a speculative insertion that picks a different set from its
	# neighbour reports a duplicate key that is not there.
	upsert_merge => {
		weight => 3,
		requires => { schema => ['upsert_keys'] },
		script => q(
			\set k random(1, :naccounts)
			\set v random(1, 100000)
			\set mode random(0, 2)
			-- Only ukey is ever written, so none of these disturbs the
			-- four-way balance; the row always exists, so all three take
			-- their matched path and no row is ever added.
			\if :mode = 0
				INSERT INTO pgbench_accounts(aid, bid, abalance, ukey)
					VALUES (:k, 1, 0, :v)
					ON CONFLICT (aid) DO UPDATE SET ukey = EXCLUDED.ukey;
			\elif :mode = 1
				INSERT INTO pgbench_accounts(aid, bid, abalance, ukey)
					VALUES (:k, 1, 0, :v)
					ON CONFLICT ON CONSTRAINT pgbench_accounts_pkey
					DO UPDATE SET ukey = EXCLUDED.ukey;
			\else
				-- The casts give the USING columns a type: without them
				-- the parameters arrive untyped under the extended and
				-- prepared protocols and come out as text.
				MERGE INTO pgbench_accounts t
					USING (SELECT :k::int AS aid, :v::int AS ukey) s
					ON t.aid = s.aid
					WHEN MATCHED THEN UPDATE SET ukey = s.ukey;
			\endif
		),
	},

	# Inserts serialized behind an advisory lock, so that commit order
	# matches the order the sequence handed the values out.
	serial_insert => {
		weight => 3,
		requires => { schema => ['gapless'] },
		script => q(
			BEGIN;
			SELECT pg_advisory_xact_lock(7);
			-- delta zero, so the history sum the balance check compares
			-- against is untouched.
			INSERT INTO pgbench_history(tid, bid, aid, delta, mtime, gval)
				VALUES (1, 1, 1, 0, CURRENT_TIMESTAMP,
					nextval('pgb_gapless_val'));
			COMMIT;
		),
	},

	# Wide values rewritten together with their md5, in one statement, so
	# that every row satisfies md5(payload) = h at every commit.
	toast_rewrite => {
		weight => 2,
		requires => { schema => ['toast'] },
		script => q(
			\set id random(1, :naccounts)
			\set len random(3000, 6000)
			-- The payload is built once in the subquery and used for both
			-- columns; computing it twice would give two different values
			-- and a mismatch that is the test's fault, not the server's.
			--
			-- Large and compressible, which is a narrower target than it
			-- looks.  It has to compress -- the interesting header is a
			-- compressed one -- but still exceed the toast threshold
			-- after compressing, or it stays in the tuple.  A repeated
			-- hash compresses about fiftyfold, so the raw value has to be
			-- a hundred kilobytes or so to leave a couple of kilobytes
			-- behind.
			UPDATE pgbench_accounts SET payload = s.p, h = md5(s.p)
				FROM (SELECT repeat(md5(random()::text), :len) AS p) s
				WHERE aid = :id;
		),
	},

	# Updates of the column a stored generated column is computed from.
	# The fix for stored generated columns under REPACK was about tuples
	# "concurrently updated or inserted", so this does both, and deletes
	# what it inserts so the table does not grow without bound.  Inserted
	# rows use ids above the ones the setup created, which keeps them out
	# of the way of the updates.
	generated_update => {
		weight => 2,
		requires => { schema => ['generated'] },
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
	},

	# Inserts, deletes and repointings of child rows, each of which fires
	# a foreign key check against the parent.
	fk_churn => {
		weight => 4,
		requires => { schema => ['fk_child'] },
		script => q(
			\set pid_a random(1, :nparents)
			\set pid_b random(1, :nparents)
			\set mode random(0, 2)
			-- Each of these fires a referential integrity check against
			-- pgb_parent, which resolves it through the primary key the
			-- rotation is rebuilding.
			\if :mode = 0
				INSERT INTO pgb_child(pid, val) VALUES (:pid_a, :pid_b);
			\elif :mode = 1
				UPDATE pgb_child SET pid = :pid_b
					WHERE cid = (SELECT cid FROM pgb_child
						WHERE pid = :pid_a LIMIT 1);
			\else
				DELETE FROM pgb_child
					WHERE cid = (SELECT cid FROM pgb_child
						WHERE pid = :pid_a ORDER BY cid DESC LIMIT 1)
					AND (SELECT COUNT(*) FROM pgb_child) > :nparents;
			\endif
		),
	},

	# Duplicate slots, which must always be rejected, and a slot freed
	# and taken again in one transaction, which must end up occupied
	# exactly once.
	exclusion_churn => {
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
	},

	# Savepoints, and a PL/pgSQL loop whose body is an exception block --
	# a subtransaction per iteration, enough of them to overflow the
	# backend's subxid cache while a CONCURRENTLY command waits on it.
	subxact_churn => {
		weight => 2,
		requires => { schema => ['ledger'] },
		setup => q(
			CREATE FUNCTION pgb_subxact_churn(lo int, hi int, diff int, n int)
			RETURNS void LANGUAGE plpgsql AS $$
			DECLARE
				i int;
			BEGIN
				FOR i IN 1 .. n LOOP
					BEGIN
						UPDATE pgbench_accounts SET ledger = ledger + diff WHERE aid = lo;
						UPDATE pgbench_accounts SET ledger = ledger - diff WHERE aid = hi;
						IF i < n THEN
							RAISE EXCEPTION 'discarding subtransaction %', i;
						END IF;
					EXCEPTION WHEN raise_exception THEN
						NULL;
					END;
				END LOOP;
			END;
			$$;
		),
		script => q(
			\set a random(1, :naccounts)
			\set b random(1, :naccounts)
			\set lo least(:a, :b)
			\set hi greatest(:a, :b)
			\set diff random(1, 10000)
			\set mode random(0, 1)
			\if :mode = 0
				BEGIN;
				SAVEPOINT sp1;
				UPDATE pgbench_accounts SET ledger = ledger + :diff WHERE aid = :lo;
				ROLLBACK TO SAVEPOINT sp1;
				UPDATE pgbench_accounts SET ledger = ledger + :diff WHERE aid = :lo;
				UPDATE pgbench_accounts SET ledger = ledger - :diff WHERE aid = :hi;
				COMMIT;
			\else
				SELECT pgb_subxact_churn(:lo, :hi, :diff, 80);
			\endif
		),
	},

	# A cursor held open across a pause, driven from PL/pgSQL so the
	# whole scan-with-a-pause happens inside one call.  What the cursor
	# reads must stay readable and consistent while the indexes under it
	# are rebuilt.
	cursor_hold => {
		weight => 1,
		requires => { schema => ['ledger'] },
		setup => q(
			CREATE FUNCTION pgb_cursor_sum(expected bigint) RETURNS void
			LANGUAGE plpgsql AS $$
			DECLARE
				c CURSOR FOR SELECT ledger FROM pgbench_accounts;
				v int;
				total bigint := 0;
				seen int := 0;
			BEGIN
				OPEN c;
				LOOP
					FETCH c INTO v;
					EXIT WHEN NOT FOUND;
					total := total + v;
					seen := seen + 1;
					IF seen = 100 THEN
						PERFORM pg_sleep(0.005);
					END IF;
				END LOOP;
				CLOSE c;
				-- REPACK (CONCURRENTLY) is not MVCC-safe yet: a snapshot
				-- spanning its swap may find the table empty.  Anything
				-- else must add up.
				IF seen <> 0 AND total <> expected THEN
					RAISE EXCEPTION 'cursor read % over % rows, not %',
						total, seen, expected;
				END IF;
			END;
			$$;
		),
		script => q(
			SELECT pgb_cursor_sum(0);
		),
	},

	# The same reads through a PL/pgSQL function, whose plans are cached
	# in its own plan cache across calls.  Combined with the scenario's
	# prepared protocol and force_generic_plan, this keeps plans alive
	# across the DDL that must invalidate them.
	plancache => {
		weight => 2,
		requires => { schema => ['ledger'] },
		setup => q(
			CREATE FUNCTION pgb_cached_sum() RETURNS bigint
			LANGUAGE plpgsql AS $$
			DECLARE
				total bigint;
			BEGIN
				SELECT COALESCE(SUM(ledger), 0) INTO total FROM pgbench_accounts;
				RETURN total;
			END;
			$$;
		),
		script => q(
			\set a random(1, :naccounts)
			\set b random(1, :naccounts)
			\set lo least(:a, :b)
			\set hi greatest(:a, :b)
			\set diff random(1, 10000)
			BEGIN;
			UPDATE pgbench_accounts SET ledger = ledger + :diff WHERE aid = :lo;
			UPDATE pgbench_accounts SET ledger = ledger - :diff WHERE aid = :hi;
			COMMIT;
			SELECT stress_assert(s = 0,
				-- The cast matters under the prepared protocol, where
				-- the variable below becomes a query parameter and
				-- format() gives the planner nothing to infer its type
				-- from.  Note also that pgbench substitutes variables
				-- inside SQL comments, so naming one here with its colon
				-- would create a parameter of its own.
				format('cached plan read %s, not zero', s))
				FROM (SELECT pgb_cached_sum() AS s) x;
		),
	},

	# Upserts over every column the access methods index, so each of them
	# has insertions to absorb while it is being rebuilt.
	am_churn => {
		weight => 2,
		requires => { schema => ['am_columns'] },
		script => q(
			\set id random(1, :naccounts)
			\set n random(1, 1000000)
			UPDATE pgbench_accounts SET tags = ARRAY[md5(random()::text)],
				p = point(:n, :n), n = :n,
				ip = ('10.0.0.' || (:n % 255))::inet
				WHERE aid = :id;
		),
	},

	# Local writes on the subscriber, against the very rows being
	# applied.  The column is the subscriber's own and is indexed, so
	# these updates are never HOT: they move the rows around underneath
	# the apply worker's lookups.
	subscriber_churn => {
		weight => 3,
		target => 'subscriber',
		# The column it writes belongs to the subscriber, and there is no
		# subscriber anywhere else.
		requires => { env => ['subscription'] },
		script => q(
			\set aid random(1, :naccounts)
			UPDATE pgbench_accounts SET sub_local = sub_local + 1
				WHERE aid = :aid;
		),
	},

	# A row deleted and inserted again under the same key in one
	# transaction.  That is atomic, so at every commit boundary the row
	# is present exactly once -- but while it runs, the row's only live
	# version belongs to an uncommitted transaction, which is the state
	# the apply worker's tuple lookup has to cope with.
	#
	# The value comes from DELETE ... RETURNING rather than a separate
	# read: the apply worker may change the row between a read and the
	# delete, and re-inserting a stale value would silently undo what it
	# applied.  The advisory lock keeps two of these off the same key.
	subscriber_delete_reinsert => {
		weight => 1,
		target => 'subscriber',
		requires => { env => ['subscription'] },
		# Deleting an account row, even for the instant this transaction
		# holds it, breaks a foreign key pointed at it.
		conflicts => { schema => ['fk_child'] },
		script => q(
			\set aid random(1, :naccounts)
			BEGIN;
			SELECT pg_advisory_xact_lock(:aid);
			-- The delete is wrapped so that this always returns exactly
			-- one row: REPACK (CONCURRENTLY) is not MVCC-safe yet, and a
			-- statement that spans its swap can find the table empty and
			-- delete nothing.  When that happens the row is still there
			-- in the new relfilenode, so there is nothing to put back.
			WITH d AS (DELETE FROM pgbench_accounts WHERE aid = :aid
					RETURNING bid, abalance)
				SELECT COUNT(*) AS n, COALESCE(MAX(bid), 0) AS bid,
					COALESCE(MAX(abalance), 0) AS abalance FROM d \gset del_
			\sleep 1 ms
			\if :del_n > 0
				-- The casts are for the extended and prepared protocols,
				-- where these arrive as query parameters and the values
				-- came back through \gset with no type attached.
				INSERT INTO pgbench_accounts(aid, bid, abalance, sub_local)
					VALUES (:aid, :del_bid::int, :del_abalance::int, 0);
			\endif
			COMMIT;
		),
	},

	# DML through the partitioned parent, routed across all partitions,
	# with both rows of the pair in the same partition so that each
	# partition's sum is invariant on its own.
	#
	# Both rows are moved by a single statement on purpose.  A partition
	# can be detached between two statements of one transaction, and then
	# the second would match nothing and leave the pair unbalanced --
	# which is a property of the test, not a bug in the server.  One
	# statement sees one partition descriptor throughout.
	partition_dml => {
		weight => 3,
		requires => { schema => ['partitioned_side'] },
		script => q(
			\set part random(0, 3)
			\set a random(17, 2500)
			\set b random(17, 2500)
			\set lo least(:a, :b) + :part * 2500
			\set hi greatest(:a, :b) + :part * 2500
			\set diff random(1, 10000)
			-- The casts are what make this work under the prepared
			-- protocol, where a pgbench variable arrives as an untyped
			-- parameter and unary minus has nothing to resolve against.
			UPDATE pgb_part SET val = val
					+ CASE WHEN id = :lo THEN (:diff::int) ELSE 0 END
					+ CASE WHEN id = :hi THEN -(:diff::int) ELSE 0 END
				WHERE id IN (:lo, :hi);
		),
	},

	# The same balanced pair, applied in the mode logical replication's
	# apply worker runs in: ordinary triggers do not fire, replica ones
	# do, and row level security is still enforced.
	replica_role_apply => {
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
	},

	# Traffic in the overflow partition, so that the partition the detach
	# commands take away is not an empty one.  Every row carries a zero
	# balance, so however many of them exist, and whether or not their
	# partition is currently attached, the four sums are unmoved.
	overflow_churn => {
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
	},

	# ON CONFLICT routed through a partitioned table, where the arbiter
	# indexes for each partition are worked out from the parent's.  An
	# index that REINDEX CONCURRENTLY built on a partition has no parent
	# of its own until the swap finishes, and has to be recognized as an
	# arbiter anyway.
	#
	# Every id already exists, so this always takes the conflict path,
	# and the update leaves val alone: the partitioned sum is untouched.
	partition_upsert => {
		weight => 3,
		requires => { schema => ['partitioned_side'] },
		script => q(
			\set id random(17, :part_rows)
			SELECT pgb_part_upsert(:id);
		),
	},

	# CONCURRENTLY on a temporary table.  These run in several
	# transactions, and ON COMMIT DELETE ROWS empties the table under
	# each one, so the command has to notice it is working on a table
	# nobody else can see and take the ordinary path instead.
	#
	# Each pgbench client has its own temporary schema, so the clients do
	# not collide with each other and this load fits any scenario.
	temp_table_cic => {
		weight => 1,
		# This is a workload client that runs index DDL, and the
		# cancellation environment picks its victims by matching the
		# query text -- so it would terminate this one and the run would
		# fail on a writer that died, which is not what that environment
		# is testing.
		conflicts => { env => ['cancellation'] },
		script => q(
			-- The table survives the whole session, so every transaction
			-- after the first would report it already exists, and the run
			-- insists on an empty stderr.
			SET client_min_messages = warning;
			CREATE TEMP TABLE IF NOT EXISTS pgb_tmp(i int)
				ON COMMIT DELETE ROWS;
			INSERT INTO pgb_tmp SELECT g FROM generate_series(1, 100) g;
			DROP INDEX IF EXISTS pgb_tmp_idx;
			CREATE INDEX CONCURRENTLY pgb_tmp_idx ON pgb_tmp(i);
			REINDEX INDEX CONCURRENTLY pgb_tmp_idx;
		),
	},

	# Rows that exist only to fire a trigger.  The insert itself is
	# trivial and touches a table nothing rebuilds; the work is in the
	# after-insert trigger, which upserts a counter row and so has to
	# resolve an arbiter index every time -- from a plan cached the first
	# time this session fired it, while the rotation rebuilds the very
	# index that arbiter resolves to.  Every other upsert here is sent by
	# pgbench and inferred afresh against the catalog as it stands.
	trigger_upsert_log => {
		weight => 3,
		requires => { schema => ['trigger_audit'] },
		script => q(
			INSERT INTO pgb_call_log(client) VALUES (:client_id);
		),
	},

	# The bitmap scan that has to agree with the heap.
	bmskip_check => {
		weight => 3,
		requires => { schema => ['bitmap_skip_fetch'] },
		script => q(
			BEGIN ISOLATION LEVEL REPEATABLE READ;
			SELECT pgb_bmskip_check();
			COMMIT;
		),
	},

	# Rows dying and coming back, so the bitmap has entries for TIDs a
	# vacuum is about to remove.  The row count is left where it started.
	bmskip_churn => {
		weight => 4,
		requires => { schema => ['bitmap_skip_fetch'] },
		script => q(
			\set k random(1, 4000)
			BEGIN;
			DELETE FROM pgb_bmskip WHERE b = :k;
			INSERT INTO pgb_bmskip(b) VALUES (:k);
			COMMIT;
		),
	},

	# The index-only scan that has to agree with the heap.  Repeatable
	# read, so that the scan and the count that checks it share one
	# snapshot; the sleep is the window a vacuum has to land in, and is
	# the whole reason this is reachable without controlling the
	# interleaving by hand.
	ios_cursor_check => {
		weight => 3,
		requires => { schema => ['ios_vacuum_race'] },
		script => q(
			\set tbl random(0, 1)
			BEGIN ISOLATION LEVEL REPEATABLE READ;
			\if :tbl = 0
				SELECT pgb_ios_cursor_check('pgb_ios_gist', 0.02);
			\else
				SELECT pgb_ios_cursor_check('pgb_ios_spgist', 0.02);
			\endif
			COMMIT;
		),
	},

	# Rows dying and coming back, so that there is always something for a
	# vacuum to remove and an index entry pointing at where it was.  The
	# row count is left where it started, which is what the scan's own
	# check relies on.
	ios_churn => {
		weight => 4,
		requires => { schema => ['ios_vacuum_race'] },
		script => q(
			\set k random(1, 200)
			\set tbl random(0, 1)
			BEGIN;
			\if :tbl = 0
				DELETE FROM pgb_ios_gist WHERE b = :k;
				INSERT INTO pgb_ios_gist(a, b)
					VALUES (point(:k, nextval('pgb_ios_seq')), :k);
			\else
				DELETE FROM pgb_ios_spgist WHERE b = :k;
				INSERT INTO pgb_ios_spgist(a, b)
					VALUES (point(:k, nextval('pgb_ios_seq')), :k);
			\endif
			COMMIT;
		),
	},

	# A read that runs from a cached plan rather than a fresh one.  The
	# plan chose an index on pgb_audit and holds it; drop_create_index
	# takes that index away and builds it again underneath.  A plan that
	# does not notice is a session holding an index list it read before
	# the drop, which is the shape the planner had to be taught to
	# tolerate on a standby.
	audit_probe => {
		weight => 2,
		requires => { schema => ['trigger_audit'] },
		setup => q(
			CREATE FUNCTION pgb_audit_probe(p_aid int) RETURNS bigint
			LANGUAGE plpgsql AS $$
			DECLARE n bigint;
			BEGIN
				SELECT count(*) INTO n FROM pgb_audit WHERE aid = p_aid;
				RETURN n;
			END $$;
		),
		script => q(
			\set aid random(1, :naccounts)
			SELECT pgb_audit_probe(:aid);
		),
	},
);

=pod

=head2 %DDL

C<variants> returns the alternatives the DDL client picks between, one
per invocation; each is an arrayref of statements that run together.  It
is called with the scenario context, so an entry expands itself over
whatever tables and indexes the scenario actually has.

Each variant names the C<table> it works on, which is what the
per-relation gate uses to keep two commands off the same relation when
several run at once.  A variant that touches relations it does not name
-- dropping one, say -- cannot be gated that way and sets C<solo>, which
restricts it to scenarios running one command at a time.

=cut

# Run a statement that competes for locks with the DDL rotation, giving
# way if the deadlock detector picks it as the victim.  Only a deadlock
# is retried: anything else is the failure the test is looking for.
sub _retry_on_deadlock
{
	my ($node, $sql) = @_;

	foreach my $try (1 .. 10)
	{
		my ($rc, $out, $err) = $node->psql('postgres', $sql);
		return if $rc == 0;
		die "$sql failed: $err" unless $err =~ /deadlock detected/;
	}
	die "$sql kept deadlocking";
}

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

our %DDL = (
	repack_concurrently => {
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
	},

	repack_using_index => {
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
	},

	reindex_table_concurrently => {
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
	},

	reindex_index_concurrently => {
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
	},

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
	reindex_pkey_concurrently => {
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
	},

	drop_create_index => {
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
	},

	# REINDEX on a table that has no indexes.  There is nothing to
	# rebuild, so the command returns early -- and that early return is
	# the one place the snapshot stack can be left wrong, which the
	# event trigger firing afterwards is there to notice.  It cannot go
	# through reindex_table_concurrently, because that walks the
	# rotation's tables and this one has no primary key for the rest of
	# the rotation to work with.
	reindex_bare_table => {
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
	},

	# REINDEX of the partitioned index itself, as opposed to a leaf's.
	# It rebuilds one child per partition, and each new child is
	# unparented until the swap, so this is what drives the code that
	# treats an unparented index as an additional arbiter.  The
	# rotation's other reindex entries only ever name a leaf.
	reindex_partitioned_index => {
		requires => { schema => ['partitioned_side'] },
		variants => sub {
			return map {
				{
					table => 'pgb_part',
					stmts => ["REINDEX INDEX CONCURRENTLY $_;"]
				}
			} (qw(pgb_part_pkey pgb_part_id_uniq));
		},
	},

	# Detach and re-attach a hash partition.  Same command as the range
	# case, different bound syntax, and a different substitute
	# constraint to leave behind if the server gets it wrong.
	detach_hash_partition => {
		requires => { schema => ['partitioned_hash'] },
		# Names the parent while removing one of its partitions, so a
		# command gated on that partition could find it gone.
		solo => 1,
		variants => sub {
			return map {
				{
					table => 'pgb_hash',
					stmts => [
						"ALTER TABLE pgb_hash DETACH PARTITION pgb_hash_$_ CONCURRENTLY;",
						"SELECT pgb_ddl_bounded('ALTER TABLE pgb_hash "
						  . "ATTACH PARTITION pgb_hash_$_ FOR VALUES WITH "
						  . "(MODULUS 4, REMAINDER $_)');"
					]
				}
			} (0 .. 3);
		},
	},

	# A constraint added unvalidated and then validated.  Neither takes
	# an exclusive lock, so both run against a live workload, and
	# VALIDATE scans the whole table while the rotation rewrites it.
	add_validate_constraint => {
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
	},

	# One command rebuilding every index in the schema, which sequences
	# its locks differently from the per-index and per-table forms.
	reindex_schema_concurrently => {
		solo => 1,
		variants => sub {
			return (
				{
					table => 'public',
					stmts => ['REINDEX SCHEMA CONCURRENTLY public;']
				});
		},
	},

	# VACUUM's index cleanup and freezing have to coexist with the
	# concurrent rebuilds and the tuple movement.
	vacuum => {
		variants => sub {
			my ($ctx) = @_;
			return map {
				{ table => $_, stmts => ["VACUUM $_;"] },
				  { table => $_, stmts => ["VACUUM (FREEZE) $_;"] },
				  { table => $_, stmts => ["VACUUM (ANALYZE) $_;"] }
			} @{ $ctx->{tables} };
		},
	},

	# A rewriting ALTER TABLE takes AccessExclusiveLock and gives the
	# table a new relfilenode with every index rebuilt, so a REINDEX or
	# REPACK that was waiting resumes against a table of a new shape.
	alter_table_rewrite => {
		# Adding and dropping a column changes the shape of a published
		# table, and the subscriber does not follow: apply then fails on
		# every change until the column is gone again, and the
		# subscription never catches up.
		conflicts => { env => ['subscription'] },
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
	},

	refresh_matview_concurrently => {
		requires => { schema => ['matview'] },
		variants => sub {
			return ({
				table => 'pgb_mv',
				stmts => ['REFRESH MATERIALIZED VIEW CONCURRENTLY pgb_mv;']
			});
		},
	},

	# Detaching a partition concurrently leaves behind a CHECK constraint
	# matching the bound, so the re-attach needs no validation scan.
	detach_partition_concurrently => {
		requires => { schema => ['partitioned_side'] },
		variants => sub {
			my ($ctx) = @_;
			my @v;
			my @bounds = (
				[ 1, 2501 ], [ 2501, 5001 ], [ 5001, 7501 ],
				[ 7501, $NROWS + 1 ]);
			for my $i (0 .. 3)
			{
				my $p = 'pgb_part_' . ($i + 1);
				push @v,
				  {
					table => 'pgb_part',
					stmts => [
						"ALTER TABLE pgb_part DETACH PARTITION $p CONCURRENTLY;",
						'\sleep 10 ms',
						"SELECT pgb_ddl_bounded('ALTER TABLE pgb_part "
						  . "ATTACH PARTITION $p FOR VALUES FROM "
						  . "($bounds[$i][0]) TO ($bounds[$i][1])');"
					]
				  };
			}
			return @v;
		},
	},

	# Detach a partition and then drop it outright.  That is the shape
	# that reaches a partition descriptor being rebuilt for a cached plan
	# whose partition no longer exists at all -- the catalog scan finds
	# nothing where the descriptor expects a tuple, and the planner opens
	# a relation that has gone.
	#
	# The rows are carried across to a replacement table before the drop,
	# so the partitioned sum is the same on the other side.  Both ids a
	# balanced update touches fall in the same partition, so while this
	# one is detached that update moves neither of them.
	detach_drop_recreate_partition => {
		requires => { schema => ['partitioned_side'] },
		# The per-relation gate serializes commands that name the same
		# relation, and this one names the parent while destroying a
		# partition -- so a command gated on that partition can find it
		# gone.  That is a collision the suite arranged, not a race worth
		# reporting, so this variant only makes sense when one command
		# runs at a time.
		solo => 1,
		variants => sub {
			my ($ctx) = @_;
			my @v;
			my @bounds = (
				[ 1, 2501 ], [ 2501, 5001 ], [ 5001, 7501 ],
				[ 7501, $NROWS + 1 ]);
			for my $i (0 .. 3)
			{
				my $p = 'pgb_part_' . ($i + 1);
				push @v,
				  {
					table => 'pgb_part',
					stmts => [
						"ALTER TABLE pgb_part DETACH PARTITION $p CONCURRENTLY;",
						"CREATE TABLE ${p}_next (LIKE $p INCLUDING ALL);",
						"INSERT INTO ${p}_next SELECT * FROM $p;",
						"DROP TABLE $p;",
						"ALTER TABLE ${p}_next RENAME TO $p;",
						"SELECT pgb_ddl_bounded('ALTER TABLE pgb_part "
						  . "ATTACH PARTITION $p FOR VALUES FROM "
						  . "($bounds[$i][0]) TO ($bounds[$i][1])');"
					]
				  };
			}
			return @v;
		},
	},

	# Detach and re-attach the overflow partition of pgbench_accounts.
	# This is the detach running against the table the whole workload is
	# on, rather than one standing to the side of it.
	detach_overflow_partition => {
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
	},

	# The same one level down, where the partition being detached has a
	# grandparent.
	detach_subpartition => {
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
	},

	# CREATE INDEX CONCURRENTLY refuses a partitioned table; the
	# documented way to build one without blocking writes is to create it
	# on ONLY the parent, build a matching index on every partition, and
	# attach them one by one, at which point the parent index becomes
	# valid.
	partitionwise_index_build => {
		requires => { schema => ['partitioned_side'] },
		variants => sub {
			my ($ctx) = @_;

			# Start by clearing up after a previous attempt: this
			# sequence is several statements long and the run can end
			# anywhere in it, leaving the parent index, or a partition
			# index that was built but not yet attached, behind.
			# Dropping the parent takes its attached indexes with it.
			# IF EXISTS says so with a NOTICE when there is nothing to
			# drop, which is the normal case and would go to pgbench's
			# stderr, where the run insists on silence.
			my @stmts = (
				'SET client_min_messages = warning;',
				'DROP INDEX IF EXISTS pgb_part_val_idx;');
			push @stmts, "DROP INDEX IF EXISTS pgb_part_${_}_val_idx;"
			  for (1 .. 4);
			push @stmts, 'RESET client_min_messages;';

			push @stmts, 'CREATE INDEX pgb_part_val_idx ON ONLY pgb_part(val);';
			for my $i (1 .. 4)
			{
				push @stmts,
				  "CREATE INDEX CONCURRENTLY pgb_part_${i}_val_idx "
				  . "ON pgb_part_$i(val);",
				  "ALTER INDEX pgb_part_val_idx ATTACH PARTITION pgb_part_${i}_val_idx;";
			}
			push @stmts, '\sleep 10 ms', 'DROP INDEX pgb_part_val_idx;';
			return ({ table => 'pgb_part', stmts => [@stmts] });
		},
	},

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
	repack_hot_small => {
		variants => sub {
			my ($ctx) = @_;
			return map {
				{ table => $_, stmts => ["REPACK (CONCURRENTLY) $_;"] }
			}
			  grep { $_ eq 'pgbench_branches' || $_ eq 'pgbench_tellers' }
			  @{ $ctx->{tables} };
		},
	},

	# The vacuum that gets ahead of a bitmap scan and empties the pages
	# its bitmap still refers to.
	vacuum_bmskip => {
		requires => { schema => ['bitmap_skip_fetch'] },
		variants => sub {
			return ({
				table => 'pgb_bmskip',
				stmts => ['VACUUM (TRUNCATE false) pgb_bmskip;']
			});
		},
	},

	# The vacuum that removes what the scan queued.  TRUNCATE false
	# because truncation wants AccessExclusiveLock, which against a
	# workload only ends in the lock timeout -- and truncation is no part
	# of what is being tested.  Aimed at these tables on their own so it
	# runs as often as the rotation will allow.
	vacuum_ios_tables => {
		requires => { schema => ['ios_vacuum_race'] },
		variants => sub {
			return map {
				{ table => $_, stmts => ["VACUUM (TRUNCATE false) $_;"] }
			} (qw(pgb_ios_gist pgb_ios_spgist));
		},
	},

	# The replica identity moved from one index to another, and back,
	# while the rotation rebuilds and repacks the table it belongs to.
	#
	# ALTER TABLE ... REPLICA IDENTITY needs AccessExclusiveLock, so it
	# goes through the bounded helper; what it races is not the lock but
	# the several transactions a concurrent command spans, having decided
	# which index it would use in the first of them.
	# Both moves in one turn, rather than one per turn.  The rotation
	# picks between several entries and a six-second run may reach this
	# one only once or not at all, so a variant that moved the identity a
	# single step could leave a run having seen one identity and proved
	# nothing -- which is what identity_moved reported when it was written
	# that way.
	move_replica_identity => {
		requires => { schema => ['movable_identity'] },
		variants => sub {
			return ({
				table => 'pgbench_accounts',
				stmts => [
					"SELECT pgb_ddl_bounded('ALTER TABLE pgbench_accounts "
					  . "REPLICA IDENTITY USING INDEX pgb_ident_b');",
					'SELECT pgb_note_identity();',
					"SELECT pgb_ddl_bounded('ALTER TABLE pgbench_accounts "
					  . "REPLICA IDENTITY USING INDEX pgb_ident_a');",
					'SELECT pgb_note_identity();'
				]
			});
		},
	},

	# Data checksums turned off and on again while the workload runs.
	#
	# Enabling them is not a switch: a background worker walks every page
	# of every relation, reads it, computes a checksum and writes it back,
	# and the cluster sits in an "inprogress-on" state until it finishes.
	# That is a whole-database rewrite running concurrently with the
	# rotation's own rewrites, which is the same shape as everything else
	# here and is reached by nothing else in the suite.
	#
	# Aimed at the cluster rather than a relation, so it cannot be gated
	# per-table the way the rest of the rotation is.
	toggle_data_checksums => {
		requires => { schema => ['data_checksum_helper'] },
		solo => 1,
		variants => sub {
			return ({
				table => 'pg_database',
				stmts => ['SELECT pgb_flip_data_checksums();']
			});
		},
	},

	# A trigger created on the table the rotation is rebuilding, and
	# dropped again.  Both ends invalidate the relation, so every backend
	# that has it open rebuilds its descriptor -- which is what a
	# concurrent build has to survive, and what the historical
	# RelationBuildDesc race got wrong.  Nothing else here invalidates a
	# table this cheaply and this often: the index commands do it too,
	# but each of them costs a whole build.
	#
	# CREATE TRIGGER takes ShareRowExclusiveLock and DROP TRIGGER an
	# AccessExclusiveLock, so both wait for the workload and both have to
	# be bounded.  OR REPLACE and IF EXISTS keep the pair idempotent, so
	# a create that timed out does not make the drop that follows it an
	# error -- and two DDL clients aimed at the same table serialize on
	# the locks rather than collide.
	create_drop_trigger => {
		requires => { schema => ['trigger_audit'] },
		variants => sub {
			my ($ctx) = @_;
			return map {
				my $t = $_;
				{
					table => $t,
					stmts => [
						# "trigger does not exist, skipping" is a NOTICE,
						# and the run insists on a silent stderr.
						'SET client_min_messages = warning;',
						"SELECT pgb_ddl_bounded('CREATE OR REPLACE TRIGGER "
						  . "pgb_noop_trg BEFORE UPDATE ON $t FOR EACH ROW "
						  . "EXECUTE FUNCTION pgb_noop_trigger()');",
						"SELECT pgb_ddl_bounded('DROP TRIGGER IF EXISTS "
						  . "pgb_noop_trg ON $t');",
						'RESET client_min_messages;'
					]
				}
			} @{ $ctx->{tables} };
		},
	},

	# The same thing as a constraint trigger, which is a different
	# catalog shape for the same idea: it carries a pg_constraint row of
	# its own, so creating and dropping it goes through the dependency
	# machinery rather than pg_trigger alone.  While it is there every
	# update queues an event for commit time instead of firing in place,
	# which is the after-trigger queue the deferred audit trigger also
	# uses.
	#
	# There is no OR REPLACE for a constraint trigger -- the server
	# rejects the combination outright -- so this drops whatever the last
	# invocation left and creates it again, leaving it in place until the
	# next one.  That is not idempotent between two DDL clients running
	# it at once, hence solo.
	create_drop_constraint_trigger => {
		requires => { schema => ['trigger_audit'] },
		solo => 1,
		variants => sub {
			my ($ctx) = @_;
			return map {
				my $t = $_;
				{
					table => $t,
					stmts => [
						'SET client_min_messages = warning;',
						"SELECT pgb_ddl_bounded('DROP TRIGGER IF EXISTS "
						  . "pgb_noop_ctrg ON $t');",
						"SELECT pgb_ddl_bounded('CREATE CONSTRAINT "
						  . "TRIGGER pgb_noop_ctrg AFTER UPDATE ON $t "
						  . 'DEFERRABLE INITIALLY DEFERRED FOR EACH ROW '
						  . "EXECUTE FUNCTION pgb_noop_trigger()');",
						'RESET client_min_messages;'
					]
				}
			} @{ $ctx->{tables} };
		},
	},

	# The audit trigger switched off and on again.  ENABLE and DISABLE
	# take ShareRowExclusiveLock -- they conflict with writers and
	# nothing else -- and they change a field of the descriptor rather
	# than the set of triggers, so they land on a different part of the
	# relcache entry from the create and drop above.  ALWAYS and REPLICA
	# are here because that field is what decides whether a trigger fires
	# for an apply worker, and this suite has an environment with one.
	#
	# The audit rows stop arriving while it is off, which nothing checks
	# a count of: the invariant that does hold is between the log and its
	# counters, and that trigger is on another table.
	toggle_trigger => {
		requires => { schema => ['trigger_audit'] },
		variants => sub {
			return map {
				{
					table => 'pgbench_accounts',
					stmts => [
						"SELECT pgb_ddl_bounded('ALTER TABLE pgbench_accounts "
						  . "DISABLE TRIGGER pgb_audit_trg');",
						"SELECT pgb_ddl_bounded('ALTER TABLE pgbench_accounts "
						  . "$_ TRIGGER pgb_audit_trg');",
						# Always back to firing, whichever state the middle
						# statement left it in.  ALWAYS and REPLICA are worth
						# visiting, but a run that ends up sitting in one of
						# them stops writing audit rows, and the audit table
						# is what amcheck and the deferred trigger work on.
						"SELECT pgb_ddl_bounded('ALTER TABLE pgbench_accounts "
						  . "ENABLE TRIGGER pgb_audit_trg');"
					]
				}
			} ('ENABLE', 'ENABLE ALWAYS', 'ENABLE REPLICA');
		},
	},

	# The trigger functions altered rather than the tables.  This takes
	# no lock on any relation: it updates pg_proc, which invalidates the
	# function and with it every plan that plpgsql cached for its body,
	# in every session at once.  So it is the cheapest invalidation the
	# rotation has, and the one that can be aimed at a running build at
	# the highest rate.  COST is chosen because it changes nothing a
	# caller can observe -- the bodies stay exactly as the schema wrote
	# them, and the invariants with them.
	alter_trigger_function => {
		requires => { schema => ['trigger_audit'] },
		variants => sub {
			my @fns = qw(pgb_audit_trigger pgb_calls_trigger
			  pgb_audit_defer_trigger);
			return map {
				my $fn = $_;
				{
					# Named for the relation this really writes to.  Naming
					# a table would gate it against the very rebuilds it is
					# supposed to interrupt, and it takes no lock on one.
					table => 'pg_proc',
					stmts => [
						"ALTER FUNCTION $fn() COST 100;",
						"ALTER FUNCTION $fn() COST 1;"
					]
				}
			} @fns;
		},
	},
);

=pod

=head2 %CHECK

C<script> is a pgbench fragment run as its own weighted script;
C<final> is a sub run against the node once the workload is over.
Either may be omitted.

=cut

# Pull every block of every permanent relation through shared buffers and
# report how many, so a read-back that reads nothing is visible instead of
# passing.  A page is only verified when it is read, so this is what makes
# the checksum counter afterwards mean anything.
my $checksum_read_all = q(
	SELECT COALESCE(sum(pg_prewarm(c.oid::regclass, 'buffer')), 0)
	FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE c.relkind IN ('r', 'i', 'm', 't')
	  AND c.relpersistence IN ('p', 'u')
	  AND n.nspname <> 'pg_toast';
);

# The same, for a standby, which cannot read unlogged relations at all.
my $checksum_read_all_standby = q(
	SELECT COALESCE(sum(pg_prewarm(c.oid::regclass, 'buffer')), 0)
	FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE c.relkind IN ('r', 'i', 'm', 't')
	  AND c.relpersistence = 'p'
	  AND n.nspname <> 'pg_toast';
);

our %CHECK = (
	# The four sums move together or not at all.  Read in one statement,
	# so they share a snapshot; the counts are only fetched when they
	# disagree, since counting every account is far too expensive to do
	# on every check.
	balances => {
		weight => 1,
		script => q(
			SELECT (SELECT COALESCE(SUM(abalance), 0) FROM pgbench_accounts) AS a,
				   (SELECT COALESCE(SUM(tbalance), 0) FROM pgbench_tellers) AS t,
				   (SELECT COALESCE(SUM(bbalance), 0) FROM pgbench_branches) AS b,
				   (SELECT COALESCE(SUM(delta), 0) FROM pgbench_history) AS h
				\gset bal_
			\if :bal_a != :bal_t or :bal_t != :bal_b or :bal_b != :bal_h
				SELECT (SELECT COUNT(*) FROM pgbench_accounts) AS a,
					   (SELECT COUNT(*) FROM pgbench_tellers) AS t,
					   (SELECT COUNT(*) FROM pgbench_branches) AS b,
					   (SELECT COUNT(*) FROM pgbench_history) AS h
					\gset cnt_
				\if :cnt_a = 0 or :cnt_t = 0 or :cnt_b = 0 or :cnt_h = 0
					SELECT 'repack: empty view tolerated' AS marker;
				\else
					SELECT stress_assert(false,
						format('balances disagree: accounts=%s tellers=%s branches=%s history=%s',
							:bal_a::bigint, :bal_t::bigint, :bal_b::bigint, :bal_h::bigint));
				\endif
			\endif
		),
		final => sub {
			my ($node, $ctx) = @_;
			my $row = $node->safe_psql(
				'postgres', q(
				SELECT (SELECT COALESCE(SUM(abalance), 0) FROM pgbench_accounts)
					|| ' ' || (SELECT COALESCE(SUM(tbalance), 0) FROM pgbench_tellers)
					|| ' ' || (SELECT COALESCE(SUM(bbalance), 0) FROM pgbench_branches)
					|| ' ' || (SELECT COALESCE(SUM(delta), 0) FROM pgbench_history)));
			my ($a, $t, $b, $h) = split / /, $row;
			Test::More::is("$t $b $h", "$a $a $a",
				'balances agree across all four tables');
		},
	},

	# The ledger's sum never moves.
	ledger_sum => {
		weight => 1,
		requires => { schema => ['ledger'] },
		script => q(
			SELECT stress_assert(cnt = 0 OR sum = 0,
				format('ledger has %s rows summing to %s', cnt, sum))
			FROM (SELECT COUNT(*) AS cnt, COALESCE(SUM(ledger), 0) AS sum
				FROM pgbench_accounts) x;
		),
		final => sub {
			my ($node, $ctx) = @_;
			Test::More::is(
				$node->safe_psql(
					'postgres', 'SELECT SUM(ledger) FROM pgbench_accounts'),
				'0', 'the balanced pairs still sum to zero');
		},
	},

	# Once the row with val = j is committed, exactly j rows have
	# val <= j -- the sequence was handed out under a lock, so commit
	# order matches value order.
	gapless_count => {
		weight => 1,
		requires => { schema => ['gapless'] },
		script => q(
			SELECT COALESCE(MAX(gval), 0) AS j FROM pgbench_history \gset g_
			\if :g_j > 0
				SELECT stress_assert(cnt = :g_j,
					format('%s rows with gval <= %s, not %s', cnt, :g_j::bigint, :g_j::bigint))
				FROM (SELECT COUNT(*) AS cnt FROM pgbench_history
					WHERE gval <= :g_j) x;
			\endif
		),
	},

	# Nothing may ever hold two rows under one key.
	distinct_keys => {
		weight => 1,
		requires => { schema => ['upsert_keys'] },
		script => q(
			SELECT stress_assert(cnt = keys,
				format('%s rows under %s keys', cnt, keys))
			FROM (SELECT COUNT(*) AS cnt, COUNT(DISTINCT aid) AS keys
				FROM pgbench_accounts) x;
		),
		final => sub {
			my ($node, $ctx) = @_;
			Test::More::is(
				$node->safe_psql('postgres',
					'SELECT COUNT(*) - COUNT(DISTINCT aid) FROM pgbench_accounts'),
				'0', 'no duplicate key got past the unique index');
		},
	},

	# Every row's out-of-line value still matches the md5 stored with it.
	toast_md5 => {
		weight => 1,
		requires => { schema => ['toast'] },
		script => sub {
			my $tol = stress_repack_tolerated('cnt');
			return qq(
			SELECT stress_assert(${tol}bad = 0,
				format('%s rows whose payload does not match its md5', bad))
			FROM (SELECT COUNT(*) FILTER (WHERE md5(payload) <> h) AS bad,
				COUNT(*) AS cnt FROM pgbench_accounts WHERE payload IS NOT NULL) x;
			);
		},
		final => sub {
			my ($node, $ctx) = @_;
			Test::More::is(
				$node->safe_psql('postgres',
					'SELECT COUNT(*) FROM pgbench_accounts WHERE payload IS NOT NULL AND md5(payload) <> h'),
				'0', 'every TOASTed payload matches its md5');
		},
	},

	# Each stored generated column is a fixed function of its base column,
	# however the table has been rewritten.  The wide one is checked as
	# well as the narrow one, because computing the value and storing it
	# out of line are separate things for the replay to get wrong.
	#
	# The virtual column is deliberately not compared against its own
	# expression: it is computed on read, so that comparison can only ever
	# hold.  What matters about it is that the table still has it and that
	# REPACK did not choke on it, which generated_defs_intact covers.
	generated_matches => {
		weight => 1,
		requires => { schema => ['generated'] },
		script => sub {
			my $tol = stress_repack_tolerated('cnt');
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
	},

	# A rewrite has to bring the generation expressions across intact.
	# Nothing the workload can observe would notice if it brought across
	# the wrong ones -- the values would simply be computed from a
	# different expression and agree with themselves -- so compare the
	# definitions against what they were before the run.
	generated_defs_intact => {
		requires => { schema => ['generated'] },
		final => sub {
			my ($node, $ctx) = @_;
			Test::More::is(
				$node->safe_psql('postgres', $GEN_DEFS_QUERY),
				$ctx->{gen_defs},
				'the generated column definitions are unchanged');
		},
	},

	# No child row may reference a parent that is not there.
	no_orphans => {
		weight => 1,
		requires => { schema => ['fk_child'] },
		script => sub {
			# The relation is in the rotation's reach, so it reading empty
			# has to be allowed for: an empty table would make every
			# reference look like an orphan.
			my $tol = stress_repack_tolerated('rows');
			return qq(
			SELECT stress_assert(${tol}orphans = 0,
				format('%s rows reference a missing parent', orphans))
			FROM (SELECT
				(SELECT COUNT(*) FROM pgb_child c WHERE NOT EXISTS
					(SELECT 1 FROM pgb_parent p WHERE p.id = c.pid)) AS orphans,
				(SELECT COUNT(*) FROM pgb_child) AS rows) x;
			);
		},
		final => sub {
			my ($node, $ctx) = @_;
			Test::More::is(
				$node->safe_psql(
					'postgres', q(
					SELECT COUNT(*) FROM pgb_child c WHERE NOT EXISTS
						(SELECT 1 FROM pgb_parent p WHERE p.id = c.pid))),
				'0', 'no orphan child rows');
		},
	},

	# One row per slot, and never two.
	distinct_slots => {
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
	},

	# The materialized view holds the bucket sums as of some snapshot,
	# and at every snapshot the ledger summed to zero, so the buckets it
	# recorded add up to zero too.
	matview_matches => {
		weight => 1,
		requires => { schema => ['matview'] },
		script => q(
			SELECT stress_assert(cnt = 0 OR sum = 0,
				format('matview has %s buckets summing to %s', cnt, sum))
			FROM (SELECT COUNT(*) AS cnt, COALESCE(SUM(s), 0) AS sum
				FROM pgb_mv) x;
		),
	},

	# Each partition's sum is invariant on its own, whether it is
	# currently attached or not, and the parent must stay queryable while
	# the partition descriptor changes underneath it.
	partition_sum => {
		weight => 1,
		requires => { schema => ['partitioned_side'] },
		script => q(
			SELECT COALESCE(SUM(val), 0) AS s, COUNT(*) AS c FROM pgb_part \gset p_
			\if :p_c = :part_rows
				-- Both sides are pgbench variables, so under the prepared
				-- protocol they arrive as parameters with nothing to
				-- resolve their type against unless they are cast.
				SELECT stress_assert(:p_s::bigint = :part_sum::bigint,
					format('partitioned sum is %s, not %s',
						:p_s::bigint, :part_sum::bigint));
			\endif
		),
	},

	# An index scan and a sequential scan of the same predicate, in one
	# snapshot, must return the same thing.
	index_vs_seq => {
		weight => 1,
		requires => { indexes => ['btree_abalance'] },
		script => q(
			BEGIN ISOLATION LEVEL REPEATABLE READ;
			SET LOCAL enable_seqscan = off;
			SET LOCAL enable_bitmapscan = off;
			SELECT COUNT(*) AS cnt, COALESCE(SUM(abalance), 0) AS sum
				FROM pgbench_accounts WHERE abalance > 0 \gset idx_
			SET LOCAL enable_seqscan = on;
			SET LOCAL enable_indexscan = off;
			SET LOCAL enable_indexonlyscan = off;
			SELECT COUNT(*) AS cnt, COALESCE(SUM(abalance), 0) AS sum
				FROM pgbench_accounts WHERE abalance > 0 \gset seq_
			COMMIT;
			-- Both reads are in one snapshot, so a swap that empties the
			-- table empties both of them; they still have to agree.
			SELECT stress_assert(:idx_cnt::bigint = :seq_cnt::bigint
					AND :idx_sum::bigint = :seq_sum::bigint,
				format('index scan (%s rows, sum %s) disagrees with seq scan (%s rows, sum %s)',
					:idx_cnt::bigint, :idx_sum::bigint, :seq_cnt::bigint, :seq_sum::bigint));
		),
	},

	# An index-only scan trusts the visibility map, so a wrong VM bit is
	# a silent wrong answer rather than an error.  Compare it against a
	# sequential scan in one snapshot.
	ios_vs_seq => {
		weight => 1,
		requires => { indexes => ['covering_aid'] },
		script => q(
			BEGIN ISOLATION LEVEL REPEATABLE READ;
			SET LOCAL enable_seqscan = off;
			SET LOCAL enable_bitmapscan = off;
			SELECT COUNT(*) AS cnt, COALESCE(SUM(abalance), 0) AS sum
				FROM pgbench_accounts WHERE aid > 0 \gset ios_
			SET LOCAL enable_seqscan = on;
			SET LOCAL enable_indexscan = off;
			SET LOCAL enable_indexonlyscan = off;
			SELECT COUNT(*) AS cnt, COALESCE(SUM(abalance), 0) AS sum
				FROM pgbench_accounts WHERE aid > 0 \gset seqio_
			COMMIT;
			SELECT stress_assert(:ios_cnt::bigint = :seqio_cnt::bigint
					AND :ios_sum::bigint = :seqio_sum::bigint,
				format('index-only scan (%s rows, sum %s) disagrees with seq scan (%s rows, sum %s)',
					:ios_cnt::bigint, :ios_sum::bigint, :seqio_cnt::bigint, :seqio_sum::bigint));
		),
	},

	# Nothing may change under a held row lock: the second read runs in a
	# fresh snapshot, so it would see any concurrent commit.
	row_lock_durability => {
		weight => 1,
		requires => { schema => ['ledger'] },
		# Takes row locks, so it cannot run on a standby.
		writes => 1,
		script => q(
			\set lo random(1, :naccounts - 4)
			BEGIN;
			SELECT COUNT(*) AS cnt, COALESCE(SUM(ledger), 0) AS sum FROM
				(SELECT ledger FROM pgbench_accounts
					WHERE aid BETWEEN :lo AND :lo + 4
					ORDER BY aid FOR UPDATE) s \gset locked_
			\sleep 20 ms
			SELECT COUNT(*) AS cnt, COALESCE(SUM(ledger), 0) AS sum
				FROM pgbench_accounts WHERE aid BETWEEN :lo AND :lo + 4 \gset reread_
			COMMIT;
			SELECT stress_assert(:reread_cnt::bigint = 0
					OR (:locked_cnt::bigint = :reread_cnt::bigint
						AND :locked_sum::bigint = :reread_sum::bigint),
				format('rows changed under a held lock: locked (%s rows, sum %s), re-read (%s rows, sum %s)',
					:locked_cnt::bigint, :locked_sum::bigint,
					:reread_cnt::bigint, :reread_sum::bigint));
		),
	},

	# A read that has to be planned, against the table whose index keeps
	# coming and going.  get_relation_info() opens every index of the
	# relation while planning, whether or not the plan ends up using it,
	# so this is enough to make the planner touch one that replay may
	# have just removed -- no index scan need be chosen.
	quiet_index_scan => {
		weight => 6,
		requires => { schema => ['quiet_index'] },
		script => q(
			SELECT COUNT(*) FROM pgb_quiet WHERE val > 0;
		),
	},

	# Every index the scenario built must still be a valid index.
	amcheck => {
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
	},

	# The statistics functions must refuse an invalid index rather than
	# reading it.  A failed concurrent build leaves one behind, and its
	# storage may be torn or half-written, so pgstatindex reading it is
	# how a "can\'t happen" corruption error gets reported for something
	# that is merely incomplete.  The invalid index is made here rather
	# than waited for: a unique build over duplicate values fails and
	# leaves exactly one.
	pgstat_rejects_invalid_index => {
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
	},

	# A concurrent drop that fails must not leave an invalid index still
	# marked as the replica identity: reindexing such an index makes it
	# valid again, and if the table\'s replica identity moved on in the
	# meantime, two indexes end up marked and the relcache picks one of
	# them arbitrarily.  The failure is forced here rather than waited
	# for -- a session holds a lock so the drop has to wait, and
	# lock_timeout ends it.
	dic_clears_replident => {
		final => sub {
			my ($node, $ctx) = @_;

			$node->safe_psql(
				'postgres', q(
				DROP TABLE IF EXISTS pgb_ri;
				CREATE TABLE pgb_ri(id int NOT NULL);
				INSERT INTO pgb_ri SELECT g FROM generate_series(1, 100) g;
				CREATE UNIQUE INDEX pgb_ri_idx ON pgb_ri(id);
				ALTER TABLE pgb_ri REPLICA IDENTITY USING INDEX pgb_ri_idx));

			my $holder = $node->background_psql('postgres');
			$holder->query_safe('BEGIN; SELECT count(*) FROM pgb_ri;');

			{
				local $ENV{PGOPTIONS} = '-c lock_timeout=2s';
				$node->psql('postgres', 'DROP INDEX CONCURRENTLY pgb_ri_idx;',
					on_error_stop => 0);
			}
			$holder->quit;

			my $state = $node->safe_psql(
				'postgres', q(
				SELECT COALESCE(string_agg(
					format('%s valid=%s replident=%s', c.relname,
						i.indisvalid, i.indisreplident), ' '), 'gone')
				FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
				WHERE c.relname = 'pgb_ri_idx'));
			Test::More::unlike(
				$state,
				qr/valid=f replident=t/,
				'a failed concurrent drop left no invalid replica identity');
			Test::More::note("pgb_ri_idx after the failed drop: $state");

			$node->safe_psql('postgres', 'DROP TABLE IF EXISTS pgb_ri');
		},
	},

	# REFRESH MATERIALIZED VIEW CONCURRENTLY needs a unique index on the
	# view, and it looks that index up while refreshing.  No other
	# session can take it away in that window -- the refresh holds
	# ExclusiveLock and a concurrent drop would need
	# ShareUpdateExclusiveLock -- so the only way in is the view\'s own
	# definition calling a function that drops it, which is what this
	# does.  Silly, and the commit says so, but it is the difference
	# between a clean error and an assertion failure.
	refresh_survives_dropped_index => {
		final => sub {
			my ($node, $ctx) = @_;

			$node->safe_psql(
				'postgres', q(
				DROP MATERIALIZED VIEW IF EXISTS pgb_mv_drop;
				CREATE OR REPLACE FUNCTION pgb_drop_mv_idx() RETURNS bool
				LANGUAGE plpgsql AS $$
				BEGIN
					-- Qualified: a refresh runs the view's query with a
					-- secure search_path, so a bare name resolves to
					-- nothing and the drop quietly does nothing.
					EXECUTE 'DROP INDEX IF EXISTS public.pgb_mv_drop_idx';
					RETURN true;
				END $$;
				CREATE MATERIALIZED VIEW pgb_mv_drop AS
					SELECT 1 AS i WHERE pgb_drop_mv_idx();
				CREATE UNIQUE INDEX pgb_mv_drop_idx ON pgb_mv_drop(i)));

			my (undef, undef, $err) = $node->psql('postgres',
				'REFRESH MATERIALIZED VIEW CONCURRENTLY pgb_mv_drop;',
				on_error_stop => 0);
			$err =~ s/\s+/ /g;
			Test::More::like(
				$err,
				qr/could not find suitable unique index/,
				'a refresh whose index vanished reports it');
			Test::More::note("refresh said: $err");

			$node->safe_psql('postgres',
				'DROP MATERIALIZED VIEW IF EXISTS pgb_mv_drop');
		},
	},

	# REPACK cannot locate tuples by a deferrable key, so it has to
	# refuse a table whose only identity is one.  Asserting the refusal
	# is the only way to gate this: a server that accepts the table is
	# the bug, and it does not announce itself.
	repack_refuses_deferrable => {
		requires => { schema => ['deferrable_pk'] },
		final => sub {
			my ($node, $ctx) = @_;
			my ($rc, $out, $err) =
			  $node->psql('postgres', 'REPACK (CONCURRENTLY) pgb_defer;',
				on_error_stop => 0);
			Test::More::like(
				$err,
				qr/does not support deferrable primary keys/,
				'REPACK refuses a table whose only identity is deferrable');
		},
	},

	# The key swaps permute the keys and put every one of them back, so
	# once the load stops the set must be exactly what it started as.
	deferred_keys_intact => {
		requires => { schema => ['deferrable_pk'] },
		final => sub {
			my ($node, $ctx) = @_;
			Test::More::is(
				$node->safe_psql(
					'postgres', 'SELECT COUNT(*) || \'/\' || COALESCE(SUM(id), 0)'
					  . ' FROM pgb_defer'),
				'2000/2001000',
				'the deferrable key set survived the swaps');
		},
	},

	# DETACH CONCURRENTLY must not leave a substitute constraint behind.
	# The one it used to create on a hash partition carries the OID of
	# the parent the table is no longer related to, which breaks a dump
	# and outlives the parent.
	no_substitute_constraints => {
		final => sub {
			my ($node, $ctx) = @_;
			my $bad = $node->safe_psql(
				'postgres', q(
				SELECT count(*) FROM pg_constraint c
				WHERE c.contype = 'c'
				  AND pg_get_constraintdef(c.oid) LIKE '%satisfies_hash_partition%'));
			Test::More::is($bad, '0',
				'no partition constraint left behind by DETACH');
		},
	},

	# The visibility map must describe the table it ended up with.  Every
	# relation the rotation could have rewritten gets checked, which is
	# also how this avoids being handed a partitioned table: those are
	# not in the rotation's list, and pg_visibility refuses them.
	visibility_map => {
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
	},

	# A cancelled or completed REPACK must not leave its transient slot
	# behind, and logical decoding must have been switched off again.
	no_slot_leak => {
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
	},

	# REPACK CONCURRENTLY turns logical decoding on for as long as it
	# needs to decode, and must turn it back off.  Leaving it on costs
	# every writer the extra WAL for no reason, and no invariant in the
	# suite can see it, so ask directly.  The checkpointer does the
	# lowering, so it does not happen the instant the command ends.
	decoding_disabled => {
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
	},

	# A cancelled build leaves an invalid index behind: that is
	# documented, and the cancellation environment expects it.  What is
	# not acceptable is one that cannot then be dropped, which is how a
	# half-finished DROP INDEX CONCURRENTLY used to strand an index for
	# good.  Indexes that belong to a constraint are skipped, since DROP
	# INDEX is not how those come out.
	invalid_indexes_droppable => {
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
	},

	# The replica identity really moved during the run.
	#
	# Without this the scenario passes when every ALTER gave up on the
	# lock, which is a real possibility: the bounded helper does not wait.
	identity_moved => {
		requires => { schema => ['movable_identity'] },
		# Sampled by the workload too, not only by the rotation, so what
		# ends up recorded does not depend on how often the DDL client
		# happened to pick the entry that moves it.
		script => q(
			SELECT pgb_note_identity();
		),
		final => sub {
			my ($node, $ctx) = @_;
			my $seen = $node->safe_psql('postgres',
				'SELECT count(*) FROM pgb_ident_seen');
			Test::More::cmp_ok($seen, '>=', 2,
				'the replica identity moved between both indexes')
			  or Test::More::diag("identities seen: $seen");
			return;
		},
	},

	# No page failed its checksum, and every page was readable.
	#
	# This is the detector for the online checksum transitions.  The
	# counter is the server's own: a page whose checksum does not match
	# what the data says increments it, and nothing else does.  Reading it
	# is not enough on its own, though, because a page is only verified
	# when it is read, so the check reads everything first -- every block
	# of every relation, through pg_relation_check_pages if this build has
	# it and by a full scan of each table otherwise.
	no_checksum_failures => {
		requires => { schema => ['data_checksum_helper'] },
		# Catching up before reading the standby, without reaching into
		# the environment for how.
		catchup => 1,
		final => sub {
			my ($node, $ctx) = @_;

			# Leave the cluster with checksums on, so that what follows
			# actually verifies them, and wait for the worker.
			$node->safe_psql('postgres', q(
				DO $$
				BEGIN
					IF current_setting('data_checksums') <> 'on' THEN
						PERFORM pg_enable_data_checksums(0, 10000);
					END IF;
					-- Generous, because a chaos profile can hold the
					-- worker on every page: the heavy one injects tens of
					-- seconds of sleep into a single run, and a wait that
					-- expires leaves the cluster in inprogress-on, which
					-- reads as a failure but is only impatience.
					FOR i IN 1..4800 LOOP
						EXIT WHEN current_setting('data_checksums') = 'on';
						PERFORM pg_sleep(0.05);
					END LOOP;
				END $$;
			));

			Test::More::is(
				$node->safe_psql('postgres', 'SHOW data_checksums'),
				'on', 'checksums are on at the end of the run');

			# Evict everything, so the reads below come from disk rather
			# than from buffers that were never written out.
			$node->safe_psql('postgres', 'CHECKPOINT');

			# Read every block of every relation.  A bad checksum is an
			# error here and a counter increment below; both are wanted,
			# since an error names the relation.
			#
			# The count of blocks read is returned and asserted, because a
			# read-back that silently reads nothing passes every check
			# after it.  That is not hypothetical: the first version of
			# this swallowed a missing pg_prewarm and proved nothing.
			my ($rc, $out, $err) = $node->psql(
				'postgres', $checksum_read_all, on_error_stop => 0);
			Test::More::is($rc, 0, 'every page could be read back')
			  or Test::More::diag($err);
			Test::More::cmp_ok($out, '>', 100,
				'the read-back really read the cluster')
			  or Test::More::diag("blocks read: $out");

			Test::More::is(
				$node->safe_psql('postgres',
					'SELECT COALESCE(sum(checksum_failures), 0) '
					  . 'FROM pg_stat_database'),
				'0', 'no page failed its checksum');

			# And the same on a standby, if the environment built one.
			#
			# This is the case the worker's own comment warns about: a
			# replica can hold a page whose checksum is invalid, from
			# unlogged changes made on the primary while checksums were
			# off, and only a full page image repairs it.  It takes
			# checksums on, then off, then on again to get there, which is
			# exactly what the rotation does.
			if (my $standby = $ctx->{standby})
			{
				$node->wait_for_catchup($standby, 'replay');

				Test::More::is(
					$standby->safe_psql('postgres', 'SHOW data_checksums'),
					'on', 'checksums are on at the standby too');

				$standby->safe_psql('postgres', 'CHECKPOINT');
				my ($src, $sout, $serr) = $standby->psql(
					'postgres', $checksum_read_all_standby, on_error_stop => 0);
				Test::More::is($src, 0, 'every standby page could be read')
				  or Test::More::diag($serr);
				Test::More::cmp_ok($sout, '>', 100,
					'the standby read-back really read the cluster')
				  or Test::More::diag("blocks read: $sout");

				Test::More::is(
					$standby->safe_psql('postgres',
						'SELECT COALESCE(sum(checksum_failures), 0) '
						  . 'FROM pg_stat_database'),
					'0', 'no standby page failed its checksum');
			}
			return;
		},
	},

	# Every row of the log fired the trigger that counts it, in the same
	# transaction that inserted it, so the counters and the log agree
	# exactly however many rebuilds ran underneath them.  What this
	# catches is a count that went missing: an upsert whose arbiter
	# resolved to an index that is no longer the one enforcing the key
	# can take the insert path where it should have taken the update
	# path, or find nothing to conflict with at all, and the total comes
	# up short.  A duplicated count would not show here -- the primary
	# key on pgb_calls makes that impossible unless the index itself is
	# wrong, which is amcheck's question rather than this one.
	trigger_counts_agree => {
		requires => { schema => ['trigger_audit'] },
		final => sub {
			my ($node, $ctx) = @_;
			my $row = $node->safe_psql(
				'postgres', q(
				SELECT (SELECT COALESCE(SUM(hits), 0) FROM pgb_calls)
					|| ' ' || (SELECT COUNT(*) FROM pgb_call_log)
					|| ' ' || (SELECT COUNT(*) FROM pgb_audit)));
			my ($counted, $logged, $audited) = split / /, $row;
			# Reported so that a run where the triggers never fired --
			# every count zero, and the comparison below true for the
			# wrong reason -- is visible in the log rather than green.
			Test::More::note(
				"trigger writes: $counted counted, $logged logged, "
				  . "$audited audit rows");
			Test::More::is($counted, $logged,
				'every logged call was counted once by its trigger');
		},
	},
);

=pod

=head2 %ENVS

An environment decides what cluster the scenario runs against, and
carries the settings that make that cluster behave.  Getting those wrong
is itself a source of false failures, so they belong here rather than in
each scenario.

=cut

our %ENVS = (
	standalone => {
		conf => ['wal_level = logical'],
	},

	# wal_level = replica, so that the transient slot REPACK takes really
	# does toggle logical decoding on and off.
	wal_replica => {
		conf => [ 'wal_level = replica', 'max_connections = 50' ],
	},

	# Aggressive autovacuum, so the visibility map is being set
	# continuously rather than once at the start.
	autovacuum => {
		conf => [
			'wal_level = logical',
			'autovacuum_naptime = 1s',
			'autovacuum_vacuum_scale_factor = 0.0',
			'autovacuum_vacuum_threshold = 100',
			'autovacuum_vacuum_insert_scale_factor = 0.0',
			'autovacuum_vacuum_insert_threshold = 100',
		],
	},

	# A deliberately small lock table, which the CONCURRENTLY commands
	# are heavy users of.
	lock_exhaustion => {
		conf => [
			'wal_level = logical',
			'max_locks_per_transaction = 16',
			'max_connections = 50',
		],
	},

	# The cluster is killed and restarted while the commands are in
	# flight, so their cleanup happens through crash recovery rather than
	# through their own code.
	crash_loop => {
		conf => ['wal_level = logical'],
		run => sub {
			my ($node, $ctx) = @_;

			foreach my $cycle (1 .. 3)
			{
				my ($out, $err) = ('', '');
				# Long enough that the kill lands mid-workload; the run is
				# ended by the kill, not by the clock.
				my $h = IPC::Run::start(
					$ctx->{pgbench_cmd}->(duration => 60),
					'>', \$out, '2>', \$err);
				sleep(2);

				# An immediate shutdown rather than SIGKILL on the
				# postmaster alone.  Both leave the cluster to recover on
				# the next start, which is what this environment is for,
				# but SIGKILL orphans the backends: they go on holding
				# the shared memory segment, and a new postmaster refuses
				# to start until every one of them has noticed and
				# exited.  Under load that took longer than any timeout
				# worth waiting.  An immediate shutdown signals the
				# children too and waits for them.
				$node->stop('immediate', fail_ok => 1);

				# pgbench cannot help but fail when the server disappears
				# under it, so its exit status says nothing here.
				eval { IPC::Run::finish($h) };

				# kill9 kills the postmaster and nothing else, so its
				# children are orphans that still hold the shared memory
				# segment, and a new postmaster refuses to start while
				# they do.  Most of them notice the postmaster is gone the
				# next time they wait for anything; one that is busy
				# rebuilding an index can take considerably longer, and on
				# a machine running the whole suite at once, longer still.
				# Waiting is the portable way to deal with it -- there is
				# no handle on those processes from here.
				my $started = 0;
				foreach my $try (1 .. 60)
				{
					last if $started = $node->start(fail_ok => 1);
					Test::More::note(
						"cycle $cycle: still waiting for the old backends "
						  . "to let go after $try seconds")
					  if $try % 15 == 0;
					sleep 1;
				}
				die 'the server did not come back after the crash'
				  unless $started;
				Test::More::pass("cycle $cycle: recovered after a crash");

				# Recovery brings back any transaction that was prepared
				# when the server went down, still holding its locks.  The
				# drop below needs AccessExclusiveLock on an index one of
				# them may well have been building, and would wait out the
				# lock timeout for a transaction nothing is going to
				# resolve.
				my $prepared = stress_rollback_prepared($node);
				Test::More::note(
					"cycle $cycle: rolled back $prepared prepared "
					  . 'transactions recovered after the crash')
				  if $prepared;

				# An interrupted concurrent build may leave an invalid
				# index behind, which is documented; it must at least be
				# droppable.
				$node->safe_psql(
					'postgres', q(
					DO $$
					DECLARE
						idx oid;
					BEGIN
						FOR idx IN SELECT indexrelid FROM pg_index
							WHERE NOT indisvalid
						LOOP
							CONTINUE WHEN NOT EXISTS
								(SELECT 1 FROM pg_class WHERE oid = idx);
							EXECUTE format('DROP INDEX %s', idx::regclass);
						END LOOP;
					END;
					$$;
				));
			}
			return;
		},
	},

	# The commands are interrupted partway rather than allowed to
	# finish, which exercises their own cleanup paths.
	cancellation => {
		conf => [ 'wal_level = replica', 'max_connections = 50' ],
		run => sub {
			my ($node, $ctx) = @_;

			# The workload runs as usual, minus the DDL script: the
			# commands are issued from here instead, so that their errors
			# can be tolerated.
			my ($out, $err) = ('', '');
			my $h = IPC::Run::start(
				$ctx->{pgbench_cmd}->(files => $ctx->{noddl_opts}),
				'>', \$out, '2>', \$err);

			my @variants = @{ $ctx->{ddl_variants} };
			my ($attempts, $interrupted) = (0, 0);
			my $deadline = time() + $ctx->{duration};
			while (time() < $deadline)
			{
				my $v = $variants[ int(rand(scalar @variants)) ];
				# pgbench meta-commands are not SQL; skip a variant that
				# is only a pause.
				my @stmts = grep { !/^\\/ } @{ $v->{stmts} };
				next unless @stmts;

				# Every so often, terminate the session running the
				# command rather than cancelling the statement.  The two
				# are not the same shape: a cancellation raises ERROR and
				# unwinds through PG_FINALLY, while termination raises
				# FATAL and does not, so cleanup hung off PG_FINALLY alone
				# is skipped.  A REPACK's decoding worker and its
				# transient slot are cleaned up there, and nothing else in
				# the suite reaches that path.
				if (int(rand(4)) == 0)
				{
					my ($to, $te) = ('', '');
					my $th = IPC::Run::start(
						[
							$node->installed_command('psql'),
							'-X', '-v', 'ON_ERROR_STOP=0',
							'-d', $node->connstr('postgres'),
							'-c', join(' ', @stmts)
						],
						'>', \$to, '2>', \$te);
					select undef, undef, undef, 0.001 * (1 + int(rand(200)));
					$node->safe_psql(
						'postgres', q(
						SELECT pg_terminate_backend(pid) FROM pg_stat_activity
							WHERE pid <> pg_backend_pid()
								AND backend_type = 'client backend'
								AND query ~* '^(REPACK|REINDEX|CREATE INDEX|DROP INDEX)'));
					eval { IPC::Run::finish($th) };
					$attempts++;
					$interrupted++ if $te ne '';
					next;
				}

				# Otherwise cancel at some arbitrary point, and sometimes
				# let the command run to completion.
				my $timeout = (int(rand(4)) == 0) ? 0 : 1 + int(rand(200));
				my (undef, undef, $stderr) = $node->psql(
					'postgres',
					"SET statement_timeout = $timeout; " . join(' ', @stmts),
					on_error_stop => 0);
				$attempts++;

				next if $stderr eq '';
				$interrupted++;
				# The only errors expected are the cancellation itself and
				# the complaints that follow from a previous one having
				# left the indexes in an unexpected state.
				# Written on one line on purpose: under /x the spaces
				# inside these messages would be ignored and none of them
				# would ever match.
				Test::More::like(
					$stderr,
					qr/canceling statement due to (?:statement|lock) timeout|(?:relation|index) "[^"]+" (?:already exists|does not exist)|skipping reindex of invalid index|cannot reindex exclusion constraint index [^ ]+ concurrently, skipping|cannot cluster on (?:invalid|partial) index|deadlock detected/,
					'interrupted command failed only in expected ways')
				  or Test::More::diag("unexpected error: $stderr");
			}

			IPC::Run::finish($h);
			Test::More::like($out, qr{actually processed}, 'writers completed');
			Test::More::like($err, $ctx->{stderr_re}, 'writers reported nothing');
			Test::More::note(
				"$attempts commands issued, $interrupted of them interrupted");

			# The point of this environment is that commands get cut off
			# partway, so a run where none did has tested nothing.  But
			# the loop can be starved down to a handful of attempts on a
			# busy machine, and interruptions run at roughly a fifth to a
			# third of attempts, so demanding one out of five is a coin
			# toss rather than a check -- zero out of five is an ordinary
			# outcome, zero out of ten is not.  Say so instead of
			# failing.
			if ($attempts < 10)
			{
				Test::More::note(
					"only $attempts commands got through; too few to "
					  . 'conclude anything about cancellation');
			}
			else
			{
				Test::More::cmp_ok($interrupted, '>', 0,
					'some were interrupted');
			}

			# Whatever was cut off, nothing may be left half-built.
			$node->safe_psql(
				'postgres', q(
				DO $$
				DECLARE
					idx oid;
				BEGIN
					FOR idx IN SELECT indexrelid FROM pg_index WHERE NOT indisvalid
					LOOP
						CONTINUE WHEN NOT EXISTS
							(SELECT 1 FROM pg_class WHERE oid = idx);
						EXECUTE format('DROP INDEX %s', idx::regclass);
					END LOOP;
				END;
				$$;
			));
			return;
		},
		final => sub {
			my ($node, $ctx) = @_;
			# A cancelled REPACK must not leave logical decoding switched
			# on behind it.
			$node->poll_query_until('postgres',
				q(SELECT current_setting('effective_wal_level') = 'replica'))
			  or die 'timed out waiting for logical decoding to be disabled';
			Test::More::pass('effective_wal_level fell back to replica');
			return;
		},
	},

	# A hot standby replaying the DDL while serving the checks.
	standby => {
		# Replication has to catch up before the checks mean anything.
		min_seconds => 5,
		init => { allows_streaming => 1 },
		conf => ['max_connections = 50'],
		setup => sub {
			my ($primary, $ctx) = @_;

			my $bkp = 'stress_bkp_' . ++$node_seq;
			$primary->backup($bkp);
			my $standby = PostgreSQL::Test::Cluster->new("stress_standby_$node_seq");
			$standby->init_from_backup($primary, $bkp, has_streaming => 1);

			# A finite delay, never -1: replay takes the
			# AccessExclusiveLocks the primary logged before it applies
			# the records that conflict with a reader's snapshot, so with
			# -1 it can wait forever on a reader that is itself blocked on
			# a lock replay holds.  Nothing detects that cycle.  A finite
			# delay lets replay cancel the reader instead, which is the
			# documented way out.
			$standby->append_conf('postgresql.conf',
				'max_standby_streaming_delay = 5s');
			# Tell the primary what the standby's queries still need, so
			# that vacuum there does not remove it.  Without this a
			# snapshot conflict does not cancel the reader's statement,
			# it terminates the connection -- FATAL, so there is nothing
			# for pgbench to retry -- and the run fails over the standby
			# behaving exactly as documented.  What this scenario is for
			# is replaying the CONCURRENTLY commands, which it still
			# does; holding the primary's horizon back is the price, and
			# the scenarios that care about pruning do not have a
			# standby.
			$standby->append_conf('postgresql.conf',
				'hot_standby_feedback = on');
			$standby->append_conf('postgresql.conf',
				'log_recovery_conflict_waits = on');
			$standby->append_conf('postgresql.conf', 'log_lock_waits = on');
			$standby->start;

			$ctx->{standby} = $standby;
			push @{ $ctx->{extra_nodes} }, $standby;
			return;
		},
		run => sub {
			my ($primary, $ctx) = @_;

			# The primary runs the whole mix; the standby runs the checks
			# alone, since it cannot write.  A query cancelled by a
			# recovery conflict fails with a serialization error, which is
			# what pgbench retries for; without that the first
			# cancellation would end the run.
			my ($po, $pe, $so, $se) = ('', '', '', '');
			my $ph =
			  IPC::Run::start($ctx->{pgbench_cmd}->(), '>', \$po, '2>', \$pe);

			# Only the checks that do not write can run against a
			# standby.  With none of them there is nothing to run there:
			# pgbench given no script of its own falls back to its
			# built-in one, which writes.
			my $sh;
			if (@{ $ctx->{ro_check_opts} })
			{
				$sh = IPC::Run::start(
					$ctx->{pgbench_cmd}->(
						node => $ctx->{standby},
						files => $ctx->{ro_check_opts},
						clients => 10,
						args => '--max-tries=100'),
					'>', \$so, '2>', \$se);
			}
			else
			{
				Test::More::note('no read-only checks; standby only replays');
			}

			IPC::Run::finish($ph);
			IPC::Run::finish($sh) if $sh;

			Test::More::like($po, qr{actually processed}, 'primary workload ran');
			Test::More::like($pe, $ctx->{stderr_re}, 'primary reported nothing');
			if ($sh)
			{
				Test::More::like($so, qr{actually processed},
					'standby workload ran');
				Test::More::like($se, $ctx->{stderr_re},
					'standby reported nothing');
			}
			return;
		},
		final => sub {
			my ($primary, $ctx) = @_;
			my $standby = $ctx->{standby};

			$primary->wait_for_catchup($standby);
			my $q = 'SELECT COALESCE(SUM(abalance), 0) FROM pgbench_accounts';
			Test::More::is(
				$standby->safe_psql('postgres', $q),
				$primary->safe_psql('postgres', $q),
				'the standby replayed the DDL churn to the same data');

			# It must also survive promotion with everything intact.
			$standby->promote;
			Test::More::is(
				$standby->safe_psql('postgres', $q),
				$primary->safe_psql('postgres', $q),
				'and still has it after promotion');
			return;
		},
	},

	# A subscriber applying what the workload produces while the
	# publisher's tables are rebuilt underneath the decoding.
	subscription => {
		# Replication has to catch up before the checks mean anything.
		min_seconds => 5,
		init => { allows_streaming => 'logical' },
		conf => ['max_connections = 50'],
		setup => sub {
			my ($publisher, $ctx) = @_;

			my $subscriber =
			  PostgreSQL::Test::Cluster->new('stress_subscriber_' . ++$node_seq);
			# The subscriber gets rebuilt underneath its own apply
			# worker, and REPACK (CONCURRENTLY) needs the slots and WAL
			# level that logical decoding asks for.
			$subscriber->init(allows_streaming => 'logical');
			$subscriber->append_conf('postgresql.conf', $_)
			  for (
				'max_logical_replication_workers = 8',
				# The test cluster default is 10, which the subscriber's
				# own workload plus its apply and sync workers exceed.
				'max_connections = 50',
				'max_worker_processes = 32',
				'log_error_verbosity = verbose',
				'log_lock_waits = on');
			$subscriber->start;

			# The subscriber needs the same tables.  Take them from the
			# publisher with pg_dump rather than describing the schema a
			# second time, so a decorator's tables come across too.
			my $dumpfile = $publisher->basedir . '/schema.sql';
			PostgreSQL::Test::Utils::system_or_bail('pg_dump', '--schema-only',
				'--file', $dumpfile, $publisher->connstr('postgres'));
			PostgreSQL::Test::Utils::system_or_bail('psql', '--no-psqlrc',
				'--quiet', '--file', $dumpfile, '--dbname',
				$subscriber->connstr('postgres'));

			# A column of the subscriber's own, indexed so that local
			# updates to it are never HOT and really do move the rows the
			# apply worker is looking up.
			$subscriber->safe_psql(
				'postgres', q(
				ALTER TABLE pgbench_accounts ADD COLUMN sub_local int DEFAULT 0;
				CREATE INDEX pgb_sub_local_idx ON pgbench_accounts(sub_local);
			));

			# Named tables rather than FOR ALL TABLES, so that one can be
			# dropped from the publication and added back to force a fresh
			# table synchronization.
			my $tables = join ', ', @{ $ctx->{tables} };
			$publisher->safe_psql('postgres',
				"CREATE PUBLICATION stress_pub FOR TABLE $tables");
			my $connstr = $publisher->connstr . ' dbname=postgres';
			$subscriber->safe_psql('postgres',
				"CREATE SUBSCRIPTION stress_sub CONNECTION '$connstr' "
				  . 'PUBLICATION stress_pub');
			$publisher->wait_for_catchup('stress_sub');

			$ctx->{subscriber} = $subscriber;
			push @{ $ctx->{extra_nodes} }, $subscriber;
			return;
		},
		run => sub {
			my ($publisher, $ctx) = @_;
			my $subscriber = $ctx->{subscriber};

			my ($po, $pe, $so, $se) = ('', '', '', '');
			my $ph = IPC::Run::start($ctx->{pgbench_cmd}->(), '>', \$po, '2>', \$pe);

			# The subscriber runs its own loads, if the scenario has any,
			# against the rows being applied to it.
			my $sh;
			if (@{ $ctx->{sub_opts} })
			{
				$sh = IPC::Run::start(
					$ctx->{pgbench_cmd}->(
						node => $subscriber,
						files => $ctx->{sub_opts},
						clients => 10),
					'>', \$so, '2>', \$se);
			}

			# Meanwhile the subscriber's own table is rebuilt underneath
			# the apply worker, and one table is resynchronized from
			# scratch by taking it out of the publication and putting it
			# back.
			#
			# Rebuilding the index behind the replica identity is part of
			# the rotation.  It used to be excluded: REINDEX CONCURRENTLY
			# gives the identity a new OID, the apply worker went on
			# using the one it had cached, and on an assertion build the
			# subscriber went down instead of an invariant breaking.
			# That was a server bug, fixed in this branch -- see
			# REGRESSIONS -- and this is what holds the fix.
			#
			# Weight it up to reproduce the old failure faster:
			#
			#   PG_TEST_EXTRA='stress_repl_identity_rebuild=1'
			my $identity_heavy =
			  (($ENV{PG_TEST_EXTRA} // '') =~ /\bstress_repl_identity_rebuild=1\b/)
			  ? 1
			  : 0;

			my $deadline = time() + $ctx->{duration};
			my $resync = 0;
			while (time() < $deadline)
			{
				my $pick =
				  $identity_heavy
				  ? (rand() < 0.75 ? 4 : int(rand(4)))
				  : int(rand(5));

				if ($pick == 4)
				{
					# The primary key is the replica identity here, so
					# this is the rebuild that used to take the apply
					# worker down.
					$subscriber->safe_psql('postgres',
						'REINDEX INDEX CONCURRENTLY pgbench_accounts_pkey');
					next;
				}

				if ($pick == 0)
				{
					$subscriber->safe_psql('postgres',
						'REPACK (CONCURRENTLY) pgbench_accounts');
				}
				elsif ($pick == 1)
				{
					$subscriber->safe_psql('postgres',
						'REINDEX INDEX CONCURRENTLY pgb_sub_local_idx');
				}
				elsif ($pick == 2)
				{
					$subscriber->safe_psql(
						'postgres', q(
						DROP INDEX CONCURRENTLY pgb_sub_local_idx;
						CREATE INDEX CONCURRENTLY pgb_sub_local_idx
							ON pgbench_accounts(sub_local);
					));
				}
				else
				{
					# Taking a table out of the publication and adding it
					# back makes the next refresh copy it from scratch,
					# which restarts the table synchronization worker.
					#
					# ALTER PUBLICATION wants ShareUpdateExclusiveLock on
					# the table, which is what the rotation's commands
					# hold and wait for, so the two can deadlock.  That is
					# the lock manager doing its job rather than a fault,
					# and the loser is whichever the deadlock detector
					# picked; try again.
					_retry_on_deadlock(
						$publisher,
						'ALTER PUBLICATION stress_pub DROP TABLE pgbench_tellers');
					$subscriber->safe_psql('postgres',
						'ALTER SUBSCRIPTION stress_sub REFRESH PUBLICATION');

					# The fresh synchronization this is about to ask for
					# starts with a COPY, and nothing empties the target
					# first: the rows from the previous synchronization
					# are still there, so the copy hits the primary key
					# and the tablesync worker fails, restarts, and fails
					# again for as long as the subscription lives.  Clear
					# the table out so the copy has somewhere to land.
					#
					# Not before the subscriber has caught up, though.
					# Dropping the table from the publication stops it
					# being published from that point, but changes to it
					# from earlier transactions are still working their
					# way through, and they arrive to find the rows they
					# meant to update gone -- which is a conflict this
					# scenario arranged rather than one worth reporting.
					$publisher->wait_for_catchup('stress_sub');
					$subscriber->safe_psql('postgres',
						'TRUNCATE pgbench_tellers');

					_retry_on_deadlock(
						$publisher,
						'ALTER PUBLICATION stress_pub ADD TABLE pgbench_tellers');
					$subscriber->safe_psql('postgres',
						'ALTER SUBSCRIPTION stress_sub REFRESH PUBLICATION');
					$resync++;
				}
			}

			IPC::Run::finish($ph);
			IPC::Run::finish($sh) if $sh;

			Test::More::like($po, qr{actually processed}, 'publisher workload ran');
			Test::More::like($pe, $ctx->{stderr_re}, 'publisher reported nothing');
			if ($sh)
			{
				Test::More::like($so, qr{actually processed},
					'subscriber workload ran');
				Test::More::like($se, $ctx->{stderr_re},
					'subscriber reported nothing');
			}
			Test::More::note("$resync table resynchronizations");
			return;
		},
		final => sub {
			my ($publisher, $ctx) = @_;
			my $subscriber = $ctx->{subscriber};

			# Everything must be subscribed again before the comparison,
			# whatever the resynchronization was doing when time ran out.
			$subscriber->safe_psql('postgres',
				'ALTER SUBSCRIPTION stress_sub REFRESH PUBLICATION');

			# Both waits are needed, and for different things.
			# wait_for_catchup follows the walsender's LSN, which says
			# nothing about the initial copy: that is done by tablesync
			# workers, and a refresh has just asked for another one.
			# Comparing without waiting for those reads a table the copy
			# has not reached yet -- which on a short run means comparing
			# an empty subscriber against a full publisher.
			$subscriber->wait_for_subscription_sync($publisher, 'stress_sub');
			$publisher->wait_for_catchup('stress_sub');
			# All four tables, not just pgbench_accounts.  An apply that
			# skips an update logs a conflict and moves on, so the damage
			# is a wrong value on the subscriber rather than an error --
			# and a comparison that leaves a table out cannot see it.
			# pgbench_branches is where a scale-1 run puts almost all of
			# its contention, and it was the table left uncompared.
			foreach my $t (
				qw(pgbench_accounts pgbench_tellers pgbench_branches))
			{
				my $col = {
					pgbench_accounts => 'abalance',
					pgbench_tellers => 'tbalance',
					pgbench_branches => 'bbalance',
				}->{$t};
				my $q = "SELECT COUNT(*), COALESCE(SUM($col), 0) FROM $t";
				Test::More::is(
					$subscriber->safe_psql('postgres', $q),
					$publisher->safe_psql('postgres', $q),
					"the subscriber's $t matches the publisher");
			}

			# An apply worker that could not find its target row reports
			# it as a conflict rather than failing, so the log is where
			# such a loss would show up.
			my $log = PostgreSQL::Test::Utils::slurp_file($subscriber->logfile);
			Test::More::unlike($log, qr/conflict=update_missing/,
				'no update_missing conflict was logged');
			Test::More::unlike($log, qr/conflict=delete_missing/,
				'no delete_missing conflict was logged');
			return;
		},
	},
);

1;
