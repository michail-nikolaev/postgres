
# Copyright (c) 2026, PostgreSQL Global Development Group

# The named GUC presets: values that change how the server does
# its work without changing what the work produces.
#
# See Stress::Registry for what each declaration means.

package Stress::Cross::Modifiers;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

# The default: whatever the environment already set.
modifier none => {};

# Durability actually engaged.  A test cluster runs with fsync off, so
# every scenario in this suite has always been testing the code path
# where the WAL writes never wait for the disk.  full_page_writes is
# pinned rather than changed -- its boot value is already on -- so that
# this stays the durable end of the axis whatever the default becomes;
# the variation in the other direction is no_full_page_writes.
modifier durable => {
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
};

# Nothing fits in memory.  Sorts spill, hash aggregates spill, hash
# joins batch, and the index builds the rotation runs use their
# external path rather than an in-memory one -- which is a different
# tuplesort code path being driven against a live workload.
modifier spill => {
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
};

# The planner denied its usual answers.  None of these changes a
# result; each moves the executor onto another node.  enable_indexscan
# is deliberately absent: it gates index-only scans too, and two
# scenarios depend on getting one.
modifier other_plans => {
		conf => [
			'enable_seqscan = off',
			'enable_hashagg = off',
			'enable_hashjoin = off',
			'enable_material = off',
			'enable_memoize = off',
			'enable_incremental_sort = off',
			'enable_partition_pruning = off',
		],
};

# Parallelism wherever it can be had.  The rotation's index builds and
# the checks' aggregates are what pick it up.
modifier parallel => {
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
};

# Freezing and pruning as eagerly as possible, which is what decides
# whether a concurrent build's horizon is respected.
modifier eager_vacuum => {
		conf => [
			'vacuum_freeze_min_age = 0',
			'vacuum_freeze_table_age = 0',
			'vacuum_multixact_freeze_min_age = 0',
			'vacuum_cost_delay = 0',
			'autovacuum_naptime = 1s',
			'autovacuum_vacuum_threshold = 1',
			'autovacuum_vacuum_scale_factor = 0.0',
		],
};

# Buffers evicted and checkpoints taken constantly, so a rewrite's
# pages are written, evicted and read back rather than staying
# resident.
modifier buffer_churn => {
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
};

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
modifier node_tests => {
		# Roughly a factor of two, which is enough to matter.
		slow => 1,
		conf => [
			'debug_copy_parse_plan_trees = on',
			'debug_write_read_parse_plan_trees = on',
			'debug_raw_expression_coverage_test = on',
		],
};

# Full page writes off.  The boot value is on and nothing here changed
# it, so the path this suite has never taken is the one where a page
# modified after a checkpoint is not written to WAL in full -- which
# is a different WAL record stream, and a different amount of it.
modifier no_full_page_writes => {
		conf => [ 'full_page_writes = off', ],
};

# Asynchronous IO turned off altogether.  io_method defaults to
# 'worker' in this tree, so the synchronous path is the one no
# scenario exercises unless it asks -- the reverse of what one might
# expect.  Every one of these commands rewrites a relation and reads
# it back, so which machinery those reads go through is a dimension in
# its own right.
modifier aio_sync => {
		conf => [ 'io_method = sync', ],
};

# The worker method leaned on: more workers than the default, kept
# alive, and a deeper queue, so the reads really are handed off rather
# than completed inline.
modifier aio_worker_busy => {
		conf => [
			'io_method = worker',
			'io_min_workers = 4',
			'io_max_workers = 8',
			'io_max_concurrency = 128',
			'io_worker_idle_timeout = 1s',
			'io_worker_launch_interval = 1ms',
		],
};

# io_uring, which completes in the issuing backend rather than through
# a worker and is a wholly separate implementation.  Gated on the
# build: io_method is a postmaster GUC, so an unsupported value cannot
# be caught after startup the way conf_optional handles the rest -- the
# server simply refuses to start.
modifier aio_uring => {
		requires_build => '#define USE_LIBURING 1',
		conf => [
			'io_method = io_uring',
			'io_max_concurrency = 128',
			'io_combine_limit = 16',
		],
};

# Reads issued through different machinery, and toast compressed by
# the other algorithm.
#
# The compression setting is optional because it depends on how the
# server was built: an unsupported value in postgresql.conf stops it
# from starting, so anything build-dependent goes here and is applied
# afterwards, where the refusal can be caught.
modifier io_variants => {
		conf => [
			'effective_io_concurrency = 16',
			'maintenance_io_concurrency = 16',
			'io_combine_limit = 8',
		],
		conf_optional => [ "default_toast_compression = 'lz4'", ],
};

# JIT for everything it can be used for.  Entirely optional: a build
# without LLVM accepts jit = on and ignores it, and the cost settings
# are meaningless there, so nothing is lost by trying.
modifier jit_always => {
		conf_optional => [
			'jit = on',
			'jit_above_cost = 0',
			'jit_inline_above_cost = 0',
			'jit_optimize_above_cost = 0',
		],
};

1;
