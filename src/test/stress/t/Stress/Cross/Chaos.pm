
# Copyright (c) 2026, PostgreSQL Global Development Group

# The injection point caps and the named chaos profiles.  What a
# profile is for, and the two rules every one of them holds to,
# are documented in Stress::Registry.
#
# See Stress::Registry for what each declaration means.

package Stress::Cross::Chaos;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

# The commit window: between the flush that lets a decoder see a
# commit and the CLOG update that lets an ordinary snapshot see it,
# and between that update and the transaction leaving the procarray.
# Reached by every commit, so the probability stays low; the sleeps
# can be long, since what has to be outlasted is a whole REPACK
# startup.
chaos_point 'commit-before-clog-update' => { max_p => 0.15, max_us => 60_000 };

chaos_point 'xact-end-before-procarray-clear' => { max_p => 0.15, max_us => 60_000 };

# Catalog staleness: a lock held over a descriptor not yet built, an
# invalidation not yet absorbed, a relcache entry half made.
chaos_point 'relation-open-after-lock' => { max_p => 0.02, max_us => 10_000 };

chaos_point 'accept-invalidation-messages' => { max_p => 0.02, max_us => 10_000 };

chaos_point 'relcache-build-catalogs-read' => { max_p => 0.1, max_us => 20_000 };

chaos_point 'catcache-list-miss-systable-scan-started' =>
  { max_p => 0.1, max_us => 20_000 };

chaos_point 'typecache-before-rel-type-cache-insert' =>
  { max_p => 0.1, max_us => 20_000 };

chaos_point 'inplace-before-pin' => { max_p => 0.2, max_us => 20_000 };

chaos_point 'transaction-end-process-inval' => { max_p => 0.05, max_us => 10_000 };

chaos_point 'invalidate-catalog-snapshot-end' => { max_p => 0.02, max_us => 10_000 };

# A snapshot taken and not yet read with, an index list held over the
# opening of its members.  The second is the planner's window on a
# standby, where replay does not wait for the reader's lock.
chaos_point 'transaction-snapshot-taken' => { max_p => 0.02, max_us => 10_000 };

chaos_point 'relation-index-list-built' => { max_p => 0.05, max_us => 20_000 };

# The phase changes of a concurrent build, each of which decides
# something on the strength of a wait that has just finished.
chaos_point 'wait-for-lockers-done' => { max_p => 1.0, max_us => 30_000 };

chaos_point 'define-index-before-set-valid' => { max_p => 1.0, max_us => 30_000 };

chaos_point 'reindex-conc-index-built' => { max_p => 1.0, max_us => 30_000 };

chaos_point 'reindex-conc-index-safe' => { max_p => 1.0, max_us => 30_000 };

chaos_point 'reindex-conc-index-not-safe' => { max_p => 1.0, max_us => 30_000 };

chaos_point 'reindex-relation-concurrently-before-swap' =>
  { max_p => 1.0, max_us => 30_000 };

chaos_point 'reindex-relation-concurrently-before-set-dead' =>
  { max_p => 1.0, max_us => 30_000 };

chaos_point 'repack-concurrently-before-lock' => { max_p => 1.0, max_us => 30_000 };

chaos_point 'detach-partition-before-finalize' => { max_p => 1.0, max_us => 30_000 };

# Speculative insertion and the checks around it, which is where an
# arbiter index that two transactions disagree about does its damage.
chaos_point 'exec-insert-before-insert-speculative' =>
  { max_p => 0.2, max_us => 10_000 };

chaos_point 'check-exclusion-or-unique-constraint-no-conflict' =>
  { max_p => 0.2, max_us => 10_000 };

# Partition routing, where the ancestors of a partition are read and
# then relied on.
chaos_point 'exec-init-partition-before-open' => { max_p => 0.2, max_us => 10_000 };

chaos_point 'exec-init-partition-after-get-partition-ancestors' =>
  { max_p => 0.2, max_us => 10_000 };

# Index page splits and deletions left incomplete, which is what a
# scan or an amcheck run has to cope with meeting.
chaos_point 'nbtree-leave-leaf-split-incomplete' => { max_p => 1.0, max_us => 20_000 };

chaos_point 'nbtree-leave-internal-split-incomplete' =>
  { max_p => 1.0, max_us => 20_000 };

chaos_point 'nbtree-finish-incomplete-split' => { max_p => 1.0, max_us => 20_000 };

chaos_point 'nbtree-leave-page-half-dead' => { max_p => 1.0, max_us => 20_000 };

chaos_point 'nbtree-finish-half-dead-page-vacuum' =>
  { max_p => 1.0, max_us => 20_000 };

chaos_point 'gin-leave-leaf-split-incomplete' => { max_p => 1.0, max_us => 20_000 };

chaos_point 'gin-leave-internal-split-incomplete' =>
  { max_p => 1.0, max_us => 20_000 };

chaos_point 'gin-finish-incomplete-split' => { max_p => 1.0, max_us => 20_000 };

# A backward scan meeting a page that split, moved or was deleted
# under it.  These are reached often, so they stay rare -- but this
# is the one place where an ordinary read is walking the same pages a
# concurrent build is rearranging, which is the class amcheck is the
# detector for.
chaos_point 'nbtree-walk-left' => { max_p => 0.05, max_us => 10_000 };

chaos_point 'nbtree-walk-left-step-right' => { max_p => 0.5, max_us => 10_000 };

chaos_point 'nbtree-walk-left-deleted' => { max_p => 0.5, max_us => 10_000 };

chaos_point 'nbtree-walk-left-restart' => { max_p => 0.5, max_us => 10_000 };

chaos_point 'nbtree-endpoint-empty' => { max_p => 1.0, max_us => 10_000 };

chaos_point 'nbtree-first-empty' => { max_p => 1.0, max_us => 10_000 };

# Multixact creation, which the row-locking loads reach whenever two
# sessions lock the same row.
chaos_point 'multixact-create-from-members' => { max_p => 0.2, max_us => 10_000 };

# Decoding, reached by the subscription environment and by every
# REPACK (CONCURRENTLY).
chaos_point 'logical-decoding-activation' => { max_p => 1.0, max_us => 20_000 };

chaos_point 'logical-replication-slot-advance-segment' =>
  { max_p => 0.5, max_us => 10_000 };

# Autovacuum starting up beside a build that is already running.
chaos_point 'autovacuum-worker-start' => { max_p => 1.0, max_us => 20_000 };

chaos_point 'autovacuum-start-parallel-vacuum' => { max_p => 1.0, max_us => 20_000 };

# The choices a vacuum makes about cleanup and truncation, each
# reached once per vacuum.
chaos_point 'vacuum-index-cleanup-auto' => { max_p => 1.0, max_us => 20_000 };

chaos_point 'vacuum-index-cleanup-enabled' => { max_p => 1.0, max_us => 20_000 };

chaos_point 'vacuum-index-cleanup-disabled' => { max_p => 1.0, max_us => 20_000 };

chaos_point 'vacuum-truncate-auto' => { max_p => 1.0, max_us => 20_000 };

chaos_point 'vacuum-truncate-enabled' => { max_p => 1.0, max_us => 20_000 };

chaos_point 'vacuum-truncate-disabled' => { max_p => 1.0, max_us => 20_000 };

# Checkpoint timing against a build that has to survive one, and the
# delay-checkpoint window a commit passes through.
chaos_point 'commit-after-delay-checkpoint' => { max_p => 0.05, max_us => 10_000 };

chaos_point 'checkpoint-before-old-wal-removal' => { max_p => 1.0, max_us => 20_000 };

chaos_point 'create-checkpoint-initial' => { max_p => 1.0, max_us => 20_000 };

chaos_point 'create-checkpoint-run' => { max_p => 1.0, max_us => 20_000 };

# The horizon a vacuum decided on, before anything is removed on the
# strength of it.
chaos_point 'vacuum-cutoffs-computed' => { max_p => 1.0, max_us => 30_000 };

# The lock queue itself.  Short and rare, always: a request that
# dawdles at the head of a queue stalls every writer behind it, and
# lock_timeout is what ends the run.
chaos_point 'lock-before-acquire' => { max_p => 0.0005, max_us => 2000 };

# Named so a scenario can say it wants none.
# Points that must never be jittered, beyond the ones excluded
# mechanically for being IS_INJECTION_POINT_ATTACHED sites.
#
# Both of these are plain INJECTION_POINT calls reached inside a
# critical section, where the first arrival in a backend cannot work:
# the lookup itself creates the local cache and dlopens the callback,
# and allocating there trips
#
#   TRAP: failed Assert("CritSectionCount == 0 ||
#                        (context)->allowInCritSection")
#
# taking the backend down.  The macro family has INJECTION_POINT_LOAD /
# _CACHED for exactly this, and these sites do not use them: upstream's
# own test_aio works only because it calls InjectionPointLoad() in every
# backend first ("Pre-load the injection points now, so we can call them
# in a critical section", test_aio.c).  Jitter attached from SQL has no
# way to arrange that, so the points are out of the pool rather than
# capped low.  Found by the first real soak over the derived pool.
chaos_exclude 'aio-process-completion-before-shared' =>
  'plain INJECTION_POINT in a critical section; needs a per-backend
   InjectionPointLoad, which jitter cannot arrange';
chaos_exclude 'aio-worker-after-reopen' =>
  'the same, in the IO worker: test_aio pre-loads it for the same
   reason';

chaos_profile off => {};

# Catalog and snapshot staleness, plus the build phase changes.  Wide
# enough to shake the ordinary races, small enough to leave
# throughput recognisable.
chaos_profile light => {
		points => {
			'relation-open-after-lock' => [ 0.002, 100, 3000 ],
			'accept-invalidation-messages' => [ 0.001, 100, 3000 ],
			'transaction-snapshot-taken' => [ 0.002, 100, 3000 ],
			'relation-index-list-built' => [ 0.01, 200, 5000 ],
			'wait-for-lockers-done' => [ 0.5, 500, 5000 ],
			'define-index-before-set-valid' => [ 0.5, 500, 5000 ],
		},
		discard_probability => 0.001,
};

# Everything at once, including the commit window and the lock queue.
# Throughput drops noticeably; this is for a hunt rather than for the
# catalogue.
chaos_profile heavy => {
		# Slow enough that the lock timeout has to be scaled with it: a
		# forced cache flush at every opportunity costs about two orders of
		# magnitude, and a healthy run then trips a timeout calibrated for
		# an ordinary server.
		slow => 1,
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
};

# Aimed at the gap between a relcache entry being built from the
# catalogs and the check for an invalidation absorbed while that was
# happening -- and at how long a backend may then go on using the
# stale entry before absorbing anything.
chaos_profile relcache_probe => {
		points => {
			'relcache-build-catalogs-read' => [ 0.2, 2000, 20000 ],
			'accept-invalidation-messages' => [ 0.05, 2000, 20000 ],
		},
};

# Only the probabilistic cache discard, and nothing else.  Used to
# tell a crash that needs a forced catalog flush from one that needs a
# widened window: they are different findings.
# The cache discard turned up hard.  Ten times the ordinary rate, for
# hunting rather than for a catalogue run: it costs a great deal of
# throughput, but a bug that needs an invalidation at one particular
# instruction needs the invalidations to be dense.
chaos_profile discard_hard => {
		slow => 1,
		discard_probability => 0.02,
};

chaos_profile discard_only => {
		# Slow enough that the lock timeout has to be scaled with it: a
		# forced cache flush at every opportunity costs about two orders of
		# magnitude, and a healthy run then trips a timeout calibrated for
		# an ordinary server.
		slow => 1,
		discard_probability => 0.002,
};

# Aimed at one window: the gap between the flush that lets a decoder
# see a commit and the CLOG update that lets an ordinary snapshot see
# it.  Sized from the measurements in REGRESSIONS -- REPACK needs the
# stall to outlast a 6-15ms gap, and nature almost never provides one.
chaos_profile decoding => {
		points => { 'commit-before-clog-update' => [ 0.125, 20000, 60000 ] },
};

1;
