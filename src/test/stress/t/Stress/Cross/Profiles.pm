
# Copyright (c) 2026, PostgreSQL Global Development Group

# The settings profiles: server configurations that are neither a
# topology nor a way of disrupting the run, just the cluster set up to
# work differently -- another wal_level, an aggressive autovacuum, a
# lock table small enough to run out.
#
# See Stress::Registry for what each declaration means.

package Stress::Cross::Profiles;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

# wal_level = replica, so that the transient slot REPACK takes really
# does toggle logical decoding on and off.
settings_profile wal_replica => {
		conf => [ 'wal_level = replica', 'max_connections = 50' ],
};

# Aggressive autovacuum, so the visibility map is being set
# continuously rather than once at the start.
settings_profile autovacuum => {
		conf => [
			'autovacuum_naptime = 1s',
			'autovacuum_vacuum_scale_factor = 0.0',
			'autovacuum_vacuum_threshold = 100',
			'autovacuum_vacuum_insert_scale_factor = 0.0',
			'autovacuum_vacuum_insert_threshold = 100',
		],
};

# A deliberately small lock table, which the CONCURRENTLY commands
# are heavy users of.
settings_profile lock_exhaustion => {
		conf => [
			'max_locks_per_transaction = 16',
			'max_connections = 50',
		],
};

1;
