
# Copyright (c) 2026, PostgreSQL Global Development Group

# The settings soak may pick at random.  Where a modifier is a curated
# set of GUCs with a story, an entry here is one knob with the two ends
# of its range: soak draws a few per combination, so pairs nobody would
# write down get tried anyway.  The modifier rules apply unchanged --
# nothing here may alter what a query returns, only how the server goes
# about it -- and a knob a chosen modifier or profile already sets is
# ineligible for that combination rather than silently overridden.
#
# Discrete choices, not ranges: a drawn value has to be readable in the
# combination line and identical for one seed, and the ends of a range
# are where the different code paths are anyway.
#
# See Stress::Registry for what each declaration means.

package Stress::Cross::SettingsPool;

use strict;
use warnings FATAL => 'all';

use Stress::Registry ':declare';

# How long WAL sits with the wal writer: from write-behind that never
# rests to one flush per ten seconds, which is where the backends do
# the writing themselves.
setting wal_writer_delay => { choices => [ '1ms', '10s' ] };

# Checkpoints as a drumbeat or nearly never, which decides how much of
# the run's WAL carries full page images.
setting checkpoint_timeout => { choices => [ '30s', '15min' ] };

# Group commit engaged, or off; with the default commit_siblings the
# delay only ever fires under real concurrency, which is the only kind
# this suite has.
setting commit_delay => { choices => [ 0, 1000 ] };

# The temp-table load and every sort that spills care about this one.
setting temp_buffers => { choices => [ '800kB', '64MB' ] };

# Reads combined into one IO or issued one page at a time.
setting io_combine_limit => { choices => [ 1, 32 ] };

# What vacuum and the concurrent index builds may keep in flight.
setting maintenance_io_concurrency => { choices => [ 0, 64 ] };

# Backends writing back as they dirty, or leaving everything to the
# checkpointer.
setting backend_flush_after => { choices => [ 0, '2MB' ] };

# How eagerly a lock wait turns into a deadlock check.  Nothing in the
# suite deadlocks by construction -- the loads take their locks in
# sorted order -- so the check may run as often as it likes and must
# find nothing.
setting deadlock_timeout => { choices => [ '50ms', '5s' ] };

1;
