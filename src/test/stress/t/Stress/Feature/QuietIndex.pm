
# Copyright (c) 2026, PostgreSQL Global Development Group

# A small table nothing writes, so an index can be dropped and
# recreated at the highest rate the commands allow.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::QuietIndex;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

# A small table nothing writes, with an index of its own.
#
# This exists to raise the rate of one thing: an index being dropped
# and recreated.  On a few hundred rows that cycle takes about a
# millisecond, where the same commands against pgbench_accounts take
# hundreds, so a standby replaying them sees the catalog entry come
# and go far more often -- which is what a reader planning against a
# stale index list has to collide with.
schema quiet_index => {
		setup => q(
			-- Carved out of pgbench_accounts rather than generated: the
			-- side tables derive from the standard schema wherever their
			-- shape allows, so their provenance is the workload's own.
			CREATE TABLE pgb_quiet AS
				SELECT aid AS id, aid AS val FROM pgbench_accounts
				ORDER BY aid LIMIT 500;
			ALTER TABLE pgb_quiet ADD PRIMARY KEY (id);
		),
		tables => ['pgb_quiet'],
		indexes => [ {
			table => 'pgb_quiet',
			name => 'pgb_quiet_val_idx',
			am => 'btree',
			defn => 'ON pgb_quiet(val)',
		} ],
};

# A read that has to be planned, against the table whose index keeps
# coming and going.  get_relation_info() opens every index of the
# relation while planning, whether or not the plan ends up using it,
# so this is enough to make the planner touch one that replay may
# have just removed -- no index scan need be chosen.
check quiet_index_scan => {
		auto => 1,
		weight => 6,
		requires => { schema => ['quiet_index'] },
		script => q(
			SELECT COUNT(*) FROM pgb_quiet WHERE val > 0;
		),
};

1;
