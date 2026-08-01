
# Copyright (c) 2026, PostgreSQL Global Development Group

# A sequence handed out under a lock, so committed values are an
# unbroken prefix.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::Gapless;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

# Inserts serialized behind an advisory lock, so the values a
# sequence hands out are committed in increasing order.  At any later
# snapshot the rows carrying one must then be an unbroken prefix, so
# their count is the largest value handed out.
#
# They go into pgbench_history, which is append-only and is one of
# the relations the rotation repacks.
schema gapless => {
		setup => q(
			ALTER TABLE pgbench_history ADD COLUMN gval bigint;
			CREATE SEQUENCE pgb_gapless_val;
		),
};

# Inserts serialized behind an advisory lock, so that commit order
# matches the order the sequence handed the values out.
load serial_insert => {
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
};

# Once the row with val = j is committed, exactly j rows have
# val <= j -- the sequence was handed out under a lock, so commit
# order matches value order.
check gapless_count => {
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
};

1;
