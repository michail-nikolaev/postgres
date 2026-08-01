
# Copyright (c) 2026, PostgreSQL Global Development Group

# A materialized view over the ledger, refreshed concurrently.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::Matview;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

# A materialized view over the ledger column, so REFRESH ...
# CONCURRENTLY has something whose contents can be predicted:
# whatever snapshot the refresh used, the ledger summed to zero at
# that instant, so the bucket sums it recorded do too.
#
# Bucketing keeps the view small enough to refresh repeatedly while
# still giving the refresh a diff of a thousand rows to work out.
schema matview => {
		requires => { schema => ['ledger'] },
		setup => q(
			CREATE MATERIALIZED VIEW pgb_mv AS
				SELECT aid % 1000 AS bucket, SUM(ledger) AS s
					FROM pgbench_accounts GROUP BY 1;
			CREATE UNIQUE INDEX pgb_mv_bucket_idx ON pgb_mv(bucket);
		),
};

ddl refresh_matview_concurrently => {
		requires => { schema => ['matview'] },
		variants => sub {
			return ({
				table => 'pgb_mv',
				stmts => ['REFRESH MATERIALIZED VIEW CONCURRENTLY pgb_mv;']
			});
		},
};

# The materialized view holds the bucket sums as of some snapshot,
# and at every snapshot the ledger summed to zero, so the buckets it
# recorded add up to zero too.
check matview_matches => {
		weight => 1,
		requires => { schema => ['matview'] },
		script => q(
			SELECT stress_assert(cnt = 0 OR sum = 0,
				format('matview has %s buckets summing to %s', cnt, sum))
			FROM (SELECT COUNT(*) AS cnt, COALESCE(SUM(s), 0) AS sum
				FROM pgb_mv) x;
		),
};

# REFRESH MATERIALIZED VIEW CONCURRENTLY needs a unique index on the
# view, and it looks that index up while refreshing.  No other
# session can take it away in that window -- the refresh holds
# ExclusiveLock and a concurrent drop would need
# ShareUpdateExclusiveLock -- so the only way in is the view\'s own
# definition calling a function that drops it, which is what this
# does.  Silly, and the commit says so, but it is the difference
# between a clean error and an assertion failure.
check refresh_survives_dropped_index => {
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
};

1;
