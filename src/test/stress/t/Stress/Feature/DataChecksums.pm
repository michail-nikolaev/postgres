
# Copyright (c) 2026, PostgreSQL Global Development Group

# The online data checksum transitions: a whole-database rewrite
# running against the rotation's own rewrites.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::DataChecksums;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

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
schema data_checksum_helper => {
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
			CREATE UNLOGGED TABLE pgb_unlogged AS
				SELECT aid AS id, repeat(md5(aid::text), 8) AS pad
				FROM pgbench_accounts ORDER BY aid LIMIT 2000;
			ALTER TABLE pgb_unlogged ADD PRIMARY KEY (id);
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
};

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
ddl toggle_data_checksums => {
		requires => { schema => ['data_checksum_helper'] },
		checks => ['no_checksum_failures'],
		solo => 1,
		variants => sub {
			return ({
				table => 'pg_database',
				stmts => ['SELECT pgb_flip_data_checksums();']
			});
		},
};

# No page failed its checksum, and every page was readable.
#
# This is the detector for the online checksum transitions.  The
# counter is the server's own: a page whose checksum does not match
# what the data says increments it, and nothing else does.  Reading it
# is not enough on its own, though, because a page is only verified
# when it is read, so the check reads everything first -- every block
# of every relation, through pg_relation_check_pages if this build has
# it and by a full scan of each table otherwise.
check no_checksum_failures => {
		requires => { schema => ['data_checksum_helper'] },
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
};

# Data checksums off.  initdb in this tree turns them on, so every
# scenario has been reading and writing pages with a checksum computed
# and verified on each one; without them that whole path is skipped.
# It is an initdb decision rather than a GUC, which is why a modifier
# can carry init options.
modifier no_checksums => {
		init => { no_data_checksums => 1 },
};

# The online data checksum worker.  It walks every page of every
# relation, and the relations are being rewritten underneath it by the
# rotation, so the gap between counting a relation's blocks and
# processing them is the one that matters.  Reached only by a scenario
# that drives the transitions.
chaos_point 'datachecksums-before-page' => { max_p => 0.25, max_us => 30_000 };

chaos_point 'datachecksums-after-page' => { max_p => 0.25, max_us => 30_000 };

chaos_point 'datachecksumsworker-startup-delay' => { max_p => 1.0, max_us => 20_000 };

chaos_point 'datachecksumsworker-launcher-delay' => { max_p => 1.0, max_us => 20_000 };

chaos_point 'datachecksums-enable-checksums-delay' =>
  { max_p => 1.0, max_us => 20_000 };

# Aimed at the online checksum worker: the gap between counting a
# relation's blocks and writing each of them, which is when the
# rotation can swap the relfilenode out from under it.
chaos_profile checksums => {
		points => {
			'datachecksums-before-page' => [ 0.01, 500, 5000 ],
			'datachecksums-after-page' => [ 0.01, 500, 5000 ],
			'datachecksumsworker-startup-delay' => [ 1.0, 1000, 15000 ],
		},
};

# The checksum worker held open as wide as the caps allow.  A quarter
# of its pages wait up to thirty milliseconds before being read and
# again after being dirtied, so a relation is in the middle of being
# checksummed for most of the time the worker is on it -- which is the
# state a concurrent rewrite has to be safe against.
chaos_profile checksums_heavy => {
		# Slow enough that the lock timeout has to be scaled with it: a
		# forced cache flush at every opportunity costs about two orders of
		# magnitude, and a healthy run then trips a timeout calibrated for
		# an ordinary server.
		slow => 1,
		points => {
			'datachecksums-before-page' => [ 0.25, 5000, 30000 ],
			'datachecksums-after-page' => [ 0.25, 5000, 30000 ],
			'datachecksumsworker-startup-delay' => [ 1.0, 5000, 20000 ],
			'datachecksumsworker-launcher-delay' => [ 1.0, 5000, 20000 ],
			'relation-open-after-lock' => [ 0.01, 500, 8000 ],
			'transaction-snapshot-taken' => [ 0.01, 500, 8000 ],
		},
		discard_probability => 0.002,
};

# Turning data checksums on and off while the relations are being
# rewritten underneath.
#
# Enabling checksums is not a switch.  A background worker walks every
# page of every relation, reads it, dirties it, logs a full page image
# and moves on, and the cluster reports "inprogress-on" until it has
# finished.  What it walks is the block count taken when it opened the
# relation -- and REPACK, CLUSTER and VACUUM FULL all swap a relation's
# relfilenode while it is open, while CREATE INDEX CONCURRENTLY adds
# relations it never enumerated.
#
# So this crosses the newest whole-database rewrite with the rewrites
# this suite already drives, and asks the only question that matters
# afterwards: with checksums on, does every page still verify?  The
# detector is the server's own counter -- pg_stat_database's
# checksum_failures, which nothing but a mismatch increments -- read
# after every page has been pulled through shared buffers, since a page
# is only verified when it is read.
#
# The chaos profile widens the gap between counting a relation's blocks
# and writing each of them, which is the window a concurrent swap has to
# land in.
#
# One template, three test files: standalone, against a standby, and
# under the crash loop.  They were three byte-identical scenario files
# before this existed, and had started to drift.
scenario_template data_checksums => {
	indexes => [ 'btree_abalance', 'btree_history_delta' ],
	load => ['tpcb_like'],
	ddl => [
		'repack_concurrently', 'reindex_table_concurrently',
		'drop_create_index', 'vacuum', 'toggle_data_checksums'
	],
	clients => 20,
	chaos => 'checksums_heavy',
	# The cluster has to start without checksums for the enabling
	# worker to have anything to do on the first turn.
	modifier => 'no_checksums',
};

1;
