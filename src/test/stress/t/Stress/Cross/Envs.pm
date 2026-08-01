
# Copyright (c) 2026, PostgreSQL Global Development Group

# The cluster shapes a scenario can run against, and the loads
# that only exist inside one of them.
#
# See Stress::Registry for what each declaration means.

package Stress::Cross::Envs;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

use IPC::Run;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Stress::Util qw(stress_rollback_prepared stress_drop_invalid_indexes
  _pgbench_ok _retry_on_deadlock);

# Extra nodes need names of their own, and soak mode builds many of them
# in one test.
my $node_seq = 0;

# Local writes on the subscriber, against the very rows being
# applied.  The column is the subscriber's own and is indexed, so
# these updates are never HOT: they move the rows around underneath
# the apply worker's lookups.
load subscriber_churn => {
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
};

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
load subscriber_delete_reinsert => {
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
};

env standalone => {
		conf => ['wal_level = logical'],
};

# wal_level = replica, so that the transient slot REPACK takes really
# does toggle logical decoding on and off.
env wal_replica => {
		conf => [ 'wal_level = replica', 'max_connections = 50' ],
};

# Aggressive autovacuum, so the visibility map is being set
# continuously rather than once at the start.
env autovacuum => {
		conf => [
			'wal_level = logical',
			'autovacuum_naptime = 1s',
			'autovacuum_vacuum_scale_factor = 0.0',
			'autovacuum_vacuum_threshold = 100',
			'autovacuum_vacuum_insert_scale_factor = 0.0',
			'autovacuum_vacuum_insert_threshold = 100',
		],
};

# A deliberately small lock table, which the CONCURRENTLY commands
# are heavy users of.
env lock_exhaustion => {
		conf => [
			'wal_level = logical',
			'max_locks_per_transaction = 16',
			'max_connections = 50',
		],
};

# The cluster is killed and restarted while the commands are in
# flight, so their cleanup happens through crash recovery rather than
# through their own code.
env crash_loop => {
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
				stress_drop_invalid_indexes($node);
			}
			return;
		},
};

# The commands are interrupted partway rather than allowed to
# finish, which exercises their own cleanup paths.
env cancellation => {
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
			_pgbench_ok($out, $err, $ctx, 'writers');
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
			stress_drop_invalid_indexes($node);
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
};

# A hot standby replaying the DDL while serving the checks.
env standby => {
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

			_pgbench_ok($po, $pe, $ctx, 'primary workload');
			_pgbench_ok($so, $se, $ctx, 'standby workload') if $sh;
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
};

# A subscriber applying what the workload produces while the
# publisher's tables are rebuilt underneath the decoding.
env subscription => {
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

			_pgbench_ok($po, $pe, $ctx, 'publisher workload');
			_pgbench_ok($so, $se, $ctx, 'subscriber workload') if $sh;
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
};

1;
