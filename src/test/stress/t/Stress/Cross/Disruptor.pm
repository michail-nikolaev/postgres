
# Copyright (c) 2026, PostgreSQL Global Development Group

# The disruptors: what happens to the cluster while the workload runs.
# A disruptor's run wrapper receives the inner runner and decides how --
# and whether -- to call it; killing the server mid-workload and
# cancelling the commands are the two shapes so far.
#
# See Stress::Registry for what each declaration means.

package Stress::Cross::Disruptor;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

use IPC::Run;
use Stress::Util qw(stress_rollback_prepared stress_drop_invalid_indexes
  _pgbench_ok);

# What a scenario gets when it names no disruptor: the workload runs
# to its end, undisturbed.
disruptor none => {};

# The cluster is killed and restarted while the commands are in
# flight, so their cleanup happens through crash recovery rather than
# through their own code.
disruptor crash_loop => {
		# The kill/restart cycle is written for one node; against a
		# standby or a subscriber the interesting recovery is a
		# different scenario, not this one with extra nodes attached,
		# so the combinations stay closed until someone builds them.
		conflicts => { topology => [ 'standby', 'subscription' ] },
		run => sub {
			# The inner runner is unused: this disruptor launches its
			# own bounded pgbench per cycle, because the workload is
			# ended by the kill rather than by the clock.
			my ($node, $ctx, $inner) = @_;

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
disruptor cancellation => {
		# wal_level = replica rather than the standalone topology's
		# logical, so that a cancelled REPACK's transient slot really
		# toggles logical decoding -- which is what the final check
		# then polls.  The disruptor's conf is applied after the
		# topology's, so this wins.
		conf => [ 'wal_level = replica', 'max_connections = 50' ],
		# The victim picker matches command text in pg_stat_activity
		# on one node; what a cancellation does to an apply worker or
		# to replay is its own scenario when someone writes it.
		conflicts => { topology => [ 'standby', 'subscription' ] },
		run => sub {
			my ($node, $ctx, $inner) = @_;

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

1;
