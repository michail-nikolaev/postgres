
# Copyright (c) 2026, PostgreSQL Global Development Group

=pod

=head1 NAME

Stress::Util - helpers shared by the framework and the bundles

=head1 DESCRIPTION

Small subs more than one bundle needs.  Nothing here reads a registry:
these are the pieces that would otherwise be copied between the
environments and the checks, which is how they had already started to
drift apart before they were pulled together.

=cut

package Stress::Util;

use strict;
use warnings FATAL => 'all';

use Exporter 'import';
use PostgreSQL::Test::Utils;
use Test::More;

our @EXPORT_OK = qw(stress_rollback_prepared stress_drop_invalid_indexes
  stress_claim_testdir stress_verify_testdir
  _pgbench_ok _retry_on_deadlock);

=pod

=head2 stress_claim_testdir(), stress_verify_testdir()

Claim this process's test directory, and check afterwards that the
claim still stands.

A TAP test's data directory is named for the test, and nothing else:
src/tools/testwrap begins every run by rmtree'ing it.  So two
concurrent runs of the SAME test -- which is what "meson test --repeat
N" does, N processes at once -- delete each other's clusters while
their postmasters are running.  The symptom is not an error but wrong
data: rows that vanish, sums that disagree, a workload that appears to
have lost updates.  That cost an evening here, chasing a server bug
that did not exist (see REGRESSIONS), which is why a run now says so
itself.

The claim is a file holding this process's pid.  If it is gone, another
copy rmtree'd our directory; if it holds someone else's pid, another
copy claimed it.  Either way what the run measured is meaningless.

=cut

sub _testdir_claim_path
{
	return ($PostgreSQL::Test::Utils::tmp_check // 'tmp_check')
	  . '/stress_testdir_claim';
}

sub stress_claim_testdir
{
	my $path = _testdir_claim_path();
	open my $fh, '>', $path or return;
	print $fh "$$\n";
	close $fh;
	return;
}

sub stress_verify_testdir
{
	my $path = _testdir_claim_path();
	my $why;

	if (!-e $path)
	{
		$why = 'it was deleted';
	}
	else
	{
		open my $fh, '<', $path or return;
		my $pid = <$fh>;
		close $fh;
		$pid //= '';
		chomp $pid;
		$why = "process $pid claimed it" if $pid ne "$$";
	}
	return unless $why;

	die "this test's directory was taken over while the run was in "
	  . "progress ($why).\n"
	  . "Two copies of one test cannot run at once: the directory is "
	  . "named for the test, and\ntestwrap deletes it at every start, so "
	  . "they remove each other's clusters mid-run.\n"
	  . "Whatever this run measured is meaningless.  To run copies "
	  . "concurrently, give each one\nits own test name rather than "
	  . "using 'meson test --repeat'.\n";
}

=pod

=head2 stress_rollback_prepared($node)

Roll back every prepared transaction, and say how many there were.

A prepared transaction outlives the session that made it and keeps every
lock it took, which is the whole point of one -- and a nuisance
afterwards.  The two-phase load leaves them behind whenever the run stops
mid-transaction, and a crash brings them back with recovery, after which
anything that needs a conflicting lock waits for a transaction that will
never be resolved.  An invalid index cannot be dropped while one holds
AccessExclusiveLock on it, and the wait ends in the lock timeout rather
than in an answer.

Called where the workload is over, or over for this cycle, so there is
nothing left to commit.  A scenario wanting to assert something about
prepared transactions surviving has to do it before this runs.

=cut

sub stress_rollback_prepared
{
	my ($node) = @_;

	# Built as statements rather than as gids, so that quoting is the
	# server's problem.
	my @rollbacks = grep { $_ ne '' } split /\n/,
	  $node->safe_psql('postgres',
		q(SELECT format('ROLLBACK PREPARED %L', gid) FROM pg_prepared_xacts));

	$node->safe_psql('postgres', $_) for @rollbacks;

	return scalar @rollbacks;
}

=pod

=head2 stress_drop_invalid_indexes($node)

Drop every invalid index.  An interrupted concurrent build may leave one
behind, which is documented; it must at least be droppable, and the
environments that interrupt commands -- a crash, a cancellation -- call
this to prove it is and to leave the cluster clean for what follows.

=cut

sub stress_drop_invalid_indexes
{
	my ($node) = @_;

	$node->safe_psql(
		'postgres', q(
		DO $$
		DECLARE
			idx oid;
		BEGIN
			FOR idx IN SELECT indexrelid FROM pg_index
				WHERE NOT indisvalid
			LOOP
				CONTINUE WHEN NOT EXISTS
					(SELECT 1 FROM pg_class WHERE oid = idx);
				EXECUTE format('DROP INDEX %s', idx::regclass);
			END LOOP;
		END;
		$$;
	));
	return;
}

# The two assertions every environment that drives pgbench itself has to
# make: the workload really ran, and its clients said nothing beyond
# what the stderr whitelist allows.  One helper, so the environments
# cannot drift apart in what they accept.
sub _pgbench_ok
{
	my ($out, $err, $ctx, $what) = @_;
	Test::More::like($out, qr{actually processed}, "$what ran");
	Test::More::like($err, $ctx->{stderr_re}, "$what reported nothing");
	return;
}

# Run a statement that competes for locks with the DDL rotation, giving
# way if the deadlock detector picks it as the victim.  Only a deadlock
# is retried: anything else is the failure the test is looking for.
sub _retry_on_deadlock
{
	my ($node, $sql) = @_;

	foreach my $try (1 .. 10)
	{
		my ($rc, $out, $err) = $node->psql('postgres', $sql);
		return if $rc == 0;
		die "$sql failed: $err" unless $err =~ /deadlock detected/;
	}
	die "$sql kept deadlocking";
}

1;
