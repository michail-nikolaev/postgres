
# Copyright (c) 2026, PostgreSQL Global Development Group

=pod

=head1 NAME

Stress::LogScan - read every node's log the way a person would have to

=head1 DESCRIPTION

Until this existed, a server-side failure was only caught if a client
happened to be attached to it: pgbench aborts on an ERROR it receives,
but an autovacuum worker's PANIC, a checksum worker tripping an
assertion, a walsender dying with something internal -- none of those
have a client, and a run could log any of them and stay green.

So the framework now scans the whole log of every node once the run is
over.  What fails the test is the unambiguous evidence of a server-side
defect:

  - PANIC anywhere;
  - an assertion trap, a segfault, a process killed by a signal;
  - FATAL lines beyond the allowlisted lifecycle chatter;
  - ERROR and WARNING lines whose SQLSTATE is internal_error or a
    corruption class (XX000, XX001, XX002).

Ordinary ERRORs are deliberately out of scope -- lock timeouts and
tolerated cancellations would drown everything -- which is why the
diagnostics conf adds %e to log_line_prefix: the SQLSTATE is what tells
"can't happen" from "happens all day".

The allowlist composes the way everything else here does: a base list
of lifecycle lines every cluster produces, plus C<log_allow> regexps
declared by whichever topology, disruptor, profile, modifier, load,
command or check earns its noise -- next to the thing that emits it,
where a reviewer can see both.  C<stress_log_scan=warn> in
PG_TEST_EXTRA downgrades a finding to a diagnostic during bring-up,
and C<stress_log_scan=off> disables the scan.

=cut

package Stress::LogScan;

use strict;
use warnings FATAL => 'all';

use Exporter 'import';
use Test::More;
use PostgreSQL::Test::Utils;
use Stress::Registry qw(:registries);

our @EXPORT_OK = qw(scan_text collect_allow scan_nodes);

# What every cluster says in the course of being started, stopped,
# promoted and torn down while sessions and workers are still attached.
# FATAL-class only: the scan does not read plain ERRORs at all.
my @BASE_ALLOW = (
	qr/terminating connection due to administrator command/,
	qr/terminating walreceiver process due to administrator command/,
	qr/terminating logical replication worker due to administrator command/,
	qr/terminating background worker .* due to administrator command/,
	qr/the database system is (?:starting up|shutting down|in recovery mode|not yet accepting connections)/,
	qr/canceling authentication due to timeout/,
	# A client that vanished mid-statement; pgbench is killed brutally by
	# more than one disruptor, and the server may be mid-send.
	qr/connection to client lost/,
	qr/could not send data to client/,
	qr/could not receive data from client/,
	qr/unexpected EOF on client connection/,
);

my $LINE_RE = qr/
	\b(?<sqlstate>[0-9A-Z]{5})\ (?<level>PANIC|FATAL|ERROR|WARNING):\ \ ?
	(?<msg>.*)
/x;

=pod

=over

=item scan_text($text, \@allow)

Return the offending lines of one log, oldest first.  Pure, so the
meta test can feed it synthetic logs and pin what fails and what does
not.

=cut

sub scan_text
{
	my ($text, $allow) = @_;
	my @bad;

	foreach my $line (split /\n/, $text)
	{
		# The evidence that needs no SQLSTATE: crashes and traps are
		# reported by the postmaster or the C runtime, not by ereport.
		if ($line =~ /TRAP: |failed Assert|stack smashing detected/
			|| $line =~ /was terminated by signal|Segmentation fault/)
		{
			push @bad, $line;
			next;
		}

		next unless $line =~ $LINE_RE;
		my ($sqlstate, $level, $msg) = @+{qw(sqlstate level msg)};

		my $suspect =
		    $level eq 'PANIC' ? 1
		  : $level eq 'FATAL' ? 1
		  : $sqlstate =~ /^XX00[012]$/ ? 1
		  : 0;
		next unless $suspect;

		# PANIC and the corruption classes are never allowlisted: a
		# lifecycle pattern broad enough to cover one would cover a
		# finding too.
		if ($level eq 'FATAL')
		{
			next if grep { $msg =~ $_ } @$allow;
		}

		push @bad, $line;
	}
	return @bad;
}

=pod

=item collect_allow($spec)

The allowlist for one resolved scenario: the base list plus whatever
every named entry declares as C<log_allow>.

=cut

sub collect_allow
{
	my ($spec) = @_;
	my @allow = @BASE_ALLOW;

	my @defns = (
		$TOPOLOGIES{ $spec->{topology} },
		$DISRUPTORS{ $spec->{disruptor} },
		defined $spec->{profile} ? $PROFILES{ $spec->{profile} } : (),
		defined $spec->{modifier} ? $MODIFIERS{ $spec->{modifier} } : (),
		(map { $LOAD{$_} } @{ $spec->{load} // [] }),
		(map { $DDL{$_} } @{ $spec->{ddl} // [] }),
		(map { $CHECK{$_} } grep { exists $CHECK{$_} }
			@{ $spec->{checks} // [] }),
	);
	foreach my $defn (@defns)
	{
		push @allow, @{ $defn->{log_allow} // [] } if $defn;
	}
	push @allow, @{ $spec->{log_allow} // [] };
	return \@allow;
}

=pod

=item scan_nodes($spec, @nodes)

Scan every node's log and report through Test::More: one ok() per
node, failing with the first offending lines.  Honors
stress_log_scan=off|warn.

=back

=cut

sub scan_nodes
{
	my ($spec, @nodes) = @_;

	my $mode = 'fail';
	$mode = $1
	  if ($ENV{PG_TEST_EXTRA} // '') =~ /\bstress_log_scan=(off|warn)\b/;
	return if $mode eq 'off';

	my $allow = collect_allow($spec);

	foreach my $node (@nodes)
	{
		next unless defined $node->logfile && -e $node->logfile;
		my @bad =
		  scan_text(PostgreSQL::Test::Utils::slurp_file($node->logfile),
			$allow);

		my $name = $node->name;
		if (!@bad)
		{
			Test::More::pass("no server-side failures in ${name}'s log");
			next;
		}

		my $summary =
		    scalar(@bad)
		  . " suspect lines in ${name}'s log ("
		  . $node->logfile . '), first '
		  . (@bad > 10 ? 10 : scalar @bad) . ':';
		if ($mode eq 'warn')
		{
			Test::More::diag($summary);
			Test::More::diag($_) for @bad[ 0 .. (@bad > 10 ? 9 : $#bad) ];
			Test::More::pass(
				"log scan of $name downgraded by stress_log_scan=warn");
		}
		else
		{
			Test::More::fail("no server-side failures in ${name}'s log");
			Test::More::diag($summary);
			Test::More::diag($_) for @bad[ 0 .. (@bad > 10 ? 9 : $#bad) ];
		}
	}
	return;
}

1;
