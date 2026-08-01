
# Copyright (c) 2026, PostgreSQL Global Development Group

=pod

=head1 NAME

Stress::Chaos - the pool of injection points chaos may jitter

=head1 DESCRIPTION

The hand-written part of the chaos machinery is small on purpose: a cap
table for the points somebody has already reasoned about
(%CHAOS_POINTS), an exclusion list for the ones somebody has judged
unjitterable (%CHAOS_EXCLUDED), and the named profiles.  Everything
else is derived.  The build scans the backend sources into
injection_points_defined(), and the pool this module assembles is that
list minus the exclusions, with the curated caps where they exist and
conservative defaults where they do not.

So a point added anywhere in the tree joins the pool on the next build,
at caps too small to break a run and large enough to widen a window --
which is the property the whole dimension rests on: coverage that grows
with the tree instead of with somebody's list.  The mechanically
excluded points are the IS_INJECTION_POINT_ATTACHED sites, where mere
attachment changes what the server decides; jitter must only ever
delay, and on those names it would not.

A build without injection points, or an installation without the
module, degrades to the curated caps alone -- which costs nothing,
because chaos is skipped on such a build anyway.

=cut

package Stress::Chaos;

use strict;
use warnings FATAL => 'all';

use Exporter 'import';
use Stress::Registry qw(:registries);

our @EXPORT_OK = qw(chaos_fetch_defined chaos_pool chaos_caps
  chaos_point_known %DEFAULT_CAPS);

# What a point nobody has curated gets: rare and short.  Rare, because
# the scan cannot tell a phase change reached twice a run from a path
# every commit takes; short, because no sleep may approach lock_timeout.
# A point worth more than this earns an entry in %CHAOS_POINTS.
our %DEFAULT_CAPS = (max_p => 0.01, max_us => 5000);

# The defined points of the build under test, fetched once per process:
# name => { kinds => { run => 1, ... } }.  undef until fetched, and
# undef for good on a build that cannot answer.
my $defined;
my $fetched = 0;

=pod

=over

=item chaos_fetch_defined($node)

Ask the server for the injection points its build defines, through the
injection_points extension, and remember the answer.  Returns the hash
of names, or undef when this build cannot say -- no injection points,
or a module too old to carry injection_points_defined().

=cut

sub chaos_fetch_defined
{
	my ($node) = @_;

	return $defined if $fetched;
	$fetched = 1;

	return undef
	  unless ($ENV{enable_injection_points} // '') eq 'yes';

	my $rows = eval {
		$node->safe_psql(
			'postgres', q(
			CREATE EXTENSION IF NOT EXISTS injection_points;
			SELECT name, kind FROM injection_points_defined()));
	};
	return undef unless defined $rows;

	my %points;
	foreach my $line (split /\n/, $rows)
	{
		my ($name, $kind) = split /\|/, $line;
		next unless defined $kind;
		$points{$name}{kinds}{$kind} = 1;
	}
	$defined = \%points;
	return $defined;
}

=pod

=item chaos_pool()

The points an invented profile may draw from, as name => { max_p,
max_us, curated }.  Derived from the defined list when one has been
fetched: everything except the mechanically excluded attached-kind
sites and the judged %CHAOS_EXCLUDED names, capped by %CHAOS_POINTS
where curated and by %DEFAULT_CAPS where not.  Before a fetch, or on a
build that cannot answer, the curated table alone.

=cut

sub chaos_pool
{
	my %pool;

	if ($defined)
	{
		foreach my $name (sort keys %$defined)
		{
			# Attachment alone changes the server's behavior at these
			# sites, so a callback that only means to add delay must
			# stay away from the whole name.
			next if $defined->{$name}{kinds}{attached};
			next if exists $CHAOS_EXCLUDED{$name};
			my $caps = $CHAOS_POINTS{$name};
			$pool{$name} = {
				max_p => $caps ? $caps->{max_p} : $DEFAULT_CAPS{max_p},
				max_us => $caps ? $caps->{max_us} : $DEFAULT_CAPS{max_us},
				curated => $caps ? 1 : 0,
			};
		}
		return \%pool;
	}

	foreach my $name (sort keys %CHAOS_POINTS)
	{
		next if exists $CHAOS_EXCLUDED{$name};
		$pool{$name} = { %{ $CHAOS_POINTS{$name} }, curated => 1 };
	}
	return \%pool;
}

=pod

=item chaos_caps($point)

The caps an invented profile must stay inside for one point: the
curated entry, or the defaults for a point the scan found and nobody
has looked at yet.

=item chaos_point_known($point)

Whether a point may be jittered at all: curated, or defined by the
build and not excluded.  With no defined list fetched, curated is all
that can be vouched for.

=back

=cut

sub chaos_caps
{
	my ($point) = @_;
	return $CHAOS_POINTS{$point} // {%DEFAULT_CAPS};
}

sub chaos_point_known
{
	my ($point) = @_;

	return 0 if exists $CHAOS_EXCLUDED{$point};
	return 1 if exists $CHAOS_POINTS{$point};
	return 0 unless $defined;
	return 0 unless exists $defined->{$point};
	return 0 if $defined->{$point}{kinds}{attached};
	return 1;
}

1;
