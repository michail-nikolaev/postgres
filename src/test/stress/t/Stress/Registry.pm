
# Copyright (c) 2026, PostgreSQL Global Development Group

=pod

=head1 NAME

Stress::Registry - the registries a stress scenario draws from

=head1 DESCRIPTION

Everything a scenario is made of lives in one of the registries here.  A
scenario names entries from each; C<Stress::Run> assembles them into a
node, a set of pgbench scripts and a set of final checks.

The entries themselves are declared by the bundle files under Cross/ and
Feature/, one file per cohesive dimension: a feature bundle carries a
schema decorator together with the loads that drive it, the checks that
hold it to its invariant and the DDL aimed at it, so everything about,
say, the ledger is in Feature/Ledger.pm and nowhere else.  Cross/ holds
what cuts across features: the shared DDL rotation, the generic checks,
the environments, the chaos machinery and the modifiers.

A bundle declares entries with the subs this module exports:

  use Stress::Registry ':declare';

  schema ledger => { ... };
  load balanced_pair => { ... };
  check ledger_sum => { ... };

Each declaration records one entry in the corresponding registry hash;
a duplicate name dies with both declaration sites.  A bundle must not
read another bundle's registry entries at load time -- every
cross-reference is by name, in C<requires> and C<conflicts>, resolved
when a scenario is validated.  C<load_all()> loads every bundle in a
fixed order and then checks the whole catalogue for names that do not
resolve, so a typo in a requirement fails the first test that loads the
framework rather than the first soak that happens to combine the wrong
entries.

A definition may declare

  requires => { schema => [...] }   entries it cannot work without
  conflicts => { ... }              entries it must not be combined with

which C<Stress::Run> validates before anything is created, so an
impossible combination fails at once and says why, rather than halfway
through a run.

The registries are:

  %SCHEMA        what tables exist: one loader plus any decorators
  %INDEXES       what is built on them
  %LOAD          what changes the data, preserving some invariant
  %DDL           what runs concurrently with that
  %CHECK         what must hold regardless
  %TOPOLOGIES    how many nodes and how they are related
  %DISRUPTORS    what happens to the cluster while the workload runs
  %PROFILES      server configurations that are neither of those
  %MODIFIERS     how the server goes about its work
  %CHAOS_POINTS  the injection points worth jittering, with their caps
  %CHAOS         named jitter profiles built from those points

A C<script> in a load or a check may be a string, or a sub called with
the scenario context when the values it needs are only known once the
schema exists.

=head2 %SCHEMA

The first entry a scenario names is the loader; the rest are
decorators.  A decorator adds its own table -- and with it its own
invariant -- on top of the pgbench schema, so that a scenario is always
some specialized dimension running alongside an ordinary workload.

  init      loader only: how the base schema is created
  setup     SQL applied after the schema exists
  tables    tables the DDL rotation and checks may target
  untables  tables an earlier entry contributed that this one replaces
  indexes   indexes created with the decorator, in %INDEXES form
  context   sub($node) returning values the scripts need to be told

=head2 %INDEXES

C<defn> is everything after the index name, so that both the blocking
and the concurrent form of the build can be generated from it;
C<table> is what the index is on, and C<am> decides which amcheck
function, if any, can verify it.

=head2 %LOAD

A load is a pgbench script that changes data while preserving the
invariant its scenario checks.  C<weight> is its share of the
transaction mix, and C<setup> is any SQL it needs beforehand.

=head2 %DDL

C<variants> returns the alternatives the DDL client picks between, one
per invocation; each is an arrayref of statements that run together.  It
is called with the scenario context, so an entry expands itself over
whatever tables and indexes the scenario actually has.

Each variant names the C<table> it works on, which is what the
per-relation gate uses to keep two commands off the same relation when
several run at once.  A variant that touches relations it does not name
-- dropping one, say -- cannot be gated that way and sets C<solo>, which
restricts it to scenarios running one command at a time.

=head2 %CHECK

C<script> is a pgbench fragment run as its own weighted script;
C<final> is a sub run against the node once the workload is over.
Either may be omitted.

=head2 %TOPOLOGIES, %DISRUPTORS, %PROFILES

What used to be one "environment" axis, on three: a scenario runs
against a topology (standalone, a hot standby, a subscription), under a
disruptor (nothing, a crash loop, a cancellation storm), with an
optional settings profile (another wal_level, an aggressive autovacuum,
a small lock table).  Each is declared with its own constraints and its
own conf, and they compose -- which is what one fused axis could never
do without a file per combination.  The settings they carry belong to
them rather than to scenarios because getting them wrong is itself a
source of false failures.

=head2 %MODIFIERS

A modifier is a set of GUCs at values that change how the server does
its work without changing what the work produces.  Where a chaos profile
widens a window, a modifier moves the whole run onto a different code
path: everything spills instead of fitting in memory, the planner picks a
different node, WAL is really flushed, a vacuum freezes at once.

The invariants have to hold under every one of them.  That is the whole
claim, and it is what makes them cheap coverage: a modifier costs no new
scenario, no new check, and no new plugin combination, and every scenario
in the catalogue can be run under each.

Two rules, both learned the hard way in this suite.  A modifier may not
change results -- so nothing here touches isolation level, or a GUC that
decides what a query returns rather than how it gets there.  And a
modifier must not silently disable the thing a scenario exists to test:
turning off enable_indexscan would leave the index-only scan scenario
comparing two sequential scans and passing for the wrong reason, which is
why those checks now pin the GUCs they need and assert the plan they got.

=head2 %CHAOS

A chaos profile widens the windows a race has to be lost in, without
changing anything the server decides.

Most of the windows this suite hunts are microseconds wide.  Repetition
does not find those: a scenario can run for hours and never place two
operations within a microsecond of each other.  What does find them is
making the window milliseconds wide for a small fraction of the
operations that reach it -- which is what jitter attached to an
injection point does, and what the probabilistic form of
debug_discard_caches does for cache-flush hazards.

C<points> maps an injection point to [ probability, min_us, max_us ].
C<discard_probability> is the chance of a forced cache flush at each
opportunity.  A profile is declared by a scenario as C<chaos>, is
orthogonal to the environment, and does nothing at all on a build
without injection points.

Two rules hold for every profile here, and both are load-bearing.
Sleeps only ever delay; a failure seen under chaos is a failure that
exists without it, which is what makes a chaos run worth reporting.  And
no sleep may approach lock_timeout: a lock request that dawdles at the
head of a queue stalls every writer behind it, which ends the run in a
cascade that tests nothing.

=cut

package Stress::Registry;

use strict;
use warnings FATAL => 'all';

use Cwd qw(abs_path);
use Exporter 'import';
use File::Basename qw(dirname);

our (%SCHEMA, %INDEXES, %LOAD, %DDL, %CHECK);
our (%TOPOLOGIES, %DISRUPTORS, %PROFILES);
our (%MODIFIERS, %CHAOS_POINTS, %CHAOS, %TEMPLATES);

our @EXPORT_OK = (
	qw(%SCHEMA %INDEXES %LOAD %DDL %CHECK
	  %TOPOLOGIES %DISRUPTORS %PROFILES
	  %MODIFIERS %CHAOS_POINTS %CHAOS %TEMPLATES),
	qw(schema index_def load ddl check
	  topology disruptor settings_profile
	  modifier chaos_point chaos_profile scenario_template),
	qw(load_all));

our %EXPORT_TAGS = (
	registries => [
		qw(%SCHEMA %INDEXES %LOAD %DDL %CHECK
		  %TOPOLOGIES %DISRUPTORS %PROFILES
		  %MODIFIERS %CHAOS_POINTS %CHAOS %TEMPLATES)
	],
	declare => [
		qw(schema index_def load ddl check
		  topology disruptor settings_profile
		  modifier chaos_point chaos_profile scenario_template)
	],
);

# The kind names here are the ones requires/conflicts use, so that a
# declaration, a requirement and an error message all speak the same
# language.
my %registry_of = (
	schema => \%SCHEMA,
	indexes => \%INDEXES,
	load => \%LOAD,
	ddl => \%DDL,
	checks => \%CHECK,
	topology => \%TOPOLOGIES,
	disruptor => \%DISRUPTORS,
	profile => \%PROFILES,
	modifier => \%MODIFIERS,
	chaos_point => \%CHAOS_POINTS,
	chaos_profile => \%CHAOS,
	scenario_template => \%TEMPLATES,
);

my %declared_at;

sub _register
{
	my ($kind, $name, $defn) = @_;
	my $reg = $registry_of{$kind};
	my (undef, $file, $line) = caller(1);

	die "duplicate $kind '$name' at $file:$line, "
	  . "first declared at $declared_at{\"$kind:$name\"}"
	  if exists $reg->{$name};

	$declared_at{"$kind:$name"} = "$file:$line";
	$reg->{$name} = $defn;
	return;
}

sub schema        { _register('schema', @_); return }
sub index_def     { _register('indexes', @_); return }
sub load          { _register('load', @_); return }
sub ddl           { _register('ddl', @_); return }
sub check         { _register('checks', @_); return }
sub topology      { _register('topology', @_); return }
sub disruptor     { _register('disruptor', @_); return }
sub settings_profile { _register('profile', @_); return }
sub modifier      { _register('modifier', @_); return }
sub chaos_point   { _register('chaos_point', @_); return }
sub chaos_profile { _register('chaos_profile', @_); return }

# A scenario shared by several test files that differ in one axis: the
# bundle declares the spec once, and each stub file runs it through
# run_template() with its own overrides.  Byte-identical scenario files
# were how the checksum triple drifted apart before this existed.
sub scenario_template { _register('scenario_template', @_); return }

=pod

=head2 load_all()

Load every bundle under Cross/ and Feature/, in sorted order, and then
check the catalogue's cross-references.  Idempotent, so whichever of
Run and Soak gets there first does the work.

=cut

my $loaded = 0;

sub load_all
{
	return if $loaded;
	$loaded = 1;

	# Required under their @INC-relative names rather than their paths,
	# so that a bundle importing a helper from another bundle with an
	# ordinary use statement finds it already in %INC instead of
	# compiling the file a second time.
	my $dir = dirname(abs_path(__FILE__));
	foreach my $sub (qw(Cross Feature))
	{
		foreach my $file (sort glob("$dir/$sub/*.pm"))
		{
			(my $base = $file) =~ s{.*/}{};
			require "Stress/$sub/$base";    ## no critic (RequireBarewordIncludes)
		}
	}

	_post_load_checks();
	return;
}

# Names that do not resolve are caught here, once, rather than by
# whichever scenario or soak combination happens to trip over them.  A
# requirement naming a kind that does not exist would otherwise be
# silently true, and a conflict naming a missing entry never fires --
# which lets soak build a combination the entry said it could not be
# part of.
sub _post_load_checks
{
	my @errors;

	foreach my $kind (qw(schema indexes load ddl checks modifier))
	{
		my $reg = $registry_of{$kind};
		foreach my $name (sort keys %$reg)
		{
			my $defn = $reg->{$name};
			foreach my $rel (qw(requires conflicts))
			{
				foreach my $rkind (sort keys %{ $defn->{$rel} // {} })
				{
					if (!exists $registry_of{$rkind})
					{
						push @errors,
						  "$kind '$name' ($declared_at{\"$kind:$name\"}) "
						  . "$rel unknown kind '$rkind'";
						next;
					}
					foreach my $rname (@{ $defn->{$rel}->{$rkind} })
					{
						push @errors,
						  "$kind '$name' ($declared_at{\"$kind:$name\"}) "
						  . "$rel $rkind '$rname', which is not declared"
						  unless exists $registry_of{$rkind}->{$rname};
					}
				}
			}
		}
	}

	# A profile jittering a point nobody declared would attach cleanly
	# and never fire: InjectionPointAttach() takes any name without
	# complaint, so this is the only place the typo can be caught.
	foreach my $pname (sort keys %CHAOS)
	{
		foreach my $point (sort keys %{ $CHAOS{$pname}->{points} // {} })
		{
			push @errors,
			  "chaos profile '$pname' jitters undeclared point '$point'"
			  unless exists $CHAOS_POINTS{$point};
		}
	}

	# The checks a load or a command declares are what joins a scenario
	# implicitly, so a typo here would silently verify nothing.
	foreach my $kind (qw(load ddl))
	{
		my $reg = $registry_of{$kind};
		foreach my $name (sort keys %$reg)
		{
			foreach my $cname (@{ $reg->{$name}->{checks} // [] })
			{
				push @errors,
				  "$kind '$name' ($declared_at{\"$kind:$name\"}) declares "
				  . "check '$cname', which is not declared"
				  unless exists $CHECK{$cname};
			}
		}
	}

	die join("\n", 'the registries do not fit together:', @errors) . "\n"
	  if @errors;
	return;
}

1;
