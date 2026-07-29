
# Copyright (c) 2026, PostgreSQL Global Development Group

=pod

=head1 NAME

Stress::Soak - the long run over the whole matrix

=head1 DESCRIPTION

The default run is a fixed list of scenarios at six seconds each, which
is broad but shallow: a race that shows up once in fifteen runs at that
duration will pass a single run most of the time.  Soak mode is where
the depth is.  It walks the catalogue, and then keeps going with
combinations assembled at random from the registries, until its time
budget runs out.

What it varies goes beyond which plugins are combined.  The protocol a
statement arrives on, what the planner does with the ones that get
cached, the client count, and how reads reach the disk are all
dimensions of their own: several of the bugs this suite is aimed at live
in when a cached plan is rebuilt rather than in any one command, and
every one of these commands rewrites a relation and reads it back, which
the three io_method settings do through quite different machinery.  A
scenario in the catalogue pins those; soak does not.

It is off unless asked for:

  PG_TEST_EXTRA='stress_mode=soak stress_concurrently=20'

and takes these further settings:

  stress_soak_minutes=N   wall-clock budget, default 12, chosen to fit
                          inside the 1000 seconds meson allows a test
  stress_seed=N           replay a previous soak exactly
  stress_soak_skip=N      start at combination N rather than the first
  stress_soak_count=M     stop after M combinations
  stress_seconds=N        run each workload for N seconds, whatever the
                          scenario asks for

The last one turns a soak into a survey.  At stress_seconds=1 a
combination costs what its cluster costs to build and nothing more, so
a slice covers several times as many of them -- enough to find the ones
that cannot run at all, which is a different question from whether the
ones that can run have races in them.  Worth doing after adding a
plugin, before spending hours on the real thing.

Every combination has an index, and which combination that index names
depends only on the seed, so the last two cut a run into pieces.  A
failure reported as invented_57 comes back in a minute:

  PG_TEST_EXTRA='stress_mode=soak stress_seed=<seed>
                 stress_soak_skip=57 stress_soak_count=1'

and a long run can be split across several shorter ones without losing
or repeating any of it.  Each run reports the index to start the next
one at.

Every combination it invents is validated the same way a hand-written
scenario is.  One that could not work -- a check needing a table the
schema does not have, a load needing an environment that is not there --
is thrown away before it is given a number, so an index always names a
combination that ran.  The seed and the combination are logged before
each run, so a failure names the thing to reproduce.

=cut

package Stress::Soak;

use strict;
use warnings FATAL => 'all';

use Exporter 'import';
use FindBin;
use Test::More;
use PostgreSQL::Test::Utils;

use Stress::Plugins qw(%SCHEMA %INDEXES %LOAD %DDL %CHECK %ENVS);

our @EXPORT = qw(soak_enabled soak_run);

# The catalogue of scenarios, gathered from the test files that describe
# them.  Each file's spec lives in the file, so there is nothing to read
# it out of but the file itself: load them with run_scenario() diverted
# into a collector, and take what they hand over.
sub _catalogue
{
	my %found;

	local $Stress::Run::COLLECT = \%found;
	foreach my $file (sort glob("$FindBin::RealBin/[0-9]*.pl"))
	{
		# Skip ourselves; loading it would recurse.
		next if $file eq $0;
		do $file;
		die "collecting scenarios from $file: $@" if $@;
	}

	die 'collected no scenarios' unless keys %found;
	return %found;
}

=pod

=over

=item soak_enabled()

Whether PG_TEST_EXTRA asks for soak mode.

=cut

sub soak_enabled
{
	return (($ENV{PG_TEST_EXTRA} // '') =~ /\bstress_mode=soak\b/) ? 1 : 0;
}

# Whether this build has io_uring, which is a build option and not
# something to find out by watching a node fail to start.  Asked once.
my $have_io_uring;

sub _have_io_uring
{
	$have_io_uring //=
	  PostgreSQL::Test::Utils::check_pg_config('#define USE_LIBURING 1')
	  ? 1
	  : 0;
	return $have_io_uring;
}

# One random element of a list.
sub _pick
{
	my (@items) = @_;
	return $items[ int(rand(scalar @items)) ];
}

# A random subset of a list, of size between $min and $max.
sub _pick_some
{
	my ($min, $max, @items) = @_;
	my @shuffled = sort { rand() <=> rand() } @items;
	my $n = $min + int(rand($max - $min + 1));
	$n = scalar @shuffled if $n > @shuffled;
	return @shuffled[ 0 .. $n - 1 ];
}

# Assemble a combination out of the registries.  It is only a candidate:
# the caller validates it, and throws it away if the pieces do not fit.
sub _invent
{
	my $env = _pick(sort keys %ENVS);

	# Decorators bring their own tables and invariants; take a couple.
	# Ones that cannot live in this environment, or alongside something
	# already taken, are left out rather than picked and thrown away:
	# every conflict declared narrows the space, and choosing blindly
	# from the whole registry came to mean ten candidates discarded for
	# each one numbered.
	my @decorators;
	foreach my $name (_pick_some(0, 2, grep { $_ ne 'pgbench' } sort keys %SCHEMA))
	{
		my $defn = $SCHEMA{$name};
		next if grep { $_ eq $env } @{ $defn->{conflicts}->{env} // [] };
		next if grep { my $d = $_; grep { $_ eq $d } @decorators }
		  @{ $defn->{conflicts}->{schema} // [] };
		next if grep { my $picked = $_;
			grep { $_ eq $name } @{ $SCHEMA{$picked}->{conflicts}->{schema} // [] } }
		  @decorators;

		# Anything it depends on has to come with it.
		push @decorators, grep {
			my $dep = $_;
			!grep { $_ eq $dep } @decorators
		} @{ $defn->{requires}->{schema} // [] };
		push @decorators, $name;
	}
	my @schema = ('pgbench', @decorators);

	# A load, check or command that cannot work against this schema or
	# this environment is not eligible either.
	my $eligible = sub {
		my ($defn) = @_;

		foreach my $rname (@{ $defn->{requires}->{schema} // [] })
		{
			return 0 unless grep { $_ eq $rname } @schema;
		}
		if (my $wanted = $defn->{requires}->{env})
		{
			return 0 unless grep { $_ eq $env } @$wanted;
		}
		return 0 if grep { $_ eq $env } @{ $defn->{conflicts}->{env} // [] };
		foreach my $cname (@{ $defn->{conflicts}->{schema} // [] })
		{
			return 0 if grep { $_ eq $cname } @schema;
		}
		return 1;
	};

	my @loads = grep { $eligible->($LOAD{$_}) } sort keys %LOAD;
	my @checks = grep { $eligible->($CHECK{$_}) } sort keys %CHECK;
	# catalogue_only entries stay in the scenarios that name them: they
	# need a workload chosen to suit them, and inventing one around them
	# produces failures that say nothing about the server.
	my @ddl = grep { $eligible->($DDL{$_}) && !$DDL{$_}->{catalogue_only} }
	  sort keys %DDL;

	# Indexes a check asks for have to be there; take them all, which is
	# also more interesting for the rotation.  All, that is, except the
	# ones whose table only exists when a particular decorator was
	# picked: an index is built by name before the run starts, so one
	# naming a table that is not there ends the combination in setup
	# rather than in anything the suite is testing.
	my @indexes = grep { $eligible->($INDEXES{$_}) } sort keys %INDEXES;

	# How a statement reaches the server decides what gets cached and
	# when it is replanned, which is the subject of several of the bugs
	# this suite is aimed at.  A scenario in the catalogue pins one
	# protocol; here it is a dimension of its own.
	my $protocol = _pick('simple', 'extended', 'prepared');

	# Some scripts can only be substituted textually, so pick them and the
	# protocol together rather than inventing a combination that
	# validation would only throw away.
	my @picked_loads =
	  ('tpcb_like', _pick_some(0, 3, grep { $_ ne 'tpcb_like' } @loads));
	my @picked_ddl = _pick_some(1, 4, @ddl);
	$protocol = 'simple'
	  if grep { $LOAD{$_}->{simple_protocol_only} } @picked_loads
	  or grep { $DDL{$_}->{simple_protocol_only} } @picked_ddl;

	# And what the planner does with the statements that are cached.
	# force_generic_plan is left out when a pgbench table is partitioned:
	# a generic plan locks every partition, so the one the rotation is
	# detaching is never free of lockers and the detach waits out the
	# lock timeout.  That is a known dead end rather than a race worth
	# rediscovering.
	my @plan_modes = ('auto', 'force_custom_plan');
	push @plan_modes, 'force_generic_plan'
	  unless grep { $_ eq 'partitioned' } @schema;

	# How reads reach the disk.  Every one of these commands rewrites a
	# relation and then reads it back, and the three methods issue those
	# reads through quite different machinery -- synchronously, through
	# worker processes, or through io_uring -- so which one is in use is
	# a dimension in its own right rather than a detail of the platform.
	my @io_methods = ('sync', 'worker');
	push @io_methods, 'io_uring' if _have_io_uring();

	return {
		schema => \@schema,
		indexes => \@indexes,
		load => \@picked_loads,
		ddl => \@picked_ddl,
		# One command at a time.  Several at once is a real dimension,
		# but inventing it here mostly produced starvation: a
		# CONCURRENTLY command waits for the transactions the other DDL
		# clients are running, and with four of them there is no gap to
		# wait for, so the run dies on the lock timeout three minutes
		# later having tested nothing.  overlapping_ddl covers N-at-once
		# as a scenario built to survive it; soak covers breadth.
		ddl_concurrency => 1,
		checks => [ _pick_some(1, 4, @checks) ],
		env => $env,
		clients => _pick(10, 20, 30),
		# Always the smallest scale.  A larger one is a real dimension --
		# it is what makes an index multi-level -- but it belongs in a
		# scenario built for it, repack_dml_s50, not here.  Inventing it
		# means occasionally combining a million-row table with the five
		# non-btree indexes am_columns brings, and one such combination
		# spent fifteen minutes building indexes and taking a base backup
		# before its workload started.  Soak buys breadth; paying for it
		# in setup buys nothing.
		pgbench_scale => 1,
		pgbench_args => "--protocol=$protocol",
		conf => [
			'plan_cache_mode = ' . _pick(@plan_modes),
			'io_method = ' . _pick(@io_methods),
		],
		tags => ['soak'],
	};
}

# The next combination worth running, and how many candidates had to be
# thrown away to find it.
#
# Each candidate is generated from a seed of its own, derived from the
# run's seed and how many have been generated, so which candidate is the
# Nth depends on nothing else.  Running a scenario consumes randomness --
# the crash and subscription environments both make choices as they go --
# so without that the sequence would depend on what came before, and an
# index would not survive being skipped to.
sub _next_invented
{
	my ($genref, $discref, $seed) = @_;

	foreach my $try (1 .. 1000)
	{
		srand($seed + ($$genref + 1) * 7919);
		my $spec = _invent();
		$$genref++;

		return $spec if Stress::Run::spec_is_runnable($spec);
		$$discref++;
	}
	die 'invented a thousand combinations without finding a runnable one';
}

=pod

=item soak_run()

Run the catalogue, then invented combinations, until the budget is
spent.

=cut

sub soak_run
{
	# meson gives a TAP test 1000 seconds, and a run it cuts off there is
	# reported as a timeout rather than as a result -- which looks like a
	# failure and says nothing.  So the default budget is chosen to fit
	# inside that with room for the last combination to finish, and a
	# longer soak is made of several runs rather than one long one; see
	# stress_soak_skip above.
	my $minutes = 8;
	my $extra = $ENV{PG_TEST_EXTRA} // '';
	$minutes = $1 if $extra =~ /\bstress_soak_minutes=(\d+)\b/;

	# Which slice of the run to do.  Every combination has an index, and
	# what it is depends only on the seed and that index, so a soak can
	# be cut into pieces that add up to the whole:
	#
	#   stress_soak_skip=N    start at index N rather than 0
	#   stress_soak_count=M   stop after M combinations
	#
	# A failure at index 57 of a two-hour run is then reproducible in a
	# minute with skip=57 count=1, and a long run can be split across
	# several shorter ones without losing or repeating any of it.
	my $skip = 0;
	$skip = $1 if $extra =~ /\bstress_soak_skip=(\d+)\b/;
	my $count;
	$count = $1 if $extra =~ /\bstress_soak_count=(\d+)\b/;

	my $seed = Stress::Run::stress_seed();
	my $deadline = time() + $minutes * 60;
	note "soaking for $minutes minutes";

	# The catalogue first: those combinations are known to be worth
	# running, and a failure in one of them is the easiest to act on.
	# They occupy the first indexes, in name order, so an index means the
	# same thing on every run.
	my %catalogue = _catalogue();
	my @names = sort keys %catalogue;
	note 'soak: catalogue has ' . scalar(@names) . ' scenarios';
	note "soak: starting at index $skip" if $skip;
	note "soak: stopping after $count combinations" if defined $count;

	my ($ran, $invented, $discarded) = (0, 0, 0);

	# Fast-forward to the starting index.  Deciding whether a combination
	# is runnable needs no cluster, so skipping over the ones that are
	# not costs nothing but arithmetic -- which is what lets an index
	# count only the combinations that actually run.
	my $gen = 0;
	my $index = 0;
	while ($index < $skip)
	{
		_next_invented(\$gen, \$discarded, $seed) if $index >= @names;
		$index++;
	}

	# Combinations are not all the same length -- a crash loop or a
	# subscription takes minutes where a plain one takes seconds -- so
	# stopping when the budget runs out would overshoot it by the length
	# of whatever had just started.  Keep the longest one seen and only
	# begin another if that much time is left.
	#
	# The starting figure matters as much as the tracking: begin at a
	# handful of seconds and the first long combination is free to start
	# with almost no time remaining, which is how a budget of twelve
	# minutes turned into a run of seventeen and got cut off by meson
	# rather than finishing.  Begin at roughly what a slow combination
	# costs -- a crash loop or a standby, which are the expensive
	# environments -- and let anything slower raise it.
	my $longest = 120;

	while (time() + $longest <= $deadline)
	{
		last if defined $count && $ran >= $count;

		my ($name, $spec);
		if ($index < @names)
		{
			$name = $names[$index];
			$spec = $catalogue{$name};
		}
		else
		{
			$spec = _next_invented(\$gen, \$discarded, $seed);
			$invented++;
			$name = 'invented_' . $index;
		}
		$index++;

		# Say what is about to run before running it, so an abort names
		# the combination rather than leaving it to be guessed.
		note "soak: $name = " . _describe($spec);

		my $started = time();
		my $ok = eval {
			Stress::Run::run_one($name, $spec);
			1;
		};
		my $took = time() - $started;
		$longest = $took if $took > $longest;
		if (!$ok)
		{
			my $err = $@ // 'unknown error';
			# Invented combinations are checked before they are numbered,
			# so this can only be a scenario from the catalogue naming
			# something that no longer exists.  That is a fault in the
			# scenario rather than in the server, but it is still a fault
			# and saying so is more use than carrying on.
			if ($err =~ /requires|cannot be combined|unknown/)
			{
				die "$name is not a runnable combination: $err";
			}
			die $err;
		}
		$ran++;
	}

	note "soak ran $ran combinations ($invented invented, $discarded "
	  . "candidates thrown away before they were numbered); "
	  . "resume with stress_soak_skip=$index";
	return;
}

sub _describe
{
	my ($spec) = @_;
	my $out = join ' ',
	  map { "$_=[" . join(',', @{ $spec->{$_} }) . ']' }
	  grep { ref $spec->{$_} eq 'ARRAY' } qw(schema load ddl checks);

	# The scalars matter as much as the lists for reproducing a failure.
	$out .= " env=$spec->{env}" if $spec->{env};
	$out .= " scale=$spec->{pgbench_scale}" if $spec->{pgbench_scale};
	$out .= " clients=$spec->{clients}" if $spec->{clients};
	$out .= " ddl_concurrency=$spec->{ddl_concurrency}"
	  if defined $spec->{ddl_concurrency};
	$out .= " args=$spec->{pgbench_args}" if $spec->{pgbench_args};
	$out .= ' conf=[' . join(',', @{ $spec->{conf} }) . ']' if $spec->{conf};
	return $out;
}

=pod

=back

=cut

1;
