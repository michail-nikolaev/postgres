
# Copyright (c) 2026, PostgreSQL Global Development Group

=pod

=head1 NAME

Stress::Run - assemble and run a stress scenario

=head1 SYNOPSIS

  use FindBin;
  use lib "$FindBin::RealBin";
  use Stress::Run;

  run_scenario('repack_dml_s1', { schema => ['pgbench'], ... });

=head1 DESCRIPTION

C<run_scenario()> takes the scenario a test file describes and runs
it: it validates the combination, builds the cluster the environment
asks for, loads the schema, builds the indexes, materializes one pgbench
script per load, per check and one for the DDL rotation, runs them
together as a weighted mix, and then applies the final checks.

Every script it generates is written into the node's basedir and the
pgbench command line is logged, so a failing run can be reproduced by
hand outside the harness.

=head1 SCENARIO FIELDS

  schema           schema plugins; the first is the loader, the rest
                   decorate it
  pgbench_scale    scale factor for the base load (default 1)
  indexes          indexes to build before the run
  load             workload scripts, mixed by their weights
  ddl              the commands the DDL client rotates through
  ddl_concurrency  how many may be in flight: 1, N, or 'none'
  checks           extra invariants, on top of the ones the loads and
                   commands bring with them; see Stress::Compose for
                   how the effective set is composed
  no_checks        checks to take back out of the effective set, by name
  topology         how many nodes and how they relate: standalone,
                   standby, subscription (default standalone)
  disruptor        what happens to the cluster while the workload runs:
                   none, crash_loop, cancellation (default none)
  profile          a named server configuration from %PROFILES, for the
                   clusters that are neither a topology nor a disruption
  conf             extra postgresql.conf lines for this scenario
  chaos            a named chaos profile from %CHAOS, or a profile soak
                   invented; widens race windows with injection-point
                   jitter and the probabilistic cache discard
  no_forced_chaos  exempt from stress_chaos=<profile>, for a scenario
                   whose dimension needs a server fast enough to run it
  no_forced_modifier  the same exemption from stress_modifier=<name>; a
                   scenario slowed by a forced modifier starves the same
                   dimensions a forced chaos profile does
  modifier         a named set of GUCs that changes how the server works
                   without changing what it produces; see %MODIFIERS
  pgbench_args     extra pgbench arguments
  clients          pgbench clients
  duration         seconds at stressval 1 (default 6)
  tags             'ci' for the default run; everything runs in soak mode

The names come from the registries in C<Stress::Registry>.  The written
scenario is only part of the effective one: C<Stress::Compose::resolve()>
pulls in the schemas the named pieces require and the checks they
declare, and C<Stress::Compose::validate()> rejects a combination whose
pieces do not fit, so a typo fails the test rather than quietly running
something smaller than intended.  What was composed is noted at the top
of every run.

=cut

package Stress::Run;

use strict;
use warnings FATAL => 'all';

use Exporter 'import';
use Test::More;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Stress::Registry qw(:registries load_all);
use Stress::Util qw(stress_rollback_prepared);

# Populate the registries before anything reads them; idempotent, so it
# does not matter whether Run or Soak gets here first.
Stress::Registry::load_all();

use Stress::Compose;

our @EXPORT =
  qw(run_scenario run_template run_one stress_seed stress_assert_defn
  @STANDARD_DDL);

# The rotation almost every scenario uses.  Exported rather than spelled
# out in each test file, because a scenario that means "the usual
# commands" should say so, and should not drift from the others.
our @STANDARD_DDL = (
	'repack_concurrently', 'repack_using_index',
	'reindex_table_concurrently', 'reindex_index_concurrently',
	'drop_create_index');

# Node names have to be unique within a run, and soak mode runs many
# scenarios in one test.
my $run_counter = 0;

=pod

=over

=item stress_seed()

Return the seed for this run, taken from C<stress_seed=N> in
PG_TEST_EXTRA or chosen at random, and seed Perl's C<rand> with it.  It
is reported in the log and handed to pgbench as C<--random-seed>, so a
run can be repeated with the same choices.

=cut

my $stress_seed;

sub stress_seed
{
	return $stress_seed if defined $stress_seed;

	my $extra = $ENV{PG_TEST_EXTRA} || '';
	if ($extra =~ /\bstress_seed=(\d+)\b/)
	{
		$stress_seed = $1;
	}
	else
	{
		$stress_seed = int(rand(2**31));
	}

	srand($stress_seed);
	note "stress_seed is $stress_seed";
	return $stress_seed;
}

=pod

=item stress_assert_defn()

SQL creating the stress_assert(ok, msg) function the check scripts call.
A failed assertion raises an error naming the invariant and the offending
values, which is what a pgbench abort then reports.

=cut

sub stress_assert_defn
{
	return <<'SQL';
CREATE FUNCTION stress_assert(ok boolean, msg text DEFAULT 'invariant violated')
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
	IF NOT ok THEN
		RAISE EXCEPTION 'stress assertion failed: %', msg;
	END IF;
END;
$$;
SQL
}

# Strip the leading tabs a heredoc-style plugin script carries, so the
# generated file is readable, and normalize the trailing newline.
sub _dedent
{
	my ($text) = @_;
	my @lines = split /\n/, $text, -1;
	shift @lines while @lines && $lines[0] !~ /\S/;
	pop @lines while @lines && $lines[-1] !~ /\S/;
	return '' unless @lines;

	my $min;
	for my $ln (@lines)
	{
		next unless $ln =~ /\S/;
		my ($lead) = $ln =~ /^(\t*)/;
		$min = length $lead if !defined $min || length $lead < $min;
	}
	$min //= 0;
	return join("\n", map { my $l = $_; $l =~ s/^\t{0,$min}//; $l } @lines)
	  . "\n";
}

sub _max
{
	my ($a, $b) = @_;
	return $a > $b ? $a : $b;
}

# The DDL client picks one of the rotation's variants per invocation.
# This is the one place a "pick one of N" switch survives: which command
# runs is a choice within one script, not a choice between scripts.
sub _ddl_script
{
	my ($spec, $ctx) = @_;

	my @variants;
	foreach my $name (@{ $spec->{ddl} })
	{
		push @variants, $DDL{$name}->{variants}->($ctx);
	}
	die 'scenario has no DDL variants' unless @variants;

	my $concurrency = $spec->{ddl_concurrency} // 1;

	# A switch over a set of variants, indented to sit under its \if.
	my $switch = sub {
		my ($indent, @vs) = @_;
		my $out = "$indent\\set variant random(0, " . (scalar @vs - 1) . ")\n";
		for my $i (0 .. $#vs)
		{
			$out .= $indent . ($i == 0 ? '\if' : '\elif') . " :variant = $i\n";
			$out .= "$indent\t$_\n" for @{ $vs[$i]->{stmts} };
		}
		$out .= "$indent\\endif\n";
		return $out;
	};

	# No gate at all: every client that picks this script runs a command,
	# so any number of them can be in flight, on any relation.
	return $switch->('', @variants) . "\\sleep 10 ms\n"
	  if $concurrency eq 'none';

	# One gate: one command at a time, anywhere.
	if ($concurrency == 1)
	{
		my $out = "SELECT pg_try_advisory_lock(42)::integer AS gotddl \\gset\n";
		$out .= "\\if :gotddl\n";
		$out .= $switch->("\t", @variants);
		$out .= "\t\\sleep 10 ms\n";
		$out .= "\tSELECT pg_advisory_unlock(42);\n";
		$out .= "\\endif\n";
		return $out;
	}

	# Several gates: one per relation, so that commands overlap but never
	# two of them on the same relation -- which would not be a race worth
	# reporting, just one command dropping an index the other is about to
	# rebuild.
	my %by_table;
	push @{ $by_table{ $_->{table} } }, $_ for @variants;
	my @tables = sort keys %by_table;
	@tables = @tables[ 0 .. $concurrency - 1 ] if @tables > $concurrency;

	my $out = "\\set gate random(1, " . scalar(@tables) . ")\n";
	$out .= "SELECT pg_try_advisory_lock(:gate)::integer AS gotddl \\gset\n";
	$out .= "\\if :gotddl\n";
	for my $i (0 .. $#tables)
	{
		$out .= "\t" . ($i == 0 ? '\if' : '\elif') . " :gate = " . ($i + 1) . "\n";
		$out .= $switch->("\t\t", @{ $by_table{ $tables[$i] } });
	}
	$out .= "\t\\endif\n";
	$out .= "\t\\sleep 10 ms\n";
	$out .= "\tSELECT pg_advisory_unlock(:gate);\n";
	$out .= "\\endif\n";
	return $out;
}

=pod

=item run_scenario($name, $spec)

Run the scenario described by $spec and finish the test.  This is what a
test file under t/ calls; the scenario it describes lives in that file
and nowhere else.

Soak mode needs the list of scenarios without running them, and gets it
by loading each test file with $Stress::Run::COLLECT set to a hashref:
in that mode this function records the scenario and returns instead.

=cut

our $COLLECT;

sub run_scenario
{
	my ($name, $spec) = @_;

	if ($COLLECT)
	{
		$COLLECT->{$name} = $spec;
		return;
	}

	my $scale = PostgreSQL::Test::Utils::stress_concurrently_scale();
	if ($scale == 0)
	{
		plan skip_all => "skipping disabled stress scenario $name";
	}

	run_one($name, $spec);
	done_testing();
	return;
}

=pod

=item run_template($name, $template, %overrides)

Run a scenario a bundle declared as a template, with this file's
overrides on top.  For the scenarios that exist in several variants --
the same combination against a standby, under a crash loop -- so that
the shared part lives once in the bundle and a variant file is the
overrides and nothing else.

=cut

sub run_template
{
	my ($name, $template, %overrides) = @_;

	my $t = $TEMPLATES{$template}
	  or die "unknown scenario template '$template'";

	run_scenario($name, { %$t, %overrides });
	return;
}

=pod

=item run_one($name, $spec)

Run one combination, given directly rather than looked up.  Used by
run_scenario() and by soak mode, which runs many in a single test and
therefore calls done_testing() itself.

=cut

sub run_one
{
	my ($name, $spec) = @_;

	my $scale = PostgreSQL::Test::Utils::stress_concurrently_scale();
	note "running scenario $name at stressval $scale";
	my $seed = stress_seed();

	$spec = Stress::Compose::resolve($spec);
	Stress::Compose::validate($spec);
	# What was composed, with the provenance of every piece that was not
	# written down in the scenario: the log is where "what did this run
	# actually verify" has to be answerable.
	note "effective $name: " . Stress::Compose::describe_effective($spec);

	# stress_seconds=N overrides how long the workload runs, ignoring both
	# the scenario's own figure and the stressval multiplier.  It is for
	# asking a different question: not "is there a race here", which needs
	# time, but "do these pieces fit together at all", which needs one
	# transaction of each kind and is answered in a second.  A whole soak
	# at stress_seconds=1 shakes out the combinations that cannot run --
	# a script the protocol rejects, a check naming a column the schema
	# does not have -- for the price of a couple of long ones.
	my $duration = ($spec->{duration} // 6) * $scale;
	$duration = $1
	  if ($ENV{PG_TEST_EXTRA} // '') =~ /\bstress_seconds=(\d+)\b/;
	my $topo = $TOPOLOGIES{ $spec->{topology} };
	my $disr = $DISRUPTORS{ $spec->{disruptor} };
	my $prof = defined $spec->{profile} ? $PROFILES{ $spec->{profile} } : {};

	# A topology that has to get a second server caught up cannot
	# answer anything in a second: the checks would be reading a
	# subscriber that is still behind, which looks like a broken
	# combination and is not one.  Such a topology names its floor
	# and survey mode respects it.
	foreach my $axis ($topo, $disr, $prof)
	{
		$duration = $axis->{min_seconds}
		  if $axis->{min_seconds} && $duration < $axis->{min_seconds};
	}
	# Already ordered by resolve(): every decorator follows the ones it
	# requires.
	my @schema = map { $SCHEMA{$_} } @{ $spec->{schema} };
	my $loader = $schema[0];
	my $pgbench_scale = $spec->{pgbench_scale} // 1;

	#
	# The cluster the environment asks for.
	#
	$run_counter++;
	my $node_name = $run_counter > 1 ? "${name}_$run_counter" : $name;
	my $node = PostgreSQL::Test::Cluster->new($node_name);
	# The topology decides how the cluster is initialized: a standby
	# or a subscriber needs the primary set up for replication before it
	# can connect at all.
	# A modifier may need something decided at initdb time -- data
	# checksums are written into the cluster, not set by a GUC -- so its
	# init options are merged with the topology's.
	my %init = (%{ $topo->{init} // {} },
		%{ $spec->{modifier} ? ($MODIFIERS{ $spec->{modifier} }->{init} // {})
			: {} });
	$node->init(%init);

	# How long to wait before deciding a statement is never getting its
	# lock.  The default is generous because a concurrent build waits for
	# whatever is already running, and cutting that short would report
	# the feature working as documented as a failure.
	#
	# It has to follow the run length down, though.  A one-second
	# workload with a three-minute lock timeout means one starved
	# combination costs longer than fifty healthy ones, which is what
	# made the first short survey no faster than a real slice.  Keep it
	# comfortably above what a rebuild needs and well below what a
	# survey can afford to lose.
	my $lock_timeout = 1000 * $PostgreSQL::Test::Utils::timeout_default;
	$lock_timeout = 1000 * _max(30, 5 * $duration)
	  if ($ENV{PG_TEST_EXTRA} // '') =~ /\bstress_seconds=\d+\b/;

	# A modifier that makes the server substantially slower has to take
	# this with it.  The timeout is calibrated to how long a rebuild needs
	# at ordinary speed; with fsync really engaged, or every sort spilling,
	# the same work takes long enough that a healthy run trips it and
	# reports a lock timeout that says nothing about the server.  Found by
	# a soak combination doing exactly that under 'durable'.
	$lock_timeout *= 3
	  if $spec->{modifier} && $MODIFIERS{ $spec->{modifier} }->{slow};

	# And the same for a chaos profile that slows the whole server rather
	# than one path.  Forcing a cache flush at every opportunity is the
	# clearest case: it costs about two orders of magnitude, and the
	# timeout is calibrated for a server that is not doing that.
	$lock_timeout *= 3 if _chaos_profile($spec)->{slow};

	$node->append_conf('postgresql.conf', "lock_timeout = $lock_timeout");
	# Layer 0: a failure should arrive with its call site attached.
	$node->append_conf('postgresql.conf', $_)
	  for (
		'log_error_verbosity = verbose',
		q(backtrace_functions = 'relation_open'),
		'log_lock_waits = on');
	# REPACK (CONCURRENTLY) runs a decoding worker of its own, and on a
	# table of any size the index builds ask for parallel workers too.
	# The default pool runs out under that combination, which shows up as
	# "out of background worker slots" and has nothing to do with what is
	# being tested.
	$node->append_conf('postgresql.conf', $_)
	  for ('max_worker_processes = 32', 'max_parallel_workers = 16');
	# Topology first, then the settings profile, then the disruptor, so
	# that a disruptor that needs its own wal_level wins over the
	# topology's -- last occurrence of a setting is the one the server
	# takes.
	$node->append_conf('postgresql.conf', $_)
	  for (@{ $topo->{conf} // [] }, @{ $prof->{conf} // [] },
		@{ $disr->{conf} // [] });
	# A load may need the server configured for it -- two-phase commit
	# has to be enabled before a client can prepare a transaction.
	$node->append_conf('postgresql.conf', $_)
	  for map { @{ $LOAD{$_}->{conf} // [] } } @{ $spec->{load} };
	# Before the scenario's own conf, so a scenario that needs a
	# particular setting still wins.
	# A forced modifier overrides the scenario's own, unless the scenario
	# opts out with no_forced_modifier -- its own flag, because being
	# exempt from a forced chaos profile and from a forced modifier are
	# separate claims, even though the scenarios that need one usually
	# need both.
	my $forced_mod = ($ENV{PG_TEST_EXTRA} // '') =~ /\bstress_modifier=([a-z_]+)\b/
	  ? $1
	  : undef;
	$spec = { %$spec, modifier => $forced_mod }
	  if defined $forced_mod
	  && exists $MODIFIERS{$forced_mod}
	  && !$spec->{no_forced_modifier};

	if (my $mod = $spec->{modifier})
	{
		my $need = $MODIFIERS{$mod}->{requires_build};

		# A modifier can rest on how the server was built.  Where that
		# setting is a postmaster GUC there is no catching it afterwards --
		# an unsupported value stops the server from starting -- so the
		# build is asked instead, and the modifier steps aside if the
		# answer is no.
		if ($need && !PostgreSQL::Test::Utils::check_pg_config($need))
		{
			note "modifier '$mod' skipped: this build has no $need";
		}
		else
		{
			$node->append_conf('postgresql.conf', $_)
			  for @{ $MODIFIERS{$mod}->{conf} // [] };
			note "modifier '$mod': "
			  . scalar(@{ $MODIFIERS{$mod}->{conf} // [] })
			  . ' settings';
		}
	}
	$node->append_conf('postgresql.conf', $_) for @{ $spec->{conf} // [] };
	# Chaos needs its settings in place before the server starts: the
	# module's counters live in shared memory it allocates at startup.
	$node->append_conf('postgresql.conf', $_) for _chaos_conf($spec);
	$node->start;

	#
	# Schema, indexes, helpers.  The first schema entry loads the base
	# tables; the rest decorate it, each bringing its own tables, indexes
	# and the values its scripts need to be told.
	#
	die "unsupported schema loader $loader->{init}"
	  unless ($loader->{init} // '') eq 'pgbench';
	$node->command_ok(
		[
			'pgbench', '-i', '-s', $pgbench_scale, '-q',
			'-p', $node->port, '-h', $node->host, 'postgres'
		],
		"loaded pgbench schema at scale $pgbench_scale");
	$node->safe_psql('postgres', 'CREATE EXTENSION amcheck');
	$node->safe_psql('postgres', stress_assert_defn());

	my $naccounts = 100_000 * $pgbench_scale;
	my %vars = (
		naccounts => $naccounts,
		ntellers => 10 * $pgbench_scale,
		nbranches => $pgbench_scale,
		# For scripts whose tolerance lives inside a server-side
		# function: 1 when every read must add up, 0 when a repack in
		# the rotation may legitimately show a table empty.
		strict_mvcc => $spec->{_mvcc_gap} ? 0 : 1,
	);
	my @tables;
	my @indexes;

	foreach my $s (@schema)
	{
		$node->safe_psql('postgres', $s->{setup}) if $s->{setup};
		push @tables, @{ $s->{tables} // [] };
		push @indexes, @{ $s->{indexes} // [] };

		# A decorator that turns one of the tables into a partitioned one
		# has to take it back out of the rotation: most of the commands
		# refuse a partitioned table, and the partitions it declares are
		# what they should be aimed at instead.
		foreach my $gone (@{ $s->{untables} // [] })
		{
			@tables = grep { $_ ne $gone } @tables;
		}
	}
	push @indexes, map { $INDEXES{$_} } @{ $spec->{indexes} // [] };

	# Both the ones a decorator brings with it and the ones the scenario
	# names: a decorator declares its indexes rather than creating them,
	# so that they are built the same way and land in the context the DDL
	# rotation expands over.
	$node->safe_psql('postgres', "CREATE INDEX $_->{name} $_->{defn}")
	  for @indexes;

	# Anything a load needs beyond the schema, such as a function it
	# calls.
	foreach my $lname (@{ $spec->{load} })
	{
		$node->safe_psql('postgres', $LOAD{$lname}->{setup})
		  if $LOAD{$lname}->{setup};
	}

	# Values the scripts refer to as pgbench variables, contributed by
	# whichever decorators the scenario uses.
	foreach my $s (@schema)
	{
		next unless $s->{context};
		my $c = $s->{context}->($node);
		%vars = (%vars, %$c);
	}

	# Any file a load needs the server or pgbench to read, written next
	# to the scripts.
	my %load_files;
	foreach my $lname (@{ $spec->{load} })
	{
		my $files = $LOAD{$lname}->{files} // {};
		foreach my $fn (sort keys %$files)
		{
			my $path = $node->basedir . '/' . $fn;
			PostgreSQL::Test::Utils::append_to_file($path, $files->{$fn});
			$load_files{$fn} = $path;
		}
	}

	# The values a scenario's scripts refer to are reachable both as
	# pgbench variables and, for the final checks, directly on the
	# context.
	my $ctx = {
		tables => \@tables,
		indexes => \@indexes,
		pgbench_scale => $pgbench_scale,
		files => \%load_files,
		vars => \%vars,
		mvcc_gap_possible => $spec->{_mvcc_gap},
		%vars,
	};

	# What the modifier actually achieved, read back from the running
	# server rather than assumed.  Twice now a setting this suite believed
	# it had applied was not applied at all and nothing failed -- the run
	# simply did less than it said -- so a dimension that cannot be seen
	# working is not worth having.
	if (my $mod = $spec->{modifier})
	{
		my @names =
		  map { /^(\S+)\s*=/ ? $1 : () }
		  @{ $MODIFIERS{$mod}->{conf} // [] };

		foreach my $name (@names)
		{
			my $got = eval { $node->safe_psql('postgres', "SHOW $name") };
			note "modifier '$mod': $name = "
			  . (defined $got ? $got : '(unreadable)');
		}

		# An initdb-time decision leaves no conf line to read back, so
		# report the state it produced instead.
		note "modifier '$mod': data_checksums = "
		  . $node->safe_psql('postgres', 'SHOW data_checksums')
		  if $MODIFIERS{$mod}->{init};
	}

	# Settings a build may not accept.  Applied here rather than in
	# postgresql.conf because an unsupported value there stops the server
	# from starting, while ALTER SYSTEM gives an ordinary error that can be
	# caught and reported.
	if (my $mod = $spec->{modifier})
	{
		foreach my $setting (@{ $MODIFIERS{$mod}->{conf_optional} // [] })
		{
			my ($name, $value) = $setting =~ /^(\S+)\s*=\s*(.+)$/;
			my $ok = eval {
				$node->safe_psql('postgres',
					"ALTER SYSTEM SET $name = $value");
				1;
			};
			note "modifier '$mod': this build will not take $name"
			  unless $ok;
		}
		$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
	}

	# Attached before anything else runs, and before the environment
	# builds its extra nodes, so that every part of the run is subject to
	# it.
	_chaos_setup($node, $spec, $ctx);

	# Anything the topology needs beyond the primary node -- a
	# standby, a subscriber -- is built now, with the schema and its
	# helper functions already in place so a backup carries them.
	$topo->{setup}->($node, $ctx) if $topo->{setup};

	# Whatever slots the environment legitimately created -- a
	# subscription owns one for as long as it exists -- are the baseline
	# the leak check measures against.  Table synchronization slots come
	# and go on their own, so they are no part of it.
	$ctx->{slot_query} =
	  q{SELECT COALESCE(string_agg(slot_name, ',' ORDER BY slot_name), '')}
	  . q{ FROM pg_replication_slots}
	  . q{ WHERE slot_name NOT LIKE 'pg\_%\_sync\_%'};
	$ctx->{baseline_slots} = $node->safe_psql('postgres', $ctx->{slot_query});

	#
	# One script per load, one per check with a script, one for the DDL.
	# pgbench mixes them by weight, which is what makes the composition
	# work without a switch at the top of a single script.
	#
	my %files;
	my %sub_files;
	foreach my $lname (@{ $spec->{load} })
	{
		my $l = $LOAD{$lname};
		my $body =
		  _dedent(ref $l->{script} eq 'CODE' ? $l->{script}->($ctx) : $l->{script});
		# A load may be aimed at the subscriber instead: it then runs
		# against that node, in its own mix, rather than joining the
		# workload the DDL rotation is running against.
		if (($l->{target} // '') eq 'subscriber')
		{
			$sub_files{ "subload_$lname.sql\@" . ($l->{weight} // 1) } = $body;
		}
		else
		{
			$files{ "load_$lname.sql\@" . ($l->{weight} // 1) } = $body;
		}
	}
	foreach my $cname (@{ $spec->{checks} // [] })
	{
		my $c = $CHECK{$cname};
		next unless $c->{script};
		$files{ "check_$cname.sql\@" . ($c->{weight} // 1) } =
		  _dedent(ref $c->{script} eq 'CODE' ? $c->{script}->($ctx) : $c->{script});
	}
	$files{'ddl.sql@1'} = _ddl_script($spec, $ctx);

	# The files themselves land in the node's basedir, which is removed
	# when the test passes, so put them in the log as well: a scenario
	# that cannot be read back is not reproducible by hand.
	foreach my $fn (sort keys %files, sort keys %sub_files)
	{
		note "--- $fn ---\n" . ($files{$fn} // $sub_files{$fn});
	}

	# Write the scripts out and remember two sets of --file options: all
	# of them, and the read-only checks alone, which is what a standby
	# can be given.
	my (@all_opts, @ro_check_opts, @noddl_opts);
	foreach my $fn (sort keys %files)
	{
		(my $bare = $fn) =~ s/\@\d+$//;
		my $path = $node->basedir . '/' . $bare;
		PostgreSQL::Test::Utils::append_to_file($path, $files{$fn});
		my $weight = ($fn =~ /\@(\d+)$/) ? "\@$1" : '';
		push @all_opts, '--file' => "$path$weight";
		push @noddl_opts, '--file' => "$path$weight" unless $bare eq 'ddl.sql';
		next unless $bare =~ /^check_(.*)\.sql$/;
		# A check that takes row locks or otherwise writes cannot run
		# against a standby, however read-only it looks.
		push @ro_check_opts, '--file' => "$path$weight"
		  unless $CHECK{$1}->{writes};
	}

	# Everything the scripts refer to is passed in explicitly rather than
	# derived, so that a script can be read on its own.
	my @defines = map { ('-D', "$_=$vars{$_}") } sort keys %vars;

	# Build a pgbench command against any node with any subset of the
	# scripts; the environments use it to drive more than one node.
	my $pgbench_cmd = sub {
		my (%opt) = @_;
		my $target = $opt{node} // $node;
		return [
			'pgbench', '--no-vacuum',
			'--client=' . ($opt{clients} // $spec->{clients}),
			'--jobs=4', '--exit-on-abort',
			'-T', $opt{duration} // $duration,
			"--random-seed=$seed", @defines,
			@{ $opt{files} // \@all_opts },
			split(/\s+/, $opt{args} // $spec->{pgbench_args} // ''),
			'-p', $target->port, '-h', $target->host, 'postgres'
		];
	};

	# What pgbench is allowed to say.  --random-seed makes it announce
	# the seed, and a rotation that can interrupt a concurrent reindex --
	# a rewriting ALTER TABLE taking the table away mid-build, say --
	# leaves invalid indexes behind, which the next REINDEX reports and
	# skips, and the one it emits for an exclusion constraint index,
	# which REINDEX CONCURRENTLY does not support and says so.  All are
	# expected; anything else, an aborted client or a failed assertion,
	# still fails the test.
	my $stderr_re = qr{
		\A
		(?:pgbench:\ setting\ random\ seed\ to\ \d+\n)?
		(?:
			WARNING:\ \ skipping\ reindex\ of\ invalid\ index\ "[^"]+"\n
			|
			WARNING:\ \ cannot\ reindex\ exclusion\ constraint\ index
				\ "[^"]+"\ concurrently,\ skipping\n
			(?:HINT:\ \ [^\n]*\n)?
		)*
		\z
	}x;

	$ctx->{pgbench_cmd} = $pgbench_cmd;
	$ctx->{ro_check_opts} = \@ro_check_opts;
	# Everything but the DDL, for an environment that issues the commands
	# itself rather than letting a pgbench client do it.
	$ctx->{noddl_opts} = \@noddl_opts;

	# The subscriber-targeted loads, written next to the others.
	my @sub_opts;
	foreach my $fn (sort keys %sub_files)
	{
		(my $bare = $fn) =~ s/\@\d+$//;
		my $path = $node->basedir . '/' . $bare;
		PostgreSQL::Test::Utils::append_to_file($path, $sub_files{$fn});
		my $weight = ($fn =~ /\@(\d+)$/) ? "\@$1" : '';
		push @sub_opts, '--file' => "$path$weight";
	}
	$ctx->{sub_opts} = \@sub_opts;
	$ctx->{ddl_variants} =
	  [ map { $DDL{$_}->{variants}->($ctx) } @{ $spec->{ddl} } ];
	$ctx->{stderr_re} = $stderr_re;
	$ctx->{duration} = $duration;

	# The workload, as a composition: the topology decides what runs
	# where -- one pgbench, or one per node -- and the disruptor decides
	# what happens to the cluster while it does.  A disruptor that
	# drives its own bounded workloads simply does not call the inner
	# runner it was handed.
	my $inner =
	  $topo->{run}
	  ? sub { $topo->{run}->($node, $ctx); return }
	  : sub {
		$node->command_checks_all(
			$pgbench_cmd->(), 0,
			[qr{actually processed}], [$stderr_re],
			"scenario $name");
		return;
	  };
	if ($disr->{run})
	{
		$disr->{run}->($node, $ctx, $inner);
	}
	else
	{
		$inner->();
	}

	#
	# A rotation that drops an index and builds it again is two separate
	# transactions, and pgbench can stop its clients between them, so an
	# index may legitimately be missing now.  Put it back before the
	# checks that want to verify it.
	#
	foreach my $idx (@indexes)
	{
		next
		  if $node->safe_psql('postgres',
			"SELECT to_regclass('$idx->{name}') IS NOT NULL") eq 't';
		note "$idx->{name} was left dropped by the rotation; rebuilding it";
		$node->safe_psql('postgres', "CREATE INDEX $idx->{name} $idx->{defn}");
	}

	# A prepared transaction outlives the workload and keeps every lock it
	# took.  The two-phase load leaves them behind whenever the run stops
	# mid-transaction, and a crash environment brings them back with
	# recovery -- after which the final checks queue behind them: an
	# invalid index cannot be dropped while a prepared transaction holds
	# AccessExclusiveLock on it, and the wait ends in the lock timeout
	# rather than in an answer.
	#
	# The workload is over, so there is nothing left to commit.  Rolling
	# them back is tidying up after it rather than part of what is being
	# tested; a scenario that wanted to assert something about prepared
	# transactions surviving would have to do it before this point.
	my $prepared = stress_rollback_prepared($node);
	note "rolled back $prepared prepared transactions left by the workload"
	  if $prepared;

	#
	# Whatever the workload did, these must hold now.
	#
	foreach my $cname (@{ $spec->{checks} // [] })
	{
		my $c = $CHECK{$cname};
		$c->{final}->($node, $ctx) if $c->{final};
	}
	$topo->{final}->($node, $ctx) if $topo->{final};
	$disr->{final}->($node, $ctx) if $disr->{final};
	_chaos_report($node, $ctx);

	$_->stop for @{ $ctx->{extra_nodes} // [] };
	$node->stop;
	return;
}


=pod

=item chaos

A chaos profile widens the windows a race has to be lost in.  Sleeps are
attached to injection points and left there while the workload runs;
nothing decides differently because of them, so a failure under chaos is
a failure that exists without it.  A build without injection points
ignores the whole thing.

=cut

# A scenario names a profile; soak hands one over ready-made.
sub _chaos_profile
{
	my ($spec) = @_;

	# stress_modifier=<name> does for modifiers what stress_chaos does for
	# chaos profiles: forces one on every scenario, so a single detector
	# can be pointed at the whole catalogue.
	#
	# stress_chaos=<profile> forces one profile on every scenario,
	# whatever it asked for.  That is how a single lever -- the cache
	# discard, say -- gets pointed at the whole catalogue at once, which
	# is how the data checksum crash was found.
	my $forced = ($ENV{PG_TEST_EXTRA} // '') =~ /\bstress_chaos=([a-z_]+)\b/
	  ? $1
	  : undef;
	return $CHAOS{$forced}
	  if defined $forced
	  && exists $CHAOS{$forced}
	  && !$spec->{no_forced_chaos};

	return $spec->{chaos} if ref $spec->{chaos} eq 'HASH';
	return $CHAOS{ $spec->{chaos} // 'off' };
}

# Settings that have to be in postgresql.conf before the server starts.

sub _chaos_conf
{
	my ($spec) = @_;
	my $profile = _chaos_profile($spec);
	my @conf;

	return () unless %$profile;
	return () unless ($ENV{enable_injection_points} // '') eq 'yes';

	# Preloaded rather than loaded on demand, so that the shared counters
	# exist in every backend from the start -- including one whose first
	# jitter point is inside a critical section, where the module cannot
	# attach to shared memory and would otherwise sleep uncounted.
	push @conf, "shared_preload_libraries = 'injection_points'"
	  if $profile->{points};
	return @conf;
}

# Attach the profile's jitter, once the server is up.
sub _chaos_setup
{
	my ($node, $spec, $ctx) = @_;
	my $profile = _chaos_profile($spec);
	my $name =
	  (($ENV{PG_TEST_EXTRA} // '') =~ /\bstress_chaos=([a-z_]+)\b/
		  && exists $CHAOS{$1}
		  && !$spec->{no_forced_chaos}) ? $1
	  : ref $spec->{chaos} ? 'invented'
	  : ($spec->{chaos} // 'off');
	my $seed = 20260729;

	return unless %$profile;

	if (($ENV{enable_injection_points} // '') ne 'yes')
	{
		Test::More::note(
			"chaos '$name' skipped: this build has no injection points");
		return;
	}

	$ctx->{chaos} = $name;

	# The cache discard is set here rather than in postgresql.conf, because
	# a build without the GUC would refuse to start on an unknown name and
	# there is no way to ask a binary about a GUC marked not-in-sample --
	# --describe-config leaves those out, which is how an earlier version of
	# this silently never applied it at all.  ALTER SYSTEM on a name the
	# server does not know is an ordinary error, so it can be tried.
	if ($profile->{discard_probability})
	{
		my $p = $profile->{discard_probability};
		my $set = eval {
			$node->safe_psql('postgres',
				"ALTER SYSTEM SET debug_discard_caches_probability = $p");
			$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
			1;
		};
		if (!$set)
		{
			Test::More::note(
				"chaos '$name': this build has no cache discard probability");
		}
		else
		{
			# Read back, so a run that thinks it is discarding caches
			# really is.
			my $got = $node->safe_psql('postgres',
				'SHOW debug_discard_caches_probability');
			Test::More::note("chaos '$name': cache discard at $got");
		}
	}

	return unless $profile->{points};
	$ctx->{chaos_points} = 1;

	# Same seed as the rest of the run, so a failure can be replayed.
	$seed = $1 if ($ENV{PG_TEST_EXTRA} // '') =~ /\bstress_seed=(\d+)\b/;

	# The build has injection points, but the module may still be missing
	# from the installation under test -- installcheck against a server
	# whose test modules were never installed.  That is a reason to skip
	# the profile with a note, not to fail a run that is testing the
	# server rather than the installation.
	if (!eval {
			$node->safe_psql('postgres', 'CREATE EXTENSION injection_points');
			1;
		})
	{
		Test::More::note("chaos '$name' skipped: "
			  . 'the injection_points extension is not installed');
		delete $ctx->{chaos_points};
		delete $ctx->{chaos};
		return;
	}

	# Same reasoning as the cache discard above: the jitter callback may not
	# be in this build's injection_points module, in which case the
	# profile does nothing rather than ending the run.
	if ($node->safe_psql(
			'postgres', q{SELECT to_regprocedure(
				'injection_points_attach_jitter(text,float8,int4,int4,int8)'
			) IS NULL}) eq 't')
	{
		Test::More::note("chaos '$name' skipped: "
			  . 'this build has no jitter callback');
		delete $ctx->{chaos_points};
		delete $ctx->{chaos};
		return;
	}

	foreach my $point (sort keys %{ $profile->{points} })
	{
		my ($probability, $min_us, $max_us) =
		  @{ $profile->{points}->{$point} };

		$node->safe_psql('postgres',
			"SELECT injection_points_attach_jitter('$point', "
			  . "$probability, $min_us, $max_us, $seed)");
	}
	$ctx->{chaos_attached} = [ sort keys %{ $profile->{points} } ];

	Test::More::note("chaos '$name': "
		  . scalar(keys %{ $profile->{points} })
		  . " points, seed $seed");
	return;
}

# What the jitter actually did.  A run where nothing slept tested nothing
# more than a run without chaos, and the two are otherwise identical from
# the outside, so the numbers are reported rather than assumed.
sub _chaos_report
{
	my ($node, $ctx) = @_;
	my ($count, $us);

	return unless $ctx->{chaos} && $ctx->{chaos_points};

	($count, $us) = split /\|/,
	  $node->safe_psql('postgres',
		'SELECT sleep_count, sleep_us FROM injection_points_stats_jitter()');

	Test::More::note(
		"chaos '$ctx->{chaos}': $count sleeps totalling ${us}us");
	Test::More::diag("chaos '$ctx->{chaos}' never fired")
	  if defined $count && $count eq '0';

	# Per point, where the module can answer.  A slot exists only once a
	# point has slept, so an attached point with no row is one that never
	# fired -- a profile of five points that fired four times zero is
	# testing one point while claiming five, and only this makes that
	# visible.
	if ($node->safe_psql(
			'postgres', q{SELECT to_regprocedure(
				'injection_points_stats_jitter_by_point()') IS NULL}) eq 'f')
	{
		my %slept;
		foreach my $row (
			split /\n/,
			$node->safe_psql(
				'postgres',
				'SELECT point_name, sleep_count, sleep_us '
				  . 'FROM injection_points_stats_jitter_by_point() '
				  . 'ORDER BY point_name'))
		{
			my ($point, $pcount, $pus) = split /\|/, $row;
			next unless defined $pus;
			$slept{$point} = 1;
			Test::More::note(
				"chaos '$ctx->{chaos}': $point: "
				  . "$pcount sleeps totalling ${pus}us");
		}
		foreach my $point (@{ $ctx->{chaos_attached} // [] })
		{
			Test::More::diag(
				"chaos '$ctx->{chaos}': point $point never fired")
			  unless $slept{$point};
		}
	}
	return;
}

=pod

=back

=cut

1;
