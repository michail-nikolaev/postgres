
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
  checks           invariants, checked during and/or after the run
  env              which cluster to run against
  conf             extra postgresql.conf lines for this scenario
  pgbench_args     extra pgbench arguments
  clients          pgbench clients
  duration         seconds at stressval 1 (default 6)
  tags             'ci' for the default run; everything runs in soak mode

The names come from the registries in C<Stress::Plugins>; _validate()
rejects a combination whose pieces do not fit, so a typo fails the test
rather than quietly running something smaller than intended.

=cut

package Stress::Run;

use strict;
use warnings FATAL => 'all';

use Exporter 'import';
use Test::More;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Stress::Plugins qw(%SCHEMA %INDEXES %LOAD %DDL %CHECK %ENVS);

our @EXPORT =
  qw(run_scenario run_one stress_seed stress_assert_defn @STANDARD_DDL);

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

# Check that every entry a scenario names exists, and that the
# requires/conflicts they declare are satisfied, before anything is
# created.
=pod

=item spec_is_runnable($spec)

Whether the pieces of $spec fit together, without building anything.
Soak mode uses this to tell a combination worth running from one that
could never work, so that the two are not confused in the numbering.

=cut

# Schema decorators in an order where each one's requirements have
# already been applied.
#
# Validation only asks whether a decorator a scenario depends on is
# present, not whether it comes first, and applying them in the order
# they happen to be listed is fine until something lists them the other
# way round: partitioned_2_levels detaches a partition that partitioned
# is the one to create, and gets "ALTER action DETACH PARTITION cannot
# be performed" if it runs first.  The hand-written scenarios all happen
# to be in a workable order, which is why this went unnoticed until soak
# invented one that was not.
sub _order_schema
{
	my ($names) = @_;

	my (@ordered, %placed);
	my $place;
	$place = sub {
		my ($name, $seen) = @_;

		return if $placed{$name};
		die "schema '$name' requires itself" if $seen->{$name};
		$seen->{$name} = 1;

		foreach my $dep (@{ $SCHEMA{$name}->{requires}->{schema} // [] })
		{
			$place->($dep, $seen) if grep { $_ eq $dep } @$names;
		}

		delete $seen->{$name};
		$placed{$name} = 1;
		push @ordered, $name;
	};

	# Walked in the order given, so the loader -- which every decorator
	# needs and none declares -- keeps its place at the front.
	$place->($_, {}) for @$names;
	return @ordered;
}

sub _max
{
	my ($a, $b) = @_;
	return $a > $b ? $a : $b;
}

sub spec_is_runnable
{
	my ($spec) = @_;

	return eval { _validate($spec); 1 } ? 1 : 0;
}

sub _validate
{
	my ($spec) = @_;

	my %registry = (
		schema => \%SCHEMA,
		indexes => \%INDEXES,
		load => \%LOAD,
		ddl => \%DDL,
		checks => \%CHECK,
	);

	foreach my $kind (sort keys %registry)
	{
		foreach my $name (@{ $spec->{$kind} // [] })
		{
			die "scenario names unknown $kind plugin '$name'"
			  unless exists $registry{$kind}->{$name};
		}
	}
	die "scenario names unknown env '$spec->{env}'"
	  unless exists $ENVS{ $spec->{env} };

	# requires/conflicts, declared as { kind => [ names ] }
	foreach my $kind (sort keys %registry)
	{
		foreach my $name (@{ $spec->{$kind} // [] })
		{
			my $defn = $registry{$kind}->{$name};

			foreach my $rkind (sort keys %{ $defn->{requires} // {} })
			{
				# 'env' is a single value rather than a list, and a
				# requirement on it means "any one of these".
				if ($rkind eq 'env')
				{
					my @want = @{ $defn->{requires}->{env} };
					die "$kind '$name' requires env "
					  . join(' or ', map { "'$_'" } @want)
					  unless grep { $_ eq ($spec->{env} // '') } @want;
					next;
				}

				foreach my $rname (@{ $defn->{requires}->{$rkind} })
				{
					die "$kind '$name' requires $rkind '$rname', "
					  . 'which the scenario does not use'
					  unless grep { $_ eq $rname } @{ $spec->{$rkind} // [] };
				}
			}
			# A script that puts a variable inside a quoted literal only
			# works when pgbench substitutes textually.
			die "$kind '$name' cannot be combined with "
			  . "pgbench_args '$spec->{pgbench_args}'"
			  if $defn->{simple_protocol_only}
			  && ($spec->{pgbench_args} // '') =~ /--protocol=(?:extended|prepared)/;

			# A variant that destroys relations other variants are gated
			# on can only run when nothing else is in flight.
			die "$kind '$name' cannot be combined with "
			  . "ddl_concurrency '"
			  . ($spec->{ddl_concurrency} // 1) . "'"
			  if $defn->{solo} && ($spec->{ddl_concurrency} // 1) ne '1';

			foreach my $ckind (sort keys %{ $defn->{conflicts} // {} })
			{
				foreach my $cname (@{ $defn->{conflicts}->{$ckind} })
				{
					# 'env' is a single value rather than a list.
					die "$kind '$name' cannot be combined with "
					  . "$ckind '$cname'"
					  if $ckind eq 'env'
					  ? $spec->{env} eq $cname
					  : grep { $_ eq $cname } @{ $spec->{$ckind} // [] };
				}
			}
		}
	}
	return;
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

	_validate($spec);

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
	my $env = $ENVS{ $spec->{env} };
	my @schema = map { $SCHEMA{$_} } _order_schema($spec->{schema});
	my $loader = $schema[0];
	my $pgbench_scale = $spec->{pgbench_scale} // 1;

	#
	# The cluster the environment asks for.
	#
	$run_counter++;
	my $node_name = $run_counter > 1 ? "${name}_$run_counter" : $name;
	my $node = PostgreSQL::Test::Cluster->new($node_name);
	# The environment decides how the cluster is initialized: a standby
	# or a subscriber needs the primary set up for replication before it
	# can connect at all.
	$node->init(%{ $env->{init} // {} });

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
	$node->append_conf('postgresql.conf', $_) for @{ $env->{conf} // [] };
	# A load may need the server configured for it -- two-phase commit
	# has to be enabled before a client can prepare a transaction.
	$node->append_conf('postgresql.conf', $_)
	  for map { @{ $LOAD{$_}->{conf} // [] } } @{ $spec->{load} };
	$node->append_conf('postgresql.conf', $_) for @{ $spec->{conf} // [] };
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
		%vars,
	};

	# Anything the environment needs beyond the primary node -- a
	# standby, a subscriber -- is built now, with the schema and its
	# helper functions already in place so a backup carries them.
	$env->{setup}->($node, $ctx) if $env->{setup};

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
	my (@all_opts, @check_opts, @ro_check_opts, @noddl_opts);
	foreach my $fn (sort keys %files)
	{
		(my $bare = $fn) =~ s/\@\d+$//;
		my $path = $node->basedir . '/' . $bare;
		PostgreSQL::Test::Utils::append_to_file($path, $files{$fn});
		my $weight = ($fn =~ /\@(\d+)$/) ? "\@$1" : '';
		push @all_opts, '--file' => "$path$weight";
		push @noddl_opts, '--file' => "$path$weight" unless $bare eq 'ddl.sql';
		next unless $bare =~ /^check_(.*)\.sql$/;
		push @check_opts, '--file' => "$path$weight";
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
	# skips.  Both are expected; anything else, an aborted client or a
	# failed assertion, still fails the test.
	my $stderr_re = qr{
		\A
		(?:pgbench:\ setting\ random\ seed\ to\ \d+\n)?
		(?:
			WARNING:\ \ skipping\ reindex\ of\ invalid\ index\ "[^"]+"\n
			(?:HINT:\ \ [^\n]*\n)?
		)*
		\z
	}x;

	$ctx->{pgbench_cmd} = $pgbench_cmd;
	$ctx->{all_opts} = \@all_opts;
	$ctx->{check_opts} = \@check_opts;
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
	$ctx->{spec} = $spec;

	if ($env->{run})
	{
		# The environment drives the run itself: more than one node, or
		# something happening to the cluster while the workload runs.
		$env->{run}->($node, $ctx);
	}
	else
	{
		$node->command_checks_all(
			$pgbench_cmd->(), 0,
			[qr{actually processed}], [$stderr_re],
			"scenario $name");
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

	#
	# Whatever the workload did, these must hold now.
	#
	foreach my $cname (@{ $spec->{checks} // [] })
	{
		my $c = $CHECK{$cname};
		$c->{final}->($node, $ctx) if $c->{final};
	}
	$env->{final}->($node, $ctx) if $env->{final};

	$_->stop for @{ $ctx->{extra_nodes} // [] };
	$node->stop;
	return;
}

=pod

=back

=cut

1;
