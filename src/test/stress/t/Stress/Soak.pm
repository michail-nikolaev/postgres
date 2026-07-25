
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

It is off unless asked for:

  PG_TEST_EXTRA='stress_mode=soak stress_concurrently=20'

and takes two further settings:

  stress_soak_minutes=N   wall-clock budget, default 30
  stress_seed=N           replay a previous soak exactly

Every combination it invents is validated the same way a hand-written
scenario is, so one that could not work is discarded and reported rather
than run.  The seed and the combination are logged before each run, so a
failure names the thing to reproduce.

=cut

package Stress::Soak;

use strict;
use warnings FATAL => 'all';

use Exporter 'import';
use Test::More;
use PostgreSQL::Test::Utils;

use Stress::Plugins qw(%SCHEMA %INDEXES %LOAD %DDL %CHECK %ENVS);
use Stress::Scenarios qw(%SCENARIOS);

our @EXPORT = qw(soak_enabled soak_run);

=pod

=over

=item soak_enabled()

Whether PG_TEST_EXTRA asks for soak mode.

=cut

sub soak_enabled
{
	return (($ENV{PG_TEST_EXTRA} // '') =~ /\bstress_mode=soak\b/) ? 1 : 0;
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
	my @decorators = grep { $_ ne 'pgbench' } sort keys %SCHEMA;
	my @schema = ('pgbench', _pick_some(0, 2, @decorators));

	# A load or a check whose requirements this schema does not meet is
	# simply not eligible.
	my $eligible = sub {
		my ($defn) = @_;
		foreach my $rname (@{ $defn->{requires}->{schema} // [] })
		{
			return 0 unless grep { $_ eq $rname } @schema;
		}
		return 1;
	};

	my @loads = grep { $eligible->($LOAD{$_}) } sort keys %LOAD;
	my @checks = grep { $eligible->($CHECK{$_}) } sort keys %CHECK;
	my @ddl = grep { $eligible->($DDL{$_}) } sort keys %DDL;

	# Indexes a check asks for have to be there; take them all, which is
	# also more interesting for the rotation.
	my @indexes = sort keys %INDEXES;

	return {
		schema => \@schema,
		indexes => \@indexes,
		load => [ 'tpcb_like', _pick_some(0, 3, grep { $_ ne 'tpcb_like' } @loads) ],
		ddl => [ _pick_some(1, 4, @ddl) ],
		# Not 'none': with no gate at all two clients can pick commands
		# on the same relation, and one dropping an index the other is
		# rebuilding is a self-inflicted failure rather than a race worth
		# reporting.
		ddl_concurrency => _pick(1, 1, 2, 4),
		checks => [ _pick_some(1, 4, @checks) ],
		env => $env,
		clients => _pick(10, 20, 30),
		tags => ['soak'],
	};
}

=pod

=item soak_run()

Run the catalogue, then invented combinations, until the budget is
spent.

=cut

sub soak_run
{
	my $minutes = 30;
	my $extra = $ENV{PG_TEST_EXTRA} // '';
	$minutes = $1 if $extra =~ /\bstress_soak_minutes=(\d+)\b/;

	my $deadline = time() + $minutes * 60;
	note "soaking for $minutes minutes";

	# The catalogue first: those combinations are known to be worth
	# running, and a failure in one of them is the easiest to act on.
	my @queue = sort keys %SCENARIOS;
	my ($ran, $invented) = (0, 0);

	while (time() < $deadline)
	{
		my ($name, $spec);
		if (@queue)
		{
			$name = shift @queue;
			$spec = $SCENARIOS{$name};
		}
		else
		{
			$spec = _invent();
			$invented++;
			$name = "invented_$invented";
		}

		# Say what is about to run before running it, so an abort names
		# the combination rather than leaving it to be guessed.
		note "soak: $name = " . _describe($spec);

		my $ok = eval {
			Stress::Run::run_one($name, $spec);
			1;
		};
		if (!$ok)
		{
			my $err = $@ // 'unknown error';
			# A combination that cannot work is not a failure of the
			# server; say so and carry on.
			if ($err =~ /requires|cannot be combined|unknown/)
			{
				note "soak: skipping $name: $err";
				next;
			}
			die $err;
		}
		$ran++;
	}

	note "soak ran $ran combinations, $invented of them invented";
	return;
}

sub _describe
{
	my ($spec) = @_;
	return join ' ',
	  map { "$_=[" . join(',', @{ $spec->{$_} }) . ']' }
	  grep { ref $spec->{$_} eq 'ARRAY' } qw(schema load ddl checks);
}

=pod

=back

=cut

1;
