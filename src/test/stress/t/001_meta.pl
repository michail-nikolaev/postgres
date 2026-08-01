
# Copyright (c) 2026, PostgreSQL Global Development Group

# The framework checking itself, with no cluster involved.
#
# Everything here would otherwise be discovered by whichever scenario or
# soak combination happened to trip over it, hours in: a requirement
# naming an entry nobody declares, a scenario that no longer resolves, a
# composition rule that quietly stopped doing what its callers assume.
# This test makes those failures arrive first, cheaply, with a message
# naming the declaration site.
use strict;
use warnings FATAL => 'all';

use Cwd qw(abs_path);
use FindBin;
use lib $FindBin::RealBin;
use Test::More;

# Loading Stress::Run runs Stress::Registry::load_all(), which dies if
# any registry cross-reference does not resolve.
use Stress::Run;
use Stress::Compose;
use Stress::Registry ':registries';

ok(1, 'the registries load and their cross-references resolve');

#
# Every scenario in the catalogue must resolve and validate.  A scenario
# that does not is a fault in the suite, and finding it here beats
# finding it in whichever CI run reaches that file first.
#
my %catalogue;
{
	local $Stress::Run::COLLECT = \%catalogue;
	foreach my $file (sort glob("$FindBin::RealBin/[1-9][0-9][0-9]_*.pl"))
	{
		next if $file =~ /999_soak/;
		my $abs = abs_path($file);
		do $abs;
		die "collecting scenarios from $file: $@" if $@;
	}
}
cmp_ok(scalar keys %catalogue, '>=', 30, 'the catalogue is populated');

foreach my $name (sort keys %catalogue)
{
	my $ok = eval {
		Stress::Compose::validate(Stress::Compose::resolve($catalogue{$name}));
		1;
	};
	ok($ok, "scenario $name resolves and validates") or diag($@);
}

#
# The composition rules themselves, on specimens small enough to reason
# about.
#
{
	my $r = Stress::Compose::resolve(
		{ load => ['tpcb_like'], ddl => ['repack_concurrently'] });

	ok((grep { $_ eq 'balances' } @{ $r->{checks} }),
		'a load brings its declared check along');
	ok((grep { $_ eq 'amcheck' } @{ $r->{checks} }),
		'the auto checks join a scenario that satisfies them');
	is($r->{schema}->[0], 'pgbench', 'the loader is ensured, first');
	is($r->{topology}, 'standalone', 'the topology defaults to standalone');
	is($r->{disruptor}, 'none', 'the disruptor defaults to none');
}

{
	my $r = Stress::Compose::resolve(
		{ load => ['balanced_pair'], ddl => ['repack_concurrently'] });

	ok((grep { $_ eq 'ledger' } @{ $r->{schema} }),
		'a load pulls the schema it requires');
	ok((grep { $_ eq 'ledger_sum' } @{ $r->{checks} }),
		'and brings the check that holds it to its invariant');
}

{
	# hot_churn moves one sum without the other three and says so; the
	# balances check tpcb_like brings must be dropped, not failed.
	my $r = Stress::Compose::resolve(
		{
			load => [ 'tpcb_like', 'hot_churn' ],
			ddl => ['repack_concurrently']
		});

	ok(!(grep { $_ eq 'balances' } @{ $r->{checks} }),
		'an implicit check a load conflicts with is dropped');
	ok((grep { $_ eq 'balances' } @{ $r->{_checks_dropped} }),
		'and the drop is recorded for the log');
}

{
	my $r = Stress::Compose::resolve(
		{
			load => ['tpcb_like'],
			ddl => ['repack_concurrently'],
			no_checks => ['visibility_map']
		});

	ok(!(grep { $_ eq 'visibility_map' } @{ $r->{checks} }),
		'no_checks takes a named check back out');

	my $died = !eval {
		Stress::Compose::resolve(
			{
				load => ['tpcb_like'],
				ddl => ['repack_concurrently'],
				no_checks => ['no_such_check']
			});
		1;
	};
	ok($died, 'excluding a check that is not in the set fails loudly');
}

{
	my $r = Stress::Compose::resolve(
		{
			schema => [ 'partitioned_2_levels', 'partitioned' ],
			load => ['tpcb_like'],
			ddl => ['vacuum']
		});
	my ($first) =
	  grep { $r->{schema}->[$_] eq 'partitioned' } 0 .. $#{ $r->{schema} };
	my ($second) =
	  grep { $r->{schema}->[$_] eq 'partitioned_2_levels' }
	  0 .. $#{ $r->{schema} };
	cmp_ok($first, '<', $second,
		'decorators are ordered after what they require');
}

{
	my $died = !eval {
		Stress::Compose::validate(
			Stress::Compose::resolve(
				{ load => ['tpcb_like'], ddl => ['vacuum'], check => [] }));
		1;
	};
	ok($died, 'an unknown scenario field fails validation');
}

{
	# The randomized settings: a knob must exist, hold one of its
	# declared values, and not fight the modifier over the same GUC.
	my $ok = eval {
		Stress::Compose::validate(
			Stress::Compose::resolve(
				{
					load => ['tpcb_like'],
					ddl => ['vacuum'],
					settings => { deadlock_timeout => '50ms' }
				}));
		1;
	};
	ok($ok, 'a declared setting at a declared value validates') or diag($@);

	my $died = !eval {
		Stress::Compose::validate(
			Stress::Compose::resolve(
				{
					load => ['tpcb_like'],
					ddl => ['vacuum'],
					settings => { deadlock_timeout => '77ms' }
				}));
		1;
	};
	ok($died, 'an undeclared value fails validation');

	$died = !eval {
		Stress::Compose::validate(
			Stress::Compose::resolve(
				{
					load => ['tpcb_like'],
					ddl => ['vacuum'],
					modifier => 'spill',
					settings => { temp_buffers => '64MB' }
				}));
		1;
	};
	ok($died, 'a knob the modifier already sets cannot be drawn over it');
}

{
	# The axes carry their own constraints: a disruptor whose victim
	# picker or restart loop is written for one node refuses the
	# topologies it has not been built for.
	my $died = !eval {
		Stress::Compose::validate(
			Stress::Compose::resolve(
				{
					load => ['tpcb_like'],
					ddl => ['vacuum'],
					topology => 'standby',
					disruptor => 'crash_loop'
				}));
		1;
	};
	ok($died, 'an axis conflict fails validation');
	ok( !Stress::Compose::fits(
			'disruptor', 'crash_loop', { topology => 'standby' }),
		'and the prefilter refuses it too');
}

#
# The log scanner, on synthetic logs, so what fails and what does not
# is pinned here rather than discovered across platforms.
#
{
	require Stress::LogScan;
	my $prefix = '2026-08-01 12:00:00.000 CEST [123]';

	my @bad = Stress::LogScan::scan_text(
		"$prefix XX000 PANIC:  stuck spinlock detected\n", []);
	is(scalar @bad, 1, 'a PANIC is a finding');

	@bad = Stress::LogScan::scan_text(
		'TRAP: failed Assert("HaveRegisteredOrActiveSnapshot()"), '
		  . "File: \"heapam.c\"\n",
		[]);
	is(scalar @bad, 1, 'an assertion trap is a finding');

	@bad = Stress::LogScan::scan_text(
		"$prefix XX001 ERROR:  could not read block 0\n", []);
	is(scalar @bad, 1, 'a corruption-class ERROR is a finding');

	@bad = Stress::LogScan::scan_text(
		"$prefix 57014 ERROR:  canceling statement due to lock timeout\n",
		[]);
	is(scalar @bad, 0, 'an ordinary ERROR is not');

	@bad = Stress::LogScan::scan_text(
		"$prefix 57P01 FATAL:  terminating connection due to "
		  . "administrator command\n",
		[qr/terminating connection due to administrator command/]);
	is(scalar @bad, 0, 'an allowlisted FATAL is not');

	@bad = Stress::LogScan::scan_text(
		"$prefix 57P01 FATAL:  some new fatal nobody allowed\n",
		[qr/terminating connection due to administrator command/]);
	is(scalar @bad, 1, 'an unallowed FATAL is a finding');

	@bad = Stress::LogScan::scan_text(
		"$prefix XX000 PANIC:  it broke\n", [qr/it broke/]);
	is(scalar @bad, 1, 'a PANIC cannot be allowlisted at all');
}

#
# The chaos catalogue against the build's own defined points.  This is
# the one section that needs a server: the list of defined points is
# compiled into the injection_points module, and asking it is the only
# way to catch a curated name the tree has renamed out from under us --
# attaching to a stale name is silent by design.
#
SKIP:
{
	skip 'this build has no injection points', 1
	  unless ($ENV{enable_injection_points} // '') eq 'yes';

	require Stress::Chaos;

	my $node = PostgreSQL::Test::Cluster->new('meta_points');
	$node->init;
	$node->start;
	my $defined = Stress::Chaos::chaos_fetch_defined($node);
	$node->stop;

	skip 'the installed injection_points module cannot list defined points',
	  1
	  unless $defined;

	cmp_ok(scalar keys %$defined, '<=', 128,
		'the defined points fit the shmem slot table');
	is(scalar(grep { length($_) >= 64 } keys %$defined),
		0, 'every defined point name fits INJ_NAME_MAXLEN');

	# Stale names rot silently everywhere else; here they fail.
	foreach my $point (sort keys %CHAOS_POINTS)
	{
		ok(exists $defined->{$point},
			"capped point '$point' still exists in the tree");
	}
	foreach my $point (sort keys %CHAOS_EXCLUDED)
	{
		ok(exists $defined->{$point},
			"excluded point '$point' still exists in the tree");
	}

	# Jitter only ever delays; on an attached-kind site the attachment
	# itself decides something, so neither the cap table nor a profile
	# may name one.
	my @attached =
	  sort grep { $defined->{$_}{kinds}{attached} } keys %$defined;
	foreach my $point (@attached)
	{
		ok(!exists $CHAOS_POINTS{$point},
			"attached-kind point '$point' is not capped for jitter");
	}
	foreach my $pname (sort keys %CHAOS)
	{
		foreach my $point (sort keys %{ $CHAOS{$pname}->{points} // {} })
		{
			ok(exists $defined->{$point},
				"profile '$pname' point '$point' still exists in the tree");
		}
	}

	# What the pool amounts to on this build, for the log: the uncurated
	# names are covered at the default caps, and the note is how a new
	# point's arrival shows up before any soak hunts it.
	my $pool = Stress::Chaos::chaos_pool();
	my @uncurated = sort grep { !$pool->{$_}{curated} } keys %$pool;
	note 'chaos pool: '
	  . scalar(keys %$pool)
	  . ' points, '
	  . scalar(@uncurated)
	  . ' of them uncurated, at default caps: '
	  . join(', ', @uncurated);
	cmp_ok(scalar keys %$pool, '>', scalar keys %CHAOS_POINTS,
		'the pool is wider than the curated table');
}

done_testing();
