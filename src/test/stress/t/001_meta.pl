
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
	foreach my $file (sort glob("$FindBin::RealBin/[0-9]*.pl"))
	{
		next if $file eq abs_path(__FILE__);
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
	is($r->{env}, 'standalone', 'the environment defaults to standalone');
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

done_testing();
