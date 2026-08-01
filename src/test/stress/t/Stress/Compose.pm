
# Copyright (c) 2026, PostgreSQL Global Development Group

=pod

=head1 NAME

Stress::Compose - resolve and validate a scenario against the registries

=head1 DESCRIPTION

The one place the rules of combination live.  C<resolve()> turns the
scenario a test file wrote into the one that runs; C<validate()> rejects
a combination whose pieces do not fit, with a message naming the piece;
C<fits()> is the cheap per-entry form soak uses to avoid inventing
candidates validation would only throw away.  Run and Soak both go
through here, so there is no second engine to drift from this one.

What resolve() adds to a written scenario:

=over

=item The pgbench loader, first, always.

=item Schemas pulled by requirement.  A load, a command, an index or a
check that requires a decorator brings it along, transitively, so a
scenario says what it drives and not what that needs.  The result is
ordered so that every decorator follows the ones it requires.

=item The checks the workload implies.  A load or a DDL entry declares
the checks that hold it to its invariant, and they join the scenario
with it: a load whose check was forgotten is how a run passes while
verifying nothing.  Checks flagged C<auto> join any scenario that
satisfies their requirements -- satisfies already, they pull nothing --
and C<no_checks> takes named ones back out.  A check that arrived
implicitly and conflicts with something present is dropped with a note;
one the scenario asked for by name still fails validation, since that
disagreement is the scenario's to resolve.

=back

The composed list is reported by Run with the provenance of every
check, so what a scenario effectively tests can be read from its log
rather than reconstructed from the registries.

=cut

package Stress::Compose;

use strict;
use warnings FATAL => 'all';

use Exporter 'import';
use Stress::Chaos ();
use Stress::Registry qw(:registries);

our @EXPORT_OK = qw(resolve validate fits spec_is_runnable describe_effective);

# The scenario fields Run understands.  An unknown key is a typo, and a
# typo'd key is worse than a typo'd name: 'check' for 'checks' does not
# fail anything, it silently runs a scenario with no checks at all.
# Keys starting with an underscore are resolve()'s own bookkeeping.
my %known_field = map { $_ => 1 }
  qw(schema pgbench_scale indexes load ddl ddl_concurrency checks
  no_checks topology disruptor profile conf chaos modifier settings
  no_forced_chaos no_forced_modifier pgbench_args clients duration
  tags);

my %registry = (
	schema => \%SCHEMA,
	indexes => \%INDEXES,
	load => \%LOAD,
	ddl => \%DDL,
	checks => \%CHECK,
);

# The single-valued axes: a scenario has one of each, and a requirement
# on one means "any of these".
my %scalar_axis = (
	topology => \%TOPOLOGIES,
	disruptor => \%DISRUPTORS,
	profile => \%PROFILES,
);

# Whether $defn's requires are satisfied by the (partial) spec, without
# pulling anything in.
sub _requires_satisfied
{
	my ($defn, $spec) = @_;

	foreach my $rkind (sort keys %{ $defn->{requires} // {} })
	{
		if ($scalar_axis{$rkind})
		{
			return 0
			  unless grep { $_ eq ($spec->{$rkind} // '') }
			  @{ $defn->{requires}->{$rkind} };
			next;
		}
		foreach my $rname (@{ $defn->{requires}->{$rkind} })
		{
			return 0
			  unless grep { $_ eq $rname } @{ $spec->{$rkind} // [] };
		}
	}
	return 1;
}

# Whether any conflict fires between $defn (named $name, of $kind) and
# the (partial) spec -- in either direction, since a conflict may be
# declared on whichever side found it first.
sub _conflicts_fire
{
	my ($kind, $name, $defn, $spec) = @_;

	foreach my $ckind (sort keys %{ $defn->{conflicts} // {} })
	{
		foreach my $cname (@{ $defn->{conflicts}->{$ckind} })
		{
			return 1
			  if $scalar_axis{$ckind}
			  ? ($spec->{$ckind} // '') eq $cname
			  : grep { $_ eq $cname } @{ $spec->{$ckind} // [] };
		}
	}

	# The other direction: anything present that conflicts with $name --
	# including the single-valued axes, whose entries declare conflicts
	# of their own (crash_loop against the standby topology, say).
	foreach my $okind (sort keys %registry)
	{
		foreach my $oname (@{ $spec->{$okind} // [] })
		{
			my $odefn = $registry{$okind}->{$oname} or next;
			return 1
			  if grep { $_ eq $name }
			  @{ $odefn->{conflicts}->{$kind} // [] };
		}
	}
	foreach my $axis (sort keys %scalar_axis)
	{
		my $aname = $spec->{$axis};
		next unless defined $aname;
		my $adefn = $scalar_axis{$axis}->{$aname} or next;
		return 1
		  if grep { $_ eq $name } @{ $adefn->{conflicts}->{$kind} // [] };
	}
	return 0;
}

=pod

=over

=item resolve($spec)

Return the effective scenario: loader ensured, required schemas pulled
and ordered, implicit checks joined, exclusions applied.  Does not die
on unknown names -- validate() owns the error messages -- but skips
them, so the two can be called in either order.

=cut

sub resolve
{
	my ($spec) = @_;
	my %eff = %$spec;

	$eff{topology} //= 'standalone';
	$eff{disruptor} //= 'none';

	#
	# Checks: what the scenario wrote, what its loads and commands
	# declare, and the auto set.  Insertion order is kept, so the
	# composed list is stable for a given spec.
	#
	my (@checks, %have, %prov);
	my $add = sub {
		my ($name, $why) = @_;
		return if $have{$name}++;
		push @checks, $name;
		$prov{$name} = $why;
	};

	$add->($_, 'spec') for @{ $eff{checks} // [] };
	foreach my $lname (@{ $eff{load} // [] })
	{
		next unless exists $LOAD{$lname};
		$add->($_, "load:$lname") for @{ $LOAD{$lname}->{checks} // [] };
	}
	foreach my $dname (@{ $eff{ddl} // [] })
	{
		next unless exists $DDL{$dname};
		$add->($_, "ddl:$dname") for @{ $DDL{$dname}->{checks} // [] };
	}

	#
	# Schemas: the loader, whatever the scenario listed, and everything
	# the loads, commands, indexes and checks so far require,
	# transitively.
	#
	my @schema = grep { $_ ne 'pgbench' } @{ $eff{schema} // [] };
	unshift @schema, 'pgbench';
	my %sch = map { $_ => 1 } @schema;
	my @queue;
	foreach my $kind (qw(load ddl indexes))
	{
		foreach my $name (@{ $eff{$kind} // [] })
		{
			my $defn = $registry{$kind}->{$name} or next;
			push @queue, @{ $defn->{requires}->{schema} // [] };
		}
	}
	push @queue, map { @{ $CHECK{$_}->{requires}->{schema} // [] } }
	  grep { exists $CHECK{$_} } @checks;
	push @queue, map { @{ $SCHEMA{$_}->{requires}->{schema} // [] } }
	  grep { exists $SCHEMA{$_} } @schema;
	while (@queue)
	{
		my $s = shift @queue;
		next unless exists $SCHEMA{$s};
		next if $sch{$s}++;
		push @schema, $s;
		push @queue, @{ $SCHEMA{$s}->{requires}->{schema} // [] };
	}
	@schema = _order_schema(\@schema);
	my %pulled = map { $_ => 1 } @schema;
	delete $pulled{$_} for ('pgbench', @{ $eff{schema} // [] });
	$eff{schema} = \@schema;

	#
	# The auto checks, now that the schema list is final.  An auto check
	# joins only where its requirements are already satisfied: it pulls
	# nothing, so index_vs_seq joins when btree_abalance is there and
	# stays away when it is not.
	#
	my %partial = (%eff, checks => [@checks]);
	foreach my $cname (sort keys %CHECK)
	{
		next unless $CHECK{$cname}->{auto};
		next if $have{$cname};
		next unless _requires_satisfied($CHECK{$cname}, \%partial);
		$add->($cname, 'auto');
	}

	#
	# A check that arrived implicitly and conflicts with something here
	# is dropped, and the drop is recorded for the effective-composition
	# note.  One the scenario named stays: validate() will die on it,
	# because that contradiction is the scenario's own.
	#
	my @dropped;
	@checks = grep {
		my $c = $_;
		my $fires = exists $CHECK{$c}
		  && $prov{$c} ne 'spec'
		  && _conflicts_fire('checks', $c, $CHECK{$c}, \%eff);
		push @dropped, $c if $fires;
		!$fires;
	} @checks;

	#
	# And the exclusions.  Validated here rather than in validate(), so
	# that a typo'd exclusion -- which would otherwise silently exclude
	# nothing -- fails loudly.
	#
	foreach my $no (@{ $eff{no_checks} // [] })
	{
		die "scenario excludes check '$no', which is not in its "
		  . 'effective set: ['
		  . join(',', @checks) . ']'
		  unless grep { $_ eq $no } @checks;
		@checks = grep { $_ ne $no } @checks;
	}

	$eff{checks} = \@checks;
	$eff{_check_prov} = \%prov;
	$eff{_checks_dropped} = \@dropped;
	$eff{_schema_pulled} = [ sort keys %pulled ];

	# Whether a snapshot in this scenario can legitimately find a table
	# empty: only when the rotation carries a command that is declared
	# not MVCC-safe, and stress_strict_mvcc=1 does not override.  The
	# checks pick their tolerant or strict form off this one flag.
	my $gap = 0;
	foreach my $dname (@{ $eff{ddl} // [] })
	{
		my $d = $DDL{$dname} or next;
		$gap = 1 if defined $d->{mvcc_safe} && !$d->{mvcc_safe};
	}
	# An axis may run commands of its own that the rotation knows
	# nothing about -- the subscription topology repacks the
	# subscriber's copy of the table between workloads -- so it declares
	# mvcc_safe for itself too.
	foreach my $axis (sort keys %scalar_axis)
	{
		my $aname = $eff{$axis} // next;
		my $adefn = $scalar_axis{$axis}->{$aname} or next;
		$gap = 1 if defined $adefn->{mvcc_safe} && !$adefn->{mvcc_safe};
	}
	$gap = 0
	  if ($ENV{PG_TEST_EXTRA} // '') =~ /\bstress_strict_mvcc=1\b/;
	$eff{_mvcc_gap} = $gap;
	return \%eff;
}

# Schema decorators in an order where each one's requirements have
# already been applied.
#
# Applying them in the order they happen to be listed is fine until
# something lists them the other way round: partitioned_2_levels
# detaches a partition that partitioned is the one to create, and gets
# "ALTER action DETACH PARTITION cannot be performed" if it runs first.
# The hand-written scenarios all happened to be in a workable order,
# which is why this went unnoticed until soak invented one that was not.
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
	foreach my $name (@$names)
	{
		$place->($name, {}) if exists $SCHEMA{$name};
	}
	return @ordered;
}

=pod

=item describe_effective($spec)

One line saying what resolve() composed: the schema with the pulled
decorators marked, every check with where it came from, and the checks
a conflict took away.  Run notes it at the top of each run, so what a
scenario effectively tested is in its log rather than in anyone's
reconstruction.

=cut

sub describe_effective
{
	my ($spec) = @_;
	my %pulled = map { $_ => 1 } @{ $spec->{_schema_pulled} // [] };
	my $prov = $spec->{_check_prov} // {};

	my $out = 'schema=['
	  . join(',',
		map { $pulled{$_} ? "$_(pulled)" : $_ } @{ $spec->{schema} // [] })
	  . ']';
	$out .= ' checks=['
	  . join(',',
		map { ($prov->{$_} // 'spec') eq 'spec' ? $_ : "$_<" . $prov->{$_} }
		  @{ $spec->{checks} // [] })
	  . ']';
	$out .= ' dropped=[' . join(',', @{ $spec->{_checks_dropped} }) . ']'
	  if @{ $spec->{_checks_dropped} // [] };
	$out .= " topology=$spec->{topology}";
	$out .= ' mvcc_gap=' . ($spec->{_mvcc_gap} ? 'tolerated' : 'strict');
	$out .= " disruptor=$spec->{disruptor}"
	  if ($spec->{disruptor} // 'none') ne 'none';
	$out .= " profile=$spec->{profile}" if defined $spec->{profile};
	return $out;
}

=pod

=item fits($kind, $name, $partial)

Whether one entry could join a combination being assembled: its
environment requirement holds and no conflict fires against what is
already there.  Schema requirements are not consulted -- resolve()
pulls those -- so this is the prefilter soak uses, not the authority;
validate() stays that.

=cut

sub fits
{
	my ($kind, $name, $partial) = @_;
	my $defn =
	    $kind eq 'modifier' ? $MODIFIERS{$name}
	  : $scalar_axis{$kind} ? $scalar_axis{$kind}->{$name}
	  : exists $registry{$kind} ? $registry{$kind}->{$name}
	  : undef;

	return 0 unless $defn;

	foreach my $axis (sort keys %scalar_axis)
	{
		if (my $wanted = $defn->{requires}->{$axis})
		{
			return 0
			  unless grep { $_ eq ($partial->{$axis} // '') } @$wanted;
		}
	}
	return 0
	  if $defn->{max_clients}
	  && ($partial->{clients} // 0) > $defn->{max_clients};
	return 0 if _conflicts_fire($kind, $name, $defn, $partial);
	return 1;
}

=pod

=item validate($spec)

Die unless every named entry exists and every requires and conflicts
holds.  Called on the resolved spec by Run; soak calls
spec_is_runnable(), which wraps this in an eval, to tell a combination
worth numbering from one that could never run.

=cut

sub validate
{
	my ($spec) = @_;

	foreach my $key (sort keys %$spec)
	{
		next if $key =~ /^_/;
		die "scenario has unknown field '$key'" unless $known_field{$key};
	}

	foreach my $kind (sort keys %registry)
	{
		foreach my $name (@{ $spec->{$kind} // [] })
		{
			die "scenario names unknown $kind plugin '$name'"
			  unless exists $registry{$kind}->{$name};
		}
	}
	die "scenario names unknown topology '$spec->{topology}'"
	  unless exists $TOPOLOGIES{ $spec->{topology} };
	die "scenario names unknown disruptor '$spec->{disruptor}'"
	  unless exists $DISRUPTORS{ $spec->{disruptor} };
	die "scenario names unknown profile '$spec->{profile}'"
	  if defined $spec->{profile} && !exists $PROFILES{ $spec->{profile} };

	die "scenario names unknown modifier '$spec->{modifier}'"
	  if defined $spec->{modifier} && !exists $MODIFIERS{ $spec->{modifier} };

	if (defined $spec->{modifier})
	{
		my $m = $MODIFIERS{ $spec->{modifier} };

		die "modifier '$spec->{modifier}' cannot be combined with "
		  . "clients '$spec->{clients}'"
		  if $m->{max_clients} && ($spec->{clients} // 0) > $m->{max_clients};

		foreach my $kind (sort keys %{ $m->{conflicts} // {} })
		{
			if ($scalar_axis{$kind})
			{
				foreach my $cname (@{ $m->{conflicts}->{$kind} })
				{
					die "modifier '$spec->{modifier}' conflicts with "
					  . "$kind '$cname'"
					  if ($spec->{$kind} // '') eq $cname;
				}
				next;
			}
			die "modifier '$spec->{modifier}' conflicts with unknown kind "
			  . "'$kind'"
			  unless exists $registry{$kind};
			foreach my $cname (@{ $m->{conflicts}->{$kind} })
			{
				die "modifier '$spec->{modifier}' conflicts with "
				  . "$kind '$cname'"
				  if grep { $_ eq $cname } @{ $spec->{$kind} // [] };
			}
		}
	}

	# Randomized settings: each must be a declared knob at one of its
	# declared values, and a knob a chosen modifier or profile already
	# sets cannot be drawn on top of it -- both would write the GUC and
	# whichever lands last in the conf would silently win.
	foreach my $sname (sort keys %{ $spec->{settings} // {} })
	{
		my $s = $SETTINGS{$sname};
		die "scenario names unknown setting '$sname'" unless $s;

		my $value = $spec->{settings}->{$sname};
		die "setting '$sname' has no choice '$value'"
		  unless grep { "$_" eq "$value" } @{ $s->{choices} };

		foreach my $owner (
			defined $spec->{modifier}
			? [ modifier => $MODIFIERS{ $spec->{modifier} } ]
			: (),
			defined $spec->{profile}
			? [ profile => $PROFILES{ $spec->{profile} } ]
			: ())
		{
			my ($okind, $odefn) = @$owner;
			die "setting '$sname' is already set by the $okind"
			  if grep { /^\s*\Q$sname\E\s*=/ }
			  @{ $odefn->{conf} // [] };
		}
	}

	# The axes' own constraints: a disruptor that cannot run against
	# this topology, a profile that cannot share the cluster with it.
	# These entries are single-valued, so the generic per-entry loop
	# below never walks them.
	foreach my $axis (sort keys %scalar_axis)
	{
		my $aname = $spec->{$axis};
		next unless defined $aname;
		my $adefn = $scalar_axis{$axis}->{$aname} // next;

		foreach my $rkind (sort keys %{ $adefn->{requires} // {} })
		{
			my @want = @{ $adefn->{requires}->{$rkind} };
			if ($scalar_axis{$rkind})
			{
				die "$axis '$aname' requires $rkind "
				  . join(' or ', map { "'$_'" } @want)
				  unless grep { $_ eq ($spec->{$rkind} // '') } @want;
				next;
			}
			foreach my $rname (@want)
			{
				die "$axis '$aname' requires $rkind '$rname', "
				  . 'which the scenario does not use'
				  unless grep { $_ eq $rname } @{ $spec->{$rkind} // [] };
			}
		}
		foreach my $ckind (sort keys %{ $adefn->{conflicts} // {} })
		{
			foreach my $cname (@{ $adefn->{conflicts}->{$ckind} })
			{
				die "$axis '$aname' cannot be combined with "
				  . "$ckind '$cname'"
				  if $scalar_axis{$ckind}
				  ? ($spec->{$ckind} // '') eq $cname
				  : grep { $_ eq $cname } @{ $spec->{$ckind} // [] };
			}
		}
	}

	if (defined $spec->{chaos} && !ref $spec->{chaos})
	{
		die "scenario names unknown chaos profile '$spec->{chaos}'"
		  unless exists $CHAOS{ $spec->{chaos} };
	}
	elsif (ref $spec->{chaos} eq 'HASH')
	{
		# Invented rather than named -- soak does this.  The points still
		# have to exist, and to stay inside the bounds declared for them:
		# those are what keep a sleep from turning into a lock cascade.
		foreach my $point (sort keys %{ $spec->{chaos}->{points} // {} })
		{
			my ($probability, undef, $max_us) =
			  @{ $spec->{chaos}->{points}->{$point} };

			# Known means curated, or defined by this build and not
			# excluded; the caps are the curated ones or the pool's
			# conservative defaults.
			die "chaos names unknown injection point '$point'"
			  unless Stress::Chaos::chaos_point_known($point);
			my $caps = Stress::Chaos::chaos_caps($point);
			die "chaos probability for '$point' above its cap"
			  if $probability > $caps->{max_p};
			die "chaos sleep for '$point' above its cap"
			  if $max_us > $caps->{max_us};
		}
	}

	# requires/conflicts, declared as { kind => [ names ] }
	foreach my $kind (sort keys %registry)
	{
		foreach my $name (@{ $spec->{$kind} // [] })
		{
			my $defn = $registry{$kind}->{$name};

			foreach my $rkind (sort keys %{ $defn->{requires} // {} })
			{
				# A requirement naming a kind that does not exist is a
				# typo that would otherwise be silently true: the lookup
				# below would find nothing to compare against.  The same
				# goes for conflicts, which is worse -- a conflict that
				# never fires lets soak build a combination the plugin
				# said it could not be part of.
				die "$kind '$name' requires unknown kind '$rkind'"
				  unless $scalar_axis{$rkind} || exists $registry{$rkind};

				# A single-valued axis is one value rather than a list,
				# and a requirement on it means "any one of these".
				if ($scalar_axis{$rkind})
				{
					my @want = @{ $defn->{requires}->{$rkind} };
					die "$kind '$name' requires $rkind "
					  . join(' or ', map { "'$_'" } @want)
					  unless grep { $_ eq ($spec->{$rkind} // '') } @want;
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
			  && ($spec->{pgbench_args} // '') =~
			  /--protocol=(?:extended|prepared)/;

			# A variant that destroys relations other variants are gated
			# on can only run when nothing else is in flight.
			die "$kind '$name' cannot be combined with "
			  . "ddl_concurrency '"
			  . ($spec->{ddl_concurrency} // 1) . "'"
			  if $defn->{solo} && ($spec->{ddl_concurrency} // 1) ne '1';

			foreach my $ckind (sort keys %{ $defn->{conflicts} // {} })
			{
				die "$kind '$name' conflicts with unknown kind '$ckind'"
				  unless $scalar_axis{$ckind} || exists $registry{$ckind};

				foreach my $cname (@{ $defn->{conflicts}->{$ckind} })
				{
					# A single-valued axis is one value rather than a
					# list.
					die "$kind '$name' cannot be combined with "
					  . "$ckind '$cname'"
					  if $scalar_axis{$ckind}
					  ? ($spec->{$ckind} // '') eq $cname
					  : grep { $_ eq $cname } @{ $spec->{$ckind} // [] };
				}
			}
		}
	}
	return;
}

=pod

=item spec_is_runnable($spec)

Whether the pieces of $spec fit together, without building anything.
Soak mode uses this to tell a combination worth running from one that
could never work, so that the two are not confused in the numbering.

=back

=cut

sub spec_is_runnable
{
	my ($spec) = @_;

	return eval {
		validate(resolve($spec));
		1;
	} ? 1 : 0;
}

1;
