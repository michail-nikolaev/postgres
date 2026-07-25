
# Copyright (c) 2026, PostgreSQL Global Development Group

=pod

=head1 NAME

StressConcurrently - helpers shared by the src/test/stress test suite

=head1 SYNOPSIS

  use FindBin;
  use lib $FindBin::RealBin;
  use StressConcurrently;

  my $scale = stress_plan();
  my $node = stress_init_node('mytest');
  ...
  $node->pgbench(
      "--no-vacuum --client=30 --jobs=4 --exit-on-abort -T " . (6 * $scale),
      0, [qr{actually processed}], [qr{^$}], 'my workload',
      { concurrent_ops => $my_pgbench_script });

=head1 DESCRIPTION

The tests under C<src/test/stress> all follow the same shape: they run a
pgbench workload in which one client drives a rotation of CONCURRENTLY
commands while the others exercise some feature against the same table,
checking invariants that must hold no matter how the concurrent DDL
interleaves.  This module collects the boilerplate they have in common
so that each test file only has to spell out what is unique to it.

=cut

package StressConcurrently;

use strict;
use warnings FATAL => 'all';

use Exporter 'import';
use Test::More;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

our @EXPORT = qw(
  stress_plan
  stress_init_node
  stress_assert_defn
  stress_variant_switch
  stress_ddl_gate
  stress_workload
);

=pod

=over

=item stress_plan(%opts)

Decide whether the calling test should run, based on the
C<stress_concurrently> value in PG_TEST_EXTRA (see
C<PostgreSQL::Test::Utils::stress_concurrently_scale>).  If the value is
0, calls C<plan skip_all> and never returns; otherwise emits a C<note>
with the scale and returns it.

Options:

  skip => message   the skip_all message (default mentions the suite)

=cut

sub stress_plan
{
	my (%opts) = @_;
	my $scale = PostgreSQL::Test::Utils::stress_concurrently_scale();

	if ($scale == 0)
	{
		my $msg = $opts{skip}
		  // 'skipping disabled CONCURRENTLY stress test';
		plan skip_all => $msg;
	}

	note "stressval is $scale";
	return $scale;
}

=pod

=item stress_init_node($name, %opts)

Create, configure and (unless told otherwise) start a
C<PostgreSQL::Test::Cluster> node with the settings the suite normally
wants: a generous C<lock_timeout>, and C<wal_level = logical> so that
REPACK (CONCURRENTLY) is allowed.

Options:

  init          => { ... }   extra arguments passed to $node->init
  wal_level     => 'replica' override the WAL level ('logical' default)
  extra_conf    => [ ... ]   additional postgresql.conf lines
  no_start      => 1         return the node without starting it
  no_asserts    => 1         do not create the stress_assert() function

Unless C<no_start> or C<no_asserts> is given, the stress_assert()
function (see stress_assert_defn) is created in the C<postgres>
database, ready for the checks in a workload to use.

=cut

sub stress_init_node
{
	my ($name, %opts) = @_;

	my $node = PostgreSQL::Test::Cluster->new($name);
	$node->init(%{ $opts{init} // {} });
	$node->append_conf('postgresql.conf',
		'lock_timeout = ' . (1000 * $PostgreSQL::Test::Utils::timeout_default));

	# allows_streaming already sets wal_level; only set it ourselves when
	# the caller has not asked init to.
	my $init = $opts{init} // {};
	unless ($init->{allows_streaming})
	{
		my $wal_level = $opts{wal_level} // 'logical';
		$node->append_conf('postgresql.conf', "wal_level = $wal_level");
	}

	$node->append_conf('postgresql.conf', join("\n", @{ $opts{extra_conf} }))
	  if $opts{extra_conf};

	return $node if $opts{no_start};

	$node->start;
	$node->safe_psql('postgres', stress_assert_defn()) unless $opts{no_asserts};
	return $node;
}

=pod

=item stress_assert_defn()

Return SQL that creates the stress_assert(ok boolean, msg text)
function.  A workload check calls it instead of the traditional
"C<SELECT 1/0>" trick: rather than a bare division-by-zero, a failed
assertion raises an error naming the invariant (and, if the caller
builds the message with C<format()>, the offending values), which is
far easier to diagnose from a pgbench abort.  stress_init_node() creates
it automatically; tests that use more than one database create it in the
others themselves.

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

=pod

=item stress_variant_switch(%opts)

Return a pgbench script fragment that picks one of several blocks at
random and runs it: a C<\set VAR random(0, N-1)> followed by an
C<\if>/C<\elif> chain over the N blocks, closed with C<\endif>.  This is
the "do one of these things this transaction" scaffolding used
throughout the suite; generating it keeps the C<random()> bound in step
with the number of blocks, which is the error-prone part.

Each block is a pgbench fragment given as a string, which may span
several lines and contain its own meta-commands (C<\if>, C<\gset>,
etc.).  The block's own indentation does not matter: its common leading
tabs are stripped and it is re-indented to sit under its C<\if>.

Options:

  variants => [ ... ]  the blocks to choose between (required).
  var      => name     selector variable (default 'variant').
  indent   => 'str'    indentation of the \if/\elif/\endif lines
                       (default three tabs).

=cut

sub _dedent
{
	my ($block) = @_;
	my @lines = split /\n/, $block, -1;
	shift @lines while @lines && $lines[0] !~ /\S/;
	pop @lines while @lines && $lines[-1] !~ /\S/;
	return ('') unless @lines;

	my $min;
	for my $ln (@lines)
	{
		next unless $ln =~ /\S/;
		my ($lead) = $ln =~ /^(\t*)/;
		$min = length $lead if !defined $min || length $lead < $min;
	}
	$min //= 0;
	return map { my $l = $_; $l =~ s/^\t{0,$min}//; $l } @lines;
}

sub stress_variant_switch
{
	my (%opts) = @_;
	my $variants = $opts{variants}
	  or die 'stress_variant_switch requires variants => [ ... ]';
	my $var = $opts{var} // 'variant';
	my $i0 = $opts{indent} // "\t\t\t";
	my $i1 = "$i0\t";

	my $n = scalar @$variants;
	die 'stress_variant_switch needs at least one variant' unless $n;

	my $out = "$i0\\set $var random(0, " . ($n - 1) . ")\n";
	for my $i (0 .. $n - 1)
	{
		my $kw = $i == 0 ? '\if' : '\elif';
		$out .= "$i0$kw :$var = $i\n";
		$out .= "$i1$_\n" for _dedent($variants->[$i]);
	}
	$out .= "$i0\\endif";
	return $out;
}

=pod

=item stress_ddl_gate(%opts)

Return a pgbench script fragment in which the client that wins an
advisory lock runs one randomly chosen command from a list, then
releases the lock.  This is the DDL-rotation scaffolding common to the
suite: it opens C<\if :gotddl>, picks a variant with C<random(0, N-1)>
for the N given commands, chains them through C<\elif>, and ends with
C<\else> for the caller to fill in.  The caller closes the block with
C<\endif>.

Generating the C<\elif> chain keeps the C<random()> bound in step with
the number of commands automatically, which is the part that is easy to
get wrong when it is written out by hand.  The list of commands is
passed in, so the call site still shows exactly which DDL runs.

Options:

  ddl   => [ ... ]   commands to choose between (required).  Each element
                     is either a single SQL string, or an arrayref of SQL
                     strings that run together for that variant (for
                     example a DROP followed by a CREATE).
  post  => 'sql'     SQL to run after the chosen command, before the lock
                     is released (typically an amcheck call).  May be a
                     single string or an arrayref of statements, each
                     emitted on its own line.  Optional.
  lock  => N         advisory lock key (default 42).  May be a pgbench
                     expression such as ':t'.
  var   => name      pgbench variable holding whether we got the lock
                     (default 'gotddl').
  sleep_ms => N      milliseconds to sleep before releasing the lock
                     (default 10; 0 to omit).
  else  => 'sql'     the \else body (typically a stress_workload).  If
                     given, it is included and the block is closed with
                     \endif; if omitted, the fragment ends at \else for
                     the caller to append to.
  indent => 'str'    leading indentation for the fragment (default two
                     tabs, matching a script built with qq()).

=cut

sub stress_ddl_gate
{
	my (%opts) = @_;
	my $ddl = $opts{ddl} or die 'stress_ddl_gate requires ddl => [ ... ]';
	my $post = $opts{post};
	my $lock = $opts{lock} // 42;
	my $var = $opts{var} // 'gotddl';
	my $sleep_ms = exists $opts{sleep_ms} ? $opts{sleep_ms} : 10;
	my $i0 = $opts{indent} // "\t\t";    # \if :gotddl / \else level
	my $i1 = "$i0\t";                    # body level

	my $n = scalar @$ddl;
	die 'stress_ddl_gate needs at least one command' unless $n;

	my $out = "$i0" . "SELECT pg_try_advisory_lock($lock)::integer AS $var \\gset\n";
	$out .= "$i0\\if :$var\n";

	if ($n > 1)
	{
		# One command per variant; join a multi-statement variant into a
		# single block for the switch.
		my @blocks =
		  map { ref $_ eq 'ARRAY' ? join("\n", @$_) : $_ } @$ddl;
		$out .= stress_variant_switch(
			var => 'stress_variant',
			indent => $i1,
			variants => \@blocks) . "\n";
	}
	else
	{
		my $cmd = $ddl->[0];
		my @stmts = ref $cmd eq 'ARRAY' ? @$cmd : ($cmd);
		$out .= "$i1$_\n" for @stmts;
	}

	if (defined $post)
	{
		$out .= "$i1$_\n" for (ref $post eq 'ARRAY' ? @$post : ($post));
	}
	$out .= "$i1\\sleep $sleep_ms ms\n" if $sleep_ms;
	$out .= "${i1}SELECT pg_advisory_unlock($lock);\n";
	$out .= "$i0\\else";

	# If the caller supplied the \else body (typically a stress_workload),
	# include it and close the block; otherwise leave it open for the
	# caller to append and close themselves.
	if (defined $opts{else})
	{
		$out .= "\n" . $opts{else} . "\n$i0\\endif";
	}

	return $out;
}

=pod

=item stress_workload(%opts)

Return a pgbench script fragment for the "other" clients in a stress
test -- the ones not driving DDL.  A stress workload consists of
mutations, which change data while preserving some invariant, and
checks, which verify that invariant; this builds a switch that performs
one randomly chosen mutation or check per transaction.

Declaring the two separately makes the intent of a test clear at a
glance: these are the operations that change the data, and these are the
properties that must nonetheless always hold.  Each block should be
self-contained (including any C<\set> it needs), so that it reads as one
complete action.

Options:

  mutations => [ ... ]  data-changing blocks (each a pgbench fragment).
  checks    => [ ... ]  invariant-verifying blocks.
  indent    => 'str'    indentation of the switch (default three tabs).

At least one of mutations or checks must be given.

=cut

sub stress_workload
{
	my (%opts) = @_;
	my @mutations = @{ $opts{mutations} // [] };
	my @checks = @{ $opts{checks} // [] };
	die 'stress_workload needs mutations or checks'
	  unless @mutations || @checks;

	return stress_variant_switch(
		var => 'stress_action',
		indent => $opts{indent} // "\t\t\t",
		variants => [ @mutations, @checks ]);
}

=pod

=back

=cut

1;
