
# Copyright (c) 2026, PostgreSQL Global Development Group

=pod

=head1 NAME

Stress::MVCC - the REPACK tolerance

=head1 DESCRIPTION

REPACK (CONCURRENTLY) is not MVCC-safe yet: a snapshot that spans its
relfilenode swap can find the table empty.  Every check that reads a
relation the rotation may repack has to allow for that, and nothing
else: an empty read is tolerated, a partial or otherwise wrong one is
not.

Which commands are exposed is declared where they are: a %DDL entry
that is not MVCC-safe carries C<< mvcc_safe => 0 >>, and
C<Stress::Compose> derives from the scenario's rotation whether the gap
is reachable at all.  A scenario that never repacks gets the strict
form of every check without asking, and setting C<stress_strict_mvcc=1>
in PG_TEST_EXTRA forces the strict form everywhere -- which is how to
find out whether REPACK has become MVCC-safe.  When it has, deleting
the C<< mvcc_safe => 0 >> keys is the whole change, and this module and
its callers are what can then be removed.

C<mvcc_or_empty($ctx, $count_expr)> returns the SQL condition prefix
that expresses the tolerance, or nothing when the scenario has no gap
to tolerate.

=cut

package Stress::MVCC;

use strict;
use warnings FATAL => 'all';

use Exporter 'import';

our @EXPORT_OK = qw(mvcc_or_empty);

sub mvcc_or_empty
{
	my ($ctx, $count_expr) = @_;
	return '' unless $ctx->{mvcc_gap_possible};
	return "$count_expr = 0 OR ";
}

1;
