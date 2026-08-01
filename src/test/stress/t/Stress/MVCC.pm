
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

C<stress_repack_tolerated($count_expr)> returns the SQL condition that
expresses it, so the caveat lives in one place.  Setting
C<stress_strict_mvcc=1> in PG_TEST_EXTRA turns the tolerance off, which
is how to find out whether REPACK has become MVCC-safe; when it has,
this function and its callers are what has to be removed.

=cut

package Stress::MVCC;

use strict;
use warnings FATAL => 'all';

use Exporter 'import';

our @EXPORT_OK = qw(stress_repack_tolerated);

sub stress_repack_tolerated
{
	my ($count_expr) = @_;
	return '' if ($ENV{PG_TEST_EXTRA} // '') =~ /\bstress_strict_mvcc=1\b/;
	return "$count_expr = 0 OR ";
}

1;
