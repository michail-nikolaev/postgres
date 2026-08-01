
# Copyright (c) 2026, PostgreSQL Global Development Group

# Values wide enough to go out of line, stored with their own
# md5.
#
# See Stress::Registry for what each declaration means.

package Stress::Feature::Toast;

use strict;
use warnings FATAL => 'all';

use Test::More;
use Stress::Registry ':declare';

use Stress::MVCC qw(mvcc_or_empty);

# Wide values that go out of line, stored with an md5 of themselves
# so that a torn or stale TOAST fetch is visible as a mismatch.  The
# columns start out null, so only the rows the load has reached are
# wide and the table does not balloon.
schema toast => {
		setup => q(
			ALTER TABLE pgbench_accounts
				ADD COLUMN payload text,
				ADD COLUMN h text;
			-- Left as EXTENDED, the default: the value should go out of
			-- line AND be compressed there.  A rewrite that reassembles
			-- such a datum has to preserve the compression flag in its
			-- header, and EXTERNAL storage -- which never compresses --
			-- would put that path out of reach.
		),
};

# Wide values rewritten together with their md5, in one statement, so
# that every row satisfies md5(payload) = h at every commit.
load toast_rewrite => {
		weight => 2,
		requires => { schema => ['toast'] },
		checks => ['toast_md5'],
		script => q(
			\set id random(1, :naccounts)
			\set len random(3000, 6000)
			-- The payload is built once in the subquery and used for both
			-- columns; computing it twice would give two different values
			-- and a mismatch that is the test's fault, not the server's.
			--
			-- Large and compressible, which is a narrower target than it
			-- looks.  It has to compress -- the interesting header is a
			-- compressed one -- but still exceed the toast threshold
			-- after compressing, or it stays in the tuple.  A repeated
			-- hash compresses about fiftyfold, so the raw value has to be
			-- a hundred kilobytes or so to leave a couple of kilobytes
			-- behind.
			UPDATE pgbench_accounts SET payload = s.p, h = md5(s.p)
				FROM (SELECT repeat(md5(random()::text), :len) AS p) s
				WHERE aid = :id;
		),
};

# Every row's out-of-line value still matches the md5 stored with it.
check toast_md5 => {
		weight => 1,
		requires => { schema => ['toast'] },
		script => sub {
			my ($ctx) = @_;
			my $tol = mvcc_or_empty($ctx, 'cnt');
			return qq(
			SELECT stress_assert(${tol}bad = 0,
				format('%s rows whose payload does not match its md5', bad))
			FROM (SELECT COUNT(*) FILTER (WHERE md5(payload) <> h) AS bad,
				COUNT(*) AS cnt FROM pgbench_accounts WHERE payload IS NOT NULL) x;
			);
		},
		final => sub {
			my ($node, $ctx) = @_;
			Test::More::is(
				$node->safe_psql('postgres',
					'SELECT COUNT(*) FROM pgbench_accounts WHERE payload IS NOT NULL AND md5(payload) <> h'),
				'0', 'every TOASTed payload matches its md5');
		},
};

1;
