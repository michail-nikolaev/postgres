# Copyright (c) 2021-2024, PostgreSQL Global Development Group

# Test nested if/elif/else working correctly in pgbench
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;

use Test::More;

my ($node, $result);

$node = PostgreSQL::Test::Cluster->new('pgbench_nested_ifs');
$node->init;
$node->start;

$node->pgbench(
	'--no-vacuum --client=1 --exit-on-abort --transactions=1',
	0,
	[qr{actually processed}],
	[qr{^$}],
	'nested ifs',
	{
		'pgbench_nested_if' => q(
			\if false
				SELECT 1 / 0;
				\if true
					SELECT 1 / 0;
				\elif true
					SELECT 1 / 0;
				\else
					SELECT 1 / 0;
				\endif
				SELECT 1 / 0;
			\elif false
				\if true
					SELECT 1 / 0;
				\elif true
					SELECT 1 / 0;
				\else
					SELECT 1 / 0;
				\endif
			\else
				\if false
					SELECT 1 / 0;
				\elif false
					SELECT 1 / 0;
				\else
					SELECT 'correct';
				\endif
			\endif
		)
	});

$node->stop;
done_testing();
