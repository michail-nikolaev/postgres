# REPACK (CONCURRENTLY);
#
# Test handling of preventing access to non-mvcc safe data.
setup
{
	CREATE EXTENSION injection_points;

	CREATE TABLE repack_test(i int PRIMARY KEY);
	INSERT INTO repack_test(i) VALUES (1), (2), (3);
}

teardown
{
	DROP TABLE repack_test;
	DROP EXTENSION injection_points;
}

session s1
step repack
{
	REPACK (CONCURRENTLY) repack_test;
}

session s2
step begin
{
	BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
	SELECT 1;
}
step check
{
	SELECT * FROM repack_test;
}

permutation
	begin
	repack
	check
