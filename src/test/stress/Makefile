#-------------------------------------------------------------------------
#
# Makefile for src/test/stress
#
# Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/test/stress/Makefile
#
#-------------------------------------------------------------------------

subdir = src/test/stress
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

EXTRA_INSTALL = contrib/amcheck contrib/pg_visibility contrib/test_decoding

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)

clean distclean:
	rm -rf tmp_check
