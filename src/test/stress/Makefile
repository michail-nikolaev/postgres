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

EXTRA_INSTALL = contrib/amcheck contrib/pg_visibility \
	src/test/modules/injection_points

# The chaos profiles need to know whether this build has injection
# points; meson passes this through, and without the export here the
# whole dimension was silently off under make check.
export enable_injection_points

check:
	$(prove_check)

installcheck:
	$(prove_installcheck)

clean distclean:
	rm -rf tmp_check
