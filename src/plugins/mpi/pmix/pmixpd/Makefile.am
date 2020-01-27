#
# Makefile for pmixpd

AUTOMAKE_OPTIONS = foreign

sbin_PROGRAMS = pmixpd
pmixp_dir = $(top_srcdir)/src/plugins/mpi/pmix

depend_libs = 				   \
	$(top_builddir)/src/common/libdaemonize.la

# If slurmstepd_LDADD or slurmstepd_LDFLAGS change
# make sure you alter the below depend_[ldadd|ldflags] to look identical in the
# slurmd.  If these libs aren't available
# on slurmstepd startup the slurmstepd will fail sliently and make debugging
# rather difficult.  Making the slurmd link to them as well shouldn't increase
# memory footprint and will fail way earlier and give the user a way to known
# what is happening.
# depend_ldadd = $(HWLOC_LIBS) $(PAM_LIBS) $(UTIL_LIBS)
# depend_ldflags = $(HWLOC_LDFLAGS)

AM_CPPFLAGS = -I$(top_srcdir) -I$(top_srcdir)/src/common $(HWLOC_CPPFLAGS)

pmixpd_LDADD = $(depend_libs) $(LIB_SLURM) $(DL_LIBS) $(depend_ldadd)

pmixpd_LDFLAGS = -export-dynamic $(CMD_LDFLAGS) $(depend_ldflags)

pmixpd_SOURCES = 	        	\
	pmixpd.c \
	$(pmixp_dir)/pmixp_info.c

pmixpd_DEPENDENCIES = $(depend_libs) $(LIB_SLURM_BUILD)

force:
$(pxmip_DEPENDENCIES) : force
	@cd `dirname $@` && $(MAKE) `basename $@`
