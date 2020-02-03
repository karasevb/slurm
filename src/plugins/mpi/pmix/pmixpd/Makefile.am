#
# Makefile for pmixpd

AUTOMAKE_OPTIONS = foreign

sbin_PROGRAMS = pmixpd
pmixp_dir = $(top_srcdir)/src/plugins/mpi/pmix

depend_libs = 				   \
	$(top_builddir)/src/common/libdaemonize.la \
	$(top_builddir)/src/slurmd/common/libslurmd_common.la \
	$(top_builddir)/src/slurmd/common/libslurmd_reverse_tree_math.la

# If slurmstepd_LDADD or slurmstepd_LDFLAGS change
# make sure you alter the below depend_[ldadd|ldflags] to look identical in the
# slurmd.  If these libs aren't available
# on slurmstepd startup the slurmstepd will fail sliently and make debugging
# rather difficult.  Making the slurmd link to them as well shouldn't increase
# memory footprint and will fail way earlier and give the user a way to known
# what is happening.
# depend_ldadd = $(HWLOC_LIBS) $(PAM_LIBS) $(UTIL_LIBS)
# depend_ldflags = $(HWLOC_LDFLAGS)

pmix_internal_libs = \
	$(top_builddir)/src/slurmd/common/libslurmd_reverse_tree_math.la \
	$(top_builddir)/src/common/libcommon.la

pmix_ldflags = $(PLUGIN_FLAGS) $(HWLOC_LDFLAGS) $(UCX_LDFLAGS)
pmix_libadd = $(pmix_internal_libs) $(HWLOC_LIBS) $(UCX_LIBS)


AM_CPPFLAGS = -I$(top_srcdir) -I$(top_srcdir)/src/common $(HWLOC_CPPFLAGS) $(UCX_CPPFLAGS) -I$(pmixp_dir) $(PMIX_V3_CPPFLAGS) -DHAVE_PMIX_VER=3

pmixpd_LDADD = $(depend_libs) $(LIB_SLURM) $(DL_LIBS) $(depend_ldadd) $(pmix_iternal_libs) $(PMIXD_UCX_LDFLAGS) -ldl

pmixpd_LDFLAGS = -export-dynamic $(CMD_LDFLAGS) $(depend_ldflags) $(PMIXD_V3_LDFLAGS)

pmixpd_SOURCES = 	        	\
	pmixpd.c \
	$(pmixp_dir)/pmixp_common.h \
	$(pmixp_dir)/pmixp_coll.h \
	$(pmixp_dir)/pmixp_agent.c $(pmixp_dir)/pmixp_client.c \
	$(pmixp_dir)/pmixp_nspaces.c $(pmixp_dir)/pmixp_info.c \
	$(pmixp_dir)/pmixp_agent.h $(pmixp_dir)/pmixp_client.h \
	$(pmixp_dir)/pmixp_nspaces.h $(pmixp_dir)/pmixp_info.h \
	$(pmixp_dir)/pmixp_server.c $(pmixp_dir)/pmixp_state.c \
	$(pmixp_dir)/pmixp_io.c $(pmixp_dir)/pmixp_utils.c \
	$(pmixp_dir)/pmixp_dmdx.c \
	$(pmixp_dir)/pmixp_server.h $(pmixp_dir)/pmixp_state.h \
	$(pmixp_dir)/pmixp_io.h $(pmixp_dir)/pmixp_utils.h \
	$(pmixp_dir)/pmixp_dmdx.h \
	$(pmixp_dir)/pmixp_conn.c $(pmixp_dir)/pmixp_dconn.c \
	$(pmixp_dir)/pmixp_dconn_tcp.c \
	$(pmixp_dir)/pmixp_conn.h $(pmixp_dir)/pmixp_dconn.h \
	$(pmixp_dir)/pmixp_dconn_tcp.h \
	$(pmixp_dir)/pmixp_coll.c $(pmixp_dir)/pmixp_coll_tree.c \
	$(pmixp_dir)/pmixp_coll_ring.c \
	$(pmixp_dir)/pmixp_dconn_ucx.c $(pmixp_dir)/pmixp_dconn_ucx.h \
	$(pmixp_dir)/pmixp_client_v2.c

pmixpd_DEPENDENCIES = $(depend_libs) $(LIB_SLURM_BUILD)

force:
$(pxmip_DEPENDENCIES) : force
	@cd `dirname $@` && $(MAKE) `basename $@`
