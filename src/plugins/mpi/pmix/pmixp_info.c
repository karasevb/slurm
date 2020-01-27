/*****************************************************************************\
 **  pmix_info.c - PMIx various environment information
 *****************************************************************************
 *  Copyright (C) 2014-2015 Artem Polyakov. All rights reserved.
 *  Copyright (C) 2015-2017 Mellanox Technologies. All rights reserved.
 *  Written by Artem Polyakov <artpol84@gmail.com, artemp@mellanox.com>.
 *
 *  This file is part of Slurm, a resource management program.
 *  For details, see <https://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  Slurm is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  Slurm is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with Slurm; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
 \*****************************************************************************/

#include <string.h>
#include "pmixp_common.h"
#include "pmixp_debug.h"
#include "pmixp_info.h"
#include "pmixp_coll.h"

/* Server communication */
static char *_srv_usock_path = NULL;
static int _srv_usock_fd = -1;
static bool _srv_use_direct_conn = true;
static bool _srv_use_direct_conn_early = false;
static bool _srv_same_arch = true;
#ifdef HAVE_UCX
static bool _srv_use_direct_conn_ucx = true;
#else
static bool _srv_use_direct_conn_ucx = false;
#endif
static int _srv_fence_coll_type = PMIXP_COLL_TYPE_FENCE_MAX;
static bool _srv_fence_coll_barrier = false;

pmix_jobinfo_t _pmixp_job_info;

static int _resources_set(char ***env);
static int _env_set(char ***env);

/* stepd global UNIX socket contact information */
void pmixp_info_srv_usock_set(char *path, int fd)
{
	_srv_usock_path = _pmixp_job_info.server_addr_unfmt;
	_srv_usock_fd = fd;
}

const char *pmixp_info_srv_usock_path(void)
{
	/* Check that Server address was initialized */
	xassert(_srv_usock_path);
	return _srv_usock_path;
}

int pmixp_info_srv_usock_fd(void)
{
	/* Check that Server fd was created */
	xassert(0 <= _srv_usock_fd);
	return _srv_usock_fd;
}

bool pmixp_info_same_arch(void){
	return _srv_same_arch;
}


bool pmixp_info_srv_direct_conn(void){
	return _srv_use_direct_conn;
}

bool pmixp_info_srv_direct_conn_early(void){
	return _srv_use_direct_conn_early && _srv_use_direct_conn;
}

bool pmixp_info_srv_direct_conn_ucx(void){
	return _srv_use_direct_conn_ucx && _srv_use_direct_conn;
}

int pmixp_info_srv_fence_coll_type(void)
{
	if (!_srv_use_direct_conn) {
		static bool printed = false;
		if (!printed && PMIXP_COLL_CPERF_RING == _srv_fence_coll_type) {
			PMIXP_ERROR("Ring collective algorithm cannot be used "
				    "with Slurm RPC's communication subsystem. "
				    "Tree-based collective will be used instead.");
			printed = true;
		}
		return PMIXP_COLL_CPERF_TREE;
	}
	return _srv_fence_coll_type;
}

bool pmixp_info_srv_fence_coll_barrier(void)
{
	return _srv_fence_coll_barrier;
}

/* Job information */
int pmixp_info_set(const stepd_step_rec_t *job, char ***env)
{
	int i, rc;
	size_t msize;
	memset(&_pmixp_job_info, 0, sizeof(_pmixp_job_info));
#ifndef NDEBUG
	_pmixp_job_info.magic = PMIXP_INFO_MAGIC;
#endif
	/* security info */
	_pmixp_job_info.uid = job->uid;
	_pmixp_job_info.gid = job->gid;

	if ((job->pack_jobid != 0) && (job->pack_jobid != NO_VAL)) {
		_pmixp_job_info.jobid = job->pack_jobid;
		_pmixp_job_info.stepid = job->stepid;
		_pmixp_job_info.node_id = job->nodeid  + job->node_offset;
		_pmixp_job_info.node_tasks = job->node_tasks;
		_pmixp_job_info.ntasks = job->pack_ntasks;
		_pmixp_job_info.nnodes = job->pack_nnodes;
		msize = _pmixp_job_info.nnodes * sizeof(uint32_t);
		_pmixp_job_info.task_cnts = xmalloc(msize);
		for (i = 0; i < _pmixp_job_info.nnodes; i++)
			_pmixp_job_info.task_cnts[i] = job->pack_task_cnts[i];

		msize = _pmixp_job_info.node_tasks * sizeof(uint32_t);
		_pmixp_job_info.gtids = xmalloc(msize);
		for (i = 0; i < job->node_tasks; i++) {
			_pmixp_job_info.gtids[i] = job->task[i]->gtid +
						   job->pack_task_offset;
		}
	} else {
		_pmixp_job_info.jobid = job->jobid;
		_pmixp_job_info.stepid = job->stepid;
		_pmixp_job_info.node_id = job->nodeid;
		_pmixp_job_info.node_tasks = job->node_tasks;
		_pmixp_job_info.ntasks = job->ntasks;
		_pmixp_job_info.nnodes = job->nnodes;
		msize = _pmixp_job_info.nnodes * sizeof(uint32_t);
		_pmixp_job_info.task_cnts = xmalloc(msize);
		for (i = 0; i < _pmixp_job_info.nnodes; i++)
			_pmixp_job_info.task_cnts[i] = job->task_cnts[i];

		msize = _pmixp_job_info.node_tasks * sizeof(uint32_t);
		_pmixp_job_info.gtids = xmalloc(msize);
		for (i = 0; i < job->node_tasks; i++)
			_pmixp_job_info.gtids[i] = job->task[i]->gtid;
	}
#if 0
	if ((job->pack_jobid != 0) && (job->pack_jobid != NO_VAL))
		info("PACK JOBID:%u", _pmixp_job_info.jobid);
	else
		info("JOBID:%u", _pmixp_job_info.jobid);
	info("STEPID:%u", _pmixp_job_info.stepid);
	info("NODEID:%u", _pmixp_job_info.node_id);
	info("NODE_TASKS:%u", _pmixp_job_info.node_tasks);
	info("NTASKS:%u", _pmixp_job_info.ntasks);
	info("NNODES:%u", _pmixp_job_info.nnodes);
	for (i = 0; i < _pmixp_job_info.nnodes; i++)
		info("TASK_CNT[%d]:%u", i,_pmixp_job_info.task_cnts[i]);
	for (i = 0; i < job->node_tasks; i++)
		info("GTIDS[%d]:%u", i, _pmixp_job_info.gtids[i]);
#endif

	/* Setup hostnames and job-wide info */
	if ((rc = _resources_set(env))) {
		return rc;
	}

	if ((rc = _env_set(env))) {
		return rc;
	}

	snprintf(_pmixp_job_info.nspace, PMIXP_MAX_NSLEN, "slurm.pmix.%d.%d",
		 pmixp_info_jobid(), pmixp_info_stepid());

	return SLURM_SUCCESS;
}

int pmixp_info_free(void)
{
	if (_pmixp_job_info.task_cnts) {
		xfree(_pmixp_job_info.task_cnts);
	}
	if (_pmixp_job_info.gtids) {
		xfree(_pmixp_job_info.gtids);
	}

	if (_pmixp_job_info.task_map_packed) {
		xfree(_pmixp_job_info.task_map_packed);
	}

	hostlist_destroy(_pmixp_job_info.job_hl);
	hostlist_destroy(_pmixp_job_info.step_hl);
	if (_pmixp_job_info.hostname) {
		xfree(_pmixp_job_info.hostname);
	}
	return SLURM_SUCCESS;
}

static eio_handle_t *_io_handle = NULL;

void pmixp_info_io_set(eio_handle_t *h)
{
	_io_handle = h;
}

eio_handle_t *pmixp_info_io(void)
{
	xassert(_io_handle);
	return _io_handle;
}

/*
 * Job and step nodes/tasks count and hostname extraction routines
 */

/*
 * Derived from src/srun/libsrun/opt.c
 * _get_task_count()
 *
 * FIXME: original _get_task_count has some additinal ckeck
 * for opt.ntasks_per_node & opt.cpus_set
 * Should we care here?
 static int _get_task_count(char ***env, uint32_t *tasks, uint32_t *cpus)
 {
 pmixp_debug_hang(1);
 char *cpus_per_node = NULL, *cpus_per_task_env = NULL, *end_ptr = NULL;
 int cpus_per_task = 1, cpu_count, node_count, task_count;
 int total_tasks = 0, total_cpus = 0;

 cpus_per_node = getenvp(*env, PMIX_CPUS_PER_NODE_ENV);
 if (cpus_per_node == NULL) {
 PMIXP_ERROR_NO(0,"Cannot find %s environment variable",
		PMIX_CPUS_PER_NODE_ENV);
 return SLURM_ERROR;
 }
 cpus_per_task_env = getenvp(*env, PMIX_CPUS_PER_TASK);
 if (cpus_per_task_env != NULL) {
 cpus_per_task = strtol(cpus_per_task_env, &end_ptr, 10);
 }

 cpu_count = strtol(cpus_per_node, &end_ptr, 10);
 task_count = cpu_count / cpus_per_task;
 while (1) {
 if ((end_ptr[0] == '(') && (end_ptr[1] == 'x')) {
 end_ptr += 2;
 node_count = strtol(end_ptr, &end_ptr, 10);
 task_count *= node_count;
 total_tasks += task_count;
 cpu_count *= node_count;
 total_cpus += cpu_count;
 if (end_ptr[0] == ')')
 end_ptr++;
 } else if ((end_ptr[0] == ',') || (end_ptr[0] == 0))
 total_tasks += task_count;
 else {
 PMIXP_ERROR_NO(0,"Invalid value for environment variable %s (%s)",
 PMIX_CPUS_PER_NODE_ENV, cpus_per_node);
 return SLURM_ERROR;
 }
 if (end_ptr[0] == ',')
 end_ptr++;
 if (end_ptr[0] == 0)
 break;
 }
 *tasks = total_tasks;
 *cpus = total_cpus;
 return 0;
 }
 */

static int _resources_set(char ***env)
{
	char *p = NULL;

	/* Initialize all memory pointers that would be allocated to NULL
	 * So in case of error exit we will know what to xfree
	 */
	_pmixp_job_info.job_hl = hostlist_create("");
	_pmixp_job_info.step_hl = hostlist_create("");
	_pmixp_job_info.hostname = NULL;

	/* Save step host list */
	p = getenvp(*env, PMIXP_STEP_NODES_ENV);
	if (!p) {
		PMIXP_ERROR_NO(ENOENT, "Environment variable %s not found",
			       PMIXP_STEP_NODES_ENV);
		goto err_exit;
	}
	hostlist_push(_pmixp_job_info.step_hl, p);

	/* Extract our node name */
	p = hostlist_nth(_pmixp_job_info.step_hl, _pmixp_job_info.node_id);
	_pmixp_job_info.hostname = xstrdup(p);
	free(p);

	/* Determine job-wide node id and job-wide node count */
	p = getenvp(*env, PMIXP_JOB_NODES_ENV);
	if (!p) {
		p = getenvp(*env, PMIXP_JOB_NODES_ENV_DEP);
		if (!p) {
			/* shouldn't happen if we are under Slurm! */
			PMIXP_ERROR_NO(ENOENT,
				       "Neither of nodelist environment variables: %s OR %s was found!",
				       PMIXP_JOB_NODES_ENV,
				       PMIXP_JOB_NODES_ENV_DEP);
			goto err_exit;
		}
	}
	hostlist_push(_pmixp_job_info.job_hl, p);
	_pmixp_job_info.nnodes_job = hostlist_count(_pmixp_job_info.job_hl);
	_pmixp_job_info.node_id_job = hostlist_find(_pmixp_job_info.job_hl,
						    _pmixp_job_info.hostname);

	/* FIXME!! ------------------------------------------------------- */
	/* TODO: _get_task_count not always works well.
	 if (_get_task_count(env, &_pmixp_job_info.ntasks_job,
		&_pmixp_job_info.ncpus_job) < 0) {
	 _pmixp_job_info.ntasks_job  = _pmixp_job_info.ntasks;
	 _pmixp_job_info.ncpus_job  = _pmixp_job_info.ntasks;
	 }
	 xassert(_pmixp_job_info.ntasks <= _pmixp_job_info.ntasks_job);
	 */
	_pmixp_job_info.ntasks_job = _pmixp_job_info.ntasks;
	_pmixp_job_info.ncpus_job = _pmixp_job_info.ntasks;

	/* Save task-to-node mapping */
	p = getenvp(*env, PMIXP_SLURM_MAPPING_ENV);
	if (!p) {
		/* Direct modex won't work */
		PMIXP_ERROR_NO(ENOENT, "No %s environment variable found!",
			       PMIXP_SLURM_MAPPING_ENV);
		goto err_exit;
	}

	_pmixp_job_info.task_map_packed = xstrdup(p);

	return SLURM_SUCCESS;
err_exit:
	hostlist_destroy(_pmixp_job_info.job_hl);
	hostlist_destroy(_pmixp_job_info.step_hl);
	if (_pmixp_job_info.hostname) {
		xfree(_pmixp_job_info.hostname);
	}
	return SLURM_ERROR;
}

static int _env_set(char ***env)
{
	char *p = NULL;

	xassert(_pmixp_job_info.hostname);

	_pmixp_job_info.server_addr_unfmt = slurm_get_slurmd_spooldir(NULL);

	_pmixp_job_info.lib_tmpdir = slurm_conf_expand_slurmd_path(
				_pmixp_job_info.server_addr_unfmt,
				_pmixp_job_info.hostname);

	xstrfmtcat(_pmixp_job_info.server_addr_unfmt,
		   "/stepd.slurm.pmix.%d.%d",
		   pmixp_info_jobid(), pmixp_info_stepid());

	_pmixp_job_info.spool_dir = xstrdup(_pmixp_job_info.lib_tmpdir);

	/* ----------- Temp directories settings ------------- */
	xstrfmtcat(_pmixp_job_info.lib_tmpdir, "/pmix.%d.%d/",
		   pmixp_info_jobid(), pmixp_info_stepid());

	/* save client temp directory if requested
	 * TODO: We want to get TmpFS value as well if exists.
	 * Need to sync with Slurm developers.
	 */
	p = getenvp(*env, PMIXP_TMPDIR_CLI);

	if (p){
		_pmixp_job_info.cli_tmpdir_base = xstrdup(p);
	} else {
		_pmixp_job_info.cli_tmpdir_base = slurm_get_tmp_fs(
					_pmixp_job_info.hostname);
	}

	_pmixp_job_info.cli_tmpdir =
			xstrdup_printf("%s/spmix_appdir_%d.%d",
				       _pmixp_job_info.cli_tmpdir_base,
				       pmixp_info_jobid(),
				       pmixp_info_stepid());


	/* ----------- Timeout setting ------------- */
	/* TODO: also would be nice to have a cluster-wide setting in Slurm */
	_pmixp_job_info.timeout = PMIXP_TIMEOUT_DEFAULT;
	p = getenvp(*env, PMIXP_TIMEOUT);
	if (p) {
		int tmp;
		tmp = atoi(p);
		if (tmp > 0) {
			_pmixp_job_info.timeout = tmp;
		}
	}

	/* ----------- Forward PMIX settings ------------- */
	/* FIXME: this may be intrusive as well as PMIx library will create
	 * lots of output files in /tmp by default.
	 * somebody can use this or annoyance */
	p = getenvp(*env, PMIXP_PMIXLIB_DEBUG);
	if (p) {
		setenv(PMIXP_PMIXLIB_DEBUG, p, 1);
		/* output into the file since we are in slurmstepd
		 * and stdout is muted.
		 * One needs to check TMPDIR for the results */
		setenv(PMIXP_PMIXLIB_DEBUG_REDIR, "file", 1);
	}

	/*------------- Flag controlling heterogeneous support ----------*/
	/* NOTE: Heterogen support is not tested */
	p = getenvp(*env, PMIXP_DIRECT_SAMEARCH);
	if (p) {
		if (!xstrcmp("1",p) || !xstrcasecmp("true", p) ||
		    !xstrcasecmp("yes", p)) {
			_srv_same_arch = true;
		} else if (!xstrcmp("0",p) || !xstrcasecmp("false", p) ||
			   !xstrcasecmp("no", p)) {
			_srv_same_arch = false;
		}
	}

	/*------------- Direct connection setting ----------*/
	p = getenvp(*env, PMIXP_DIRECT_CONN);
	if (p) {
		if (!xstrcmp("1",p) || !xstrcasecmp("true", p) ||
		    !xstrcasecmp("yes", p)) {
			_srv_use_direct_conn = true;
		} else if (!xstrcmp("0",p) || !xstrcasecmp("false", p) ||
			   !xstrcasecmp("no", p)) {
			_srv_use_direct_conn = false;
		}
	}
	p = getenvp(*env, PMIXP_DIRECT_CONN_EARLY);
	if (p) {
		if (!xstrcmp("1", p) || !xstrcasecmp("true", p) ||
		    !xstrcasecmp("yes", p)) {
			_srv_use_direct_conn_early = true;
		} else if (!xstrcmp("0", p) || !xstrcasecmp("false", p) ||
			   !xstrcasecmp("no", p)) {
			_srv_use_direct_conn_early = false;
		}
	}

	/*------------- Fence coll type setting ----------*/
	p = getenvp(*env, PMIXP_COLL_FENCE);
	if (p) {
		if (!xstrcmp("mixed", p)) {
			_srv_fence_coll_type = PMIXP_COLL_CPERF_MIXED;
		} else if (!xstrcmp("tree", p)) {
			_srv_fence_coll_type = PMIXP_COLL_CPERF_TREE;
		} else if (!xstrcmp("ring", p)) {
			_srv_fence_coll_type = PMIXP_COLL_CPERF_RING;
		}
	}
	p = getenvp(*env, SLURM_PMIXP_FENCE_BARRIER);
	if (p) {
		if (!xstrcmp("1",p) || !xstrcasecmp("true", p) ||
		    !xstrcasecmp("yes", p)) {
			_srv_fence_coll_barrier = true;
		} else if (!xstrcmp("0",p) || !xstrcasecmp("false", p) ||
			   !xstrcasecmp("no", p)) {
			_srv_fence_coll_barrier = false;
		}
	}

#ifdef HAVE_UCX
	p = getenvp(*env, PMIXP_DIRECT_CONN_UCX);
	if (p) {
		if (!xstrcmp("1",p) || !xstrcasecmp("true", p) ||
		    !xstrcasecmp("yes", p)) {
			_srv_use_direct_conn_ucx = true;
		} else if (!xstrcmp("0",p) || !xstrcasecmp("false", p) ||
			   !xstrcasecmp("no", p)) {
			_srv_use_direct_conn_ucx = false;
		}
	}

	/* Propagate UCX env */
	p = getenvp(*env, "UCX_NET_DEVICES");
	if (p) {
		setenv("UCX_NET_DEVICES", p, 1);
	}

	p = getenvp(*env, "UCX_TLS");
	if (p) {
		setenv("UCX_TLS", p, 1);
	}

#endif

	return SLURM_SUCCESS;
}

#define PMIXP_PACKHOSTLIST(_hl, _buf)			\
{							\
	char *hosts;					\
	hosts = hostlist_deranged_string_xmalloc(_hl);	\
	packstr(hosts, _buf);				\
	xfree(hosts);					\
}

#define PMIXP_UNPACHOSTLIST(_hl, _buf)			\
{							\
	char *tmp_str;					\
	uint32_t len;					\
	unpackstr_xmalloc(&tmp_str, &len,		\
			  serialized_buf);		\
	_hl = hostlist_create(tmp_str);			\
	xfree(tmp_str);					\
}

#define PMIXP_UNPACKINT(_intval, _buf)			\
{							\
	uint32_t val32;					\
	unpack32(&val32, _buf);				\
	_intval = (int)val32;				\
}

uint32_t pmixp_info_serialize(pmix_jobinfo_t *jobinfo,
			      Buf *serialized_buf) {
	Buf buffer = init_buf(0);
	uint32_t i, size;

#ifndef NDEBUG
	xassert(jobinfo->magic == PMIXP_INFO_MAGIC);
	pack32(jobinfo->magic, buffer);
#endif
	packstr(jobinfo->nspace, buffer);
	pack32(jobinfo->jobid, buffer);
	pack32(jobinfo->stepid, buffer);
	pack32(jobinfo->nnodes, buffer);
	pack32(jobinfo->nnodes_job, buffer);
	pack32(jobinfo->ntasks, buffer);
	pack32(jobinfo->ntasks_job, buffer);
	pack32(jobinfo->ncpus_job, buffer);
	/* task_cnts */
	for (i = 0; i < jobinfo->nnodes; i++) {
		pack32(jobinfo->task_cnts[i], buffer);
	}
	pack32((uint32_t)jobinfo->node_id, buffer);
	pack32((uint32_t)jobinfo->node_id_job, buffer);
	PMIXP_PACKHOSTLIST(jobinfo->job_hl, buffer);
	PMIXP_PACKHOSTLIST(jobinfo->step_hl, buffer);
	packstr(jobinfo->hostname, buffer);
	pack32((uint32_t)jobinfo->node_tasks, buffer);
	for (i = 0; i < jobinfo->node_tasks; i++) {
		pack32(jobinfo->gtids[i], buffer);
	}
	packstr(jobinfo->task_map_packed, buffer);
	pack32((uint32_t)jobinfo->timeout, buffer);
	packstr(jobinfo->cli_tmpdir, buffer);
	packstr(jobinfo->cli_tmpdir_base, buffer);
	packstr(jobinfo->lib_tmpdir, buffer);
	packstr(jobinfo->server_addr_unfmt, buffer);
	packstr(jobinfo->spool_dir, buffer);
	pack32((uint32_t)jobinfo->uid, buffer);
	pack32((uint32_t)jobinfo->gid, buffer);
	*serialized_buf = buffer;

	size = get_buf_offset(buffer);
	set_buf_offset(buffer, 0);

	return size;
}

void pmixp_info_deserialize(Buf serialized_buf,
			    pmix_jobinfo_t *jobinfo) {
	uint32_t len, i;
	char *tmp_str = NULL;
	size_t msize;

#ifndef NDEBUG
	unpack32(jobinfo->magic, serialized_buf);
	xassert(jobinfo->magic == PMIXP_INFO_MAGIC);
#endif
	unpackstr_xmalloc(&tmp_str, &len, serialized_buf);
	xassert(strlen(tmp_str)+1 < PMIXP_MAX_NSLEN);
	memcpy(jobinfo->nspace, tmp_str, strlen(tmp_str)+1);
	xfree(tmp_str);

	unpack32(&jobinfo->jobid, serialized_buf);
	unpack32(&jobinfo->stepid, serialized_buf);
	unpack32(&jobinfo->nnodes, serialized_buf);
	unpack32(&jobinfo->nnodes_job, serialized_buf);
	unpack32(&jobinfo->ntasks, serialized_buf);
	unpack32(&jobinfo->ntasks_job, serialized_buf);
	unpack32(&jobinfo->ncpus_job, serialized_buf);

	msize = jobinfo->nnodes * sizeof(uint32_t);
	jobinfo->task_cnts = xmalloc(msize);
	for (i = 0; i < jobinfo->nnodes; i++) {
		unpack32(&jobinfo->task_cnts[i], serialized_buf);
	}
	PMIXP_UNPACKINT(jobinfo->node_id, serialized_buf);
	PMIXP_UNPACKINT(jobinfo->node_id_job, serialized_buf);

	PMIXP_UNPACHOSTLIST(jobinfo->job_hl, serialized_buf);
	PMIXP_UNPACHOSTLIST(jobinfo->step_hl, serialized_buf);

	unpackstr_xmalloc(&jobinfo->hostname, &len, serialized_buf);
	PMIXP_UNPACKINT(jobinfo->node_tasks, serialized_buf);

	msize = jobinfo->node_tasks * sizeof(uint32_t);
	jobinfo->gtids = xmalloc(msize);
	for (i = 0; i < jobinfo->node_tasks; i++) {
		unpack32(&jobinfo->gtids[i], serialized_buf);
	}

	unpackstr_xmalloc(&jobinfo->task_map_packed, &len, serialized_buf);
	PMIXP_UNPACKINT(jobinfo->timeout, serialized_buf);
	unpackstr_xmalloc(&jobinfo->cli_tmpdir, &len, serialized_buf);
	unpackstr_xmalloc(&jobinfo->cli_tmpdir_base, &len, serialized_buf);
	unpackstr_xmalloc(&jobinfo->lib_tmpdir, &len, serialized_buf);
	unpackstr_xmalloc(&jobinfo->server_addr_unfmt, &len, serialized_buf);
	unpackstr_xmalloc(&jobinfo->spool_dir, &len, serialized_buf);
	PMIXP_UNPACKINT(jobinfo->uid, serialized_buf);
	PMIXP_UNPACKINT(jobinfo->gid, serialized_buf);
}
