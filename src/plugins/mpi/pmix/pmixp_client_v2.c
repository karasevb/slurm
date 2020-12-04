/*****************************************************************************\
 **  pmix_client_v2.c - PMIx v2 client communication code
 *****************************************************************************
 *  Copyright (C) 2014-2015 Artem Polyakov. All rights reserved.
 *  Copyright (C) 2015-2020 Mellanox Technologies. All rights reserved.
 *  Written by Artem Polyakov <artpol84@gmail.com, artemp@mellanox.com>,
 *             Boris Karasev <karasev.b@gmail.com, boriska@mellanox.com>.
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

#include "pmixp_common.h"
#include "pmixp_state.h"
#include "pmixp_io.h"
#include "pmixp_nspaces.h"
#include "pmixp_debug.h"
#include "pmixp_coll.h"
#include "pmixp_server.h"
#include "pmixp_dmdx.h"
#include "pmixp_client.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <pmix.h>
#include <pmix_server.h>

#if (HAVE_PMIX_VER >= 4)
typedef struct {
	pmix_status_t rc;
	volatile int active;
	char *base64_setup_str;
} setup_app_caddy_t;
#endif

static int _client_connected(const pmix_proc_t *proc, void *server_object,
			     pmix_op_cbfunc_t cbfunc, void *cbdata)
{
	/* we don't do anything by now */
	return PMIX_SUCCESS;
}

static void _op_callbk(pmix_status_t status, void *cbdata)
{
	PMIXP_DEBUG("op callback is called with status=%d", status);
}

static void _errhandler_reg_callbk(pmix_status_t status,
				   size_t errhandler_ref, void *cbdata)
{
	PMIXP_DEBUG("Error handler registration callback is called with status=%d, ref=%d",
		    status, (int)errhandler_ref);
}

static pmix_status_t _client_finalized(const pmix_proc_t *proc,
				       void *server_object,
				       pmix_op_cbfunc_t cbfunc,
				       void *cbdata)
{
	/* don'n do anything by now */
	if (NULL != cbfunc) {
		cbfunc(PMIX_SUCCESS, cbdata);
	}
	return PMIX_SUCCESS;
}

static pmix_status_t _abort_fn(const pmix_proc_t *proc, void *server_object,
			       int status, const char msg[],
			       pmix_proc_t procs[], size_t nprocs,
			       pmix_op_cbfunc_t cbfunc, void *cbdata)
{
	/* Just kill this stepid for now. Think what we can do for FT here? */
	PMIXP_DEBUG("called: status = %d, msg = %s", status, msg);
	slurm_kill_job_step(pmixp_info_jobid(), pmixp_info_stepid(), SIGKILL);

	if (NULL != cbfunc) {
		cbfunc(PMIX_SUCCESS, cbdata);
	}
	return PMIX_SUCCESS;
}

static pmix_status_t _fencenb_fn(const pmix_proc_t procs_v2[], size_t nprocs,
				 const pmix_info_t info[], size_t ninfo,
				 char *data, size_t ndata,
				 pmix_modex_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");
	int ret;
	size_t i;
	pmixp_proc_t *procs = xmalloc(sizeof(*procs) * nprocs);
	bool collect = false;

	for (i = 0; i < nprocs; i++) {
		procs[i].rank = procs_v2[i].rank;
		strncpy(procs[i].nspace, procs_v2[i].nspace, PMIXP_MAX_NSLEN);
	}
	/* check the info keys */
	if (info) {
		for (i = 0; i < ninfo; i++) {
			if (0 == strncmp(info[i].key, PMIX_COLLECT_DATA, PMIX_MAX_KEYLEN)) {
				collect = true;
				break;
			}
		}
	}
	ret = pmixp_lib_fence(procs, nprocs, collect, data, ndata, cbfunc, cbdata);
	xfree(procs);

	return ret;
}

static pmix_status_t _dmodex_fn(const pmix_proc_t *proc,
				const pmix_info_t info[], size_t ninfo,
				pmix_modex_cbfunc_t cbfunc, void *cbdata)
{
	int rc;
	PMIXP_DEBUG("called");

	rc = pmixp_dmdx_get(proc->nspace, proc->rank, cbfunc, cbdata);

	return (SLURM_SUCCESS == rc) ? PMIX_SUCCESS : PMIX_ERROR;
}

static pmix_status_t _job_control(const pmix_proc_t *proct,
                                  const pmix_proc_t targets[], size_t ntargets,
                                  const pmix_info_t directives[], size_t ndirs,
                                  pmix_info_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");
	return PMIX_ERR_NOT_SUPPORTED;
}

static pmix_status_t _publish_fn(const pmix_proc_t *proc,
				 const pmix_info_t info[], size_t ninfo,
				 pmix_op_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");
	return PMIX_ERR_NOT_SUPPORTED;
}

static pmix_status_t _lookup_fn(const pmix_proc_t *proc, char **keys,
				const pmix_info_t info[], size_t ninfo,
				pmix_lookup_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");
	return PMIX_ERR_NOT_SUPPORTED;
}

static pmix_status_t _unpublish_fn(const pmix_proc_t *proc, char **keys,
				   const pmix_info_t info[], size_t ninfo,
				   pmix_op_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");
	return PMIX_ERR_NOT_SUPPORTED;
}

static pmix_status_t _spawn_fn(const pmix_proc_t *proc,
			       const pmix_info_t job_info[], size_t ninfo,
			       const pmix_app_t apps[], size_t napps,
			       pmix_spawn_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");
	return PMIX_ERR_NOT_SUPPORTED;
}

static pmix_status_t _connect_fn(const pmix_proc_t procs[], size_t nprocs,
				 const pmix_info_t info[], size_t ninfo,
				 pmix_op_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");
	return PMIX_ERR_NOT_SUPPORTED;
}

static pmix_status_t _disconnect_fn(const pmix_proc_t procs[], size_t nprocs,
				    const pmix_info_t info[], size_t ninfo,
				    pmix_op_cbfunc_t cbfunc, void *cbdata)
{
	PMIXP_DEBUG("called");
	return PMIX_ERR_NOT_SUPPORTED;
}

static void _errhandler(size_t evhdlr_registration_id,
			pmix_status_t status,
			const pmix_proc_t *source,
			pmix_info_t info[], size_t ninfo,
			pmix_info_t *results, size_t nresults,
			pmix_event_notification_cbfunc_fn_t cbfunc,
			void *cbdata)
{
	/* TODO: do something more sophisticated here */
	/* FIXME: use proper specificator for nranges */
	PMIXP_ERROR_STD("Error handler invoked: status = %d",
			status);
	slurm_kill_job_step(pmixp_info_jobid(), pmixp_info_stepid(), SIGKILL);
}

static pmix_server_module_t slurm_pmix_cb = {
	.client_connected = _client_connected,
	.client_finalized = _client_finalized,
	.abort = _abort_fn,
	.fence_nb = _fencenb_fn,
	.direct_modex = _dmodex_fn,
	.publish = _publish_fn,
	.lookup = _lookup_fn,
	.unpublish = _unpublish_fn,
	.spawn = _spawn_fn,
	.connect = _connect_fn,
	.disconnect = _disconnect_fn,
	.job_control = _job_control
};

int pmixp_lib_init(uint32_t jobuid, char *tmpdir)
{
	pmix_info_t *kvp = NULL;
	pmix_status_t rc;
#ifdef PMIX_SERVER_SCHEDULER
	bool flag = 1;
#endif

	PMIXP_KVP_ADD(kvp, PMIX_USERID, &jobuid, PMIX_UINT32);
#ifdef PMIX_SERVER_TMPDIR
	PMIXP_KVP_ADD(kvp, PMIX_SERVER_TMPDIR, tmpdir, PMIX_STRING);
#endif
#ifdef PMIX_SERVER_SCHEDULER
	PMIXP_KVP_ADD(kvp, PMIX_SERVER_SCHEDULER,
		      &flag, PMIX_BOOL);
#endif

	/* setup the server library */
	if (PMIX_SUCCESS != (rc = PMIx_server_init(&slurm_pmix_cb, kvp,
						   PMIXP_INFO_SIZE(kvp)))) {
		PMIXP_ERROR_STD("PMIx_server_init failed with error %d\n", rc);
		return SLURM_ERROR;
	}

	PMIXP_FREE_KEY(kvp);
	/* register the errhandler */
	PMIx_Register_event_handler(NULL, 0, NULL, 0, _errhandler,
				    _errhandler_reg_callbk, NULL);

	return SLURM_SUCCESS;
}

int pmixp_lib_finalize(void)
{
	int rc = SLURM_SUCCESS;
	/* deregister the errhandler */
	PMIx_Deregister_event_handler(0, _op_callbk, NULL);

	if (PMIX_SUCCESS != PMIx_server_finalize()) {
		rc = SLURM_ERROR;
	}
	return rc;
}

#if (HAVE_PMIX_VER < 4)
int pmixp_lib_srun_init(const mpi_plugin_client_info_t *job, char ***env)
{
	return SLURM_SUCCESS;
}

int pmixp_lib_srun_finalize(void)
{
	return SLURM_SUCCESS;
}

int pmixp_libpmix_local_setup(char ***env)
{
	return SLURM_SUCCESS;
}

#else
static void pmixp_wait_active(volatile int *active)
{
	struct timespec ts;

	while (1) {
		ts.tv_sec = 0;
		ts.tv_nsec = 10;

		if (!(*active)) {
			break;
		}
		nanosleep(&ts, NULL);
	}
}

static void _setup_app_cb(pmix_status_t status,
			  pmix_info_t info[], size_t ninfo,
			  void *provided_cbdata,
			  pmix_op_cbfunc_t cbfunc, void *cbdata)
{
	setup_app_caddy_t *caddy = (setup_app_caddy_t*)provided_cbdata;
	pmix_data_buffer_t pbkt;
	pmix_byte_object_t pbo;
	size_t len;
	int rc;

	if (!ninfo) {
		/* nothing to do */
		goto fast_exit;
	}

	PMIX_DATA_BUFFER_CONSTRUCT(&pbkt);

	rc = PMIx_Data_pack(NULL, &pbkt, &ninfo, 1, PMIX_SIZE);
	if (rc != PMIX_SUCCESS) {
		status = rc;
		goto err_exit;
	}
	rc = PMIx_Data_pack(NULL, &pbkt, info, ninfo, PMIX_INFO);
	if (rc != PMIX_SUCCESS) {
		status = rc;
		goto err_exit;
	}

	PMIX_DATA_BUFFER_UNLOAD(&pbkt, pbo.bytes, pbo.size);
	caddy->base64_setup_str =
			pmixp_base64_encode((char *)pbo.bytes, pbo.size, &len);

err_exit:
	PMIX_DATA_BUFFER_DESTRUCT(&pbkt);
fast_exit:
	caddy->active = 0;

	if (NULL != cbfunc) {
	    cbfunc(status, cbdata);
	}
	caddy->rc = status;
}

static void _setup_loc_cb(pmix_status_t status, void *cbdata)
{
	volatile int *active = (volatile int *)cbdata;
	*active = 0;
}

int pmixp_libpmix_setup_local_app(char ***env)
{
	char *p = NULL;
	char *base64_setup_str = NULL;
	pmix_info_t *info;
	int32_t ninfo, cnt;
	size_t len;
	pmix_data_buffer_t pbuf;
	void *buf;
	int rc, i;
	volatile int active;
	char *env_name = NULL;

	i = 0;
	xstrfmtcat(env_name, PMIXP_SERVER_SETUP_APP_FMT, i);
	p = getenvp(*env, env_name);
	if (!p) {
		xfree(env_name);
		return SLURM_SUCCESS;
	}
	do {
		i++;
		xstrcat(base64_setup_str, p);
		unsetenvp(*env, env_name);
		xfree(env_name);
		xstrfmtcat(env_name, PMIXP_SERVER_SETUP_APP_FMT, i);
		p = getenvp(*env, env_name);
	} while (p);
	xfree(env_name);

	PMIX_DATA_BUFFER_CONSTRUCT(&pbuf);
	buf = pmixp_base64_decode(base64_setup_str,
				  strlen(base64_setup_str), &len);
	xfree(base64_setup_str);
	PMIX_DATA_BUFFER_LOAD(&pbuf, buf, len);

	cnt = 1;
	rc = PMIx_Data_unpack(NULL, &pbuf, &ninfo, &cnt, PMIX_SIZE);
	if (rc != PMIX_SUCCESS) {
		PMIXP_ERROR("ninfo unpack failed");
		rc = SLURM_ERROR;
		goto err_exit;
	}
	PMIX_INFO_CREATE(info, ninfo);
	rc = PMIx_Data_unpack(NULL, &pbuf, info, &ninfo, PMIX_INFO);
	if (rc != PMIX_SUCCESS) {
		PMIXP_ERROR("info unpack failed");
		rc = SLURM_ERROR;
		goto err_exit;
	}

	active = 1;
	rc = PMIx_server_setup_local_support(pmixp_info_namespace(),
					     info, ninfo,
					     _setup_loc_cb, (void*)&active);
	if (rc == PMIX_OPERATION_SUCCEEDED) {
		active = 0;
	} else if (rc != PMIX_SUCCESS) {
		PMIXP_ERROR("PMIx_server_setup_local_support() failed, rc = %d",
			    rc);
		rc = SLURM_ERROR;
		goto err_exit;
	}
	pmixp_wait_active(&active);

	rc = SLURM_SUCCESS;
err_exit:
	return rc;
}

extern int pmixp_srun_libpmix_init(void)
{
	int rc;
	mode_t rights = (S_IRUSR | S_IWUSR | S_IXUSR) |
			(S_IRGRP | S_IWGRP | S_IXGRP);

	if (0 != (rc = pmixp_mkdir(pmixp_srun_tmpdir_lib(),
				   rights, getuid()))) {
		PMIXP_ERROR_STD("Cannot create srun pmix lib tmpdir: \"%s\"",
				pmixp_srun_tmpdir_lib());
		return errno;
	}

	rc = pmixp_lib_init(getuid(), pmixp_srun_tmpdir_lib());
	if (SLURM_SUCCESS != rc) {
		PMIXP_ERROR_STD("PMIx_server_init failed with error %d\n", rc);
		return SLURM_ERROR;
	}

	return SLURM_SUCCESS;
}
int pmixp_srun_libpmix_finalize(void)
{
	int rc, rc1;

	rc = pmixp_lib_finalize();

	rc1 = pmixp_rmdir_recursively(pmixp_srun_tmpdir_lib());
	if (0 != rc1) {
		PMIXP_ERROR_STD("Failed to remove %s\n",
				pmixp_srun_tmpdir_lib());
		/* Not considering this as fatal error */
	}

	return rc;
}

int pmixp_libpmix_setup_application(const mpi_plugin_client_info_t *job,
				    char ***env)
{
	char nspace[PMIXP_MAX_NSLEN];
	const uint32_t task_cnt = job->step_layout->task_cnt;
	hostlist_t strp_hl;
	char *node_map = NULL, *proc_map = NULL;
	uint32_t *task_map, *task_cnts;
	int rc, i;
	pmix_info_t *info;
	int ninfo = 0;
	setup_app_caddy_t *setup_app_caddy = NULL;
	char *mapping;

	PMIX_INFO_CREATE(info, 2);

	pmixp_info_gen_nspace(job->step_id.job_id, job->step_id.job_id, nspace);
	strp_hl = hostlist_create(job->step_layout->node_list);
	if (strp_hl == NULL){
		rc = SLURM_ERROR;
		goto err_exit;
	}

	/* generate PMIX_NODE_MAP */
	node_map = pmixp_info_get_node_map(strp_hl);
	if (NULL == node_map) {
		rc = SLURM_ERROR;
		goto err_exit;
	}

	PMIX_INFO_LOAD(&info[ninfo], PMIX_NODE_MAP, node_map, PMIX_STRING);
	ninfo++;
	node_map = NULL;

	/* generate PMIX_PROC_MAP */
	mapping = pack_process_mapping(job->step_layout->node_cnt,
				       job->step_layout->task_cnt,
				       job->step_layout->tasks,
				       job->step_layout->tids);
	task_map = unpack_process_mapping_flat(mapping,
					       job->step_layout->node_cnt,
					       job->step_layout->task_cnt,
					       NULL);
	xfree(mapping);
	if (task_map == NULL) {
		rc = SLURM_ERROR;
		goto err_exit;
	}
	task_cnts = xmalloc(sizeof(uint32_t) * task_cnt);
	for (i = 0; i < task_cnt; i++) {
		task_cnts[i] = job->step_layout->tasks[i];
	}
	proc_map = pmixp_info_get_proc_map(strp_hl, job->step_layout->node_cnt,
					   task_cnt, task_cnts, task_map);
	xfree(task_cnts);
	if (NULL == proc_map) {
		rc = SLURM_ERROR;
		goto err_exit;
	}
	PMIX_INFO_LOAD(&info[ninfo], PMIX_PROC_MAP, proc_map, PMIX_STRING);
	ninfo++;
	proc_map = NULL;

	/* invoke PMIx_server_setup_application */
	setup_app_caddy = xmalloc(sizeof(setup_app_caddy_t));
	setup_app_caddy->base64_setup_str = NULL;
	setup_app_caddy->active = 1;

	rc = PMIx_server_setup_application(nspace, info, ninfo,
					   _setup_app_cb, setup_app_caddy);
	if (PMIX_SUCCESS != rc) {
		PMIXP_ERROR_STD("PMIx_server_setup_application failed with error: %d",
				rc);
		rc = SLURM_ERROR;
		goto err_exit;
	}
	pmixp_wait_active(&setup_app_caddy->active);

	if (PMIX_SUCCESS != setup_app_caddy->rc) {
		PMIXP_ERROR("PMIx_server_setup_application callback function failed with error: %d",
			    setup_app_caddy->rc);
		rc = SLURM_ERROR;
		goto err_exit;
	}

	if (setup_app_caddy->base64_setup_str) {
		int size = strlen(setup_app_caddy->base64_setup_str);
		char *chunk_value = (char *)xmalloc(slurm_env_get_val_maxlen(NULL));
		char *env_name = NULL;
		int i;
		int remain = size;
		int chunk_size, offset = 0;

		i = 0;
		while (remain) {
			xstrfmtcat(env_name, PMIXP_SERVER_SETUP_APP_FMT , i);
			if (remain > slurm_env_get_val_maxlen(env_name)) {
				chunk_size = slurm_env_get_val_maxlen(env_name);
			} else {
				chunk_size = remain;
			}

			memcpy(chunk_value, &setup_app_caddy->base64_setup_str[offset], chunk_size);
			chunk_value[chunk_size] = '\0';
			setenvf(env, env_name, "%s", chunk_value);
			remain -= chunk_size;
			offset += chunk_size;
			xfree(env_name);
			i++;
		}
		xfree(chunk_value);
	}

err_exit:
	if (task_map != NULL) {
		xfree(task_map);
	}
	if (node_map != NULL) {
		free(node_map);
	}
	if (proc_map != NULL) {
		free(proc_map);
	}
	if (setup_app_caddy) {
		if (setup_app_caddy->base64_setup_str) {
			xfree(setup_app_caddy->base64_setup_str);
		}
		xfree(setup_app_caddy);
	}

	return rc;
}
int pmixp_libpmix_local_setup(char ***env)
{
	return pmixp_libpmix_setup_local_app(env);
}

int pmixp_lib_srun_init(const mpi_plugin_client_info_t *job, char ***env)
{
	int rc;
	char *tmpdir_prefix = getenv(PMIXP_OS_TMPDIR_ENV);

	pmixp_srun_info_set(job, env);

	if (!tmpdir_prefix) {
		tmpdir_prefix = PMIXP_TMPDIR_DEFAULT;
	}
	xstrfmtcat(_pmixp_srun_info.proc_info.lib_tmpdir,
		   "%s/srun.slurm.pmix.%d.%d", tmpdir_prefix,
		   job->step_id.job_id, job->step_id.step_id);
	if (!_pmixp_srun_info.proc_info.lib_tmpdir) {
		PMIXP_ERROR("Cannot create srun pmix tmpdir");
		return SLURM_ERROR;
	}

	rc = pmixp_srun_libpmix_init();
	if (SLURM_SUCCESS != rc) {
		PMIXP_ERROR("pmixp_libpmix_init() failed");
		pmixp_lib_srun_finalize();
		return rc;
	}

	rc = pmixp_libpmix_setup_application(job, env);
	if (SLURM_SUCCESS != rc) {
		PMIXP_ERROR("pmixp_libpmix_setup_application() failed");
		return rc;
	}

	return SLURM_SUCCESS;
}

int pmixp_lib_srun_finalize()
{
	char *tmpdir;

	pmixp_srun_libpmix_finalize();
	tmpdir = pmixp_srun_tmpdir_lib();
	xfree(tmpdir);

	return SLURM_SUCCESS;
}
#endif
