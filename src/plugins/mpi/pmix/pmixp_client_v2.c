/*****************************************************************************\
 **  pmix_client_v2.c - PMIx v2 client communication code
 *****************************************************************************
 *  Copyright (C) 2014-2015 Artem Polyakov. All rights reserved.
 *  Copyright (C) 2015-2018 Mellanox Technologies. All rights reserved.
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
#include <pmix_server.h>

#include "slurm.h"

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
    pid_t child_pid;


	PMIXP_DEBUG("called");

    {
        int delay = 0;
        while (delay) {
            sleep(1);
        }
    }

    child_pid = fork();
    if (child_pid < 0) {
        error("mpi/pmix: failed to fork srun");
        if (NULL != cbfunc) {
            cbfunc(PMIX_ERROR, NULL, cbdata);
        }
        return SLURM_ERROR;
    } else if (child_pid == 0) { /* child */
        char **argv = NULL;
        xrealloc(argv, 6 * sizeof(char *));
        int j = 0, i;
        argv[j ++] = "srun";
        argv[j ++] = "--mpi=pmix";
        argv[j ++] = "--ntasks=1";
        argv[j ++] = "--slurmd-debug=4";
        argv[j ++] = "/home/user/pmix/v3.0/test/simple/.libs/simpclient";
        argv[j ++] = NULL;

        {
            PMIXP_DEBUG("mpi/mpix: to execve");
            for (i = 0; i < j; i ++) {
                PMIXP_DEBUG("mpi/pmix:   argv[%d]=%s", i, argv[i]);
            }
        }

        execv(SLURM_PREFIX"/bin/srun", argv);
        error("mpi/pmi2: failed to exec srun: %m");
        return SLURM_ERROR;

    } else {
        PMIXP_DEBUG("Spawn child pid: %d", child_pid);
        if (NULL != cbfunc) {
            cbfunc(PMIX_SUCCESS, strdup(proc->nspace), cbdata);
        }
        return SLURM_SUCCESS;
    }

    return PMIX_SUCCESS;
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

static pmix_status_t _allocate_fn(const pmix_proc_t *client,
                                pmix_alloc_directive_t directive,
                                const pmix_info_t info[], size_t ninfo,
                                pmix_info_cbfunc_t cbfunc, void *cbdata)
{
    int rc = PMIX_SUCCESS;
    job_desc_msg_t job_req;
    resource_allocation_response_msg_t *job_resp;
    //pmixp_alloc_t alloc;
    //char *alloc_id = NULL;
    uint64_t num_nodes = 0, num_cpus = 0;
    uint32_t alloc_time = 0, jobid;

    //alloc.user_id = pmixp_info_jobuid();
    //alloc.group_id = pmixp_info_jobgid();

    PMIXP_DEBUG("called");

    /* check the info keys */
    if (info) {
        int i;
        for (i = 0; i < ninfo; i++) {
            if (0 == strncmp(info[i].key, PMIX_ALLOC_ID, PMIX_MAX_KEYLEN)) {
                //alloc_id = info[i].value.data.string;
                continue;
            }
            if (0 == strncmp(info[i].key, PMIX_ALLOC_NUM_NODES, PMIX_MAX_KEYLEN)) {
                num_nodes = info[i].value.data.uint64;
                continue;
            }
            if (0 == strncmp(info[i].key, PMIX_ALLOC_NUM_CPUS, PMIX_MAX_KEYLEN)) {
                num_cpus = info[i].value.data.uint64;
                continue;
            }
            if (0 == strncmp(info[i].key, PMIX_ALLOC_TIME, PMIX_MAX_KEYLEN)) {
                alloc_time = info[i].value.data.uint32;
                continue;
            }
            if (0 == strncmp(info[i].key, PMIX_USERID, PMIX_MAX_KEYLEN)) {
                //alloc.user_id = info[i].value.data.uint32;
                continue;
            }
            if (0 == strncmp(info[i].key, PMIX_GRPID, PMIX_MAX_KEYLEN)) {
                //alloc.group_id = info[i].value.data.uint32;
                continue;
            }
        }
    }

    switch (directive) {
        case PMIX_ALLOC_NEW:
            break;
        case PMIX_ALLOC_EXTEND:
            break;
        case PMIX_ALLOC_RELEASE:
            break;
        case PMIX_ALLOC_REAQUIRE:
            break;
        default:
            rc = PMIX_ERROR;
            goto fini;
    }

    /* Create a job allocation */
    slurm_init_job_desc_msg( &job_req );
    job_req.min_nodes  = num_nodes;
    job_req.max_nodes  = num_nodes;
    if (num_cpus) {
        job_req.min_cpus = num_cpus;
        job_req.max_cpus = num_cpus;
    }
    //job_req.user_id = user_id;
    //job_req.group_id = group_id;
    if (alloc_time) {
        job_req.time_limit = alloc_time;
    }

    xstrfmtcat(job_req.dependency, "expand:%u", pmixp_info_jobid());
    if (slurm_allocate_resources(&job_req, &job_resp)) {
        xfree(job_req.dependency);
        rc = PMIX_ERROR;
        goto fini;
    }
    jobid = job_resp->job_id;

    slurm_init_job_desc_msg(&job_req);
    job_req.job_id = jobid;
    job_req.min_nodes = 0;
    slurm_update_job(&job_req);


    slurm_kill_job(jobid, 9, 0);
    xfree(job_req.dependency);

    slurm_init_job_desc_msg(&job_req);
    job_req.job_id = pmixp_info_jobid();
    job_req.min_nodes = INFINITE;
    slurm_update_job(&job_req);

fini:
    if (NULL != cbfunc) {
        cbfunc(rc, NULL, 0, cbdata, NULL, NULL);
    }

    return rc;
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
    .job_control = _job_control,
    .allocate = _allocate_fn
};

int pmixp_lib_init(void)
{
	pmix_info_t *kvp = NULL;
	pmix_status_t rc;

	PMIXP_INFO_ADD(kvp, PMIX_USERID, uint32_t, pmixp_info_jobuid());

#ifdef PMIX_SERVER_TMPDIR
	PMIXP_INFO_ADD(kvp, PMIX_SERVER_TMPDIR, string,
		       pmixp_info_tmpdir_lib());
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
