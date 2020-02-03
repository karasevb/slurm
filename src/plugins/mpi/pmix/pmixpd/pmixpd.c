
#include "config.h"

#include <stdio.h>
#include <unistd.h>

#include <pmixp_common.h>
#include "pmixp_debug.h"
#include "pmixp_info.h"
#include "pmixp_agent.h"
#include "pmixp_server.h"
#include "pmixp_utils.h"

pmix_jobinfo_t _pmixp_job_info;

int main(int argc, char **argv, char* env[])
{
	log_options_t lopts = LOG_OPTS_INITIALIZER;
	Buf buffer;
	uint32_t len;
	FILE *logfp;
	task_env_t *task_env;
	int ret;
	stepd_step_rec_t *job; // not used,
	volatile int delay = 1;

	//while(delay) sleep(1);

	safe_read(STDIN_FILENO, &len, sizeof(len));
	buffer = init_buf(len);
	safe_read(STDIN_FILENO, get_buf_data(buffer), len);
	set_buf_offset(buffer, 0);
	pmixp_info_deserialize(buffer, &_pmixp_job_info);
	free_buf(buffer);

	log_init(argv[0], lopts, LOG_DAEMON, NULL);
	lopts.logfile_level = _pmixp_job_info.log_level;
	logfp = fdopen(_pmixp_job_info.log_fd, "a");
	log_alter_with_fp(lopts, LOG_DAEMON, logfp);

	PMIXP_DEBUG("started");

	if (SLURM_SUCCESS != (ret = pmixp_env_set(&env))) {
		PMIXP_ERROR("pmixp_env_set() failed");
		goto err_ext;
	}

	if (SLURM_SUCCESS != (ret = pmixp_stepd_init(job, &env))) {
		PMIXP_ERROR("pmixp_stepd_init() failed");
		goto err_ext;
	}
	if (SLURM_SUCCESS != (ret = pmixp_agent_start())) {
		PMIXP_ERROR("pmixp_agent_start() failed");
		goto err_ext;
	}
	task_env = xmalloc(sizeof(task_env_t) * (pmixp_info_tasks_loc() + 1));
	uint32_t localid;
	for (localid = 0; localid < pmixp_info_tasks_loc(); localid++) {
		uint32_t taskid = pmixp_info_taskid(localid);
		pmixp_lib_setup_fork(taskid, pmixp_info_namespace(),
				     &task_env[localid].env);
		task_env[localid].taskid = taskid;
		task_env[localid].cnt = envcount(task_env[localid].env);
		PMIXP_DEBUG("Patch environment for task %d", taskid);
	}

	pmixp_task_env_serialize(pmixp_info_tasks_loc(), task_env, &buffer);
	len = get_buf_offset(buffer);
	safe_write(STDOUT_FILENO, &len, sizeof(len));
	safe_write(STDOUT_FILENO, get_buf_data(buffer), len);
	free_buf(buffer);

err_ext:
rwfail:

	while(delay) sleep(1);

	log_fini();
	return 0;
}
