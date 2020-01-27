
#include "config.h"

#include <stdio.h>
#include <unistd.h>

#include "../pmixp_common.h"
#include "../pmixp_debug.h"
#include "../pmixp_info.h"

pmix_jobinfo_t _pmixp_job_info;

int main(int argc, char **argv)
{
	log_options_t lopts = LOG_OPTS_INITIALIZER;
	Buf buffer;
	uint32_t len;

	volatile int delay = 0;
	while(delay) sleep(1);

	log_init(argv[0], lopts, LOG_DAEMON, NULL);

	PMIXP_DEBUG("started");
	PMIXP_ERROR("started");

	safe_read(STDIN_FILENO, &len, sizeof(len));
	buffer = init_buf(len);
	PMIXP_DEBUG("config size %d", len);
	safe_read(STDIN_FILENO, get_buf_data(buffer), len);

	set_buf_offset(buffer, 0);

	pmixp_info_deserialize(buffer, &_pmixp_job_info);


rwfail:
	log_fini();
	return 0;
}
