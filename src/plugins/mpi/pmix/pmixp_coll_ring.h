/*****************************************************************************\
 **  pmix_coll_ring.h - PMIx collective primitives
 *****************************************************************************
 *  Copyright (C) 2018      Mellanox Technologies. All rights reserved.
 *  Written by Boris Karasev <karasev.b@gmail.com, boriska@mellanox.com>.
 *
 *  This file is part of SLURM, a resource management program.
 *  For details, see <https://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  SLURM is free software; you can redistribute it and/or modify it under
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
 *  SLURM is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with SLURM; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
 \*****************************************************************************/

#ifndef PMIXP_COLL_RING_H
#define PMIXP_COLL_RING_H
#include "pmixp_common.h"
#include "pmixp_debug.h"
#include "pmixp_coll_common.h"
#include "pmixp_debug.h"

#define PMIXP_COLL_RING_DEBUG 1

#define PMIXP_COLL_RING_CTX_NUM 2

#ifndef NDEBUG
#define PMIXP_COLL_RING_STATE_MAGIC 0xC011CAFE
#endif

typedef enum {
	PMIXP_COLL_RING_SYNC,
	PMIXP_COLL_RING_COLLECT,
	PMIXP_COLL_RING_DONE,
} pmixp_coll_ring_state_t;

struct pmixp_coll_ring_s;

typedef struct {
#ifndef NDEBUG
	int magic;
#endif
	/* ptr to coll data */
	struct pmixp_coll_ring_s *coll;

	/* context data */
	uint32_t id;
	uint32_t seq;
	bool contrib_local;
	uint32_t contrib_prev;
	bool *contrib_map;
	pmixp_coll_ring_state_t state;
	Buf ring_buf;
	List fwrd_buf_pool;
	pthread_mutex_t lock;

	/* timestamp for stale collectives detection */
	time_t ts;
} pmixp_coll_ring_ctx_t;

typedef struct pmixp_coll_ring_s {
#ifndef NDEBUG
	int magic;
#endif
	/* general collective data */
	int my_peerid;
	int peers_cnt;
	pmixp_coll_general_t *cinfo;

	/* coll contexts data */
	uint32_t ctx_cur;
	pmixp_coll_ring_ctx_t *ctx;
	pmixp_coll_ring_ctx_t ctx_array[PMIXP_COLL_RING_CTX_NUM];

	/* libpmix callback data */
	void *cbfunc;
	void *cbdata;
} pmixp_coll_ring_t;

typedef struct {
	uint32_t type;
	uint32_t contrib_id;
	uint32_t seq;
	uint32_t hop_seq;
	uint32_t nodeid;
	size_t msgsize;
} pmixp_coll_ring_msg_hdr_t;

typedef struct {
	size_t size;
	char *ptr;
	uint32_t contrib_id;
	uint32_t hop_seq;
} pmixp_coll_msg_ring_data_t;

inline static char *
pmixp_coll_ring_state2str(pmixp_coll_ring_state_t state)
{
	switch (state) {
	case PMIXP_COLL_RING_SYNC:
		return "COLL_RING_SYNC";
	case PMIXP_COLL_RING_COLLECT:
		return "COLL_RING_COLLECT";
	case PMIXP_COLL_RING_DONE:
		return "COLL_RING_DONE";
	default:
		return "COLL_UNKNOWN";
	}
}

static inline void pmixp_coll_ring_sanity_check(pmixp_coll_ring_ctx_t *coll_ctx)
{
	xassert(NULL != coll_ctx->coll);
#ifndef NDEBUG
	xassert(coll_ctx->magic == PMIXP_COLL_RING_STATE_MAGIC);
#endif
}


int pmixp_coll_ring_init(pmixp_coll_ring_t *coll, const pmixp_proc_t *procs,
	     size_t nprocs, pmixp_coll_general_t *cinfo);
void pmixp_coll_ring_free(pmixp_coll_ring_t *coll);
/*
static inline int pmixp_coll_ctx_check_seq(pmixp_coll_ring_ctx_t *coll, uint32_t seq);
*/
int pmixp_coll_ring_contrib_local(pmixp_coll_ring_t *coll, char *data, size_t size,
				  void *cbfunc, void *cbdata);
int pmixp_coll_ring_contrib_prev(pmixp_coll_ring_t *coll, pmixp_coll_ring_msg_hdr_t *hdr,
				 Buf buf);
void pmixp_coll_ring_reset(pmixp_coll_ring_ctx_t *coll);
int pmixp_coll_ring_unpack_info(Buf buf, pmixp_coll_type_t *type,
                pmixp_coll_ring_msg_hdr_t *ring_hdr,
                pmixp_proc_t **r, size_t *nr);
void pmixp_coll_ring_reset_if_to(pmixp_coll_ring_t *coll, time_t ts);

#endif // PMIXP_COLL_RING_H
