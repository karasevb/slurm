/*****************************************************************************\
 **  pmix_coll.c - PMIx collective primitives
 *****************************************************************************
 *  Copyright (C) 2015-2017 Mellanox Technologies. All rights reserved.
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

#include "pmixp_common.h"
#include "src/common/slurm_protocol_api.h"
#include "pmixp_coll_ring.h"
#include "pmixp_nspaces.h"
#include "pmixp_server.h"
#include "pmixp_client.h"

static int _hostset_from_ranges(const pmixp_proc_t *procs, size_t nprocs,
				hostlist_t *hl_out)
{
	int i;
	hostlist_t hl = hostlist_create("");
	pmixp_namespace_t *nsptr = NULL;
	for (i = 0; i < nprocs; i++) {
		char *node = NULL;
		hostlist_t tmp;
		nsptr = pmixp_nspaces_find(procs[i].nspace);
		if (NULL == nsptr) {
			goto err_exit;
		}
		if (pmixp_lib_is_wildcard(procs[i].rank)) {
			tmp = hostlist_copy(nsptr->hl);
		} else {
			tmp = pmixp_nspace_rankhosts(nsptr, &procs[i].rank, 1);
		}
		while (NULL != (node = hostlist_pop(tmp))) {
			hostlist_push(hl, node);
			free(node);
		}
		hostlist_destroy(tmp);
	}
	hostlist_uniq(hl);
	*hl_out = hl;
	return SLURM_SUCCESS;
err_exit:
	hostlist_destroy(hl);
	return SLURM_ERROR;
}

static inline int _ring_prev_id(pmixp_coll_ring_t *coll)
{
	return (coll->my_peerid + coll->peers_cnt - 1) % coll->peers_cnt;
}

static inline int _ring_next_id(pmixp_coll_ring_t *coll)
{
	return (coll->my_peerid + 1) % coll->peers_cnt;
}

static pmixp_coll_ring_ctx_t * _shift_coll_ctx(pmixp_coll_ring_t *coll)
{
	uint32_t id = (coll->ctx_cur + 1) % PMIXP_COLL_RING_CTX_NUM;
	return &coll->ctx_array[id];
}

static inline pmixp_coll_ring_t *_ctx_get_coll(pmixp_coll_ring_ctx_t *coll_ctx)
{
	return (pmixp_coll_ring_t*)(coll_ctx->coll);
}

static void _msg_send_nb(pmixp_coll_ring_ctx_t *coll_ctx, uint32_t sender,
			 uint32_t hop_seq, char *data, size_t size)
{
	pmixp_coll_ring_msg_hdr_t hdr;
	pmixp_coll_ring_t *coll = _ctx_get_coll(coll_ctx);
	int nodeid = _ring_next_id(coll);
	hdr.nodeid = coll->my_peerid;
	hdr.msgsize = size;
	hdr.seq = coll_ctx->seq;
	hdr.hop_seq = hop_seq;
	hdr.contrib_id = sender;

	assert(PMIXP_COLL_RING_STATE_MAGIC != coll_ctx->magic);

#ifdef PMIXP_COLL_RING_DEBUG
	PMIXP_DEBUG("%p: coll=%d, hop=%d, size=%d, src=%d, contrib=%d",
		    coll_ctx, hdr.seq, hdr.hop_seq, hdr.msgsize, nodeid, hdr.contrib_id);
#endif
	/* TODO
	MPI_Isend((void*) &hdr, sizeof(msg_hdr_t), MPI_BYTE, nodeid, coll_ctx->seq, MPI_COMM_WORLD, &request);
	if (size) {
		MPI_Isend((void*) data, size, MPI_BYTE, nodeid, coll_ctx->seq, MPI_COMM_WORLD, &request);
	}
	*/
}

static void _coll_send_all(pmixp_coll_ring_ctx_t *coll_ctx)
{
	pmixp_coll_msg_ring_data_t *msg;
	assert(coll_ctx);

	while (!list_is_empty(coll_ctx->send_list)) {
		msg = list_dequeue(coll_ctx->send_list);
		_msg_send_nb(coll_ctx, msg->contrib_id, msg->hop_seq, msg->ptr, msg->size);
		/* double send test*/
		/*if (_rank == 1)
			_msg_send_nb(coll_ctx, msg->contrib_id, msg->hop_seq, msg->ptr, msg->size);
		*/
		free(msg);
	}
}


static void _reset_coll_ring(pmixp_coll_ring_ctx_t *coll_ctx)
{
	pmixp_coll_ring_t *coll = _ctx_get_coll(coll_ctx);
	//ring_cbfunc_t cbfunc;

	/* TODO
	cbfunc = coll->cbfunc;
	if (cbfunc) {
		cbfunc(coll);
	}
	*/
	coll_ctx->state = PMIXP_COLL_RING_SYNC;
	coll_ctx->contrib_local = false;
	coll_ctx->contrib_prev = 0;
	memset(coll->ctx->contrib_map, 0, sizeof(bool) * coll->peers_cnt);
	coll_ctx->seq += PMIXP_COLL_RING_CTX_NUM;
	set_buf_offset(coll_ctx->ring_buf, 0);
}

static void _progress_coll_ring(pmixp_coll_ring_ctx_t *coll_ctx)
{
	int ret = 0;
	pmixp_coll_ring_t *coll = _ctx_get_coll(coll_ctx);

	assert(PMIXP_COLL_RING_STATE_MAGIC == coll->magic);

	do {
		switch(coll_ctx->state) {
			case PMIXP_COLL_RING_SYNC:
				//LOG("PMIXP_COLL_RING_SYNC");
				if (coll_ctx->contrib_local || coll_ctx->contrib_prev) {
					coll_ctx->state = PMIXP_COLL_RING_COLLECT;
					ret = true;
				} else {
					ret = false;
				}
				break;
			case PMIXP_COLL_RING_COLLECT:
				ret = false;
				//LOG("PMIXP_COLL_RING_COLLECT");
				_coll_send_all(coll_ctx);
				if (!coll_ctx->contrib_local) {
					ret = false;
				} else if ((coll->peers_cnt - 1) == coll->ctx->contrib_prev) {
					coll_ctx->state = PMIXP_COLL_RING_DONE;
					ret = true;
					pmixp_debug_hang(0);
				}
				break;
			case PMIXP_COLL_RING_DONE:
				ret = false;
				_coll_send_all(coll->ctx);
				_reset_coll_ring(coll_ctx);
				/* shift to new coll */
				coll->ctx = _shift_coll_ctx(coll);
				coll->ctx_cur = coll->ctx->id;
				coll->ctx->state = PMIXP_COLL_RING_SYNC;
				/* send the all collected ring contribs for the new collective */
				break;
			default:
				PMIXP_ERROR("%p: unknown state = %d",
					    coll_ctx, (int)coll_ctx->state);

		}
	} while(ret);
}

int pmixp_coll_ring_init(pmixp_coll_ring_t *coll, const pmixp_proc_t *procs,
			 size_t nprocs)
{
	int i;
	pmixp_coll_ring_ctx_t *coll_ctx = NULL;
	hostlist_t hl;
#ifndef NDEBUG
	coll->magic = PMIXP_COLL_RING_STATE_MAGIC;
#endif
	if (SLURM_SUCCESS != _hostset_from_ranges(procs, nprocs, &hl)) {
		/* TODO: provide ranges output routine */
		PMIXP_ERROR("Bad ranges information");
		goto err_exit;
	}

	coll->ctx_cur = 0;
	coll->ctx = &coll->ctx_array[coll->ctx_cur];
	coll->peers_cnt = hostlist_count(hl);
	coll->my_peerid = hostlist_find(hl, pmixp_info_hostname());

	for (i = 0; i < PMIXP_COLL_RING_CTX_NUM; i++) {
		coll_ctx = &coll->ctx_array[i];
		coll_ctx->seq = i;
		coll_ctx->contrib_local = false;
		coll_ctx->contrib_prev = 0;
		coll_ctx->ring_buf = create_buf(NULL, 0);
		// TODO pmixp_server_buf_new();
		coll_ctx->state = PMIXP_COLL_RING_SYNC;
		coll_ctx->send_list = list_create(NULL);
		coll_ctx->contrib_map = xmalloc(sizeof(bool) * nprocs);
		memset(coll_ctx->contrib_map, 0, sizeof(bool) * nprocs);
		slurm_mutex_init(&coll_ctx->lock);
	}

	return SLURM_SUCCESS;
err_exit:
	return SLURM_ERROR;
}

int pmixp_coll_ring_contrib_local(pmixp_coll_ring_t *coll, char *data, size_t size,
				  void *cbfunc, void *cbdata)
{
	int ret = SLURM_SUCCESS;

	pmixp_coll_msg_ring_data_t *msg;
	assert(coll->ctx);
	pmixp_coll_ring_ctx_t *coll_ctx = coll->ctx;

	/* sanity check */
	pmixp_coll_ring_sanity_check(coll_ctx);

	/* lock the structure */
	slurm_mutex_lock(&coll->ctx->lock);

	if (coll->ctx->contrib_local) {
		/* Double contribution - reject */
		ret = SLURM_ERROR;
		slurm_mutex_unlock(&coll->ctx->lock);
		goto exit;
	}

	/* save & mark local contribution */
	if (!size_buf(coll_ctx->ring_buf)) {
		pmixp_server_buf_reserve(coll_ctx->ring_buf, size * coll->peers_cnt);
	} else if(remaining_buf(coll_ctx->ring_buf) < size) {
		/* grow sbuf size to 15% */
		size_t new_size = size_buf(coll_ctx->ring_buf) * 0.15 + size_buf(coll_ctx->ring_buf);
		pmixp_server_buf_reserve(coll_ctx->ring_buf, new_size);
	}

	memcpy(get_buf_data(coll_ctx->ring_buf) + get_buf_offset(coll_ctx->ring_buf),
	       data, size);

	msg = malloc(sizeof(pmixp_coll_ring_t));
	msg->ptr = get_buf_data(coll_ctx->ring_buf) + get_buf_offset(coll_ctx->ring_buf);
	msg->size = size;
	msg->contrib_id = coll->my_peerid;
	msg->hop_seq = 0;
	list_enqueue(coll_ctx->send_list, msg);

	set_buf_offset(coll_ctx->ring_buf, get_buf_offset(coll_ctx->ring_buf) + size);

	coll_ctx->contrib_local = true;

	/* setup callback info */
	coll->cbfunc = cbfunc;
	coll->cbdata = cbdata;

	_progress_coll_ring(coll_ctx);

	/* unlock the structure */
	slurm_mutex_unlock(&coll_ctx->lock);
exit:
	return ret;
}

int pmixp_coll_ring_contrib_prev(pmixp_coll_ring_t *coll, pmixp_coll_ring_msg_hdr_t *hdr,
				 Buf buf)
{
	int ret = SLURM_SUCCESS;
	uint32_t size = 0;
	pmixp_coll_msg_ring_data_t *msg;
	char *data_src = NULL, *data_dst = NULL;
	pmixp_coll_ring_ctx_t *coll_ctx = coll->ctx;
	uint32_t expexted_seq;

	// TODO sanity check

	/* lock the structure */
	slurm_mutex_lock(&coll->ctx->lock);

	if (hdr->nodeid != _ring_prev_id(coll)) {
		PMIXP_ERROR("%p: unexpected peerid=%d, expect=%d",
			    coll, hdr->nodeid, _ring_prev_id(coll))
		goto exit;
	}

	/* shift the coll context if that contrib belongs to the next coll  */
	if ((coll_ctx->seq +1) == hdr->seq) {
		coll_ctx = _shift_coll_ctx(coll);
	}

	/* compute the actual hops of ring: (src - dst + size) % size */
	expexted_seq = (coll->my_peerid + coll->peers_cnt - hdr->contrib_id) % coll->peers_cnt - 1;
	if (hdr->hop_seq != expexted_seq) {
		PMIXP_ERROR("%p: unexpected ring seq number=%d, expect=%d, coll seq=%d",
			    coll_ctx, hdr->hop_seq, expexted_seq, coll_ctx->seq);
		goto exit;
	}

	if (coll_ctx->contrib_map[hdr->contrib_id]) {
#ifdef PMIXP_COLL_RING_DEBUG
		PMIXP_DEBUG("%p: double receiving was detected from %d, rejected",
			    coll_ctx, hdr->contrib_id);
#endif
		goto exit;
	}

	/* mark number of individual contributions */
	coll_ctx->contrib_map[hdr->contrib_id] = true;

	/* save & mark contribution */
	if (!size_buf(coll_ctx->ring_buf)) {
		pmixp_server_buf_reserve(coll_ctx->ring_buf, size * coll->peers_cnt);
	} else if(remaining_buf(coll_ctx->ring_buf) < size) {
		/* grow sbuf size to 15% */
		size_t new_size = size_buf(coll_ctx->ring_buf) * 0.15 + size_buf(coll_ctx->ring_buf);
		pmixp_server_buf_reserve(coll_ctx->ring_buf, new_size);
	}
	data_src = get_buf_data(buf) + get_buf_offset(buf);
	size = remaining_buf(buf);
	pmixp_server_buf_reserve(coll_ctx->ring_buf, size);
	data_dst = get_buf_data(coll_ctx->ring_buf) +
			get_buf_offset(coll_ctx->ring_buf);
	memcpy(data_dst, data_src, size);
	set_buf_offset(coll_ctx->ring_buf, get_buf_offset(coll_ctx->ring_buf) + size);

	/* set to transit ring contriburion */
	if (hdr->contrib_id != _ring_next_id(coll)) {
		msg = xmalloc(sizeof(pmixp_coll_ring_t));
		msg->size = size;
		msg->ptr = data_dst;
		msg->contrib_id = hdr->contrib_id;
		/* increasing hop sequence */
		msg->hop_seq = hdr->hop_seq +1;
		list_enqueue(coll->ctx->send_list, msg);
	}
	/* increase number of ring contributions */
	coll_ctx->contrib_prev++;

	/* ring coll progress */
	_progress_coll_ring(coll_ctx);
exit:
	/* unlock the structure */
	slurm_mutex_unlock(&coll_ctx->lock);
	free_buf(buf);
	return ret;
}
