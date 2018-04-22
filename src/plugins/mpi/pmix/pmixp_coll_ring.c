/*****************************************************************************\
 **  pmix_coll_ring.c - PMIx collective primitives
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

#include "pmixp_common.h"
#include "src/common/slurm_protocol_api.h"
#include "pmixp_coll_common.h"
#include "pmixp_coll_ring.h"
#include "pmixp_nspaces.h"
#include "pmixp_server.h"
#include "pmixp_client.h"

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

typedef struct {
	pmixp_coll_ring_t *coll;
	uint32_t seq;
	Buf buf;
	volatile uint32_t refcntr;
} pmixp_coll_ring_cbdata_t;

static void _ring_sent_cb(int rc, pmixp_p2p_ctx_t ctx, void *_cbdata)
{
	pmixp_coll_ring_cbdata_t *cbdata = (pmixp_coll_ring_cbdata_t*)_cbdata;
	free_buf(cbdata->buf);
}

int pmixp_coll_ring_unpack_info(Buf buf, pmixp_coll_type_t *type,
				pmixp_coll_ring_msg_hdr_t *ring_hdr,
				pmixp_proc_t **r, size_t *nr)
{
	pmixp_proc_t *procs = NULL;
	uint32_t nprocs = 0;
	uint32_t tmp;
	int rc, i;

	/* 1. extract the type of collective */
	if (SLURM_SUCCESS != (rc = unpack32(&tmp, buf))) {
		PMIXP_ERROR("Cannot unpack collective type");
		return rc;
	}
	*type = tmp;

	/* 2. get the number of ranges */
	if (SLURM_SUCCESS != (rc = unpack32(&nprocs, buf))) {
		PMIXP_ERROR("Cannot unpack collective type");
		return rc;
	}
	*nr = nprocs;

	procs = xmalloc(sizeof(pmixp_proc_t) * nprocs);
	*r = procs;

	for (i = 0; i < (int)nprocs; i++) {
	/* 3. get namespace/rank of particular process */
		rc = unpackmem(procs[i].nspace, &tmp, buf);
		if (SLURM_SUCCESS != rc) {
			PMIXP_ERROR("Cannot unpack namespace for process #%d", i);
			return rc;
		}
		procs[i].nspace[tmp] = '\0';

		unsigned int tmp;
		rc = unpack32(&tmp, buf);
		procs[i].rank = tmp;
		if (SLURM_SUCCESS != rc) {
			PMIXP_ERROR("Cannot unpack ranks for process #%d, nsp=%s",
				    i, procs[i].nspace);
			return rc;
		}
	}

	/* 3. extract the ring info */
	if (SLURM_SUCCESS != (rc = unpackmem((char *)ring_hdr, &tmp, buf))) {
		PMIXP_ERROR("Cannot unpack ring info");
		return rc;
	}

	return SLURM_SUCCESS;
}

static int _pack_coll_ring_info(pmixp_coll_ring_t *coll,
		pmixp_coll_ring_msg_hdr_t *ring_hdr, Buf buf)
{
	pmixp_proc_t *procs = coll->cinfo->pset.procs;
	size_t nprocs = coll->cinfo->pset.nprocs;
	uint32_t type = PMIXP_COLL_TYPE_FENCE_RING;
	int i;

	/* 1. store the type of collective */
	pack32(type, buf);

	/* 2. Put the number of ranges */
	pack32(nprocs, buf);
	for (i = 0; i < (int)nprocs; i++) {
		/* Pack namespace */
		packmem(procs->nspace, strlen(procs->nspace) + 1, buf);
		pack32(procs->rank, buf);
	}

	/* 3. pack the ring header info */
	packmem((char*)ring_hdr, sizeof(pmixp_coll_ring_msg_hdr_t), buf);

	return SLURM_SUCCESS;
}

static void _ring_msg_send_nb(pmixp_coll_ring_ctx_t *coll_ctx,
			 pmixp_coll_msg_ring_data_t *msg)
{
	pmixp_coll_ring_msg_hdr_t hdr;
	pmixp_coll_ring_t *coll = _ctx_get_coll(coll_ctx);
	int next_nodeid = _ring_next_id(coll);
	hdr.nodeid = coll->my_peerid;
	hdr.msgsize = msg->size;
	hdr.seq = coll_ctx->seq;
	hdr.hop_seq = msg->hop_seq;
	hdr.contrib_id = msg->contrib_id;
	pmixp_ep_t ep = {0};
	pmixp_coll_ring_cbdata_t *cbdata = NULL;
	Buf buf = NULL;
	uint32_t offset = 0;
	size_t size = msg->size;
	void *data = msg->ptr;

	assert(PMIXP_COLL_RING_STATE_MAGIC == coll_ctx->magic);

#ifdef PMIXP_COLL_RING_DEBUG
	PMIXP_DEBUG("%p: hop=%d, size=%lu, next_nodeid=%d, contrib=%d",
	    coll->ctx, hdr.hop_seq, hdr.msgsize, next_nodeid, hdr.contrib_id);
#endif
	ep.type = PMIXP_EP_NOIDEID;
	ep.ep.nodeid = next_nodeid;

	buf = pmixp_server_buf_new();

	/* pack ring info */
	_pack_coll_ring_info(coll, &hdr, buf);

	/* insert payload to buf */
	offset = get_buf_offset(buf);
	pmixp_server_buf_reserve(buf, size);
	memcpy(buf->head + offset, data, size);
	set_buf_offset(buf, offset + size);

	cbdata = xmalloc(sizeof(pmixp_coll_ring_cbdata_t));
	cbdata->coll = coll;
	cbdata->seq = coll_ctx->seq;
	cbdata->refcntr = 1;
	cbdata->buf = buf;
	pmixp_server_send_nb(&ep, PMIXP_MSG_RING, coll_ctx->seq, buf, _ring_sent_cb, cbdata);
}

static void _ring_send_all(pmixp_coll_ring_ctx_t *coll_ctx)
{
	pmixp_coll_msg_ring_data_t *msg;
	assert(coll_ctx);

	while (!list_is_empty(coll_ctx->send_list)) {
		msg = list_dequeue(coll_ctx->send_list);
#ifdef PMIXP_COLL_RING_DEBUG
		PMIXP_DEBUG("%p: dequeue transit msg=%p, size=%lu",
			    coll_ctx, msg->ptr, msg->size);
#endif
		_ring_msg_send_nb(coll_ctx, msg);
		xfree(msg);
	}
}


static void _reset_coll_ring(pmixp_coll_ring_ctx_t *coll_ctx)
{
	pmixp_coll_ring_t *coll = _ctx_get_coll(coll_ctx);

#ifdef PMIXP_COLL_RING_DEBUG
	PMIXP_DEBUG("%p: called", coll_ctx);
#endif
	if (coll->cbfunc) {
		char *data = get_buf_data(coll_ctx->ring_buf);
		size_t size = get_buf_offset(coll_ctx->ring_buf);
		pmixp_lib_modex_invoke(coll->cbfunc, SLURM_SUCCESS, data, size,
				       coll->cbdata, NULL, NULL);
	}
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
				if (coll_ctx->contrib_local || coll_ctx->contrib_prev) {
					coll_ctx->state = PMIXP_COLL_RING_COLLECT;
					ret = true;
				} else {
					ret = false;
				}
				break;
			case PMIXP_COLL_RING_COLLECT:
				ret = false;
				_ring_send_all(coll_ctx);
				if (!coll_ctx->contrib_local) {
					ret = false;
				} else if ((coll->peers_cnt - 1) == coll->ctx->contrib_prev) {
					coll_ctx->state = PMIXP_COLL_RING_DONE;
					ret = true;
					pmixp_debug_hang(0);
					PMIXP_ERROR("%p: PMIXP_COLL_RING_COLLECT done", coll_ctx);
				}
				break;
			case PMIXP_COLL_RING_DONE:
				ret = false;
				_ring_send_all(coll_ctx);
				_reset_coll_ring(coll_ctx);
				/* shift to new coll */
				coll->ctx = _shift_coll_ctx(coll);
				coll->ctx_cur = coll->ctx->id;
				coll->ctx->state = PMIXP_COLL_RING_SYNC;
				/* update global coll seq */
				pmixp_coll_seq++;
				/* send the all collected ring contribs for the new collective */
				break;
			default:
				PMIXP_ERROR("%p: unknown state = %d",
					    coll_ctx, (int)coll_ctx->state);

		}
	} while(ret);
}

int pmixp_coll_ring_init(pmixp_coll_ring_t *coll, const pmixp_proc_t *procs,
	     size_t nprocs, pmixp_coll_general_t *cinfo)
{
	int i;
	pmixp_coll_ring_ctx_t *coll_ctx = NULL;
	hostlist_t hl;
#ifndef NDEBUG
	coll->magic = PMIXP_COLL_RING_STATE_MAGIC;
#endif
	pmixp_debug_hang(0);

	if (SLURM_SUCCESS != pmixp_hostset_from_ranges(procs, nprocs, &hl)) {
		/* TODO: provide ranges output routine */
		PMIXP_ERROR("Bad ranges information");
		goto err_exit;
	}
	coll->ctx_cur = 0;
	coll->ctx = &coll->ctx_array[coll->ctx_cur];
	coll->peers_cnt = hostlist_count(hl);
	coll->my_peerid = hostlist_find(hl, pmixp_info_hostname());
	hostlist_destroy(hl);

	coll->cinfo = cinfo;

	for (i = 0; i < PMIXP_COLL_RING_CTX_NUM; i++) {
		coll_ctx = &coll->ctx_array[i];
		coll_ctx->coll = coll;
#ifndef NDEBUG
		coll_ctx->magic = PMIXP_COLL_RING_STATE_MAGIC;
#endif
		coll_ctx->seq = pmixp_coll_seq + i;
		coll_ctx->contrib_local = false;
		coll_ctx->contrib_prev = 0;
		coll_ctx->ring_buf = create_buf(NULL, 0);
		coll_ctx->state = PMIXP_COLL_RING_SYNC;
		coll_ctx->send_list = list_create(NULL);
		coll_ctx->contrib_map = xmalloc(sizeof(bool) * coll->peers_cnt);
		memset(coll_ctx->contrib_map, 0, sizeof(bool) * coll->peers_cnt);
		slurm_mutex_init(&coll_ctx->lock);
	}
#ifdef PMIXP_COLL_RING_DEBUG
    PMIXP_DEBUG("%p: nprocs=%lu", coll->ctx, nprocs);
#endif
	return SLURM_SUCCESS;
err_exit:
	return SLURM_ERROR;
}

void pmixp_coll_ring_free(pmixp_coll_ring_t *coll)
{
	int i;
	pmixp_coll_ring_ctx_t *coll_ctx;
	for (i = 0; i < PMIXP_COLL_RING_CTX_NUM; i++) {
		coll_ctx = &coll->ctx_array[i];
		free_buf(coll_ctx->ring_buf);
		list_destroy(coll_ctx->send_list);
		slurm_mutex_destroy(&coll_ctx->lock);
		xfree(coll_ctx->contrib_map);
	}
}

int pmixp_coll_ring_contrib_local(pmixp_coll_ring_t *coll, char *data, size_t size,
				  void *cbfunc, void *cbdata)
{
	int ret = SLURM_SUCCESS;

	pmixp_coll_msg_ring_data_t *msg;
	assert(coll->ctx);
	pmixp_coll_ring_ctx_t *coll_ctx = coll->ctx;

#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("%p: contrib/loc: seqnum=%u, state=%d, size=%lu",
		    coll->ctx, coll_ctx->seq, coll_ctx->state, size);
#endif
	/* sanity check */
	pmixp_coll_ring_sanity_check(coll_ctx);

	/* lock the structure */
	slurm_mutex_lock(&coll->ctx->lock);

	/* assign the coll sequence */
	coll_ctx->seq = pmixp_coll_seq;

	/* change the state */
	coll_ctx->ts = time(NULL);

	if (coll->ctx->contrib_local) {
		/* Double contribution - reject */
		ret = SLURM_ERROR;
		slurm_mutex_unlock(&coll->ctx->lock);
		goto exit;
	}

	/* save local contribution */
	if (!size_buf(coll_ctx->ring_buf)) {
		pmixp_server_buf_reserve(coll_ctx->ring_buf, size * coll->peers_cnt);
	} else if(remaining_buf(coll_ctx->ring_buf) < size) {
		/* grow sbuf size to 15% */
		size_t new_size = size_buf(coll_ctx->ring_buf) * 0.15 + size_buf(coll_ctx->ring_buf);
		pmixp_server_buf_reserve(coll_ctx->ring_buf, new_size);
	}

	memcpy(get_buf_data(coll_ctx->ring_buf) + get_buf_offset(coll_ctx->ring_buf),
	       data, size);
	coll_ctx->contrib_local = true;
	msg = xmalloc(sizeof(pmixp_coll_ring_t));
	msg->ptr = get_buf_data(coll_ctx->ring_buf) + get_buf_offset(coll_ctx->ring_buf);
	msg->size = size;
	msg->contrib_id = coll->my_peerid;
	msg->hop_seq = 0;
	list_enqueue(coll_ctx->send_list, msg);
	set_buf_offset(coll_ctx->ring_buf, get_buf_offset(coll_ctx->ring_buf) + size);

	/* setup callback info */
	coll->cbfunc = cbfunc;
	coll->cbdata = cbdata;

	_progress_coll_ring(coll_ctx);

	/* unlock the structure */
	slurm_mutex_unlock(&coll_ctx->lock);
exit:
	return ret;
}

static int _ring_hdr_sanity_check(pmixp_coll_ring_t *coll, pmixp_coll_ring_msg_hdr_t *hdr)
{
	if (hdr->contrib_id >= coll->peers_cnt) {
		return 1;
	}
	if (hdr->nodeid != _ring_prev_id(coll)) {
		return 1;
	}
}

int pmixp_coll_ring_contrib_prev(pmixp_coll_ring_t *coll, pmixp_coll_ring_msg_hdr_t *hdr,
				 Buf buf)
{
	int ret = SLURM_SUCCESS;
	size_t size = hdr->msgsize;
	pmixp_coll_msg_ring_data_t *msg;
	char *data_src = NULL, *data_dst = NULL;
	pmixp_coll_ring_ctx_t *coll_ctx = coll->ctx;
	uint32_t expexted_seq;

#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("%p: contrib/prev: seqnum=%u, state=%d, nodeid=%d, contrib=%d, "
		    "seq=%d, size=%lu",
		    coll_ctx, coll_ctx->seq, coll_ctx->state, hdr->nodeid,
		    hdr->contrib_id, hdr->hop_seq, size);
#endif
	if (_ring_hdr_sanity_check(coll, hdr)) {

	}

	/* lock the structure */
	slurm_mutex_lock(&coll->ctx->lock);

	/* change the state */
	coll_ctx->ts = time(NULL);

	/* verify msg size */
	if (hdr->msgsize != remaining_buf(buf)) {
		PMIXP_ERROR("%p: unexpected message size=%d, expect=%zd",
			    coll, remaining_buf(buf), hdr->msgsize);
		goto exit;
	}

	if (hdr->nodeid != _ring_prev_id(coll)) {
		PMIXP_ERROR("%p: unexpected peerid=%d, expect=%d",
			    coll, hdr->nodeid, _ring_prev_id(coll))
		goto exit;
	}

	/* shift the coll context if that contrib belongs to the next coll  */
	if ((pmixp_coll_seq +1) == hdr->seq) {
		coll_ctx = _shift_coll_ctx(coll);
		coll_ctx->seq = pmixp_coll_seq +1;
	}
	if (hdr->seq > coll_ctx->seq) {
		char *nodename = pmixp_info_job_host(hdr->nodeid);
		PMIXP_ERROR("%p: unexpected contrib from %s:%d: "
			    "contrib_seq = %d, coll->seq = %d, "
			    "state=%s",
			    coll_ctx, nodename, hdr->nodeid,
			    hdr->seq, coll_ctx->seq,
			    pmixp_coll_ring_state2str(coll_ctx->state));
		xfree(nodename);
	}

	/* compute the actual hops of ring: (src - dst + size) % size */
	expexted_seq = (coll->my_peerid + coll->peers_cnt - hdr->contrib_id) % coll->peers_cnt - 1;
	if (hdr->hop_seq != expexted_seq) {
		PMIXP_ERROR("%p: unexpected ring seq number=%d, expect=%d, coll seq=%d",
			    coll_ctx, hdr->hop_seq, expexted_seq, coll_ctx->seq);
		goto exit;
	}

	if (hdr->contrib_id >= coll->peers_cnt) {
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

	/* save contribution */
	if (!size_buf(coll_ctx->ring_buf)) {
		pmixp_server_buf_reserve(coll_ctx->ring_buf, size * coll->peers_cnt);
	} else if(remaining_buf(coll_ctx->ring_buf) < size) {
		/* grow sbuf size to 15% */
		size_t new_size = size_buf(coll_ctx->ring_buf) * 0.15 + size_buf(coll_ctx->ring_buf);
		pmixp_server_buf_reserve(coll_ctx->ring_buf, new_size);
	}
	data_src = get_buf_data(buf) + get_buf_offset(buf);
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
#ifdef PMIXP_COLL_RING_DEBUG
		PMIXP_DEBUG("%p: enqueue transit msg=%p, size=%lu",
			    coll->ctx, msg->ptr, msg->size);
#endif
		list_enqueue(coll->ctx->send_list, msg);
	}
	/* increase number of ring contributions */
	coll_ctx->contrib_prev++;

	/* ring coll progress */
	_progress_coll_ring(coll_ctx);
exit:
	/* unlock the structure */
	slurm_mutex_unlock(&coll_ctx->lock);
	return ret;
}

void pmixp_coll_ring_reset_if_to(pmixp_coll_ring_t *coll, time_t ts) {
	int i;
	pmixp_coll_ring_ctx_t *coll_ctx;


	for (i=0; i < PMIXP_COLL_RING_CTX_NUM; i++) {
		coll_ctx = &coll->ctx_array[i];

		/* lock the structure */
		slurm_mutex_lock(&coll_ctx->lock);

		if (PMIXP_COLL_RING_SYNC == coll->ctx->state) {
			slurm_mutex_unlock(&coll_ctx->lock);
			continue;
		}
		if (ts - coll_ctx->ts > pmixp_info_timeout()) {
			/* respond to the libpmix */
			if (coll->ctx->contrib_local && coll->cbfunc) {
				pmixp_lib_modex_invoke(coll->cbfunc, PMIXP_ERR_TIMEOUT, NULL,
					       0, coll->cbdata, NULL, NULL);
			}
			/* drop the collective */
			_reset_coll_ring(coll_ctx);
			/* report the timeout event */
			PMIXP_ERROR("Collective timeout!");
		}
		/* unlock the structure */
		slurm_mutex_unlock(&coll_ctx->lock);
	}
}
