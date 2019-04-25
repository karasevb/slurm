/*****************************************************************************\
 **  pmix_coll_ring.c - PMIx collective primitives
 *****************************************************************************
 *  Copyright (C) 2018      Mellanox Technologies. All rights reserved.
 *  Written by Artem Polyakov <artpol84@gmail.com, artemp@mellanox.com>,
 *             Boris Karasev <karasev.b@gmail.com, boriska@mellanox.com>.
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
#include "pmixp_coll.h"
#include "pmixp_nspaces.h"
#include "pmixp_server.h"
#include "pmixp_client.h"
#include <limits.h>

typedef struct {
	uint32_t seq;
	pmixp_coll_t *coll;
	pmixp_coll_bruck_ctx_t *coll_ctx;
	Buf buf;
} pmixp_coll_bruck_cbdata_t;

typedef struct {
	pmixp_coll_bruck_msg_hdr_t hdr;
	Buf buf;
} pmixp_bruck_recv_buf_item_t;

static void _progress_coll_bruck(pmixp_coll_bruck_ctx_t *coll_ctx);

static inline pmixp_coll_bruck_t *_ctx_get_coll_bruck(
		pmixp_coll_bruck_ctx_t *coll_ctx)
{
	return &coll_ctx->coll->state.bruck;
}

static inline int _get_bruck_recv_id(pmixp_coll_bruck_ctx_t *coll_ctx)
{
	pmixp_coll_t *coll = coll_ctx->coll;
	return ((coll->my_peerid + coll->peers_cnt) + (1 << coll_ctx->step_cnt)) %
			coll->peers_cnt;
}

static inline int _get_bruck_send_id(pmixp_coll_bruck_ctx_t *coll_ctx)
{
	pmixp_coll_t *coll = coll_ctx->coll;
	return ((coll->my_peerid + coll->peers_cnt) - (1 << coll_ctx->step_cnt)) %
			coll->peers_cnt;
}

static Buf _get_contrib_buf(pmixp_coll_bruck_ctx_t *coll_ctx)
{
	pmixp_coll_bruck_t *bruck = _ctx_get_coll_bruck(coll_ctx);
	Buf bruck_buf = list_pop(bruck->recv_buf_pool);
	if (!bruck_buf) {
		bruck_buf = create_buf(NULL, 0);
	}
	return bruck_buf;
}


static Buf _get_fwd_buf(pmixp_coll_bruck_ctx_t *coll_ctx)
{
	pmixp_coll_bruck_t *bruck = _ctx_get_coll_bruck(coll_ctx);
	Buf buf = list_pop(bruck->bruck_buf_pool);
	if (!buf) {
		buf = pmixp_server_buf_new();
	}
	return buf;
}

static int _pack_coll_bruck_info(pmixp_coll_bruck_ctx_t *coll_ctx,
				 pmixp_coll_bruck_msg_hdr_t *bruck_hdr,
				 Buf buf)
{
	pmixp_coll_t *coll = coll_ctx->coll;
	pmixp_proc_t *procs = coll->pset.procs;
	size_t nprocs = coll->pset.nprocs;
	uint32_t type = PMIXP_COLL_TYPE_FENCE_BRUCK;
	int i;

	/* 1. store the type of collective */
	pack32(type, buf);

	/* 2. Put the number of ranges */
	pack32(nprocs, buf);

	/* 3. Put namespace/rank of particular process */
	for (i = 0; i < (int)nprocs; i++) {
		/* Pack namespace */
		packmem(procs->nspace, strlen(procs->nspace) + 1, buf);
		pack32(procs->rank, buf);
	}

	/* 4. pack the bruck header info */
	packmem((char*)bruck_hdr, sizeof(pmixp_coll_bruck_msg_hdr_t), buf);

	return SLURM_SUCCESS;
}

int pmixp_coll_bruck_unpack(Buf buf, pmixp_coll_type_t *type,
			    pmixp_coll_bruck_msg_hdr_t *bruck_hdr,
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

	/* 3. get namespace/rank of particular process */
	for (i = 0; i < (int)nprocs; i++) {
		rc = unpackmem(procs[i].nspace, &tmp, buf);
		if (SLURM_SUCCESS != rc) {
			PMIXP_ERROR("Cannot unpack namespace for process #%d",
				    i);
			return rc;
		}
		procs[i].nspace[tmp] = '\0';

		rc = unpack32(&tmp, buf);
		procs[i].rank = tmp;
		if (SLURM_SUCCESS != rc) {
			PMIXP_ERROR("Cannot unpack ranks for process #%d, nsp=%s",
				    i, procs[i].nspace);
			return rc;
		}
	}

	/* 4. extract the Bruck info */
	if (SLURM_SUCCESS != (rc = unpackmem((char *)bruck_hdr, &tmp, buf))) {
		PMIXP_ERROR("Cannot unpack ring info");
		return rc;
	}

	return SLURM_SUCCESS;
}

static inline uint32_t _bruck_remain_step(pmixp_coll_bruck_ctx_t *coll_ctx)
{
	if (!coll_ctx->step_num) {
		return false;
	}
	return (coll_ctx->step_num - coll_ctx->step_cnt);
}

static void _reset_coll_bruck(pmixp_coll_bruck_ctx_t *coll_ctx)
{
	pmixp_coll_t *coll = coll_ctx->coll;
#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("%p: called", coll_ctx);
#endif
	coll_ctx->in_use = false;
	coll_ctx->state = PMIXP_COLL_BRUCK_SYNC;
	coll_ctx->contrib_local = false;
	coll_ctx->recv_complete = false;
	coll_ctx->send_complete = false;
	coll->ts = time(NULL);
	coll_ctx->bruck_buf = NULL;
	coll_ctx->step_cnt = 0;
}

static void _libpmix_cb(void *_cbdata)
{
	pmixp_coll_bruck_cbdata_t *cbdata = (pmixp_coll_bruck_cbdata_t*)_cbdata;
	pmixp_coll_t *coll = cbdata->coll;
	Buf buf = cbdata->buf;

	pmixp_coll_sanity_check(coll);

	/* lock the structure */
	slurm_mutex_lock(&coll->lock);

#ifdef PMIXP_COLL_TIMING
	PMIXP_ERROR("coll seq %d size %d time %lf", cbdata->coll_ctx->seq,
		    buf->processed, PMIXP_COLL_GET_TS() - cbdata->coll_ctx->ts);
	cbdata->coll_ctx->ts = 0.0;
#endif

	/* reset buf */
	buf->processed = 0;
	/* push it back to pool for reuse */
	list_push(coll->state.bruck.recv_buf_pool, buf);
	_reset_coll_bruck(cbdata->coll_ctx);

	/* unlock the structure */
	slurm_mutex_unlock(&coll->lock);

	xfree(cbdata);
}

static void _invoke_callback(pmixp_coll_bruck_ctx_t *coll_ctx)
{
	pmixp_coll_t *coll = coll_ctx->coll;
	pmixp_coll_bruck_cbdata_t *cbdata;
	char *data;
	size_t data_sz;

	if (!coll->cbfunc)
		return;

	data = get_buf_data(coll_ctx->bruck_buf);
	data_sz = get_buf_offset(coll_ctx->bruck_buf);
	cbdata = xmalloc(sizeof(pmixp_coll_bruck_cbdata_t));

#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("%p: called, size %lu", coll_ctx, data_sz);
#endif

	cbdata->coll = coll;
	cbdata->buf = coll_ctx->bruck_buf;
	cbdata->seq = coll->seq;
	cbdata->coll_ctx = coll_ctx;
	pmixp_lib_modex_invoke(coll->cbfunc, SLURM_SUCCESS,
			       data, data_sz,
			       coll->cbdata, _libpmix_cb, (void *)cbdata);
	/*
	 * Clear callback info as we are not allowed to use it second time
	 */
	coll->cbfunc = NULL;
	coll->cbdata = NULL;
}

/*
 * use it for internal collective
 * performance evaluation tool.
 */
pmixp_coll_t *pmixp_coll_bruck_from_cbdata(void *cbdata)
{
	pmixp_coll_bruck_cbdata_t *ptr = (pmixp_coll_bruck_cbdata_t*)cbdata;
	pmixp_coll_sanity_check(ptr->coll);
	return ptr->coll;
}

static void _bruck_sent_cb(int rc, pmixp_p2p_ctx_t ctx, void *_cbdata)
{
	pmixp_coll_bruck_cbdata_t *cbdata = (pmixp_coll_bruck_cbdata_t*)_cbdata;
	pmixp_coll_bruck_ctx_t *coll_ctx = cbdata->coll_ctx;
	pmixp_coll_t *coll = coll_ctx->coll;
	Buf buf = cbdata->buf;
#ifdef PMIXP_COLL_TIMING
	pmixp_coll_timing_t *tm;
#endif
#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("%p: called", coll_ctx);
#endif
	pmixp_coll_sanity_check(coll);

	if (PMIXP_P2P_REGULAR == ctx) {
		/* lock the collective */
		slurm_mutex_lock(&coll->lock);
	}
	if (cbdata->seq != coll->seq) {
		/* it seems like this collective was reset since the time
		 * we initiated this send.
		 * Just exit to avoid data corruption.
		 */
		PMIXP_DEBUG("%p: collective was reset!", coll_ctx);
		goto exit;
	}
#ifdef PMIXP_COLL_TIMING
	tm = pmixp_coll_timing_get(coll_ctx->coll, coll_ctx->seq,
				   coll_ctx->step_cnt);
	tm->snd_complete_ts = PMIXP_COLL_GET_TS();
#endif
	coll_ctx->send_complete = true;
	if (PMIXP_P2P_REGULAR == ctx) {
		_progress_coll_bruck(coll_ctx);
	}
exit:
	pmixp_server_buf_reset(buf);
	list_push(coll->state.bruck.bruck_buf_pool, buf);

	if (PMIXP_P2P_REGULAR == ctx) {
		/* unlock the collective */
		slurm_mutex_unlock(&coll->lock);
	}
	xfree(cbdata);
}

static int _bruck_forward_data(pmixp_coll_bruck_ctx_t *coll_ctx)
{
	pmixp_coll_bruck_msg_hdr_t hdr;
	pmixp_coll_t *coll = coll_ctx->coll;

	pmixp_ep_t *ep = (pmixp_ep_t*)xmalloc(sizeof(*ep));
	pmixp_coll_bruck_cbdata_t *cbdata = NULL;
	uint32_t offset = 0;
	Buf buf = _get_fwd_buf(coll_ctx);
	int rc = SLURM_SUCCESS;
	uint32_t size = get_buf_offset(coll_ctx->bruck_buf);
#ifdef PMIXP_COLL_TIMING
	pmixp_coll_timing_t *tm = NULL;
#endif

	/* check for last step, overwrite size if offset expected */
	if ((coll_ctx->step_cnt + 1) == coll_ctx->step_num) {
	    if (coll_ctx->bruck_offset) {
    		size = coll_ctx->bruck_offset;
#ifdef PMIXP_COLL_DEBUG
	    PMIXP_DEBUG("%p: step %d, check offset %x", coll_ctx,
			coll_ctx->step_num, coll_ctx->bruck_offset);
#endif
	    }
	}

	hdr.nodeid = coll->my_peerid;
	hdr.msgsize = size;
	hdr.contrib_id = coll->my_peerid;
	hdr.step = coll_ctx->step_cnt;
	hdr.seq = coll_ctx->seq;

	// TODO sanity check

#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("%p: seq=%d forward data to nodeid=%d, step_id=%d, size=%lu, contrib=%d",
		    coll_ctx, coll_ctx->seq, _get_bruck_send_id(coll_ctx), hdr.step, hdr.msgsize,
		    hdr.contrib_id);
#endif
	if (!buf) {
		rc = SLURM_ERROR;
		goto exit;
	}
	ep->type = PMIXP_EP_NOIDEID;
	ep->ep.nodeid = _get_bruck_send_id(coll_ctx);

	/* pack ring info */
	_pack_coll_bruck_info(coll_ctx, &hdr, buf);

	/* pack the Bruck auxiliary buffer offset */
	xassert(coll_ctx->step_num > 0);
	if (coll_ctx->bruck_remain & (1 << coll_ctx->step_cnt)) {
	    pack32(coll_ctx->bruck_offset, buf);
	}

	/* insert payload to buf */
	offset = get_buf_offset(buf);
	pmixp_server_buf_reserve(buf, size);
	memcpy(get_buf_data(buf) + offset,
	       get_buf_data(coll_ctx->bruck_buf), size);
	set_buf_offset(buf, offset + size);

	cbdata = xmalloc(sizeof(pmixp_coll_bruck_cbdata_t));
	cbdata->buf = buf;
	cbdata->coll_ctx = coll_ctx;
	cbdata->seq = coll->seq;
#ifdef PMIXP_COLL_TIMING
	tm = pmixp_coll_timing_get(coll_ctx->coll, coll_ctx->seq,
				   coll_ctx->step_cnt);
	tm->snd_ts = PMIXP_COLL_GET_TS();
#endif
	rc = pmixp_server_send_nb(ep, PMIXP_MSG_BRUCK, coll->seq, buf,
				  _bruck_sent_cb, cbdata);
exit:
	return rc;
}

inline static int _bruck_buffer_grow(pmixp_coll_bruck_ctx_t *coll_ctx,
				    char *data, size_t size)
{
	char *data_ptr = NULL;

	/* save contribution */
	if (!size_buf(coll_ctx->bruck_buf)) {
		grow_buf(coll_ctx->bruck_buf, size * coll_ctx->coll->peers_cnt);
	} else if(remaining_buf(coll_ctx->bruck_buf) < size) {
		uint32_t new_size = size_buf(coll_ctx->bruck_buf) + size;
		grow_buf(coll_ctx->bruck_buf, new_size);
	}

	data_ptr = get_buf_data(coll_ctx->bruck_buf) +
		get_buf_offset(coll_ctx->bruck_buf);
	memcpy(data_ptr, data, size);
	set_buf_offset(coll_ctx->bruck_buf,
		       get_buf_offset(coll_ctx->bruck_buf) + size);

	return SLURM_SUCCESS;
}

static int _bruck_step_done(pmixp_coll_bruck_ctx_t *coll_ctx)
{
	if (coll_ctx->coll->peers_cnt > 1) {
		return coll_ctx->recv_complete && coll_ctx->send_complete;
	} else {
		return true;
	}
}

static bool _bruck_match_msg(pmixp_coll_bruck_ctx_t *coll_ctx,
			     pmixp_coll_bruck_msg_hdr_t *hdr)
{
	bool ret = false;

	if ((hdr->seq == coll_ctx->seq) &&
			(hdr->step == coll_ctx->step_cnt)) {
		ret = true;
	}
	return ret;
}

static pmixp_bruck_recv_buf_item_t *_search_match_message(pmixp_coll_bruck_ctx_t *coll_ctx)
{
	pmixp_coll_t *coll = coll_ctx->coll;
	pmixp_bruck_recv_buf_item_t *contrib, *match = NULL;
#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("%p: called size %d", coll_ctx, list_count(coll->state.bruck.recv_message_pool));
#endif
	if (list_count(coll->state.bruck.recv_message_pool)) {
		ListIterator itr = NULL;

		itr = list_iterator_create(coll->state.bruck.recv_message_pool);
		while ((contrib = list_next(itr))) {
			if (_bruck_match_msg(coll_ctx, &contrib->hdr)) {
				list_remove(itr);
#ifdef PMIXP_COLL_DEBUG
				PMIXP_DEBUG("%p: match message seq=%d, step=%d, from=%d",
					    coll_ctx, contrib->hdr.seq,
					    contrib->hdr.step,
					    contrib->hdr.contrib_id);
#endif
				match = contrib;
				break;
			}
		}
		list_iterator_destroy(itr);
	}
	return match;
}

static int _remote_contrib(pmixp_coll_bruck_ctx_t *coll_ctx, pmixp_p2p_ctx_t ctx,
			   pmixp_coll_bruck_msg_hdr_t *hdr, Buf buf)
{
	int ret = SLURM_SUCCESS;
	char *data_ptr = NULL;

#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("%p: called", coll_ctx);
#endif
	/* unpack the Bruck auxiliary buffer offset */
	if( coll_ctx->bruck_lsb & (1 << coll_ctx->step_cnt)) {
	    coll_ctx->bruck_offset = get_buf_offset(coll_ctx->bruck_buf);
	}
	if( coll_ctx->bruck_remain & (1 << coll_ctx->step_cnt) ){
	    uint32_t offset;
	    if (0 != unpack32(&offset, buf)) {
		abort();
	    }
	    coll_ctx->bruck_offset =
			    get_buf_offset(coll_ctx->bruck_buf) + offset;
#ifdef PMIXP_COLL_DEBUG
	    PMIXP_DEBUG("%p: step %d, unpack offset %u, new_offset %d", coll_ctx,
			coll_ctx->step_cnt, offset, coll_ctx->bruck_offset );
#endif
	}

	/* verify msg size */
	if (hdr->msgsize != remaining_buf(buf)) {
#ifdef PMIXP_COLL_DEBUG
		PMIXP_DEBUG("%p: unexpected message size=%d, expect=%zu",
			    coll_ctx, remaining_buf(buf), hdr->msgsize);
#endif
		goto exit;
	}

	/* increase number of bruck contributions */
	coll_ctx->recv_complete = true;
	data_ptr = get_buf_data(buf) + get_buf_offset(buf);
	if (_bruck_buffer_grow(coll_ctx, data_ptr, remaining_buf(buf))) {
		goto exit;
	}

	if (PMIXP_P2P_REGULAR == ctx) {
		_progress_coll_bruck(coll_ctx);
	}
exit:
	return ret;
}

static int progress_next_step(pmixp_coll_bruck_ctx_t *coll_ctx)
{
	int ret = SLURM_SUCCESS;
	pmixp_bruck_recv_buf_item_t *contrib;
#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("%p: called, step %d", coll_ctx, coll_ctx->step_cnt);
#endif

	coll_ctx->step_cnt++;
	coll_ctx->send_complete = false;
	coll_ctx->recv_complete = false;

	if (coll_ctx->step_cnt >= coll_ctx->step_num) {
		return ret;
	}

	_bruck_forward_data(coll_ctx);

	if (NULL != (contrib = _search_match_message(coll_ctx))) {
		ret = _remote_contrib(coll_ctx, PMIXP_P2P_INLINE,
				      &contrib->hdr, contrib->buf);
		FREE_NULL_BUFFER(contrib->buf);
		xfree(contrib);
	}

	return ret;
}

static void _progress_coll_bruck(pmixp_coll_bruck_ctx_t *coll_ctx)
{
	int ret = 0;
	pmixp_coll_t *coll = coll_ctx->coll;

#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("%p: called", coll_ctx);
#endif
	do {
#ifdef PMIXP_COLL_DEBUG
		PMIXP_DEBUG("%p: state %d, seq=%d, step=%d, recv_complete=%d, send_complete=%d",
			    coll_ctx, coll_ctx->state, coll_ctx->seq,
			    coll_ctx->step_cnt, coll_ctx->recv_complete,
			    coll_ctx->send_complete);
#endif
		ret = false;
		switch(coll_ctx->state){
		case PMIXP_COLL_BRUCK_SYNC:
			if (coll_ctx->contrib_local) {
				ret = true;
				progress_next_step(coll_ctx);
				coll_ctx->state = PMIXP_COLL_BRUCK_PROGRESS;
			}
			break;
		case PMIXP_COLL_BRUCK_PROGRESS:
			if (_bruck_step_done(coll_ctx)) {
#ifdef PMIXP_COLL_DEBUG
				PMIXP_DEBUG("%p: Bruck step=%d DONE, size %u",
					    coll_ctx, coll_ctx->step_cnt,
					    get_buf_offset(coll_ctx->bruck_buf));
#endif
				/* go to the next Bruck step */
				ret = true;
				progress_next_step(coll_ctx);

				if (_bruck_remain_step(coll_ctx)) {
					break;
				}
#ifdef PMIXP_COLL_DEBUG
				PMIXP_DEBUG("%p: %s seq=%d is DONE", coll_ctx,
					    pmixp_coll_type2str(coll->type),
					    coll->seq);
#endif

				coll_ctx->state = PMIXP_COLL_BRUCK_SYNC;
				_invoke_callback(coll_ctx);
				coll->seq++;
			}
			break;
		default:
			PMIXP_ERROR("%p: unknown state = %d",
				    coll_ctx, (int)coll_ctx->state);
			slurm_kill_job_step(pmixp_info_jobid(),
					    pmixp_info_stepid(), SIGKILL);
			break;
		}
	} while(ret);
}

void pmixp_free_recv_pool(void *x)
{
	pmixp_bruck_recv_buf_item_t *item = (pmixp_bruck_recv_buf_item_t*)x;
	FREE_NULL_BUFFER(item->buf);
	xfree(item);
}

int pmixp_coll_bruck_init(pmixp_coll_t *coll, hostlist_t *hl)
{
#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("called");
#endif
	pmixp_coll_bruck_ctx_t *coll_ctx = NULL;
	pmixp_coll_bruck_t *bruck = &coll->state.bruck;
	//char *p;
	int i;
	//int rel_id = hostlist_find(*hl, pmixp_info_hostname());

	// TODO calculate relative nodeid

	bruck->bruck_buf_pool = list_create(pmixp_free_buf);
	bruck->recv_buf_pool = list_create(pmixp_free_buf);
	bruck->recv_message_pool = list_create(pmixp_free_recv_pool);


	for (i = 0; i < PMIXP_COLL_BRUCK_CTX_NUM; i++) {
		coll_ctx = &bruck->ctx_array[i];
		coll_ctx->coll = coll;
		coll_ctx->step_num = 0;
		coll_ctx->in_use = false;
	}

	return SLURM_SUCCESS;
}

static void pmixp_coll_bruck_ctx_init(pmixp_coll_bruck_ctx_t *coll_ctx,
				      const int seq)
{
	pmixp_coll_t *coll = coll_ctx->coll;
	uint32_t remain;

	coll_ctx->in_use = true;
	coll_ctx->seq = seq;
	coll_ctx->step_cnt = -1;
	coll_ctx->seq = seq;
	coll_ctx->bruck_buf = _get_contrib_buf(coll_ctx);
	coll_ctx->contrib_local = false;
	coll_ctx->send_complete = false;
	coll_ctx->recv_complete = false;
	coll_ctx->state = PMIXP_COLL_BRUCK_SYNC;
	coll_ctx->bruck_offset = 0;
	coll_ctx->bruck_remain = coll->peers_cnt -
			(1 << ((int)pmixp_int_log2(coll->peers_cnt)));
	coll_ctx->step_num = (int)pmixp_int_log2(coll->peers_cnt) +
			!!(coll_ctx->bruck_remain);
	remain = coll_ctx->bruck_remain;
	if (0 == remain) {
		coll_ctx->bruck_lsb = 0;
	} else {
		coll_ctx->bruck_lsb = 1;
		while (((remain & 1) == 0) && (remain != 0)) {
		remain >>= 1;
		coll_ctx->bruck_lsb <<= 1;
		}
	}
}

pmixp_coll_bruck_ctx_t *pmixp_coll_bruck_ctx_select(pmixp_coll_t *coll,
						   const int seq)
{
	int i;
	pmixp_coll_bruck_ctx_t *coll_ctx = NULL, *ret = NULL;
	pmixp_coll_bruck_t *bruck = &coll->state.bruck;

	/* finding the appropriate ring context */
	for (i = 0; i < PMIXP_COLL_BRUCK_CTX_NUM; i++) {
		coll_ctx = &bruck->ctx_array[i];
		if (coll_ctx->in_use && coll_ctx->seq == seq) {
			return coll_ctx;
		} else if (!coll_ctx->in_use) {
			ret = coll_ctx;
			continue;
		}
	}
	/* add this context to use */
	if (ret && !ret->in_use) {
		pmixp_coll_bruck_ctx_init(ret, seq);
#ifdef PMIXP_COLL_TIMING
		ret->ts = PMIXP_COLL_GET_TS();
#endif
	}

	return ret;
}

int pmixp_coll_bruck_local(pmixp_coll_t *coll, char *data, size_t size,
			   void *cbfunc, void *cbdata)
{
	int ret = SLURM_SUCCESS;
	pmixp_coll_bruck_ctx_t *coll_ctx = NULL;
#ifdef PMIXP_COLL_TIMING
	pmixp_coll_timing_t *tm = NULL;
#endif
	pmixp_debug_hang(0);

	/* lock the structure */
	slurm_mutex_lock(&coll->lock);

	/* sanity check */
	pmixp_coll_sanity_check(coll);

	/* setup callback info */
	coll->cbfunc = cbfunc;
	coll->cbdata = cbdata;

	coll_ctx = pmixp_coll_bruck_ctx_select(coll, coll->seq);
#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("%p: called offset %d", coll_ctx, coll_ctx->bruck_offset);
#endif

#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("%p: contrib/loc: seq=%d, step_cnt=%u, recv_complete=%d, send_complete=%d, state=%d, size=%lu",
		    coll_ctx, coll->seq, coll_ctx->step_cnt, coll_ctx->recv_complete,
		    coll_ctx->send_complete, coll_ctx->state, size);
#endif
	/* mark local contribution */
	coll_ctx->contrib_local = true;
	if (_bruck_buffer_grow(coll_ctx, data, size)) {
		goto exit;
	}
#ifdef PMIXP_COLL_TIMING
	tm = pmixp_coll_timing_get(coll_ctx->coll, coll_ctx->seq,
				   coll_ctx->step_cnt);
	tm->rcv_ts = PMIXP_COLL_GET_TS();
#endif

	_progress_coll_bruck(coll_ctx);
exit:
	/* unlock the structure */
	slurm_mutex_unlock(&coll->lock);

	return ret;
}

static void _store_unexpected_contrib(pmixp_coll_t *coll,
				      pmixp_coll_bruck_msg_hdr_t *hdr, Buf buf)
{
	pmixp_bruck_recv_buf_item_t *item = xmalloc(sizeof(*item));
#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("%p: recv_message_pool %d/%d from %d ",
		    coll, hdr->seq, hdr->step, hdr->contrib_id);
#endif
	memcpy(&item->hdr, hdr, sizeof(*hdr));
	item->buf = buf;
	list_append(coll->state.bruck.recv_message_pool, (void*)item);
}

int pmixp_coll_bruck_remote(pmixp_coll_t *coll, pmixp_coll_bruck_msg_hdr_t *hdr,
			    Buf *buf)
{
	int ret = SLURM_SUCCESS;
	pmixp_coll_bruck_ctx_t *coll_ctx = NULL;
#ifdef PMIXP_COLL_TIMING
	pmixp_coll_timing_t *tm = NULL;
#endif

	/* lock the structure */
	slurm_mutex_lock(&coll->lock);

	coll_ctx = pmixp_coll_bruck_ctx_select(coll, hdr->seq);
	if (!coll_ctx) {
		PMIXP_ERROR("Can not get bruck collective context, seq=%u",
			    hdr->seq);
		ret = SLURM_ERROR;
		goto exit;
	}
#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("%p: called", coll_ctx);
#endif

#ifdef PMIXP_COLL_TIMING
	tm = pmixp_coll_timing_get(coll_ctx->coll, hdr->seq, hdr->step);
	tm->rcv_ts = PMIXP_COLL_GET_TS();
#endif
	if (!coll_ctx->contrib_local || !_bruck_match_msg(coll_ctx, hdr)) {
		_store_unexpected_contrib(coll, hdr, *buf);
		*buf = NULL;
		goto exit;
	}
	_remote_contrib(coll_ctx, PMIXP_P2P_REGULAR, hdr, *buf);
exit:
	/* unlock the structure */
	slurm_mutex_unlock(&coll->lock);
	return ret;
}

void pmixp_coll_bruck_free(pmixp_coll_bruck_t *bruck)
{
	list_destroy(bruck->recv_buf_pool);
	list_destroy(bruck->bruck_buf_pool);
	list_destroy(bruck->recv_message_pool);
}
