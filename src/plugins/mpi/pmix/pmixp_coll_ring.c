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

typedef struct {
	pmixp_coll_ring_ctx_t *coll_ctx;
	Buf buf;
} pmixp_coll_ring_cbdata_t;

static inline int _ring_prev_id(pmixp_coll_ring_t *coll)
{
	return (coll->my_peerid + coll->peers_cnt - 1) % coll->peers_cnt;
}

static inline int _ring_next_id(pmixp_coll_ring_t *coll)
{
	return (coll->my_peerid + 1) % coll->peers_cnt;
}

static inline pmixp_coll_ring_t *_ctx_get_coll(pmixp_coll_ring_ctx_t *coll_ctx)
{
	return coll_ctx->coll;
}

static void _ring_sent_cb(int rc, pmixp_p2p_ctx_t ctx, void *_cbdata)
{
	pmixp_coll_ring_cbdata_t *cbdata = (pmixp_coll_ring_cbdata_t*)_cbdata;
	pmixp_coll_ring_ctx_t *coll_ctx = cbdata->coll_ctx;

	pmixp_server_buf_reset(cbdata->buf);
	list_push(coll_ctx->fwrd_buf_pool, cbdata->buf);
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

static Buf _get_fwd_buf(pmixp_coll_ring_ctx_t *coll_ctx)
{
	Buf buf = list_pop(coll_ctx->fwrd_buf_pool);
	if (!buf) {
		buf = pmixp_server_buf_new();
	}
	return buf;
}

static int _ring_forward_data(pmixp_coll_ring_ctx_t *coll_ctx, uint32_t contrib_id,
			       uint32_t hop_seq, void *data, size_t size)
{
	pmixp_coll_ring_msg_hdr_t hdr;
	pmixp_coll_ring_t *coll = _ctx_get_coll(coll_ctx);
	int next_nodeid = _ring_next_id(coll);
	hdr.nodeid = coll->my_peerid;
	hdr.msgsize = size;
	hdr.seq = coll_ctx->seq;
	hdr.hop_seq = hop_seq;
	hdr.contrib_id = contrib_id;
	pmixp_ep_t *ep = (pmixp_ep_t*)xmalloc(sizeof(*ep));//{0};
	pmixp_coll_ring_cbdata_t *cbdata = NULL;
	uint32_t offset = 0;
	Buf buf = _get_fwd_buf(coll_ctx);
	int rc = SLURM_SUCCESS;

#ifdef PMIXP_COLL_RING_DEBUG
	PMIXP_DEBUG("%p: transit data to nodeid=%d, seq=%d, hop=%d, size=%lu, contrib=%d",
		    coll->ctx, next_nodeid, hdr.seq, hdr.hop_seq, hdr.msgsize, hdr.contrib_id);
#endif
	if (!buf) {
		rc = SLURM_ERROR;
		goto exit;
	}
	ep->type = PMIXP_EP_NOIDEID;
	ep->ep.nodeid = next_nodeid;


	/* pack ring info */
	_pack_coll_ring_info(coll, &hdr, buf);

	/* insert payload to buf */
	offset = get_buf_offset(buf);
	pmixp_server_buf_reserve(buf, size);
	memcpy(get_buf_data(buf) + offset, data, size);
	set_buf_offset(buf, offset + size);

	cbdata = xmalloc(sizeof(pmixp_coll_ring_cbdata_t));
	cbdata->buf = buf;
	cbdata->coll_ctx = coll_ctx;
	rc = pmixp_server_send_nb(ep, PMIXP_MSG_RING, coll_ctx->seq, buf,
				  _ring_sent_cb, cbdata);
exit:
	return rc;
}

static void _reset_coll_ring(pmixp_coll_ring_ctx_t *coll_ctx)
{
	pmixp_coll_ring_t *coll = _ctx_get_coll(coll_ctx);

#ifdef PMIXP_COLL_RING_DEBUG
	PMIXP_DEBUG("%p: called", coll_ctx);
#endif
	coll_ctx->in_use = false;
	coll_ctx->state = PMIXP_COLL_RING_SYNC;
	coll_ctx->contrib_local = false;
	coll_ctx->contrib_prev = 0;
	memset(coll->ctx->contrib_map, 0, sizeof(bool) * coll->peers_cnt);
	set_buf_offset(coll_ctx->ring_buf, 0);
}

static void _libpmix_cb(void *_vcbdata)
{
	pmixp_coll_ring_cbdata_t *cbdata = (pmixp_coll_ring_cbdata_t*)_vcbdata;
	pmixp_coll_ring_ctx_t *coll_ctx = cbdata->coll_ctx;

	/* lock the collective */
	slurm_mutex_lock(&coll_ctx->lock);

	slurm_mutex_unlock(&coll_ctx->lock);
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
					coll_ctx->in_use = true;
					coll_ctx->state = PMIXP_COLL_RING_COLLECT;
					ret = true;
				} else {
					ret = false;
				}
				break;
			case PMIXP_COLL_RING_COLLECT:
				ret = false;
				if (!coll_ctx->contrib_local) {
					ret = false;
				} else if ((coll->peers_cnt - 1) == coll->ctx->contrib_prev) {
					coll_ctx->state = PMIXP_COLL_RING_DONE;
					ret = true;
				}
				break;
			case PMIXP_COLL_RING_DONE:
				PMIXP_ERROR("%p: ring collective seq=%d is done", coll_ctx, coll_ctx->seq);
				ret = false;

				if (coll->cbfunc) {
					pmixp_coll_ring_cbdata_t *cbdata;
					cbdata = xmalloc(sizeof(pmixp_coll_ring_cbdata_t));
					cbdata->coll_ctx = coll_ctx;
					pmixp_lib_modex_invoke(coll->cbfunc, SLURM_SUCCESS,
							       get_buf_data(coll_ctx->ring_buf),
							       get_buf_offset(coll_ctx->ring_buf),
							       coll->cbdata, _libpmix_cb, (void *)cbdata);
				}
				_reset_coll_ring(coll_ctx);
				/* update global coll seq */
				pmixp_coll_seq++;
				break;
			default:
				PMIXP_ERROR("%p: unknown state = %d",
					    coll_ctx, (int)coll_ctx->state);

		}
	} while(ret);
}

static void _fwrd_pool_free(void *p)
{
	Buf buf = (Buf)p;
	free_buf(buf);
}

pmixp_coll_ring_ctx_t *pmixp_coll_ring_ctx_shift(pmixp_coll_ring_t *coll, const uint32_t seq)
{
	int i;
	pmixp_coll_ring_ctx_t *coll_ctx = NULL;

	for (i = 0; i < PMIXP_COLL_RING_CTX_NUM; i++) {
		if (coll->ctx_array[i].in_use) {
			if (seq == coll->ctx_array[i].seq) {
				/* the correspond coll seq is found */
				coll_ctx = &coll->ctx_array[i];
				/* bind the CXT to coll */
				coll->ctx = coll_ctx;
				return coll_ctx;
			}
		}
	}
	/* this coll seq wasn't found and that isn't old coll seq */
	if (!coll_ctx && (seq >= pmixp_coll_seq)) {
		/* find the free coll ctx */
		for (i = 0; i < PMIXP_COLL_RING_CTX_NUM; i++) {
			if (!coll->ctx_array[i].in_use) {
				coll_ctx = &coll->ctx_array[i];
				coll_ctx->in_use = true;
				coll_ctx->seq = seq;
				/* bind the CXT to coll */
				coll->ctx = coll_ctx;
				return coll_ctx;
			}
		}
	}
	return coll_ctx;
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
	coll->cinfo = cinfo;
	coll->ctx_cur = 0;
	coll->ctx = &coll->ctx_array[coll->ctx_cur];
	coll->peers_cnt = hostlist_count(hl);
	coll->my_peerid = hostlist_find(hl, pmixp_info_hostname());
	hostlist_destroy(hl);

	for (i = 0; i < PMIXP_COLL_RING_CTX_NUM; i++) {
		coll_ctx = &coll->ctx_array[i];
		coll_ctx->coll = coll;
#ifndef NDEBUG
		coll_ctx->magic = PMIXP_COLL_RING_STATE_MAGIC;
#endif
		coll_ctx->in_use = false;
		coll_ctx->seq = 0;
		coll_ctx->contrib_local = false;
		coll_ctx->contrib_prev = 0;
		coll_ctx->ring_buf = create_buf(NULL, 0);
	coll_ctx->fwrd_buf_pool = list_create(_fwrd_pool_free);
		coll_ctx->state = PMIXP_COLL_RING_SYNC;
		coll_ctx->contrib_map = xmalloc(sizeof(bool) * coll->peers_cnt); // TODO bit vector
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
		list_destroy(coll_ctx->fwrd_buf_pool);
		slurm_mutex_destroy(&coll_ctx->lock);
		xfree(coll_ctx->contrib_map);
	}
}

int pmixp_coll_ring_contrib_local(pmixp_coll_ring_t *coll, char *data, size_t size,
				  void *cbfunc, void *cbdata)
{
	int ret = SLURM_SUCCESS;
	pmixp_coll_ring_ctx_t *coll_ctx =
			pmixp_coll_ring_ctx_shift(coll, pmixp_coll_seq);

	if (!coll_ctx) {
		PMIXP_ERROR("Can not get ring collective context, "
			    "seq=%u", pmixp_coll_seq);
		goto exit;
	}

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
		/* Double contribution - there is no error, just reject*/
		PMIXP_DEBUG("Double contribution detected");
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
	set_buf_offset(coll_ctx->ring_buf, get_buf_offset(coll_ctx->ring_buf) + size);

	/* mark local contribution */
	coll_ctx->contrib_local = true;

	/* forward data to the next node */
	ret = _ring_forward_data(coll_ctx, coll->my_peerid, 0,
			   data, size);
	if (ret) {
		PMIXP_ERROR("Can not forward ring data, seq=%u", coll_ctx->seq);
		goto exit;
	}

	/* setup callback info */
	coll->cbfunc = cbfunc;
	coll->cbdata = cbdata;

	_progress_coll_ring(coll_ctx);

exit:
	/* unlock the structure */
	slurm_mutex_unlock(&coll_ctx->lock);
	return ret;
}

int pmixp_coll_ring_hdr_sanity_check(pmixp_coll_ring_t *coll, pmixp_coll_ring_msg_hdr_t *hdr)
{
	if (hdr->contrib_id >= coll->peers_cnt) {
		PMIXP_ERROR("Unexpected contrib_id=%u, peers_cnt=%u", hdr->contrib_id, coll->peers_cnt);
		return SLURM_ERROR;
	}
	if (hdr->nodeid != _ring_prev_id(coll)) {
		PMIXP_ERROR("Unexpected nodeid=%u, expected=%u", hdr->nodeid, _ring_prev_id(coll));
		return SLURM_ERROR;
	}
	return SLURM_SUCCESS;
}

int pmixp_coll_ring_contrib_prev(pmixp_coll_ring_t *coll, pmixp_coll_ring_msg_hdr_t *hdr,
				 Buf buf)
{
	int ret = SLURM_SUCCESS;
	size_t size = hdr->msgsize;
	char *data_src = NULL, *data_dst = NULL;
	pmixp_coll_ring_ctx_t *coll_ctx = coll->ctx;
	uint32_t expexted_seq;

#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("%p: contrib/prev: seqnum=%u, state=%d, nodeid=%d, contrib=%d, "
		    "seq=%d, size=%lu",
		    coll_ctx, coll_ctx->seq, coll_ctx->state, hdr->nodeid,
		    hdr->contrib_id, hdr->hop_seq, size);
#endif
	/* lock the structure */
	slurm_mutex_lock(&coll->ctx->lock);

	/* change the state */
	coll_ctx->ts = time(NULL);

	/* verify msg size */
	if (hdr->msgsize != remaining_buf(buf)) {
		PMIXP_DEBUG("%p: unexpected message size=%d, expect=%zd",
			    coll, remaining_buf(buf), hdr->msgsize);
		goto exit;
	}

	if (hdr->nodeid != _ring_prev_id(coll)) {
		PMIXP_DEBUG("%p: unexpected peerid=%d, expect=%d",
			    coll, hdr->nodeid, _ring_prev_id(coll))
		goto exit;
	}

	/* compute the actual hops of ring: (src - dst + size) % size */
	expexted_seq = (coll->my_peerid + coll->peers_cnt - hdr->contrib_id) % coll->peers_cnt - 1;
	if (hdr->hop_seq != expexted_seq) {
		PMIXP_DEBUG("%p: unexpected ring seq number=%d, expect=%d, coll seq=%d",
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
		ret = _ring_forward_data(coll_ctx, hdr->contrib_id, hdr->hop_seq +1,
					 data_dst, size);
		if (ret) {
			PMIXP_ERROR("Cannot forward ring data");
			goto exit;
		}
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
	pmixp_coll_ring_ctx_t *coll_ctx = coll->ctx;

	/* lock the structure */
	slurm_mutex_lock(&coll_ctx->lock);

	if (PMIXP_COLL_RING_SYNC == coll->ctx->state) {
		slurm_mutex_unlock(&coll_ctx->lock);
		return;
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
