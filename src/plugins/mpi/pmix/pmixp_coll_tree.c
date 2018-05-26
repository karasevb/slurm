/*****************************************************************************\
 **  pmix_coll_tree.c - PMIx tree collective primitives
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
#include "src/slurmd/common/reverse_tree_math.h"
#include "src/common/slurm_protocol_api.h"
#include "pmixp_coll.h"
#include "pmixp_nspaces.h"
#include "pmixp_server.h"
#include "pmixp_client.h"

static void _progress_coll(pmixp_coll_t *coll);
static void _reset_coll(pmixp_coll_t *coll);

/*
 * This is important routine that takes responsibility to decide
 * what messages may appear and what may not. In absence of errors
 * we won't need this routine. Unfortunately they are exist.
 * There can be 3 general types of communication errors:
 * 1. We are trying to send our contribution to a parent and it fails.
 *    In this case we will be blocked in send function. At some point
 *    we either succeed or fail after predefined number of trials.
 *
 *    If we succeed - we are OK. Otherwise we will abort the whole job step.
 *
 * 2. A child of us sends us the message and gets the error, however we receive
 *    this message (false negative). Child will try again while we might be:
 *    (a) at FAN-IN step waiting for other contributions.
 *    (b) at FAN-OUT since we get all we need.
 *    (c) 2 step forward (SYNC) with coll->seq = (child_seq+1) if root of the
 *        tree successfuly broadcasted the whole database to us.
 *    (d) 3 step forward (next FAN-IN) with coll->seq = (child_seq+1)
 *        if somebody initiated next collective.
 *    (e) we won't move further because the child with problem won't send us
 *        next contribution.
 *
 *    Cases (a) and (b) can't be noticed here since child and we have the
 *    same seq number. They will later be detected  in pmixp_coll_contrib_node()
 *    based on collective contribution accounting vector.
 *
 *    Cases (c) and (d) would be visible here and should be treated as possible
 *    errors that should be ignored discarding the contribution.
 *
 *    Other cases are obvious error, we can abort in this case or ignore with
 *    error.
 *
 * 3. Root of the tree broadcasts the data and we get it, however root gets
 *    false negative. In this case root will try again. We might be:
 *    (a) at SYNC since we just got the DB and we are fine
 *        (coll->seq == root_seq+1)
 *    (b) at FAN-IN if somebody initiated next collective
 *        (coll->seq == root_seq+1)
 *    (c) at FAN-OUT if we will collect all necessary contributions and send
 *        it to our parent.
 *    (d) we won't be able to switch to SYNC since root will be busy dealing
 *        with previous DB broadcast.
 *    (e) at FAN-OUT waiting for the fan-out msg while receiving next fan-in
 *        message from one of our children (coll->seq + 1 == child_seq).
 */
inline int pmixp_coll_tree_check_seq(pmixp_coll_t *coll, uint32_t seq)
{
	if (coll->seq == seq) {
		/* accept this message */
		return PMIXP_COLL_TREE_REQ_PROGRESS;
	} else if ((coll->seq+1) == seq) {
		/* practice shows that because of Slurm communication
		 * infrastructure our child can switch to the next Fence
		 * and send us the message before the current fan-out message
		 * arrived. This is accounted in current state machine, so we
		 * allow if we receive message with seq number grater by one */
		return PMIXP_COLL_TREE_REQ_PROGRESS;
	} else if ((coll->seq - 1) == seq) {
		/* his may be our child OR root of the tree that
		 * had false negatives from Slurm protocol.
		 * It's normal situation, return error because we
		 * want to discard this message */
		return PMIXP_COLL_TREE_REQ_SKIP;
	}
	/* maybe need more sophisticated handling in presence of
	 * several steps. However maybe it's enough to just ignore */
	return PMIXP_COLL_TREE_REQ_FAILURE;
}

static int _pack_coll_info(pmixp_coll_t *coll, Buf buf)
{
	pmixp_proc_t *procs = coll->pset.procs;
	size_t nprocs = coll->pset.nprocs;
	uint32_t size;
	int i;

	/* 1. store the type of collective */
	size = coll->type;
	pack32(size, buf);

	/* 2. Put the number of ranges */
	pack32(nprocs, buf);
	for (i = 0; i < (int)nprocs; i++) {
		/* Pack namespace */
		packmem(procs->nspace, strlen(procs->nspace) + 1, buf);
		pack32(procs->rank, buf);
	}

	return SLURM_SUCCESS;
}

int pmixp_coll_tree_unpack_info(Buf buf, pmixp_coll_type_t *type,
				int *nodeid, pmixp_proc_t **r, size_t *nr)
{
	pmixp_proc_t *procs = NULL;
	uint32_t nprocs = 0;
	uint32_t tmp;
	int i, rc;

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
			PMIXP_ERROR("Cannot unpack namespace for process #%d",
				    i);
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
	return SLURM_SUCCESS;
}

int pmixp_coll_tree_belong_chk(pmixp_coll_type_t type,
			       const pmixp_proc_t *procs, size_t nprocs)
{
	int i;
	pmixp_namespace_t *nsptr = pmixp_nspaces_local();
	/* Find my namespace in the range */
	for (i = 0; i < nprocs; i++) {
		if (0 != xstrcmp(procs[i].nspace, nsptr->name)) {
			continue;
		}
		if (pmixp_lib_is_wildcard(procs[i].rank))
			return 0;
		if (0 <= pmixp_info_taskid2localid(procs[i].rank)) {
			return 0;
		}
	}
	/* we don't participate in this collective! */
	PMIXP_ERROR("Have collective that doesn't include this job's namespace");
	return -1;
}

static void _reset_coll_ufwd(pmixp_coll_t *coll)
{
	pmixp_coll_tree_t *tree = &coll->state.tree;

	/* upward status */
	tree->contrib_children = 0;
	tree->contrib_local = false;
	memset(tree->contrib_chld, 0,
	       sizeof(tree->contrib_chld[0]) * tree->chldrn_cnt);
	tree->serv_offs = pmixp_server_buf_reset(tree->ufwd_buf);
	if (SLURM_SUCCESS != _pack_coll_info(coll, tree->ufwd_buf)) {
		PMIXP_ERROR("Cannot pack ranges to message header!");
	}
	tree->ufwd_offset = get_buf_offset(tree->ufwd_buf);
	tree->ufwd_status = PMIXP_COLL_TREE_SND_NONE;
}

static void _reset_coll_dfwd(pmixp_coll_t *coll)
{
	/* downwards status */
	(void)pmixp_server_buf_reset(coll->state.tree.dfwd_buf);
	if (SLURM_SUCCESS != _pack_coll_info(coll, coll->state.tree.dfwd_buf)) {
		PMIXP_ERROR("Cannot pack ranges to message header!");
	}
	coll->state.tree.dfwd_cb_cnt = 0;
	coll->state.tree.dfwd_cb_wait = 0;
	coll->state.tree.dfwd_status = PMIXP_COLL_TREE_SND_NONE;
	coll->state.tree.contrib_prnt = false;
	/* Save the toal service offset */
	coll->state.tree.dfwd_offset = get_buf_offset(coll->state.tree.dfwd_buf);
}

static void _reset_coll(pmixp_coll_t *coll)
{
	pmixp_coll_tree_t *tree = &coll->state.tree;

	switch (tree->state) {
	case PMIXP_COLL_TREE_SYNC:
		/* already reset */
		xassert(!tree->contrib_local && !tree->contrib_children &&
			!tree->contrib_prnt);
		break;
	case PMIXP_COLL_TREE_COLLECT:
	case PMIXP_COLL_TREE_UPFWD:
	case PMIXP_COLL_TREE_UPFWD_WSC:
		coll->seq++;
		tree->state = PMIXP_COLL_TREE_SYNC;
		_reset_coll_ufwd(coll);
		_reset_coll_dfwd(coll);
		coll->cbdata = NULL;
		coll->cbfunc = NULL;
		break;
	case PMIXP_COLL_TREE_UPFWD_WPC:
		/* If we were waiting for the parent contrib,
		 * upward portion is already reset, and may contain
		 * next collective's data */
	case PMIXP_COLL_TREE_DOWNFWD:
		/* same with downward state */
		coll->seq++;
		_reset_coll_dfwd(coll);
		if (tree->contrib_local || tree->contrib_children) {
			/* next collective was already started */
			tree->state = PMIXP_COLL_TREE_COLLECT;
		} else {
			tree->state = PMIXP_COLL_TREE_SYNC;
		}

		if (!tree->contrib_local) {
			/* drop the callback info if we haven't started
			 * next collective locally
			 */
			coll->cbdata = NULL;
			coll->cbfunc = NULL;
		}
		break;
	default:
		PMIXP_ERROR("Bad collective state = %d", (int)tree->state);
		abort();
	}
}


/*
 * Based on ideas provided by Hongjia Cao <hjcao@nudt.edu.cn> in PMI2 plugin
 */
int pmixp_coll_tree_init(pmixp_coll_t *coll, hostlist_t *hl)
{
	int max_depth, width, depth, i;
	char *p;
	pmixp_coll_tree_t *tree = NULL;

	tree = &coll->state.tree;
	tree->state = PMIXP_COLL_TREE_SYNC;

	width = slurm_get_tree_width();
	reverse_tree_info(coll->my_peerid, coll->peers_cnt, width,
			  &tree->prnt_peerid, &tree->chldrn_cnt, &depth,
			  &max_depth);

	/* We interested in amount of direct childs */
	tree->contrib_children = 0;
	tree->contrib_local = false;
	tree->chldrn_ids = xmalloc(sizeof(int) * width);
	tree->contrib_chld = xmalloc(sizeof(int) * width);
	tree->chldrn_cnt = reverse_tree_direct_children(coll->my_peerid,
							coll->peers_cnt,
							  width, depth,
							  tree->chldrn_ids);
	if (tree->prnt_peerid == -1) {
		/* if we are the root of the tree:
		 * - we don't have a parent;
		 * - we have large list of all_childrens (we don't want
		 * ourselfs there)
		 */
		tree->prnt_host = NULL;
		tree->all_chldrn_hl = hostlist_copy(*hl);
		hostlist_delete_host(tree->all_chldrn_hl,
				     pmixp_info_hostname());
		tree->chldrn_str =
			hostlist_ranged_string_xmalloc(tree->all_chldrn_hl);
	} else {
		/* for all other nodes in the tree we need to know:
		 * - nodename of our parent;
		 * - we don't need a list of all_childrens and hl anymore
		 */

		/*
		 * setup parent id's
		 */
		p = hostlist_nth(*hl, tree->prnt_peerid);
		tree->prnt_host = xstrdup(p);
		free(p);
		/* reset prnt_peerid to the global peer */
		tree->prnt_peerid = pmixp_info_job_hostid(tree->prnt_host);

		/*
		 * setup root id's
		 * (we need this for the Slurm API communication case)
		 */
		p = hostlist_nth(*hl, 0);
		tree->root_host = xstrdup(p);
		free(p);
		/* reset prnt_peerid to the global peer */
		tree->root_peerid = pmixp_info_job_hostid(tree->root_host);

		/* use empty hostlist here */
		tree->all_chldrn_hl = hostlist_create("");
		tree->chldrn_str = NULL;
	}

	/* fixup children peer ids to te global ones */
	for(i=0; i<tree->chldrn_cnt; i++){
		p = hostlist_nth(*hl, tree->chldrn_ids[i]);
		tree->chldrn_ids[i] = pmixp_info_job_hostid(p);
		free(p);
	}

	/* Collective state */
	tree->ufwd_buf = pmixp_server_buf_new();
	tree->dfwd_buf = pmixp_server_buf_new();
	_reset_coll_ufwd(coll);
	_reset_coll_dfwd(coll);
	coll->cbdata = NULL;
	coll->cbfunc = NULL;

	/* init fine grained lock */
	slurm_mutex_init(&coll->lock);

	return SLURM_SUCCESS;
}

void pmixp_coll_tree_free(pmixp_coll_t *coll)
{
	pmixp_coll_tree_t *tree = &coll->state.tree;

	if (NULL != coll->pset.procs) {
		xfree(coll->pset.procs);
	}
	if (NULL != tree->prnt_host) {
		xfree(tree->prnt_host);
	}
	if (NULL != tree->root_host) {
		xfree(tree->root_host);
	}
	hostlist_destroy(tree->all_chldrn_hl);
	if (tree->chldrn_str) {
		xfree(tree->chldrn_str);
	}
#ifdef PMIXP_COLL_DEBUG
	hostlist_destroy(coll->peers_hl);
#endif
	if (NULL != tree->contrib_chld) {
		xfree(tree->contrib_chld);
	}
	free_buf(tree->ufwd_buf);
	free_buf(tree->dfwd_buf);
}

typedef struct {
	pmixp_coll_t *coll;
	uint32_t seq;
	volatile uint32_t refcntr;
} pmixp_coll_cbdata_t;

/*
 * use it for internal collective
 * performance evaluation tool.
 */
pmixp_coll_t *pmixp_coll_tree_from_cbdata(void *cbdata)
{
	pmixp_coll_cbdata_t *ptr = (pmixp_coll_cbdata_t*)cbdata;
	pmixp_coll_sanity_check(ptr->coll);
	return ptr->coll;
}

static void _ufwd_sent_cb(int rc, pmixp_p2p_ctx_t ctx, void *_vcbdata)
{
	pmixp_coll_cbdata_t *cbdata = (pmixp_coll_cbdata_t*)_vcbdata;
	pmixp_coll_t *coll = cbdata->coll;
	pmixp_coll_tree_t *tree = &coll->state.tree;

	if( PMIXP_P2P_REGULAR == ctx ){
		/* lock the collective */
		slurm_mutex_lock(&coll->lock);
	}
	if (cbdata->seq != coll->seq) {
		/* it seems like this collective was reset since the time
		 * we initiated this send.
		 * Just exit to avoid data corruption.
		 */
		PMIXP_DEBUG("Collective was reset!");
		goto exit;
	}

	xassert(PMIXP_COLL_TREE_UPFWD == tree->state ||
		PMIXP_COLL_TREE_UPFWD_WSC == tree->state);


	/* Change  the status */
	if( SLURM_SUCCESS == rc ){
		tree->ufwd_status = PMIXP_COLL_TREE_SND_DONE;
	} else {
		tree->ufwd_status = PMIXP_COLL_TREE_SND_FAILED;
	}

#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("%p: state: %s, snd_status=%s",
		    coll, pmixp_coll_tree_state2str(tree->state),
		    pmixp_coll_tree_sndstatus2str(tree->ufwd_status));
#endif

exit:
	xassert(0 < cbdata->refcntr);
	cbdata->refcntr--;
	if (!cbdata->refcntr) {
		xfree(cbdata);
	}

	if( PMIXP_P2P_REGULAR == ctx ){
		/* progress, in the inline case progress
		 * will be invoked by the caller */
		_progress_coll(coll);

		/* unlock the collective */
		slurm_mutex_unlock(&coll->lock);
	}
}

static void _dfwd_sent_cb(int rc, pmixp_p2p_ctx_t ctx, void *_vcbdata)
{
	pmixp_coll_cbdata_t *cbdata = (pmixp_coll_cbdata_t*)_vcbdata;
	pmixp_coll_t *coll = cbdata->coll;
	pmixp_coll_tree_t *tree = &coll->state.tree;

	if( PMIXP_P2P_REGULAR == ctx ){
		/* lock the collective */
		slurm_mutex_lock(&coll->lock);
	}

	if (cbdata->seq != coll->seq) {
		/* it seems like this collective was reset since the time
		 * we initiated this send.
		 * Just exit to avoid data corruption.
		 */
		PMIXP_DEBUG("Collective was reset!");
		goto exit;
	}

	xassert(PMIXP_COLL_TREE_DOWNFWD == tree->state);

	/* Change  the status */
	if( SLURM_SUCCESS == rc ){
		tree->dfwd_cb_cnt++;
	} else {
		tree->dfwd_status = PMIXP_COLL_TREE_SND_FAILED;
	}

#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("%p: state: %s, snd_status=%s, compl_cnt=%d/%d",
		    coll, pmixp_coll_tree_state2str(tree->state),
		    pmixp_coll_tree_sndstatus2str(tree->dfwd_status),
		    tree->dfwd_cb_cnt, tree->dfwd_cb_wait);
#endif

exit:
	xassert(0 < cbdata->refcntr);
	cbdata->refcntr--;
	if (!cbdata->refcntr) {
		xfree(cbdata);
	}

	if( PMIXP_P2P_REGULAR == ctx ){
		/* progress, in the inline case progress
		 * will be invoked by the caller */
		_progress_coll(coll);

		/* unlock the collective */
		slurm_mutex_unlock(&coll->lock);
	}
}

static void _libpmix_cb(void *_vcbdata)
{
	pmixp_coll_cbdata_t *cbdata = (pmixp_coll_cbdata_t*)_vcbdata;
	pmixp_coll_t *coll = cbdata->coll;
	pmixp_coll_tree_t *tree = &coll->state.tree;

	/* lock the collective */
	slurm_mutex_lock(&coll->lock);

	if (cbdata->seq != coll->seq) {
		/* it seems like this collective was reset since the time
		 * we initiated this send.
		 * Just exit to avoid data corruption.
		 */
		PMIXP_ERROR("%p: collective was reset: myseq=%u, curseq=%u",
			    coll, cbdata->seq, coll->seq);
		goto exit;
	}

	xassert(PMIXP_COLL_TREE_DOWNFWD == tree->state);

	tree->dfwd_cb_cnt++;
#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("%p: state: %s, snd_status=%s, compl_cnt=%d/%d",
		    coll, pmixp_coll_tree_state2str(tree->state),
		    pmixp_coll_tree_sndstatus2str(tree->dfwd_status),
		    tree->dfwd_cb_cnt, tree->dfwd_cb_wait);
#endif
	_progress_coll(coll);

exit:
	xassert(0 < cbdata->refcntr);
	cbdata->refcntr--;
	if (!cbdata->refcntr) {
		xfree(cbdata);
	}

	/* unlock the collective */
	slurm_mutex_unlock(&coll->lock);
}

static int _progress_collect(pmixp_coll_t *coll)
{
	pmixp_ep_t ep = {0};
	int rc;
	pmixp_coll_tree_t *tree = &coll->state.tree;

	xassert(PMIXP_COLL_TREE_COLLECT == tree->state);

	ep.type = PMIXP_EP_NONE;
#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("%p: state=%s, local=%d, child_cntr=%d",
		    coll, pmixp_coll_tree_state2str(tree->state),
		    (int)tree->contrib_local, tree->contrib_children);
#endif
	/* sanity check */
	pmixp_coll_sanity_check(coll);

	if (PMIXP_COLL_TREE_COLLECT != tree->state) {
		/* In case of race condition between libpmix and
		 * slurm threads we can be called
		 * after we moved to the next step. */
		return 0;
	}

	if (!tree->contrib_local ||
	    tree->contrib_children != tree->chldrn_cnt) {
		/* Not yet ready to go to the next step */
		return 0;
	}

	if (pmixp_info_srv_direct_conn()) {
		/* We will need to forward aggregated
		 * message back to our children */
		tree->state = PMIXP_COLL_TREE_UPFWD;
	} else {
		/* If we use Slurm API (SAPI) - intermediate nodes
		 * don't need to forward data as the root will do
		 * SAPI broadcast.
		 * So, only root has to go through the full UPFWD
		 * state and send the message back.
		 * Other procs have to go through other route. The reason for
		 * that is the fact that som of out children can receive bcast
		 * message early and initiate next collective. We need to handle
		 * that properly.
		 */
		if (0 > tree->prnt_peerid) {
			tree->state = PMIXP_COLL_TREE_UPFWD;
		} else {
			tree->state = PMIXP_COLL_TREE_UPFWD_WSC;
		}
	}

	/* The root of the collective will have parent_host == NULL */
	if (NULL != tree->prnt_host) {
		ep.type = PMIXP_EP_NOIDEID;
		ep.ep.nodeid = tree->prnt_peerid;
		tree->ufwd_status = PMIXP_COLL_TREE_SND_ACTIVE;
		PMIXP_DEBUG("%p: send data to %s:%d",
			    coll, tree->prnt_host, tree->prnt_peerid);
	} else {
		/* move data from input buffer to the output */
		char *dst, *src = get_buf_data(tree->ufwd_buf) +
				tree->ufwd_offset;
		size_t size = get_buf_offset(tree->ufwd_buf) -
				tree->ufwd_offset;
		pmixp_server_buf_reserve(tree->dfwd_buf, size);
		dst = get_buf_data(tree->dfwd_buf) + tree->dfwd_offset;
		memcpy(dst, src, size);
		set_buf_offset(tree->dfwd_buf, tree->dfwd_offset + size);
		/* no need to send */
		tree->ufwd_status = PMIXP_COLL_TREE_SND_DONE;
		/* this is root */
		tree->contrib_prnt = true;
	}

	if (PMIXP_EP_NONE != ep.type) {
		pmixp_coll_cbdata_t *cbdata;
		cbdata = xmalloc(sizeof(pmixp_coll_cbdata_t));
		cbdata->coll = coll;
		cbdata->seq = coll->seq;
		cbdata->refcntr = 1;
		char *nodename = tree->prnt_host;
		rc = pmixp_server_send_nb(&ep, PMIXP_MSG_FAN_IN, coll->seq,
					  tree->ufwd_buf,
					  _ufwd_sent_cb, cbdata);

		if (SLURM_SUCCESS != rc) {
			PMIXP_ERROR("Cannot send data (size = %lu), "
				    "to %s:%d",
				    (uint64_t) get_buf_offset(tree->ufwd_buf),
				    nodename, ep.ep.nodeid);
			tree->ufwd_status = PMIXP_COLL_TREE_SND_FAILED;
		}
#ifdef PMIXP_COLL_DEBUG
		PMIXP_DEBUG("%p: fwd to %s:%d, size = %lu",
			    coll, nodename, ep.ep.nodeid,
			    (uint64_t) get_buf_offset(tree->dfwd_buf));
#endif
	}

	/* events observed - need another iteration */
	return true;
}

static int _progress_ufwd(pmixp_coll_t *coll)
{
	pmixp_coll_tree_t *tree = &coll->state.tree;
	pmixp_ep_t ep[tree->chldrn_cnt];
	int ep_cnt = 0;
	int rc, i;
	char *nodename = NULL;
	pmixp_coll_cbdata_t *cbdata = NULL;

	xassert(PMIXP_COLL_TREE_UPFWD == tree->state);

	/* for some reasons doesnt switch to downfwd */

	switch (tree->ufwd_status) {
	case PMIXP_COLL_TREE_SND_FAILED:
		/* something went wrong with upward send.
		 * notify libpmix about that and abort
		 * collective */
		if (coll->cbfunc) {
			pmixp_lib_modex_invoke(coll->cbfunc, SLURM_ERROR, NULL,
					       0, coll->cbdata, NULL, NULL);
		}
		_reset_coll(coll);
		/* Don't need to do anything else */
		return false;
	case PMIXP_COLL_TREE_SND_ACTIVE:
		/* still waiting for the send completion */
		return false;
	case PMIXP_COLL_TREE_SND_DONE:
		if (tree->contrib_prnt) {
			/* all-set to go to the next stage */
			break;
		}
		return false;
	default:
		/* Should not happen, fatal error */
		abort();
	}

	/* We now can upward part for the next collective */
	_reset_coll_ufwd(coll);

	/* move to the next state */
	tree->state = PMIXP_COLL_TREE_DOWNFWD;
	tree->dfwd_status = PMIXP_COLL_TREE_SND_ACTIVE;
	if (!pmixp_info_srv_direct_conn()) {
		/* only root of the tree should get here */
		xassert(0 > tree->prnt_peerid);
		if (tree->chldrn_cnt) {
			/* We can run on just one node */
			ep[ep_cnt].type = PMIXP_EP_HLIST;
			ep[ep_cnt].ep.hostlist = tree->chldrn_str;
			ep_cnt++;
		}
	} else {
		for(i=0; i<tree->chldrn_cnt; i++){
			ep[i].type = PMIXP_EP_NOIDEID;
			ep[i].ep.nodeid = tree->chldrn_ids[i];
			ep_cnt++;
		}
	}

	/* We need to wait for ep_cnt send completions + the local callback */
	tree->dfwd_cb_wait = ep_cnt;

	if (ep_cnt || coll->cbfunc) {
		/* allocate the callback data */
		cbdata = xmalloc(sizeof(pmixp_coll_cbdata_t));
		cbdata->coll = coll;
		cbdata->seq = coll->seq;
		cbdata->refcntr = ep_cnt;
		if (coll->cbfunc) {
			cbdata->refcntr++;
		}
	}

	for(i=0; i < ep_cnt; i++){
		rc = pmixp_server_send_nb(&ep[i], PMIXP_MSG_FAN_OUT, coll->seq,
					  tree->dfwd_buf,
					  _dfwd_sent_cb, cbdata);

		if (SLURM_SUCCESS != rc) {
			if (PMIXP_EP_NOIDEID == ep[i].type){
				nodename = pmixp_info_job_host(ep[i].ep.nodeid);
				PMIXP_ERROR("Cannot send data (size = %lu), "
				    "to %s:%d",
				    (uint64_t) get_buf_offset(tree->dfwd_buf),
				    nodename, ep[i].ep.nodeid);
				xfree(nodename);
			} else {
				PMIXP_ERROR("Cannot send data (size = %lu), "
				    "to %s",
				    (uint64_t) get_buf_offset(tree->dfwd_buf),
				    ep[i].ep.hostlist);
			}
			tree->dfwd_status = PMIXP_COLL_TREE_SND_FAILED;
		}
#ifdef PMIXP_COLL_DEBUG
		if (PMIXP_EP_NOIDEID == ep[i].type) {
			nodename = pmixp_info_job_host(ep[i].ep.nodeid);
			PMIXP_DEBUG("%p: fwd to %s:%d, size = %lu",
				    coll, nodename, ep[i].ep.nodeid,
				    (uint64_t) get_buf_offset(tree->dfwd_buf));
			xfree(nodename);
		} else {
			PMIXP_DEBUG("%p: fwd to %s, size = %lu",
				    coll, ep[i].ep.hostlist,
				    (uint64_t) get_buf_offset(tree->dfwd_buf));
		}
#endif
	}

	if (coll->cbfunc) {
		char *data = get_buf_data(tree->dfwd_buf) + tree->dfwd_offset;
		size_t size = get_buf_offset(tree->dfwd_buf) -
				tree->dfwd_offset;
		tree->dfwd_cb_wait++;
		pmixp_lib_modex_invoke(coll->cbfunc, SLURM_SUCCESS, data, size,
				       coll->cbdata, _libpmix_cb, (void*)cbdata);
#ifdef PMIXP_COLL_DEBUG
		PMIXP_DEBUG("%p: local delivery, size = %lu",
			    coll, (uint64_t)size);
#endif
	}

	/* events observed - need another iteration */
	return true;
}

static int _progress_ufwd_sc(pmixp_coll_t *coll)
{
	pmixp_coll_tree_t *tree = &coll->state.tree;

	xassert(PMIXP_COLL_TREE_UPFWD_WSC == tree->state);

	/* for some reasons doesnt switch to downfwd */
	switch (tree->ufwd_status) {
	case PMIXP_COLL_TREE_SND_FAILED:
		/* something went wrong with upward send.
		 * notify libpmix about that and abort
		 * collective */
		if (coll->cbfunc) {
			pmixp_lib_modex_invoke(coll->cbfunc, SLURM_ERROR, NULL,
					       0, coll->cbdata, NULL, NULL);
		}
		_reset_coll(coll);
		/* Don't need to do anything else */
		return false;
	case PMIXP_COLL_TREE_SND_ACTIVE:
		/* still waiting for the send completion */
		return false;
	case PMIXP_COLL_TREE_SND_DONE:
		/* move to the next step */
		break;
	default:
		/* Should not happen, fatal error */
		abort();
	}

	/* We now can upward part for the next collective */
	_reset_coll_ufwd(coll);

	/* move to the next state */
	tree->state = PMIXP_COLL_TREE_UPFWD_WPC;
	return true;
}

static int _progress_ufwd_wpc(pmixp_coll_t *coll)
{
	pmixp_coll_tree_t *tree = &coll->state.tree;

	xassert(PMIXP_COLL_TREE_UPFWD_WPC == tree->state);

	if (!tree->contrib_prnt) {
		return false;
	}

	/* Need to wait only for the local completion callback if installed*/
	tree->dfwd_status = PMIXP_COLL_TREE_SND_ACTIVE;
	tree->dfwd_cb_wait = 0;


	/* move to the next state */
	tree->state = PMIXP_COLL_TREE_DOWNFWD;

	/* local delivery */
	if (coll->cbfunc) {
		pmixp_coll_cbdata_t *cbdata;
		cbdata = xmalloc(sizeof(pmixp_coll_cbdata_t));
		cbdata->coll = coll;
		cbdata->seq = coll->seq;
		cbdata->refcntr = 1;

		char *data = get_buf_data(tree->dfwd_buf) + tree->dfwd_offset;
		size_t size = get_buf_offset(tree->dfwd_buf) -
				tree->dfwd_offset;
		pmixp_lib_modex_invoke(coll->cbfunc, SLURM_SUCCESS, data, size,
				       coll->cbdata, _libpmix_cb, (void *)cbdata);
		tree->dfwd_cb_wait++;
#ifdef PMIXP_COLL_DEBUG
		PMIXP_DEBUG("%p: local delivery, size = %lu",
			    coll, (uint64_t)size);
#endif
	}

	/* events observed - need another iteration */
	return true;
}

static int _progress_dfwd(pmixp_coll_t *coll)
{
	pmixp_coll_tree_t *tree = &coll->state.tree;

	xassert(PMIXP_COLL_TREE_DOWNFWD == tree->state);

	/* if all childrens + local callbacks was invoked */
	if (tree->dfwd_cb_wait == tree->dfwd_cb_cnt) {
		tree->dfwd_status = PMIXP_COLL_TREE_SND_DONE;
	}

	switch (tree->dfwd_status) {
	case PMIXP_COLL_TREE_SND_ACTIVE:
		return false;
	case PMIXP_COLL_TREE_SND_FAILED:
		/* something went wrong with upward send.
		 * notify libpmix about that and abort
		 * collective */
		PMIXP_ERROR("%p: failed to send, abort collective", coll);
		if (coll->cbfunc) {
			pmixp_lib_modex_invoke(coll->cbfunc, SLURM_ERROR, NULL,
					       0, coll->cbdata, NULL, NULL);
		}
		_reset_coll(coll);
		/* Don't need to do anything else */
		return false;
	case PMIXP_COLL_TREE_SND_DONE:
		break;
	default:
		/* Should not happen, fatal error */
		abort();
	}
#ifdef PMIXP_COLL_DEBUG
	PMIXP_ERROR("%p: %s seq=%d is DONE", coll,
		    pmixp_coll_type2str(coll->type), coll->seq);
#endif
	_reset_coll(coll);

	return true;
}

static void _progress_coll(pmixp_coll_t *coll)
{
	pmixp_coll_tree_t *tree = &coll->state.tree;
	int ret = 0;

	do {
		switch (tree->state) {
		case PMIXP_COLL_TREE_SYNC:
			/* check if any activity was observed */
			if (tree->contrib_local || tree->contrib_children) {
				tree->state = PMIXP_COLL_TREE_COLLECT;
				ret = true;
			} else {
				ret = false;
			}
			break;
		case PMIXP_COLL_TREE_COLLECT:
			ret = _progress_collect(coll);
			break;
		case PMIXP_COLL_TREE_UPFWD:
			ret = _progress_ufwd(coll);
			break;
		case PMIXP_COLL_TREE_UPFWD_WSC:
			ret = _progress_ufwd_sc(coll);
			break;
		case PMIXP_COLL_TREE_UPFWD_WPC:
			ret = _progress_ufwd_wpc(coll);
			break;
		case PMIXP_COLL_TREE_DOWNFWD:
			ret = _progress_dfwd(coll);
			break;
		default:
			PMIXP_ERROR("%p: unknown state = %d",
				    coll, tree->state);
		}
	} while(ret);
}

int pmixp_coll_tree_contrib_local(pmixp_coll_t *coll, char *data, size_t size,
				  void *cbfunc, void *cbdata)
{
	pmixp_coll_tree_t *tree = &coll->state.tree;
	int ret = SLURM_SUCCESS;

	pmixp_debug_hang(0);

	/* sanity check */
	pmixp_coll_sanity_check(coll);

	/* lock the structure */
	slurm_mutex_lock(&coll->lock);

#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("%p: contrib/loc: seqnum=%u, state=%s, size=%zd",
		    coll, coll->seq, pmixp_coll_tree_state2str(tree->state), size);
#endif

	switch (tree->state) {
	case PMIXP_COLL_TREE_SYNC:
		/* change the state */
		coll->ts = time(NULL);
		/* fall-thru */
	case PMIXP_COLL_TREE_COLLECT:
		/* sanity check */
		break;
	case PMIXP_COLL_TREE_DOWNFWD:
		/* We are waiting for some send requests
		 * to be finished, but local node has started
		 * the next contribution.
		 * This is an OK situation, go ahead and store
		 * it, the buffer with the contribution is not used
		 * now.
		 */
#ifdef PMIXP_COLL_DEBUG
		PMIXP_DEBUG("%p: contrib/loc: next coll!", coll);
#endif
		break;
	case PMIXP_COLL_TREE_UPFWD:
	case PMIXP_COLL_TREE_UPFWD_WSC:
	case PMIXP_COLL_TREE_UPFWD_WPC:
		/* this is not a correct behavior, respond with an error. */
#ifdef PMIXP_COLL_DEBUG
		PMIXP_DEBUG("%p: contrib/loc: before prev coll is finished!",
			    coll);
#endif
		ret = SLURM_ERROR;
		goto exit;
	default:
		/* FATAL: should not happen in normal workflow */
		PMIXP_ERROR("%p: local contrib while active collective, "
			    "state = %s",
			    coll, pmixp_coll_tree_state2str(tree->state));
		xassert(0);
		abort();
	}

	if (tree->contrib_local) {
		/* Double contribution - reject */
		ret = SLURM_ERROR;
		goto exit;
	}

	/* save & mark local contribution */
	tree->contrib_local = true;
	pmixp_server_buf_reserve(tree->ufwd_buf, size);
	memcpy(get_buf_data(tree->ufwd_buf) + get_buf_offset(tree->ufwd_buf),
	       data, size);
	set_buf_offset(tree->ufwd_buf, get_buf_offset(tree->ufwd_buf) + size);

	/* setup callback info */
	coll->cbfunc = cbfunc;
	coll->cbdata = cbdata;

	/* check if the collective is ready to progress */
	_progress_coll(coll);

#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("%p: finish, state=%s",
		    coll, pmixp_coll_tree_state2str(tree->state));
#endif

exit:
	/* unlock the structure */
	slurm_mutex_unlock(&coll->lock);
	return ret;
}

static int _chld_id(pmixp_coll_tree_t *tree, uint32_t nodeid)
{
	int i;

	for (i=0; i<tree->chldrn_cnt; i++) {
		if (tree->chldrn_ids[i] == nodeid) {
			return i;
		}
	}
	return -1;
}

static char *_chld_ids_str(pmixp_coll_tree_t *tree)
{
	char *p = NULL;
	int i;

	for (i=0; i<tree->chldrn_cnt; i++) {
		if ((tree->chldrn_cnt-1) > i) {
			xstrfmtcat(p, "%d, ", tree->chldrn_ids[i]);
		} else {
			xstrfmtcat(p, "%d", tree->chldrn_ids[i]);
		}
	}
	return p;
}


int pmixp_coll_tree_contrib_child(pmixp_coll_t *coll, uint32_t peerid,
				  uint32_t seq, Buf buf)
{
	char *data_src = NULL, *data_dst = NULL;
	uint32_t size;
	int chld_id;
	pmixp_coll_tree_t *tree = &coll->state.tree;

	/* lock the structure */
	slurm_mutex_lock(&coll->lock);
	pmixp_coll_sanity_check(coll);
	if (0 > (chld_id = _chld_id(tree, peerid))) {
		char *nodename = pmixp_info_job_host(peerid);
		char *avail_ids = _chld_ids_str(tree);
		PMIXP_DEBUG("%p: contribution from the non-child node "
			    "%s:%d, acceptable ids: %s",
			    coll, nodename, peerid, avail_ids);
		xfree(nodename);
		xfree(avail_ids);
	}

#ifdef PMIXP_COLL_DEBUG
	char *nodename = pmixp_info_job_host(peerid);
	int lpeerid = hostlist_find(coll->peers_hl, nodename);
	PMIXP_DEBUG("%p: contrib/rem from %s:%d(%d:%d):, state=%s, size=%u",
		    coll, nodename, peerid, lpeerid, chld_id,
		    pmixp_coll_tree_state2str(tree->state),
		    remaining_buf(buf));
#endif

	switch (tree->state) {
	case PMIXP_COLL_TREE_SYNC:
		/* change the state */
		coll->ts = time(NULL);
		/* fall-thru */
	case PMIXP_COLL_TREE_COLLECT:
		/* sanity check */
		if (coll->seq != seq) {
			char *nodename = pmixp_info_job_host(peerid);
			/* FATAL: should not happen in normal workflow */
			PMIXP_ERROR("%p: unexpected contrib from %s:%d "
				    "(child #%d) seq = %d, coll->seq = %d, "
				    "state=%s",
				    coll, nodename, peerid, chld_id,
				    seq, coll->seq,
				    pmixp_coll_tree_state2str(tree->state));
			xassert(coll->seq == seq);
			abort();
		}
		break;
	case PMIXP_COLL_TREE_UPFWD:
	case PMIXP_COLL_TREE_UPFWD_WSC:
		/* FATAL: should not happen in normal workflow */
		PMIXP_ERROR("%p: unexpected contrib from %s:%d, state = %s",
			    coll, nodename, peerid,
			    pmixp_coll_tree_state2str(tree->state));
		xassert(0);
		abort();
	case PMIXP_COLL_TREE_UPFWD_WPC:
	case PMIXP_COLL_TREE_DOWNFWD:
#ifdef PMIXP_COLL_DEBUG
		/* It looks like a retransmission attempt when remote side
		 * identified transmission failure, but we actually successfuly
		 * received the message */
		PMIXP_DEBUG("%p: contrib for the next collective "
			    "from=%s:%d(%d:%d) contrib_seq=%u, coll->seq=%u, "
			    "state=%s",
			    coll, nodename, peerid, lpeerid, chld_id,
			    seq, coll->seq, pmixp_coll_tree_state2str(tree->state));
#endif
		if ((coll->seq +1) != seq) {
			char *nodename = pmixp_info_job_host(peerid);
			/* should not happen in normal workflow */
			PMIXP_ERROR("%p: unexpected contrib from %s:%d(x:%d) "
				    "seq = %d, coll->seq = %d, "
				    "state=%s",
				    coll, nodename, peerid, chld_id,
				    seq, coll->seq,
				    pmixp_coll_tree_state2str(tree->state));
			xfree(nodename);
			xassert((coll->seq +1) == seq);
			abort();
		}
		break;
	default:
		/* should not happen in normal workflow */
		PMIXP_ERROR("%p: unknown collective state %s",
			    coll, pmixp_coll_tree_state2str(tree->state));
		abort();
	}

	/* Because of possible timeouts/delays in transmission we
	 * can receive a contribution second time. Avoid duplications
	 * by checking our records. */
	if (tree->contrib_chld[chld_id]) {
		char *nodename = pmixp_info_job_host(peerid);
		/* May be 0 or 1. If grater - transmission skew, ignore.
		 * NOTE: this output is not on the critical path -
		 * don't preprocess it out */
		PMIXP_DEBUG("%p: multiple contribs from %s:%d(x:%d)",
			    coll, nodename, peerid, chld_id);
		/* this is duplication, skip. */
		xfree(nodename);
		goto proceed;
	}

	data_src = get_buf_data(buf) + get_buf_offset(buf);
	size = remaining_buf(buf);
	pmixp_server_buf_reserve(tree->ufwd_buf, size);
	data_dst = get_buf_data(tree->ufwd_buf) +
			get_buf_offset(tree->ufwd_buf);
	memcpy(data_dst, data_src, size);
	set_buf_offset(tree->ufwd_buf, get_buf_offset(tree->ufwd_buf) + size);

	/* increase number of individual contributions */
	tree->contrib_chld[chld_id] = true;
	/* increase number of total contributions */
	tree->contrib_children++;

proceed:
	_progress_coll(coll);

#ifdef PMIXP_COLL_DEBUG
	PMIXP_DEBUG("%p: finish: node=%s:%d(%d:%d), state=%s",
		    coll, nodename, peerid, lpeerid, chld_id,
		    pmixp_coll_tree_state2str(tree->state));
	xfree(nodename);
#endif
	/* unlock the structure */
	slurm_mutex_unlock(&coll->lock);

	return SLURM_SUCCESS;
}

int pmixp_coll_tree_contrib_parent(pmixp_coll_t *coll, uint32_t peerid,
				   uint32_t seq, Buf buf)
{
	pmixp_coll_tree_t *tree = &coll->state.tree;
#ifdef PMIXP_COLL_DEBUG
	char *nodename = NULL;
	int lpeerid = -1;
#endif
	char *data_src = NULL, *data_dst = NULL;
	uint32_t size;
	int expected_peerid;

	/* lock the structure */
	slurm_mutex_lock(&coll->lock);

	if (pmixp_info_srv_direct_conn()) {
		expected_peerid = tree->prnt_peerid;
	} else {
		expected_peerid = tree->root_peerid;
	}

	/* Sanity check */
	pmixp_coll_sanity_check(coll);
	if (expected_peerid != peerid) {
		char *nodename = pmixp_info_job_host(peerid);
		/* protect ourselfs if we are running with no asserts */
		PMIXP_ERROR("%p: parent contrib from bad nodeid=%s:%u, "
			    "expect=%d",
			    coll, nodename, peerid, expected_peerid);
		xfree(nodename);
		goto proceed;
	}

#ifdef PMIXP_COLL_DEBUG
	nodename = pmixp_info_job_host(peerid);
	lpeerid = hostlist_find(coll->peers_hl, nodename);
	/* Mark this event */
	PMIXP_DEBUG("%p: contrib/rem from %s:%d(%d): state=%s, size=%u",
		    coll, nodename, peerid, lpeerid,
		    pmixp_coll_tree_state2str(tree->state), remaining_buf(buf));
#endif

	switch (tree->state) {
	case PMIXP_COLL_TREE_SYNC:
	case PMIXP_COLL_TREE_COLLECT:
		/* It looks like a retransmission attempt when remote side
		 * identified transmission failure, but we actually successfuly
		 * received the message */
#ifdef PMIXP_COLL_DEBUG
		PMIXP_DEBUG("%p: prev contrib from %s:%d(%d): "
			    "seq=%u, cur_seq=%u, state=%s",
			    coll, nodename, peerid, lpeerid,
			    seq, coll->seq,
			    pmixp_coll_tree_state2str(tree->state));
#endif
		/* sanity check */
		if ((coll->seq - 1) != seq) {
			/* FATAL: should not happen in normal workflow */
			char *nodename = pmixp_info_job_host(peerid);
			PMIXP_ERROR("%p: unexpected contrib from %s:%d: "
				    "contrib_seq = %d, coll->seq = %d, "
				    "state=%s",
				    coll, nodename, peerid,
				    seq, coll->seq,
				    pmixp_coll_tree_state2str(tree->state));
			xfree(nodename);
			xassert((coll->seq - 1) == seq);
			abort();
		}
		goto proceed;
	case PMIXP_COLL_TREE_UPFWD_WSC:{
		/* we are not actually ready to receive this contribution as
		 * the upward portion of the collective wasn't received yet.
		 * This should not happen as SAPI (Slurm API) is blocking and
		 * we chould transit to PMIXP_COLL_UPFWD_WPC immediately */
		/* FATAL: should not happen in normal workflow */
		char *nodename = pmixp_info_job_host(peerid);
		PMIXP_ERROR("%p: unexpected contrib from %s:%d: "
			    "contrib_seq = %d, coll->seq = %d, "
			    "state=%s",
			    coll, nodename, peerid,
			    seq, coll->seq,
			    pmixp_coll_tree_state2str(tree->state));
		xfree(nodename);
		xassert((coll->seq - 1) == seq);
		abort();
	}
	case PMIXP_COLL_TREE_UPFWD:
	case PMIXP_COLL_TREE_UPFWD_WPC:
		/* we were waiting for this */
		break;
	case PMIXP_COLL_TREE_DOWNFWD:
		/* It looks like a retransmission attempt when remote side
		 * identified transmission failure, but we actually successfuly
		 * received the message */
#ifdef PMIXP_COLL_DEBUG
		PMIXP_DEBUG("%p: double contrib from %s:%d(%d) "
			    "seq=%u, cur_seq=%u, state=%s",
			    coll, nodename, peerid, lpeerid,
			    seq, coll->seq, pmixp_coll_tree_state2str(tree->state));
#endif
		/* sanity check */
		if (coll->seq != seq) {
			char *nodename = pmixp_info_job_host(peerid);
			/* FATAL: should not happen in normal workflow */
			PMIXP_ERROR("%p: unexpected contrib from %s:%d: "
				    "seq = %d, coll->seq = %d, state=%s",
				    coll, nodename, peerid,
				    seq, coll->seq,
				    pmixp_coll_tree_state2str(tree->state));
			xassert((coll->seq - 1) == seq);
			xfree(nodename);
			abort();
		}
		goto proceed;
	default:
		/* should not happen in normal workflow */
		PMIXP_ERROR("%p: unknown collective state %s",
			    coll, pmixp_coll_tree_state2str(tree->state));
		abort();
	}

	/* Because of possible timeouts/delays in transmission we
	 * can receive a contribution second time. Avoid duplications
	 * by checking our records. */
	if (tree->contrib_prnt) {
		char *nodename = pmixp_info_job_host(peerid);
		/* May be 0 or 1. If grater - transmission skew, ignore.
		 * NOTE: this output is not on the critical path -
		 * don't preprocess it out */
		PMIXP_DEBUG("%p: multiple contributions from parent %s:%d",
			    coll, nodename, peerid);
		xfree(nodename);
		/* this is duplication, skip. */
		goto proceed;
	}
	tree->contrib_prnt = true;

	data_src = get_buf_data(buf) + get_buf_offset(buf);
	size = remaining_buf(buf);
	pmixp_server_buf_reserve(tree->dfwd_buf, size);

	data_dst = get_buf_data(tree->dfwd_buf) +
			get_buf_offset(tree->dfwd_buf);
	memcpy(data_dst, data_src, size);
	set_buf_offset(tree->dfwd_buf,
		       get_buf_offset(tree->dfwd_buf) + size);
proceed:
	_progress_coll(coll);

#ifdef PMIXP_COLL_DEBUG
	if (nodename) {
		PMIXP_DEBUG("%p: finish: node=%s:%d(%d), state=%s",
			    coll, nodename, peerid, lpeerid,
			    pmixp_coll_tree_state2str(tree->state));
		xfree(nodename);
	}
#endif
	/* unlock the structure */
	slurm_mutex_unlock(&coll->lock);

	return SLURM_SUCCESS;
}

void pmixp_coll_tree_reset_if_to(pmixp_coll_t *coll, time_t ts)
{
	pmixp_coll_tree_t *tree = &coll->state.tree;

	/* lock the */
	slurm_mutex_lock(&coll->lock);

	if (PMIXP_COLL_TREE_SYNC == tree->state) {
		goto unlock;
	}

	if (ts - coll->ts > pmixp_info_timeout()) {
		/* respond to the libpmix */
		if (tree->contrib_local && coll->cbfunc) {
			/* Call the callback only if:
			 * - we were asked to do that (coll->cbfunc != NULL);
			 * - local contribution was received.
			 * TODO: we may want to mark this event to respond with
			 * to the next local request immediately and with the
			 * proper (status == PMIX_ERR_TIMEOUT)
			 */
			pmixp_lib_modex_invoke(coll->cbfunc, PMIXP_ERR_TIMEOUT, NULL,
					       0, coll->cbdata, NULL, NULL);
		}
		/* drop the collective */
		_reset_coll(coll);
		/* report the timeout event */
		PMIXP_ERROR("Collective timeout!");
	}
unlock:
	/* unlock the structure */
	slurm_mutex_unlock(&coll->lock);
}
