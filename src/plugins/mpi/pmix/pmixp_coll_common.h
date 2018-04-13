#ifndef PMIXP_COLL_COMMON_H
#define PMIXP_COLL_COMMON_H


/*
typedef int (*pmixp_coll_contrib_local_fn_t)(pmixp_coll_t *coll, char *data,
					     size_t size, void *cbfunc,
					     void *cbdata);
typedef int (*pmixp_coll_contrib_child_fn_t)(pmixp_coll_t *coll, uint32_t nodeid,
					     uint32_t seq, Buf buf);
typedef int (*pmixp_coll_contrib_parent)(pmixp_coll_t *coll, uint32_t nodeid,
					 uint32_t seq, Buf buf);
typedef void (*pmixp_coll_bcast)(pmixp_coll_t *coll);
typedef bool (*pmixp_coll_progress)(pmixp_coll_t *coll, char *fwd_node,
				    void **data, uint64_t size);
typedef int (*pmixp_coll_unpack_info)(Buf buf, pmixp_coll_type_t *type,
				      int *nodeid, pmixp_proc_t **r,
				      size_t *nr);
typedef int (*pmixp_coll_belong_chk)(pmixp_coll_type_t type,
				     const pmixp_proc_t *procs, size_t nprocs);
typedef void (*pmixp_coll_reset_if_to)(pmixp_coll_t *coll, time_t ts);

typedef struct {
	pmixp_coll_contrib_local_fn_t contrib_local;
	pmixp_coll_contrib_child_fn_t contrib_child;
} pmixp_coll_handler_t
*/


#endif // PMIXP_COLL_COMMON_H
