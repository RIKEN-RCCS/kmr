/* kmrmapmp.c (2015-06-04) */
/* Copyright (C) 2012-2015 RIKEN AICS */

/** \file kmrmapmp.c MPI Parallel Mapping on Key-Value Stream. */


#include <mpi.h>
#include "kmr.h"
#include "kmrimpl.h"

/* Adds a given key-value pair unmodified on rank0 only.
   It is a map-function. */

static int
kmr_add_on_rank_zero_fn(const struct kmr_kv_box kv, const KMR_KVS *kvi,
                        KMR_KVS *kvo, void *arg, const long i)
{
    if (kvi->c.mr->rank == 0) {
        kmr_add_kv(kvo, kv);
    }
    return MPI_SUCCESS;
}

/** Maps key-value pairs using multiple processes.
    All ranks that call this function are grouped to form
    sub-communicators whose maximum communicator sizes are
    MAX_NPROCS.  The number of sub-communicators can be calculated
    by kvi->c.mr->nprocs / MAX_NPROCS (kvi is a parameter to this
    function).  The sub-communicator can be accessed by
    'kvi->c.mr->comm' or 'kvo->c.mr->comm' from the user-defined
    map-function.  Here kvi and kvo are parameters for the
    map-function, not the parameter to this function.
    Any MPI functions can be called by using the sub-communicator.

    Without any option, all key-value pairs in each group are processed
    inside the group.  When processing a key-value pair, all ranks
    in a group call the map-function given the same key-value pair
    as a parameter.  This means that a target key-value pair is
    broadcasted to all ranks in the group before processing.
    This is repeated until all the key-value pairs in the group
    are processed.

    When RANK_ZERO option is given, only key-value pairs in rank 0
    are processed and all other key-value pairs stored on other
    ranks are ignored.  In this case, key-value pairs in rank 0
    are scattered to rank 0 processes in the sub-communicators.
    In each sub-communicator, the rank 0 process broadcasts the
    key-values to other ranks, and then each rank calls the
    map-function as the above.

    It is a collective operation.  It supports checkpoint/restart,
    but whole resultant KVO is taken as a checkpoint file once when
    all key-value pairs are completely processed.

    It consumes the input key-value stream KVI unless INSPECT option
    is marked.  The output key-value stream KVO can be null, but in
    that case, a map-function cannot add key-value pairs.
    The pointer ARG is just passed to a map-function as a general
    argument.  M is the map-function.  See the description on
    the type ::kmr_mapfn_t.
    Effective-options:  INSPECT, RANK_ZERO, TAKE_CKPT.
    See struct kmr_option.
*/

int
kmr_map_multiprocess(KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
                     struct kmr_option opt, int max_nprocs, kmr_mapfn_t m)
{
    kmr_assert_kvs_ok(kvi, kvo, 1, 0);
    KMR *mr = kvi->c.mr;
    struct kmr_option opt_supported = {.inspect = 1, .rank_zero = 1,
                                       .take_ckpt = 1};
    kmr_check_fn_options(mr, opt_supported, opt, __func__);
    int cc;

    if (kmr_ckpt_enabled(mr)) {
        if (kmr_ckpt_progress_init(kvi, kvo, opt)) {
            if (!opt.inspect) {
                kmr_free_kvs(kvi);
            }
            return MPI_SUCCESS;
        }
    }
    int kcdc = kmr_ckpt_disable_ckpt(mr);

    /* split communicator */
    int rem = mr->nprocs % max_nprocs;
    if (rem != 0) {
        if (mr->rank == 0) {
            kmr_warning(mr, 1, "Not all the number of processes in "
                        "sub-communicators are same");
        }
    }
    MPI_Comm task_comm;
    int task_color = mr->rank / max_nprocs;
    MPI_Comm_split(mr->comm, task_color, mr->rank, &task_comm);
    KMR *task_mr = kmr_create_context(task_comm, MPI_INFO_NULL, 0);
    task_mr->preset_block_size = mr->preset_block_size;

    enum kmr_kv_field kvi_keyf = kmr_unit_sized_or_opaque(kvi->c.key_data);
    enum kmr_kv_field kvi_valf = kmr_unit_sized_or_opaque(kvi->c.value_data);

    KMR_KVS *kvs0 = kmr_create_kvs(task_mr, kvi_keyf, kvi_valf);
    if (opt.rank_zero) {
        /* distribute key-values in root (rank0 in mr->comm) to
           rank0 processes in sub-communicators */
        MPI_Comm root_comm;
        int task_rank;
        MPI_Comm_rank(task_comm, &task_rank);
        int root_color = (task_rank == 0)? 1 : 0;
        MPI_Comm_split(mr->comm, root_color, mr->rank, &root_comm);

        if (task_rank == 0) {
            KMR *rmr = kmr_create_context(root_comm, MPI_INFO_NULL, 0);
            rmr->preset_block_size = mr->preset_block_size;
            KMR_KVS *_kvs0 = kmr_create_kvs(rmr, kvi_keyf, kvi_valf);
            struct kmr_option inspect = {.inspect = 1};
            cc = kmr_map(kvi, _kvs0, 0, inspect, kmr_add_on_rank_zero_fn);
            assert(cc == MPI_SUCCESS);
            KMR_KVS *_kvs1 = kmr_create_kvs(rmr, kvi_keyf, kvi_valf);
            cc = kmr_distribute(_kvs0, _kvs1, 0, kmr_noopt);
            assert(cc == MPI_SUCCESS);
            cc = kmr_map(_kvs1, kvs0, 0, kmr_noopt, kmr_add_identity_fn);
            assert(cc == MPI_SUCCESS);
            cc = kmr_free_context(rmr);
            assert(cc == MPI_SUCCESS);
        } else {
            kmr_add_kv_done(kvs0);
        }
        MPI_Comm_free(&root_comm);
    } else {
        /* prepare for replicating key-values in each sub-communicator */
        struct kmr_option inspect = {.inspect = 1};
        cc = kmr_map(kvi, kvs0, 0, inspect, kmr_add_identity_fn);
        assert(cc == MPI_SUCCESS);
    }

    /* distribute key-values in each sub-communicator */
    KMR_KVS *kvs1 = kmr_create_kvs(task_mr, kvi_keyf, kvi_valf);
    cc = kmr_replicate(kvs0, kvs1, kmr_noopt);

    /* call map function */
    KMR_KVS *kvs2 = 0;
    if (kvo != 0) {
        enum kmr_kv_field kvo_keyf = kmr_unit_sized_or_opaque(kvo->c.key_data);
        enum kmr_kv_field kvo_valf = kmr_unit_sized_or_opaque(kvo->c.value_data);
        kvs2 = kmr_create_kvs(task_mr, kvo_keyf, kvo_valf);
    }
    struct kmr_option nothreading = {.nothreading=1};
    cc = kmr_map(kvs1, kvs2, arg, nothreading, m);
    assert(cc == MPI_SUCCESS);

    /* copy results to kvo and post-process */
    if (kvo != 0) {
        cc = kmr_map(kvs2, kvo, 0, kmr_noopt, kmr_add_identity_fn);
        assert(cc == MPI_SUCCESS);
    }
    cc = kmr_free_context(task_mr);
    assert(cc == MPI_SUCCESS);
    MPI_Comm_free(&task_comm);

    kmr_ckpt_enable_ckpt(mr, kcdc);
    if (kmr_ckpt_enabled(mr)) {
        kmr_ckpt_save_kvo_whole(mr, kvo);
    }
    if (!opt.inspect) {
        kmr_free_kvs(kvi);
    }
    return MPI_SUCCESS;
}

static int
kmr_remove_val_fn(const struct kmr_kv_box kv, const KMR_KVS *kvi,
                  KMR_KVS *kvo, void *arg, const long i)
{
    struct kmr_kv_box nkv = {
        .klen = kv.klen,      .k = kv.k,
        .vlen = sizeof(long), .v.i = 0 };
        kmr_add_kv(kvo, nkv);
        return MPI_SUCCESS;
}

static int
kmr_zero_key_fn(const struct kmr_kv_box kv[], const long n,
                const KMR_KVS *kvi, KMR_KVS *kvo, void *p)
{
    struct kmr_kv_box nkv = {
        .klen = sizeof(long), .k.i = 0,
        .vlen = kv[0].klen,   .v = kv[0].k };
    kmr_add_kv(kvo, nkv);
    return MPI_SUCCESS;
}

#define KMR_MAPMP_BASE 1000

static int
kmr_define_color_fn(const struct kmr_kv_box kv, const KMR_KVS *kvi,
                    KMR_KVS *kvo, void *arg, const long i)
{
    struct kmr_kv_box nkv = {
        .klen = kv.vlen,      .k = kv.v,
        .vlen = sizeof(long), .v.i = i + KMR_MAPMP_BASE };
    kmr_add_kv(kvo, nkv);
    return MPI_SUCCESS;
}

/** Group processes by key-value pairs that have the same keys, create
    an MPI sub-communicators that contain the processes with the same
    key and then run the specified task in each communicator in parallel.
    Each process should have at most only one key-value pair in its
    input KVS (KVI).  The sizes of sub-communicators depend on number
    of key-value pairs that have same keys.

    The parent communicator is 'KVI->c.mr->comm'.  Keys of key-value
    pair in KVIs is used as color (group id) for splitting the
    communicator.  RANK_KEY is used as key when assigning ranks to
    processes in a sub-communicator.  A process which has a smaller
    value of RANK_KEY is given a smaller value of rank.  The
    user-defined map-function, M, is called against the key-value pair
    in KVI.  In the map-function, the sub-communicator can be accessed
    by 'kvi->c.mr->comm' or 'kvo->c.mr->comm'.  Any MPI functions can
    be called by through the sub-communicator.

    It is a collective operation.  It supports checkpoint/restart,
    but whole resultant KVO is taken as a checkpoint file once when
    all key-value pairs are completely processed.

    It consumes the input key-value stream KVI unless INSPECT option
    is marked.  The output key-value stream KVO can be null, but in
    that case, a map-function cannot add key-value pairs.
    The pointer ARG is just passed to a map-function as a general
    argument.  M is the map-function.  See the description on
    the type ::kmr_mapfn_t.
    Effective-options:  INSPECT, TAKE_CKPT.  See struct kmr_option.
*/

int
kmr_map_multiprocess_by_key(KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
                            struct kmr_option opt, int rank_key, kmr_mapfn_t m)
{
    kmr_assert_kvs_ok(kvi, kvo, 1, 0);
    KMR *mr = kvi->c.mr;
    _Bool tracing5 = (mr->trace_map_mp && (5 <= mr->verbosity));
    struct kmr_option opt_supported = {.inspect = 1, .take_ckpt = 1};
    kmr_check_fn_options(mr, opt_supported, opt, __func__);
    int cc;

    if (kmr_ckpt_enabled(mr)) {
        if (kmr_ckpt_progress_init(kvi, kvo, opt)) {
            if (!opt.inspect) {
                kmr_free_kvs(kvi);
            }
            return MPI_SUCCESS;
        }
    }
    int kcdc = kmr_ckpt_disable_ckpt(mr);

    enum kmr_kv_field kvi_keyf = kmr_unit_sized_or_opaque(kvi->c.key_data);
    enum kmr_kv_field kvi_valf = kmr_unit_sized_or_opaque(kvi->c.value_data);
    struct kmr_option inspect = {.inspect = 1};
    double timestamp[7];

    /* gather key info. on rank0 in mr->comm to assign color */

    timestamp[0] = MPI_Wtime();
    KMR_KVS *kvs0 = kmr_create_kvs(mr, kvi_keyf, KMR_KV_INTEGER);
    cc = kmr_map(kvi, kvs0, 0, inspect, kmr_remove_val_fn);
    KMR_KVS *kvs1 = kmr_create_kvs(mr, kvi_keyf, KMR_KV_INTEGER);
    cc = kmr_shuffle(kvs0, kvs1, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_INTEGER, kvi_keyf);
    cc = kmr_reduce(kvs1, kvs2, 0, kmr_noopt, kmr_zero_key_fn);
    assert(cc == MPI_SUCCESS);
    /* gather on a rank (rank0) as the previously generated key is 0 */
    KMR_KVS *kvs3 = kmr_create_kvs(mr, KMR_KV_INTEGER, kvi_keyf);
    cc = kmr_shuffle(kvs2, kvs3, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    timestamp[1] = MPI_Wtime();

    /* define comm colors to keys on a rank (rank0) */
    KMR_KVS *kvs4 = kmr_create_kvs(mr, kvi_keyf, KMR_KV_INTEGER);
    cc = kmr_map(kvs3, kvs4, 0, kmr_noopt, kmr_define_color_fn);
    assert(cc == MPI_SUCCESS);
    timestamp[2] = MPI_Wtime();

    /* distribute the color assignment to all ranks */
    KMR_KVS *kvs5 = kmr_create_kvs(mr, kvi_keyf, KMR_KV_INTEGER);
    cc = kmr_replicate(kvs4, kvs5, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    /* assign color */
    int task_color = 0;
    long kvi_count;
    cc = kmr_local_element_count(kvi, &kvi_count);
    assert(cc == MPI_SUCCESS);
    if (kvi_count > 0) {
        struct kmr_kv_box ki, ko;
        cc = kmr_take_one(kvi, &ki);
        assert(cc == MPI_SUCCESS);
        cc = kmr_find_key(kvs5, ki, &ko);
        assert(cc == MPI_SUCCESS);
        task_color = (int)ko.v.i;
    }
    kmr_free_kvs(kvs5);
    timestamp[3] = MPI_Wtime();

    /* split communicator */
    MPI_Comm task_comm;
    MPI_Comm_split(mr->comm, task_color, rank_key, &task_comm);
    KMR *task_mr = kmr_create_context(task_comm, MPI_INFO_NULL, 0);
    task_mr->preset_block_size = mr->preset_block_size;

    /* create input kvs that uses task_comm */
    KMR_KVS *task_kvs0 = kmr_create_kvs(task_mr, kvi_keyf, kvi_valf);
    cc = kmr_map(kvi, task_kvs0, 0, inspect, kmr_add_identity_fn);
    assert(cc == MPI_SUCCESS);

    /* create output kvs that uses task_comm */
    KMR_KVS *task_kvs1 = 0;
    if (kvo != 0) {
        enum kmr_kv_field kvo_keyf = kmr_unit_sized_or_opaque(kvo->c.key_data);
        enum kmr_kv_field kvo_valf = kmr_unit_sized_or_opaque(kvo->c.value_data);
        task_kvs1 = kmr_create_kvs(task_mr, kvo_keyf, kvo_valf);
    }
    timestamp[4] = MPI_Wtime();

    /* call map function */
    struct kmr_option nothreading = {.nothreading=1};
    cc = kmr_map(task_kvs0, task_kvs1, arg, nothreading, m);
    assert(cc == MPI_SUCCESS);
    timestamp[5] = MPI_Wtime();

    /* copy results to kvo and post-process */
    if (kvo != 0) {
        cc = kmr_map(task_kvs1, kvo, 0, kmr_noopt, kmr_add_identity_fn);
        assert(cc == MPI_SUCCESS);
    }
    cc = kmr_free_context(task_mr);
    assert(cc == MPI_SUCCESS);
    MPI_Comm_free(&task_comm);
    timestamp[6] = MPI_Wtime();

    if (tracing5) {
	fprintf(stderr, (";;KMR [%05d] timing of kmr_map_multiprocess_by_key:"
			 " collect=%f calc=%f share=%f setup=%f exec=%f"
			 " clenup=%f (msec)\n"),
		mr->rank,
		timestamp[1] - timestamp[0],
		timestamp[2] - timestamp[1],
		timestamp[3] - timestamp[2],
		timestamp[4] - timestamp[3],
		timestamp[5] - timestamp[4],
		timestamp[6] - timestamp[5]);
	fflush(0);
    }

    kmr_ckpt_enable_ckpt(mr, kcdc);
    if (kmr_ckpt_enabled(mr)) {
        kmr_ckpt_save_kvo_whole(mr, kvo);
    }
    if (!opt.inspect) {
        kmr_free_kvs(kvi);
    }
    return MPI_SUCCESS;
}

/*
Copyright (C) 2012-2015 RIKEN AICS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/
