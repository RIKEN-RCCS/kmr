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

/** Maps key-value pair stored in rank 0 in parallel using multiple
    processes.  All other key-value pairs stored in other ranks are
    ignored.  All ranks that call this function are grouped to form
    sub-communicators whose maximum communicator sizes are MAX_NPROCS.
    The maximum number of sub-communicators is
    kvi->c.mr->nprocs / MAX_NPROCS.

    The key-value pairs in rank 0 are scattered to the sub-communicators.
    In each sub-communicator, each rank receives the same key-value
    pair at the same map-function calls.  In the user-defined
    map-function, M, the sub-communicator can be accessed by
    'kvi->c.mr->comm' or 'kvo->c.mr->comm'.  Any MPI functions can
    be called by specifying the sub-communicator to process the given
    key-value pair using multiple processes.

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
kmr_map_multiprocess(KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
                     struct kmr_option opt, int max_nprocs, kmr_mapfn_t m)
{
    kmr_assert_kvs_ok(kvi, kvo, 1, 0);
    KMR *mr = kvi->c.mr;
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

    /* distribute key-values in root (rank0 in mr->comm) to
       rank0 processes in sub-communicators */
    MPI_Comm root_comm;
    int task_rank;
    MPI_Comm_rank(task_comm, &task_rank);
    int root_color = (task_rank == 0)? 1 : 0;
    MPI_Comm_split(mr->comm, root_color, mr->rank, &root_comm);

    enum kmr_kv_field kvi_keyf = kmr_unit_sized_or_opaque(kvi->c.key_data);
    enum kmr_kv_field kvi_valf = kmr_unit_sized_or_opaque(kvi->c.value_data);
    KMR_KVS *kvs0 = kmr_create_kvs(task_mr, kvi_keyf, kvi_valf);
    if (task_rank == 0) {
        KMR *rmr = kmr_create_context(root_comm, MPI_INFO_NULL, 0);
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
kmr_reduce_zero_key_fn(const struct kmr_kv_box kv[], const long n,
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

/** Group processes by Key-Values that have the same keys, create an
    MPI sub-communicators that contain the processes with the same key
    and then run the specified task in each communicator in parallel.
    Each process should have at most only one Key-Value in its input
    KVS (KVI).  The sizes of sub-communicators depend on number of
    Key-Values that have same keys.

    The parent communicator is 'KVI->c.mr->comm'.  Keys of Key-Value
    in KVI is used as color (group id) for splitting the communicator.
    RANK_KEY is used as key when assigning ranks to processes in a
    sub-communicator.  A process which has a smaller value of RANK_KEY
    is given a smaller value of rank.  The user-defined map-function,
    M, is called against the Key-Value in KVI.  In the map-function,
    the sub-communicator can be accessed by 'kvi->c.mr->comm' or
    'kvo->c.mr->comm'.  Any MPI functions can be called by through
    the sub-communicator.

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

    /* gather key info. on rank0 in mr->comm to assign color */

    KMR_KVS *kvs0 = kmr_create_kvs(mr, kvi_keyf, kvi_valf);
    cc = kmr_shuffle(kvi, kvs0, inspect);
    assert(cc == MPI_SUCCESS);
    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_INTEGER, kvi_keyf);
    cc = kmr_reduce(kvs0, kvs1, 0, kmr_noopt, kmr_reduce_zero_key_fn);
    assert(cc == MPI_SUCCESS);
    /* gather on a rank (rank0) as the previously generated key is 0 */
    KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_INTEGER, kvi_keyf);
    cc = kmr_shuffle(kvs1, kvs2, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    /* define comm colors to keys on a rank (rank0) */
    KMR_KVS *kvs3 = kmr_create_kvs(mr, kvi_keyf, KMR_KV_INTEGER);
    cc = kmr_map(kvs2, kvs3, 0, kmr_noopt, kmr_define_color_fn);
    assert(cc == MPI_SUCCESS);
    /* distribute the color assignment to all ranks */
    KMR_KVS *kvs4 = kmr_create_kvs(mr, kvi_keyf, KMR_KV_INTEGER);
    cc = kmr_replicate(kvs3, kvs4, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    /* assign color */
    int task_color = 0;
    {
        struct kmr_kv_box ki, ko;
        cc = kmr_take_one(kvi, &ki);
        assert(cc == MPI_SUCCESS);
        cc = kmr_find_key(kvs4, ki, &ko);
        assert(cc == MPI_SUCCESS);
        task_color = (int)ko.v.i;
    }
    kmr_free_kvs(kvs4);

    /* split communicator */
    MPI_Comm task_comm;
    MPI_Comm_split(mr->comm, task_color, rank_key, &task_comm);
    KMR *task_mr = kmr_create_context(task_comm, MPI_INFO_NULL, 0);

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

    /* call map function */
    struct kmr_option nothreading = {.nothreading=1};
    cc = kmr_map(task_kvs0, task_kvs1, arg, nothreading, m);
    assert(cc == MPI_SUCCESS);

    /* copy results to kvo and post-process */
    if (kvo != 0) {
        cc = kmr_map(task_kvs1, kvo, 0, kmr_noopt, kmr_add_identity_fn);
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

/*
Copyright (C) 2012-2015 RIKEN AICS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/
