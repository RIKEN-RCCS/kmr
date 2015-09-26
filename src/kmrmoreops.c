/* kmrmoreops.c (2014-02-04) */
/* Copyright (C) 2012-2015 RIKEN AICS */

/** \file kmrmoreops.c More Operatons on Key-Value Stream. */

#include <mpi.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <errno.h>
#include <assert.h>
//#define __XPG4_CHAR_CLASS__
//#include <ctype.h>
#include "kmr.h"
#include "kmrimpl.h"

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))
#define NEVERHERE 0

static int
kmr_find_key_fn(const struct kmr_kv_box kv,
		const KMR_KVS *kvs, KMR_KVS *kvo, void *p, const long i)
{
    const struct kmr_kv_box *p0 = p;
    const struct kmr_kv_box kv0 = *p0;
    kmr_sorter_t cmp = kmr_choose_sorter(kvs);
    if (cmp(&kv0, &kv) == 0) {
	kmr_add_kv(kvo, kv);
    }
    return MPI_SUCCESS;
}

/** Finds a key-value pair for a key.  It is an error when not exactly
    one entry is found.  It does not consume the input KVS KVI.  The
    returned key-value entry must be used before freeing the input
    KVS, when it points to an opaque data.  It maps internally, so it
    is slow.  It is tricky that the internally created KVS KVS0 points
    to the key-value area in the input KVS KVI. */

int
kmr_find_key(KMR_KVS *kvi, struct kmr_kv_box ki, struct kmr_kv_box *ko)
{
    enum kmr_kv_field keyf = kvi->c.key_data;
    enum kmr_kv_field valf = kmr_unit_sized_with_unmanaged(kvi->c.value_data);
    KMR_KVS *kvs0 = kmr_create_kvs(kvi->c.mr, keyf, valf);
    struct kmr_option insepct = {.inspect = 1};
    int cc = kmr_map(kvi, kvs0, &ki, insepct, kmr_find_key_fn);
    if (kvs0->c.element_count == 1) {
	cc = kmr_take_one(kvs0, ko);
	assert(cc == MPI_SUCCESS);
    } else {
	cc = ((kvs0->c.element_count == 0) ? MPI_ERR_ARG : MPI_ERR_COUNT);
	{
	    KMR *mr = kvi->c.mr;
	    char ee[80];
	    snprintf(ee, 80, "kmr_find_key with not one key (keys=%ld)",
		     kvs0->c.element_count);
	    kmr_error(mr, ee);
	}
    }
    kmr_free_kvs(kvs0);
    return cc;
}

/** Finds the key K in the key-value stream KVS.  It returns a pointer
    pointing inside the key-value stream.  It is an error when not
    exactly one entry is found.  It does not consume the input KVS.
    It maps internally, so slow. */

int
kmr_find_string(KMR_KVS *kvi, const char *k, const char **vq)
{
    assert(k != 0 && vq != 0);
    assert(kvi->c.key_data == KMR_KV_OPAQUE
	   || kvi->c.key_data == KMR_KV_CSTRING);
    int klen = ((int)strlen(k) + 1);
    struct kmr_kv_box ki = {.klen = klen, .k.p = k, .vlen = 0, .v.p = 0};
    struct kmr_kv_box vo;
    int cc = kmr_find_key(kvi, ki, &vo);
    if (cc != MPI_SUCCESS) {
	if (cc == MPI_ERR_ARG) {
	    kmr_warning(kvi->c.mr, 1, "kmr_find_key no key found");
	} else {
	    kmr_warning(kvi->c.mr, 1, "kmr_find_key multiple keys found");
	}
    }
    *vq = vo.v.p;
    return cc;
}

/* Totals the collected element-counts from all ranks.  It is called
   once with as many pairs as the number of ranks. */

static int
kmr_get_element_count_fn(const struct kmr_kv_box kv[], const long n,
			 const KMR_KVS *kvs, KMR_KVS *kvo, void *p)
{
    long *cntq = p;
    assert(kvo == 0 && kvs->c.mr->nprocs == n && *cntq == 0);
    long cnt = 0;
    for (int i = 0; i < n; i++) {
	cnt += kv[i].v.i;
    }
    *cntq = cnt;
    return MPI_SUCCESS;
}

/** Gets the total number of key-value pairs.  It uses replication and
    reduction. */

int
kmr_get_element_count(KMR_KVS *kvs, long *v)
{
    kmr_assert_kvs_ok(kvs, 0, 1, 0);
    assert(v != 0);
    int cc;
    KMR *mr = kvs->c.mr;
    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    struct kmr_kv_box kv = {.klen = (int)sizeof(long),
			    .vlen = (int)sizeof(long),
			    .k.i = 0,
			    .v.i = kvs->c.element_count};
    cc = kmr_add_kv(kvs0, kv);
    assert(cc == MPI_SUCCESS);
    kmr_add_kv_done(kvs0);
    /* Replicate and reduce to get a total. */
    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    cc = kmr_replicate(kvs0, kvs1, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    long cnt = 0;
    cc = kmr_reduce(kvs1, 0, &cnt, kmr_noopt, kmr_get_element_count_fn);
    assert(cc == MPI_SUCCESS);
    *v = cnt;
    return MPI_SUCCESS;
}

/* ================================================================ */

int
kmr_reverse_fn(const struct kmr_kv_box kv,
	       const KMR_KVS *kvs, KMR_KVS *kvo, void *p, const long i)
{
    struct kmr_kv_box nkv = {.klen = kv.vlen,
			     .vlen = kv.klen,
			     .k = kv.v,
			     .v = kv.k};
    kmr_add_kv(kvo, nkv);
    return MPI_SUCCESS;
}

/** Makes a new pair by swapping the key and the value in each pair.
    That is, it makes new pairs (v0,k0) from (k0,v0).  This is a
    simple mapper.  Effective-options: NOTHREADING, INSPECT,
    KEEP_OPEN, TAKE_CKPT.  See struct kmr_option. */

int
kmr_reverse(KMR_KVS *kvi, KMR_KVS *kvo, struct kmr_option opt)
{
    return kmr_map(kvi, kvo, 0, opt, kmr_reverse_fn);
}

/* Adds a pairing of a key-value KV under a given key.  Field
   data-types are for the old key and the value. */

static inline int
kmr_add_pairing_under_key(KMR_KVS *kvo, int klen, union kmr_unit_sized k,
			  const struct kmr_kv_box kv,
			  enum kmr_kv_field keyf, enum kmr_kv_field valf)
{
    assert(kvo->c.value_data == KMR_KV_OPAQUE
	   || kvo->c.value_data == KMR_KV_CSTRING);
    size_t sz = kmr_kvs_entry_netsize_of_box(kv);
    assert(kmr_check_alignment(sz));
    char buf[10 * 1024];
    struct kmr_kvs_entry *e;
    if (sz <= sizeof(buf)) {
	e = (void *)buf;
    } else {
	e = kmr_malloc(sz);
    }
    kmr_poke_kv2(e, kv, 0, keyf, valf, 0);
    struct kmr_kv_box kv1 = {.klen = klen,
			     .vlen = (int)sz,
			     .k = k,
			     .v.p = (void *)e};
    kmr_add_kv(kvo, kv1);
    if (e != (void *)buf) {
	kmr_free(e, sz);
    }
    return MPI_SUCCESS;
}

int
kmr_pairing_fn(const struct kmr_kv_box kv,
	       const KMR_KVS *kvi, KMR_KVS *kvo, void *p, const long i)
{
    int cc;
    cc = kmr_add_pairing_under_key(kvo, kv.klen, kv.k, kv,
				   kvi->c.key_data, kvi->c.value_data);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/** Replaces a value part with a key-value pairing.  That is, it makes
    new pairs (k0,(k0,v0)) from (k0,v0).  See kmr_unpairing().  This
    is a simple mapper.  Effective-options: NOTHREADING, INSPECT,
    KEEP_OPEN, TAKE_CKPT.  See struct kmr_option.  */

int
kmr_pairing(KMR_KVS *kvi, KMR_KVS *kvo, struct kmr_option opt)
{
    return kmr_map(kvi, kvo, 0, opt, kmr_pairing_fn);
}

int
kmr_unpairing_fn(const struct kmr_kv_box kv,
		 const KMR_KVS *kvs, KMR_KVS *kvo, void *p, const long i)
{
    struct kmr_kvs_entry *e = (void *)kv.v.p;
    struct kmr_kv_box nkv = kmr_pick_kv(e, kvo);
    kmr_add_kv(kvo, nkv);
    return MPI_SUCCESS;
}

/** Extracts a key-value pair from a pairing in the value part,
    discarding the original key.  It is the inverse of kmr_pairing.
    That is, it makes new pairs (k1,v1) from (k0,(k1,v1)).  See
    kmr_pairing().  This is a simple mapper.  Effective-options:
    NOTHREADING, INSPECT, KEEP_OPEN, TAKE_CKPT.  See struct kmr_option. */

int
kmr_unpairing(KMR_KVS *kvs, KMR_KVS *kvo, struct kmr_option opt)
{
    return kmr_map(kvs, kvo, 0, opt, kmr_unpairing_fn);
}

/* Packs two integers into an array. */

#if 0
static int
kmr_pack2_fn(const struct kmr_kv_box kv,
	     const KMR_KVS *kvs, KMR_KVS *kvo, void *p, const long i)
{
    assert(kvo == 0 && p != 0);
    long *samples2 = p;
    int rank = kv.k.i;
    long *mm = (void *)kv.v.p;
    long *ss = &samples2[2 * rank];
    ss[0] = mm[0];
    ss[1] = mm[1];
    return MPI_SUCCESS;
}
#endif

/* Assigns destination ranks for integer keys. */

static int
kmr_partition_by_ranking_fn(const struct kmr_kv_box kv,
			    const KMR_KVS *kvi, KMR_KVS *kvo, void *p,
			    const long i)
{
    int cc;
    KMR *mr = kvi->c.mr;
    int nprocs = mr->nprocs;
    long *range = p;
    long minkey = range[0];
    long maxkey = range[1];
    long v = kmr_stable_key(kv, kvi);
    assert(minkey <= v && v < (maxkey + 1));
    long d = (((maxkey + 1) - minkey + nprocs - 1) / nprocs);
    int rank = (int)((v - minkey) / d);
    assert(0 <= rank && rank < nprocs);
    union kmr_unit_sized k = {.i = rank};
    cc = kmr_add_pairing_under_key(kvo, sizeof(long), k, kv,
				   kvi->c.key_data, kvi->c.value_data);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/* Puts a pairing of a key-value under a rank, where a rank is a sort
   bucket, a pairing is an original key-value pair.  A proper rank is
   searched in the partition of the key range. */

static int
kmr_rank_for_sort(const struct kmr_kv_box kv,
		  const KMR_KVS *kvi, KMR_KVS *kvo, void *p, const long i)
{
    struct kmr_kv_box *bv = p;
    KMR *mr = kvi->c.mr;
    int nprocs = mr->nprocs;
    kmr_sorter_t cmp = kmr_choose_sorter(kvi);
    struct kmr_kv_box *q = kmr_bsearch(&kv, bv, (size_t)(nprocs - 1),
				       sizeof(struct kmr_kv_box),
				       (kmr_qsorter_t)cmp);
    long rank = (q - bv);
    assert(0 <= rank && rank < nprocs);
    union kmr_unit_sized k = {.i = rank};
    kmr_add_pairing_under_key(kvo, sizeof(long), k, kv,
			      kvi->c.key_data, kvi->c.value_data);
    return MPI_SUCCESS;
}

/* Samples key-value pairs, collecting one at index zero and the
   following by a fixed stride.  NSAMPLES be positive.  It does not
   consume the input as INSPECT is asserted. */

static int
kmr_sample_kv(long nsamples, KMR_KVS *kvi, KMR_KVS *kvo)
{
    assert(nsamples >= 0);
    if (nsamples > 0) {
	struct kmr_option inspect = {.inspect = 1};
	long cnt = kvi->c.element_count;
	long stride = ((cnt < nsamples) ? 1 : (cnt / nsamples));
	long limit = (stride * nsamples);
	int cc = kmr_map_skipping(0, stride, limit, 0, kvi, kvo,
				  0, inspect, kmr_add_identity_fn);
	assert(cc == MPI_SUCCESS);
	assert(kvo->c.element_count == MIN(cnt, nsamples));
    } else {
	int cc = kmr_add_kv_done(kvo);
	assert(cc == MPI_SUCCESS);
    }
    return MPI_SUCCESS;
}

static int
kmr_sample_to_array(long nsamples, KMR_KVS *kvi, struct kmr_kv_box *bv)
{
    assert(nsamples >= 0);
    if (nsamples > 0) {
	int cc;
	long cnt = kvi->c.element_count;
	assert(cnt >= nsamples);
	long start, stride, limit;
	if (cnt == nsamples) {
	    stride = 1;
	    start = 0;
	    limit = nsamples;
	} else {
	    stride = (cnt / (nsamples + 1));
	    start = stride;
	    limit = (start + (stride * nsamples));
	}
	struct kmr_option inspect = {.inspect = 1};
	cc = kmr_map_skipping(start, stride, limit, 0, kvi, 0,
			      bv, inspect, kmr_copy_to_array_fn);
	assert(cc == MPI_SUCCESS);
    }
    return MPI_SUCCESS;
}

/* Packs two integers into an array. */

static int
kmr_minmax2_fn(const struct kmr_kv_box kv[], const long n,
	       const KMR_KVS *kvs, KMR_KVS *kvo, void *p)
{
    long *range = p;
    long range0 = LONG_MAX;
    long range1 = LONG_MIN;
    for (int i = 0; i < n; i++) {
	long *mm = (void *)kv[i].v.p;
	long m0 = mm[0];
	long m1 = mm[1];
	if (m0 < range0) {
	    range0 = m0;
	}
	if (m1 > range1) {
	    range1 = m1;
	}
    }
    range[0] = range0;
    range[1] = range1;
    return MPI_SUCCESS;
}

/** Sorts a key-value stream, by partitioning to equal ranges.  It is
    NOT-STABLE due to quick-sort used inside.  It consumes an input
    key-value stream unless INSPECT is specified.  It assumes uniform
    distribution, and partioning is simply determined by the range of
    keys (MIN-MAX range is divided by nprocs).  Effective-options:
    NOTHREADING, INSPECT.  See struct kmr_option.  */

int
kmr_sort_small(KMR_KVS *kvi, KMR_KVS *kvo, struct kmr_option opt)
{
    kmr_assert_kvs_ok(kvi, kvo, 1, 1);
    assert(kmr_shuffle_compatible_p(kvo, kvi));
    KMR *mr = kvi->c.mr;
    struct kmr_option kmr_supported = {.nothreading = 1, .inspect = 1};
    kmr_check_fn_options(mr, kmr_supported, opt, __func__);
    int cc;
    //int nprocs = mr->nprocs;
    int rank = mr->rank;
    struct kmr_option i_opt = kmr_copy_options_i_part(opt);
    struct kmr_option o_opt = kmr_copy_options_o_part(opt);
    struct kmr_option m_opt = kmr_copy_options_m_part(opt);
    enum kmr_kv_field keyf = kvi->c.key_data;
    enum kmr_kv_field valf = kvi->c.value_data;
    KMR_KVS *kvs1 = kmr_create_kvs(mr, keyf, valf);
    cc = kmr_sort_locally(kvi, kvs1, 0, i_opt);
    assert(cc == MPI_SUCCESS);
    /* Find sort key range (collect min/max keys). */
    long mm[2];
    long cnt = kvs1->c.element_count;
    if (cnt == 0) {
	mm[0] = 0;
	mm[1] = 0;
    } else if (cnt == 1) {
        struct kmr_kvs_entry *
	    e0 = kmr_kvs_first_entry(kvs1, kvs1->c.first_block);
	struct kmr_kv_box b0 = kmr_pick_kv(e0, kvs1);
	long v = kmr_stable_key(b0, kvs1);
	mm[0] = v;
	mm[1] = v;
    } else {
        struct kmr_kvs_entry *
	    e0 = kmr_kvs_first_entry(kvs1, kvs1->c.first_block);
	struct kmr_kvs_entry *e1 = kmr_find_kvs_last_entry(kvs1);
	struct kmr_kv_box b0 = kmr_pick_kv(e0, kvs1);
	struct kmr_kv_box b1 = kmr_pick_kv(e1, kvs1);
	mm[0] = kmr_stable_key(b0, kvs1);
	mm[1] = kmr_stable_key(b1, kvs1);
    }
    KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    struct kmr_kv_box kv = {
	.klen = (int)sizeof(long),
	.vlen = (int)sizeof(long[2]),
	.k.i = rank,
	.v.p = (void *)mm
    };
    cc = kmr_add_kv(kvs2, kv);
    assert(cc == MPI_SUCCESS);
    cc = kmr_add_kv_done(kvs2);
    assert(cc == MPI_SUCCESS);
    KMR_KVS *kvs3 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    cc = kmr_replicate(kvs2, kvs3, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    long range[2] = {LONG_MAX, LONG_MIN};
    cc = kmr_reduce_as_one(kvs3, 0, range, m_opt, kmr_minmax2_fn);
    assert(cc == MPI_SUCCESS);
    /* Partiton the array by destination ranks. */
    KMR_KVS *kvs4 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    cc = kmr_map(kvs1, kvs4, range, m_opt, kmr_partition_by_ranking_fn);
    assert(cc == MPI_SUCCESS);
    KMR_KVS *kvs5 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    struct kmr_option ranking = {.key_as_rank = 1};
    cc = kmr_shuffle(kvs4, kvs5, ranking);
    assert(cc == MPI_SUCCESS);
    KMR_KVS *kvs6 = kmr_create_kvs(mr, keyf, valf);
    cc = kmr_unpairing(kvs5, kvs6, m_opt);
    assert(cc == MPI_SUCCESS);
    /* Locally sort. */
    cc = kmr_sort_locally(kvs6, kvo, 0, o_opt);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/** Sorts a key-value stream by the regular or the random
    sampling-sort.  It is NOT-STABLE due to quick-sort used inside.
    It consumes an input key-value stream unless INSPECT is specified.
    It can be used for "GraySort".  Effective-options: NOTHREADING,
    INSPECT.  See struct kmr_option. */

int
kmr_sort_large(KMR_KVS *kvi, KMR_KVS *kvo, struct kmr_option opt)
{
    kmr_assert_kvs_ok(kvi, kvo, 1, 1);
    assert(kmr_shuffle_compatible_p(kvo, kvi));
    int cc;
    KMR *mr = kvi->c.mr;
    struct kmr_option kmr_supported = {.nothreading = 1, .inspect = 1};
    kmr_check_fn_options(mr, kmr_supported, opt, __func__);
    int nprocs = mr->nprocs;
    struct kmr_option i_opt = kmr_copy_options_i_part(opt);
    struct kmr_option o_opt = kmr_copy_options_o_part(opt);
    struct kmr_option m_opt = kmr_copy_options_m_part(opt);
    /* Let the number of samples less than factor^2 (100M by default). */
    long factor = mr->sort_sample_factor;
    long nsamples;
    if (nprocs < factor) {
	nsamples = (nprocs - 1);
    } else {
	nsamples = MAX(10, ((factor*factor) / nprocs));
    }
    if (mr->verbosity >= 7) {
	if (mr->rank == 0) {
	    char ee[80];
	    snprintf(ee, sizeof(ee), "sort-oversampling=%e",
		     ((double)nsamples)/nprocs);
	    kmr_warning(mr, 7, ee);
	}
    }
    /* Collect samples on each rank. */
    enum kmr_kv_field keyf = kvi->c.key_data;
    enum kmr_kv_field valf = kvi->c.value_data;
    KMR_KVS *kvs1 = kmr_create_kvs(mr, keyf, valf);
    cc = kmr_sample_kv(nsamples, kvi, kvs1);
    assert(kvs1->c.element_count == MIN(kvi->c.element_count, nsamples));
    KMR_KVS *kvs2 = kmr_create_kvs(mr, keyf, valf);
    cc = kmr_replicate(kvs1, kvs2, m_opt);
    assert(cc == MPI_SUCCESS);
    KMR_KVS *kvs3 = kmr_create_kvs(mr, keyf, valf);
    cc = kmr_sort_locally(kvs2, kvs3, 0, m_opt);
    assert(cc == MPI_SUCCESS);
    /* Choose boundary values. */
    int nbreaks = (nprocs - 1);
    assert(kvs3->c.element_count >= nbreaks);
    struct kmr_kv_box *bv = kmr_malloc(sizeof(struct kmr_kv_box) * (size_t)nbreaks);
    cc = kmr_sample_to_array(nbreaks, kvs3, bv);
    assert(cc == MPI_SUCCESS);
    /* Partition. */
    KMR_KVS *kvs4 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    size_t prealloc = ((size_t)(16 * kvi->c.element_count) + kvi->c.storage_netsize);
    cc = kmr_allocate_block(kvs4, prealloc);
    assert(cc == MPI_SUCCESS);
    cc = kmr_map(kvi, kvs4, bv, i_opt, kmr_rank_for_sort);
    assert(cc == MPI_SUCCESS);
    kmr_free(bv, (sizeof(struct kmr_kv_box) * (size_t)nbreaks));
    cc = kmr_free_kvs(kvs3);
    assert(cc == MPI_SUCCESS);
    KMR_KVS *kvs5 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    struct kmr_option ranking = {.key_as_rank = 1};
    cc = kmr_shuffle(kvs4, kvs5, ranking);
    assert(cc == MPI_SUCCESS);
    KMR_KVS *kvs6 = kmr_create_kvs(mr, keyf, valf);
    cc = kmr_unpairing(kvs5, kvs6, m_opt);
    assert(cc == MPI_SUCCESS);
    /* Locally sort. */
    cc = kmr_sort_locally(kvs6, kvo, 0, o_opt);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/** Sort by rank0, a degenerated case for small number of keys.  It is
    NOT-STABLE due to quick-sort used inside.  It consumes an input
    key-value stream unless INSPECT is specified.  Effective-options:
    INSPECT.  See struct kmr_option. */

int
kmr_sort_by_one(KMR_KVS *kvi, KMR_KVS *kvo, struct kmr_option opt)
{
    kmr_assert_kvs_ok(kvi, kvo, 1, 1);
    assert(kmr_shuffle_compatible_p(kvo, kvi));
    int cc;
    KMR *mr = kvi->c.mr;
    struct kmr_option kmr_supported = {.inspect = 1};
    kmr_check_fn_options(mr, kmr_supported, opt, __func__);
    struct kmr_option i_opt = kmr_copy_options_i_part(opt);
    i_opt.rank_zero = 1;
    struct kmr_option o_opt = kmr_copy_options_o_part(opt);
    enum kmr_kv_field keyf = kvi->c.key_data;
    enum kmr_kv_field valf = kvi->c.value_data;
    KMR_KVS *kvs1 = kmr_create_kvs(mr, keyf, valf);
    cc = kmr_replicate(kvi, kvs1, i_opt);
    assert(cc == MPI_SUCCESS);
    /* Locally sort. */
    cc = kmr_sort_locally(kvs1, kvo, 0, o_opt);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/** Sorts a key-value stream globally.  It is NOT-STABLE due to
    quick-sort used inside.  It consumes an input key-value stream
    unless INSPECT is specified.  It selects a sorting routine on the
    total number of keys.  See kmr_sort_large(), kmr_sort_small(), or
    kmr_sort_by_one().  The results are stored as ascending ranks,
    thus the rank0 holds the minimum.  Effective-options: INSPECT.
    See struct kmr_option.  */

int
kmr_sort(KMR_KVS *kvi, KMR_KVS *kvo, struct kmr_option opt)
{
    kmr_assert_kvs_ok(kvi, kvo, 1, 1);
    struct kmr_option kmr_supported = {.inspect = 1};
    kmr_check_fn_options(kvi->c.mr, kmr_supported, opt, __func__);
    const long lb = kvi->c.mr->sort_trivial;
    const long threshold = kvi->c.mr->sort_threshold;
    int cc;
    long cnt;
    cc = kmr_get_element_count(kvi, &cnt);
    assert(cc == MPI_SUCCESS);
    if (cnt <= lb) {
	return kmr_sort_by_one(kvi, kvo, opt);
    } else if (cnt <= (threshold * kvi->c.mr->nprocs)) {
	return kmr_sort_small(kvi, kvo, opt);
    } else {
	return kmr_sort_large(kvi, kvo, opt);
    }
}

/* Tags the value part with a given integer.  The new key-value pair
   is (key,(tag,value)) for each origial (key,value).  */

static int
kmr_tag_value_fn(const struct kmr_kv_box kv,
		 const KMR_KVS *kvi, KMR_KVS *kvo, void *p, const long i)
{
    int cc;
    long *tagp = (long *)p;
    long tag = *tagp;
    struct kmr_kv_box v = {.klen = (int)sizeof(long), .vlen = kv.vlen,
			   .k.i = tag, .v = kv.v};
    cc = kmr_add_pairing_under_key(kvo, kv.klen, kv.k, v,
				   KMR_KV_INTEGER, kvi->c.value_data);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/* Creates key-value pairs from the values, collecting keys as
   0-tagged values, and collecting values as 1-tagged values.  That
   is, for example, given a set {(k,(0,v0)), (k,(0,v1)), (k,(0,v2)),
   (k,(1,v3)), (k,(1,v4))}, it creates {(v0,v3), (v0,v4), (v1,v3),
   (v1,v4), (v2,v3), (v2,v4)}.  MEMO: The dummyf is the fake data-type
   of the value field, which is unknown until checking the key is 0 or
   1. */

static int
kmr_make_product_fn(const struct kmr_kv_box kv[], const long n,
		    const KMR_KVS *kvi, KMR_KVS *kvo, void *p)
{
    int cc;
    enum kmr_kv_field keyf = KMR_KV_INTEGER;
    enum kmr_kv_field dummyf = KMR_KV_INTEGER;
    enum kmr_kv_field valkf = kvo->c.key_data;
    enum kmr_kv_field valvf = kvo->c.value_data;
    long kcnt = 0;
    long vcnt = 0;
    for (long i = 0; i < n; i++) {
	struct kmr_kvs_entry *e = (void *)kv[i].v.p;
	struct kmr_kv_box vkv0 = kmr_pick_kv2(e, keyf, dummyf);
	/*assert(vkv0.klen == sizeof(long));*/
	long tag = vkv0.k.i;
	assert(tag == 0 || tag == 1);
	if (tag == 0) {
	    kcnt++;
	} else {
	    vcnt++;
	}
    }
    if (kcnt == 0 || vcnt == 0) {
	return MPI_SUCCESS;
    }
    struct kmr_kv_box *
	keys = kmr_malloc(sizeof(struct kmr_kv_box) * (size_t)kcnt);
    struct kmr_kv_box *
	vals = kmr_malloc(sizeof(struct kmr_kv_box) * (size_t)vcnt);
    long ki = 0;
    long vi = 0;
    for (long i = 0; i < n; i++) {
	struct kmr_kvs_entry *e = (void *)kv[i].v.p;
	struct kmr_kv_box vkv0 = kmr_pick_kv2(e, keyf, dummyf);
	/*assert(vkv0.klen == sizeof(long));*/
	long tag = vkv0.k.i;
	assert(tag == 0 || tag == 1);
	if (tag == 0) {
	    struct kmr_kv_box vkv = kmr_pick_kv2(e, keyf, valkf);
	    keys[ki].klen = vkv.vlen;
	    keys[ki].k = vkv.v;
	    ki++;
	} else {
	    struct kmr_kv_box vkv = kmr_pick_kv2(e, keyf, valvf);
	    vals[vi].vlen = vkv.vlen;
	    vals[vi].v = vkv.v;
	    vi++;
	}
    }
    assert(ki == kcnt && vi == vcnt);
    for (long i = 0; i < kcnt; i++) {
	for (long j = 0; j < vcnt; j++) {
	    struct kmr_kv_box nkv = {.klen = keys[i].klen, .k = keys[i].k,
				     .vlen = vals[j].vlen, .v = vals[j].v};
	    cc = kmr_add_kv(kvo, nkv);
	    assert(cc == MPI_SUCCESS);
	}
    }
    kmr_free(keys, (sizeof(struct kmr_kv_box) * (size_t)kcnt));
    kmr_free(vals, (sizeof(struct kmr_kv_box) * (size_t)vcnt));
    return MPI_SUCCESS;
}

/** Makes key-value pairs as products of the two values in two
    key-value stream.  It creates a set of key-value pairs (ai,bj) of
    the pairs (key,ai) from KVS0 and (key,bj) from KVS1 for the
    matching key.  It makes a direct-product of the values when
    multiple values exist for a matching key.  That is, for example,
    given a set {(k,a0), (k,a1), (k,a2)} in KVS0 and {(k,b3), (k,b4)}
    in KVS1 for some distinct key, it creates {(a0,b3), (a0,b4),
    (a1,b3), (a1,b4), (a2,b3), (a2,b4)}.  Effective-options:
    NOTHREADNG.  See struct kmr_option. */

int
kmr_match(KMR_KVS *kvi0, KMR_KVS *kvi1, KMR_KVS *kvo, struct kmr_option opt)
{
    kmr_assert_kvs_ok(kvi0, kvo, 1, 1);
    kmr_assert_kvs_ok(kvi1, kvo, 1, 1);
    struct kmr_option kmr_supported = {.nothreading = 1};
    kmr_check_fn_options(kvi0->c.mr, kmr_supported, opt, __func__);
    assert(kvi0->c.key_data == kvi1->c.key_data
	   && kvo->c.key_data == kvi0->c.value_data
	   && kvo->c.value_data == kvi1->c.value_data);
    int cc;
    KMR *mr = kvi0->c.mr;
    struct kmr_option i_opt = kmr_copy_options_i_part(opt);
    struct kmr_option o_opt = kmr_copy_options_o_part(opt);
    /* Store two key-value streams into one. */
    enum kmr_kv_field keyf = kvi0->c.key_data;
    struct kmr_option keepopen = i_opt;
    keepopen.keep_open = 1;
    KMR_KVS *kvs2 = kmr_create_kvs(mr, keyf, KMR_KV_OPAQUE);
    long tag0 = 0;
    cc = kmr_map(kvi0, kvs2, &tag0, keepopen, kmr_tag_value_fn);
    assert(cc == MPI_SUCCESS);
    long tag1 = 1;
    cc = kmr_map(kvi1, kvs2, &tag1, keepopen, kmr_tag_value_fn);
    assert(cc == MPI_SUCCESS);
    kmr_add_kv_done(kvs2);
    /* Make products of two values. */
    KMR_KVS *kvs3 = kmr_create_kvs(mr, keyf, KMR_KV_OPAQUE);
    cc = kmr_shuffle(kvs2, kvs3, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    cc = kmr_reduce(kvs3, kvo, 0, o_opt, kmr_make_product_fn);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

static int
kmr_get_integer_values_fn(const struct kmr_kv_box kv[], const long n,
			  const KMR_KVS *kvs, KMR_KVS *kvo, void *p)
{
    assert(kvo == 0 && kvs->c.mr->nprocs == n);
    long *vec = p;
    for (int i = 0; i < n; i++) {
	vec[i] = kv[i].v.i;
    }
    return MPI_SUCCESS;
}

static int
kmr_ranking_fn(const struct kmr_kv_box kv,
	       const KMR_KVS *kvi, KMR_KVS *kvo, void *p, const long i)
{
    long rankingbase = *(long *)p;
    int cc;
    int klen = sizeof(long);
    union kmr_unit_sized k = {.i = (rankingbase + i)};
    cc = kmr_add_pairing_under_key(kvo, klen, k, kv,
				   kvi->c.key_data, kvi->c.value_data);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/** Assigns a ranking to key-value pairs, and returns the number of
    the total elements in COUNT.  Ranking is a position in the
    key-value stream.  That is, for example, given a sequence
    {(k0,v0), (k1,v1), (k2,v2)}, it creates {(0,(k0,v0)), (1,(k1,v1)),
    (2,(k2,v2))}.  Effective-options: NOTHREADING, INSPECT, KEEP_OPEN.
    See struct kmr_option.*/

int
kmr_ranking(KMR_KVS *kvi, KMR_KVS *kvo, long *count, struct kmr_option opt)
{
    /* Do the first part of kmr_get_element_count(). */
    kmr_assert_kvs_ok(kvi, 0, 1, 0);
    KMR *mr = kvi->c.mr;
    struct kmr_option kmr_supported = {.nothreading = 1, .inspect = 1,
                                       .keep_open = 1};
    kmr_check_fn_options(mr, kmr_supported, opt, __func__);
    const int nprocs = mr->nprocs;
    const int rank = mr->rank;
    struct kmr_option m_opt = kmr_copy_options_m_part(opt);
    int cc;
    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    struct kmr_kv_box kv = {.klen = (int)sizeof(long),
			    .vlen = (int)sizeof(long),
			    .k.i = 0,
			    .v.i = kvi->c.element_count};
    cc = kmr_add_kv(kvs0, kv);
    assert(cc == MPI_SUCCESS);
    kmr_add_kv_done(kvs0);
    /* Replicate and reduce to get a total. */
    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    cc = kmr_replicate(kvs0, kvs1, m_opt);
    assert(cc == MPI_SUCCESS);
    long *vec0 = kmr_malloc(sizeof(long) * (size_t)nprocs);
    cc = kmr_reduce(kvs1, 0, vec0, m_opt, kmr_get_integer_values_fn);
    assert(cc == MPI_SUCCESS);
    long *vec1 = kmr_malloc(sizeof(long) * (size_t)nprocs);
    long scan = 0;
    for (int i = 0; i < nprocs; i++) {
	vec1[i] = scan;
	scan += vec0[i];
    }
    cc = kmr_map(kvi, kvo, &vec1[rank], opt, kmr_ranking_fn);
    assert(cc == MPI_SUCCESS);
    if (count != 0) {
	*count = scan;
    }
    kmr_free(vec1, (sizeof(long) * (size_t)nprocs));
    kmr_free(vec0, (sizeof(long) * (size_t)nprocs));
    return MPI_SUCCESS;
}

struct kmr_ranking_to_rank {_Bool cyclic; long factor;};

static int
kmr_ranking_to_rank_fn(const struct kmr_kv_box kv,
		       const KMR_KVS *kvi, KMR_KVS *kvo, void *p, const long i)
{
    struct kmr_ranking_to_rank *u = p;
    int cc;
    struct kmr_kv_box kv1 = {.klen = kv.klen,
			     .vlen = kv.vlen,
			     .k.i = (u->cyclic
				     ? (kv.k.i % u->factor)
				     : (kv.k.i / u->factor)),
			     .v = kv.v};
    cc = kmr_add_kv(kvo, kv1);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/** Distributes key-value pairs approximately evenly to ranks.  It is
    used to level the load of mapping among ranks by calling it before
    mapping.  kmr_shuffle() can be sufficient to distribute pairs in
    most cases, but sometimes it results in uneven distribution
    because shuffling is based on hashing on the keys.
    Effective-options: NOTHREADING, INSPECT, KEEP_OPEN.  See struct
    kmr_option.  */

int
kmr_distribute(KMR_KVS *kvi, KMR_KVS *kvo, _Bool cyclic, struct kmr_option opt)
{
    kmr_assert_kvs_ok(kvi, 0, 1, 0);
    struct kmr_option kmr_supported = {.nothreading = 1, .inspect = 1,
                                       .keep_open = 1};
    kmr_check_fn_options(kvi->c.mr, kmr_supported, opt, __func__);
    struct kmr_option i_opt = kmr_copy_options_i_part(opt);
    struct kmr_option o_opt = kmr_copy_options_o_part(opt);
    struct kmr_option m_opt = kmr_copy_options_m_part(opt);
    KMR *mr = kvi->c.mr;
    const int nprocs = mr->nprocs;
    int cc;
    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    long count;
    cc = kmr_ranking(kvi, kvs0, &count, i_opt);
    assert(cc == MPI_SUCCESS);
    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    struct kmr_ranking_to_rank u = {
	.cyclic = cyclic,
	.factor = (cyclic ? nprocs : ((count + nprocs - 1) / nprocs))
    };
    cc = kmr_map(kvs0, kvs1, &u, m_opt, kmr_ranking_to_rank_fn);
    assert(cc == MPI_SUCCESS);
    KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    struct kmr_option ranking = {.key_as_rank = 1};
    cc = kmr_shuffle(kvs1, kvs2, ranking);
    assert(cc == MPI_SUCCESS);
    cc = kmr_unpairing(kvs2, kvo, o_opt);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/* See kmr_unpairing_fn(). */

static int
kmr_first_n_elements_fn(const struct kmr_kv_box kv,
			const KMR_KVS *kvi, KMR_KVS *kvo, void *p,
			const long i)
{
    long n = *(long *)p;
    if (kv.k.i < n) {
	struct kmr_kvs_entry *e = (void *)kv.v.p;
	struct kmr_kv_box nkv = kmr_pick_kv(e, kvo);
	kmr_add_kv(kvo, nkv);
    }
    return MPI_SUCCESS;
}

/** Chooses the first N entries from a key-value stream KVI.  The
    option nothreading is implied to keep the ordering.
    Effective-options: INSPECT, KEEP_OPEN.  See struct kmr_option.  */

int
kmr_choose_first_part(KMR_KVS *kvi, KMR_KVS *kvo, long n,
		      struct kmr_option opt)
{
    kmr_assert_kvs_ok(kvi, kvo, 1, 1);
    KMR *mr = kvi->c.mr;
    struct kmr_option kmr_supported = {.inspect = 1, .keep_open = 1};
    kmr_check_fn_options(mr, kmr_supported, opt, __func__);
    struct kmr_option i_opt = kmr_copy_options_i_part(opt);
    int cc;
    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    long count;
    cc = kmr_ranking(kvi, kvs0, &count, i_opt);
    assert(cc == MPI_SUCCESS);
    struct kmr_option opt1 = kmr_copy_options_o_part(opt);
    opt1.nothreading = 1;
    cc = kmr_map(kvs0, kvo, &n, opt1, kmr_first_n_elements_fn);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/** Maps until some key-value are added.  It stops processing, when
    the output is non-empty.  It does not guarantee singleness.
    Existence/emptiness be checked by kmr_get_element_count(). */

int
kmr_map_for_some(KMR_KVS *kvi, KMR_KVS *kvo,
		 void *arg, struct kmr_option opt, kmr_mapfn_t m)
{
    int cc;
    cc = kmr_map9(1, kvi, kvo, arg, opt, m, __FILE__, __LINE__, __func__);
    return cc;
}

/** Reduces until some key-value are added.  It stops processing, when
    the output is non-empty.  It does not guarantee singleness.
    Existence/emptiness be checked by kmr_get_element_count(). */

int
kmr_reduce_for_some(KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
		    struct kmr_option opt, kmr_redfn_t r)
{
    int cc;
    cc = kmr_reduce9(1, kvi, kvo, arg, opt, r,  __FILE__, __LINE__, __func__);
    return cc;
}

/* ================================================================ */

/** Returns an NTH entry of an n-tuple.  It returns a pair of a length
    and a pointer. */

struct kmr_ntuple_entry
kmr_nth_ntuple(struct kmr_ntuple *u, int nth)
{
    struct kmr_ntuple_entry e;
    assert(nth < u->n && u->index == u->n);
    char *p = (void *)u;
    size_t off = kmr_ntuple_nth_offset(u, nth);
    e.p = (p + off);
    e.len = u->len[nth];
    return e;
}

/** Returns the storage size of an n-tuple. */

int
kmr_size_ntuple(struct kmr_ntuple *u)
{
    assert(u->index == u->n);
    return (int)kmr_ntuple_nth_offset(u, u->n);
}

/** Returns the storage size of an n-tuple for N entries with LEN[i]
    size each. */

int
kmr_size_ntuple_by_lengths(int n, int len[])
{
    size_t off;
    off = kmr_ntuple_data_offset(n);
    for (int i = 0; i < n; i++) {
	off += (size_t)kmr_ntuple_entry_size(len[i]);
    }
    return (int)off;
}

/** Resets an n-tuple U with N entries and a MARKER. */

void
kmr_reset_ntuple(struct kmr_ntuple *u, int n, int marker)
{
    assert(n <= USHRT_MAX);
    u->marker = marker;
    u->n = (unsigned short)n;
    u->index = 0;
    int w = (KMR_ALIGN((int)sizeof(u->len[0]) * n) / (int)sizeof(u->len[0]));
    for (int i = 0; i < w; i++) {
	u->len[i] = 0;
    }
}

/** Adds an entry V with LEN in an n-tuple U whose size is limited to
    SIZE.  An n-tuple should be initialized by kmr_reset_ntuple()
    first.  Note it fills with zeros the gap of the alignment padding,
    allowing the n-tuples be used as opaque keys. */

int
kmr_put_ntuple(KMR *mr, struct kmr_ntuple *u, const int size,
	       const void *v, const int len)
{
    assert(u->index < u->n);
    char *p = kmr_ntuple_insertion_point(u);
    int sz = kmr_ntuple_entry_size(len);
    char *end = ((char *)p + sz);
    if (!(end <= ((char *)(void *)u + size))) {
	kmr_error(mr, "kmr_put_ntuple exceeds the buffer size");
	return -1;
    }
    u->len[u->index] = (unsigned short)len;
    memcpy(p, v, (size_t)len);
    memset((p + len), 0, (size_t)(end - (p + len)));
    u->index++;
    return MPI_SUCCESS;
}

/** Adds an integer value in an n-tuple U whose size is limited to
    SIZE.  See kmr_put_ntuple(). */

int
kmr_put_ntuple_long(KMR *mr, struct kmr_ntuple *u, const int sz, long v)
{
    int cc = kmr_put_ntuple(mr, u, sz, &v, sizeof(long));
    return cc;
}

/** Adds an n-tuple entry E in an n-tuple U whose size is limited to
    SIZE.  See kmr_put_ntuple(). */

int
kmr_put_ntuple_entry(KMR *mr, struct kmr_ntuple *u, const int sz,
		     struct kmr_ntuple_entry e)
{
    int cc = kmr_put_ntuple(mr, u, sz, e.p, e.len);
    return cc;
}

/** Adds an n-tuple U with a given key K and KLEN in a key-value
    stream KVO. */

int
kmr_add_ntuple(KMR_KVS *kvo, void *k, int klen, struct kmr_ntuple *u)
{
    assert(u->index == u->n);
    assert(kvo->c.value_data == KMR_KV_OPAQUE
	   || kvo->c.value_data == KMR_KV_CSTRING);
    struct kmr_kv_box kv = {
	.klen = klen,
	.vlen = kmr_size_ntuple(u),
	.k.p = k,
	.v.p = (void *)u
    };
    kmr_add_kv(kvo, kv);
    return MPI_SUCCESS;
}

/** Separates the n-tuples stored in the value part of KV into the two
    sets by their marker values.  It is intended to be used in reduce
    functions.  It separates the n-tuples to the first set by
    marker=MARKERS[0] and to the second set by marker=MARKERS[1].  It
    returns two malloced arrays in VV with their sizes in CNT.  The
    arrays VV[0] and VV[1] should be freed by the caller. */

int
kmr_separate_ntuples(KMR *mr,
		     const struct kmr_kv_box kv[], const long n,
		     struct kmr_ntuple **vv[2], long cnt[2],
		     int markers[2], _Bool disallow_other_entries)
{
    long c0;
    long c1;

    c0 = 0;
    c1 = 0;
    for (long i = 0; i < n; i++) {
	struct kmr_ntuple *u = (void *)kv[i].v.p;
	if (u->marker == markers[0]) {
	    c0++;
	} else if (u->marker == markers[1]) {
	    c1++;
	} else {
	    /* Ignore. */
	    if (disallow_other_entries) {
		assert(u->marker == markers[0] || u->marker == markers[1]);
	    }
	}
    }

    long cnt0 = c0;
    long cnt1 = c1;
    struct kmr_ntuple **
	v0 = kmr_malloc(sizeof(struct kmr_ntuple *) * (size_t)cnt0);
    struct kmr_ntuple **
	v1 = kmr_malloc(sizeof(struct kmr_ntuple *) * (size_t)cnt1);

    c0 = 0;
    c1 = 0;
    for (long i = 0; i < n; i++) {
	struct kmr_ntuple *u = (void *)kv[i].v.p;
	if (u->marker == markers[0]) {
	    v0[c0] = u;
	    c0++;
	} else if (u->marker == markers[1]) {
	    v1[c1] = u;
	    c1++;
	} else {
	    /* Ignore. */
	}
    }
    assert(cnt0 == c0 && cnt1 == c1);

    vv[0] = v0;
    vv[1] = v1;
    cnt[0] = cnt0;
    cnt[1] = cnt1;
    return MPI_SUCCESS;
}

static inline int
kmr_product_ntuples_by_space(KMR_KVS *kvo,
			     struct kmr_ntuple **vv[2], long cnt[2],
			     int marker,
			     int slots[][2], int nslots,
			     int keys[][2], int nkeys,
			     _Bool byspace)
{
    KMR *mr = kvo->c.mr;
    const int AUNIT = 1024;

    void *keyp;
    void *valuep;
    size_t keysz;
    size_t valuesz;

    keyp = 0;
    keysz = 0;
    valuep = 0;
    valuesz = 0;

    struct kmr_ntuple *ee[2];
    for (long k0 = 0; k0 < cnt[0]; k0++) {
	ee[0] = vv[0][k0];
	for (long k1 = 0; k1 < cnt[1]; k1++) {
	    ee[1] = vv[1][k1];

	    int klen;
	    if (nkeys == 1) {
		int *choice = keys[0];
		struct kmr_ntuple_entry
		    e = kmr_nth_ntuple(ee[choice[1]], choice[0]);
		klen = e.len;
	    } else {
		int sz;
		sz = (int)kmr_ntuple_data_offset(nkeys);
		for (int i = 0; i < nkeys; i++) {
		    int *choice = keys[i];
		    struct kmr_ntuple_entry
			e = kmr_nth_ntuple(ee[choice[1]], choice[0]);
		    sz += kmr_ntuple_entry_size(e.len);
		}
		klen = sz;
	    }

	    int vlen;
	    {
		int sz;
		sz = (int)kmr_ntuple_data_offset(nslots);
		for (int i = 0; i < nslots; i++) {
		    int *choice = slots[i];
		    struct kmr_ntuple_entry
			e = kmr_nth_ntuple(ee[choice[1]], choice[0]);
		    sz += kmr_ntuple_entry_size(e.len);
		}
		vlen = sz;
	    }

	    if (byspace) {
		struct kmr_kv_box nkv = {
		    .klen = klen,
		    .k.p = 0,
		    .vlen = vlen,
		    .v.p = 0
		};
		keyp = 0;
		valuep = 0;
		kmr_add_kv_space(kvo, nkv, &keyp, &valuep);
	    } else {
		if ((size_t)klen > keysz) {
		    kmr_free(keyp, keysz);
		    keysz = (size_t)((klen + AUNIT - 1) & ~(AUNIT - 1));
		    keyp = kmr_malloc(keysz);
		}
		if ((size_t)vlen > valuesz) {
		    kmr_free(valuep, valuesz);
		    valuesz = (size_t)((vlen + AUNIT - 1) & ~(AUNIT - 1));
		    valuep = kmr_malloc(valuesz);
		}
		assert(keysz > (size_t)klen);
		assert(valuesz > (size_t)vlen);
	    }

	    /* Fill key. */

	    if (nkeys == 1) {
		int *keychoice = keys[0];
		struct kmr_ntuple_entry
		    e = kmr_nth_ntuple(ee[keychoice[1]], keychoice[0]);
		memcpy(keyp, e.p, (size_t)e.len);
		assert(klen == e.len);
	    } else {
		struct kmr_ntuple *k = (void *)keyp;
		kmr_reset_ntuple(k, nkeys, 0);
		for (int i = 0; i < nkeys; i++) {
		    int *choice = keys[i];
		    struct kmr_ntuple_entry
			e = kmr_nth_ntuple(ee[choice[1]], choice[0]);
		    assert(i == k->index);
		    kmr_put_ntuple_entry(mr, k, (int)klen, e);
		}
		assert(klen == kmr_size_ntuple(k));
	    }

	    /* Fill value. */

	    {
		struct kmr_ntuple *v = (void *)valuep;
		kmr_reset_ntuple(v, nslots, marker);
		for (int i = 0; i < nslots; i++) {
		    int *choice = slots[i];
		    struct kmr_ntuple_entry
			e = kmr_nth_ntuple(ee[choice[1]], choice[0]);
		    assert(i == v->index);
		    kmr_put_ntuple_entry(mr, v, (int)vlen, e);
		}
		assert(vlen == kmr_size_ntuple(v));
	    }

	    if (!byspace) {
		struct kmr_kv_box nkv = {
		    .klen = klen,
		    .k.p = keyp,
		    .vlen = vlen,
		    .v.p = valuep
		};
		kmr_add_kv(kvo, nkv);
	    }
	}
    }

    if (!byspace) {
	if (keyp != 0) {
	    kmr_free(keyp, keysz);
	}
 	if (valuep != 0) {
	    kmr_free(valuep, valuesz);
	}
    }

    return MPI_SUCCESS;
}

/** Makes a direct product of the two sets of n-tuples VV[0] and VV[1]
    with their counts in CNT[0] and CNT[1].  It is intended to be used
    in reduce functions.  The resulting n-tuples are created by SLOTS,
    which chooses i-th entry of the n-tuples by the SLOTS[i][0]-th
    entry from the the SLOTS[i][1] set, 0 from the first set and 1
    from the second set.  The product n-tuples have MARKER and are
    inserted into KVO under the new key.  The new key is selected like
    values using KEYS[j][0] and KEYS[j][1].  The key is not an n-tuple
    when NKEYS=1, or an n-tuple of KEYS[j] entries.  The n-tuple key
    has zero as a marker.  Note that it does not remove duplicate
    entries. */

int
kmr_product_ntuples(KMR_KVS *kvo,
		    struct kmr_ntuple **vv[2], long cnt[2],
		    int marker,
		    int slots[][2], int nslots,
		    int keys[][2], int nkeys)
{
    int cc;
    if (kvo->c.magic == KMR_KVS_ONCORE) {
	cc = kmr_product_ntuples_by_space(kvo, vv, cnt, marker,
					  slots, nslots, keys, nkeys,
					  1);
	return cc;
    } else if (kvo->c.magic == KMR_KVS_PUSHOFF) {
	cc = kmr_product_ntuples_by_space(kvo, vv, cnt, marker,
					  slots, nslots, keys, nkeys,
					  0);
	return cc;
    } else {
	assert((kvo->c.magic == KMR_KVS_ONCORE)
	       || (kvo->c.magic == KMR_KVS_PUSHOFF));
	assert(NEVERHERE);
	return 0;
    }
}

static int
kmr_put_integer_to_array_fn(const struct kmr_kv_box kv,
			    const KMR_KVS *kvi, KMR_KVS *kvo,
			    void *p, const long i)
{
    long *v = p;
    v[i] = kv.v.i;
    return MPI_SUCCESS;
}

/** Fills an integer array FRQ[i] with the count of the elements of
    each rank.  The array FRQ be as large as nprocs.  It also fills
    VAR[0]=average, VAR[1]=variance, VAR[2]=min, and VAR[3]=max.  FRQ
    or VAR can be null. */

int
kmr_histogram_count_by_ranks(KMR_KVS *kvs, long *frq, double *var,
			     _Bool rankzeroonly)
{
    kmr_assert_kvs_ok(kvs, 0, 1, 0);
    assert(kvs->c.magic == KMR_KVS_ONCORE);
    KMR *mr = kvs->c.mr;
    int nprocs = mr->nprocs;
    int cc;

    long *vec;
    if (frq != 0) {
	vec = frq;
    } else {
	vec = kmr_malloc(sizeof(long) * (size_t)nprocs);
    }

    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    struct kmr_kv_box nkv = {
	.klen = sizeof(long),
	.k.i = mr->rank,
	.vlen = sizeof(long),
	.v.i = kvs->c.element_count
    };
    kmr_add_kv(kvs0, nkv);
    kmr_add_kv_done(kvs0);
    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    struct kmr_option opt = {.rank_zero = rankzeroonly};
    cc = kmr_replicate(kvs0, kvs1, opt);
    assert(cc == MPI_SUCCESS);
    KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    cc = kmr_sort_locally(kvs1, kvs2, 0, opt);
    assert(cc == MPI_SUCCESS);
    cc = kmr_map(kvs2, 0, vec, opt, kmr_put_integer_to_array_fn);
    assert(cc == MPI_SUCCESS);

    if (var != 0 && (!rankzeroonly || mr->rank == 0)) {
	double min = (double)LONG_MAX;
	double max = 0.0;
	double s1 = 0.0;
	double s2 = 0.0;
	for (int r = 0; r < nprocs; r++) {
	    double x = (double)vec[r];
	    s1 += x;
	    s2 += (x * x);
	    min = MIN(min, x);
	    max = MAX(max, x);
	}
	double a1 = (s1 / (double)nprocs);
	double a2 = (s2 / (double)nprocs);
	var[0] = a1;
	var[1] = a2 - (a1 * a1);
	var[2] = min;
	var[3] = max;
    }

    if (frq == 0) {
	kmr_free(vec, sizeof(long) * (size_t)nprocs);
    };

    return MPI_SUCCESS;
}

/*
Copyright (C) 2012-2015 RIKEN AICS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/
