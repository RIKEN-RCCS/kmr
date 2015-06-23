/* test0.c (2014-02-04) */

/* Check KMR basic operations. */

/* Run it with "mpirun -np n a.out".  Most of the checks are in the
   form of assertion "assert(x)".  Do not use this code as an example.
   KMR makes rank-aware code unnecessary at all.  Also, KMR makes MPI
   calls unnecessary, too, except initialization and finalization.
   But, this code includes many rank-aware code. */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "kmr.h"

#define MAX(a,b) (((a)>(b))?(a):(b))

/* Puts five key-value pairs to output KVO.  It is a map-function.  It
   runs only on rank0.  Inputs (KV0 and KVS0) are dummy. */

static int
addfivekeysfn(const struct kmr_kv_box kv0,
	      const KMR_KVS *kvs0, KMR_KVS *kvo, void *p, const long i_)
{
    assert(kvs0 == 0 && kv0.klen == 0 && kv0.vlen == 0 && kvo != 0);
    struct kmr_kv_box kv[] = {
	{.klen = 5, .vlen = 7, .k.p = "key0", .v.p = "value0"},
	{.klen = 5, .vlen = 7, .k.p = "key1", .v.p = "value1"},
	{.klen = 5, .vlen = 7, .k.p = "key2", .v.p = "value2"},
	{.klen = 5, .vlen = 7, .k.p = "key3", .v.p = "value3"},
	{.klen = 5, .vlen = 7, .k.p = "key4", .v.p = "value4"}
    };
    int cc;
    for (int i = 0; i < 5; i++) {
	cc = kmr_add_kv(kvo, kv[i]);
	assert(cc == MPI_SUCCESS);
    }
    return MPI_SUCCESS;
}

static int
replacevaluefn(const struct kmr_kv_box kv0,
	       const KMR_KVS *kvs0, KMR_KVS *kvo, void *p, const long i)
{
    assert(kvs0 != 0 && kvo != 0);
    int cc, x;
    char gomi;
    cc = sscanf((&((char *)kv0.k.p)[3]), "%d%c", &x, &gomi);
    assert(cc == 1);
    char buf[10];
    snprintf(buf, 10, "newvalue%d", x);
    struct kmr_kv_box kv = {.klen = kv0.klen,
			    .vlen = 10,
			    .k.p = kv0.k.p,
			    .v.p = buf};
    cc = kmr_add_kv(kvo, kv);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/* Aggregates reduction pairs.  It asserts key equality for
   checking. */

static int
aggregatevaluesfn(const struct kmr_kv_box kv[], const long n,
		  const KMR_KVS *kvs, KMR_KVS *kvo, void *p)
{
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    char kbuf[48];
    char vbuf[48];
    printf("[%03d] pairs n=%ld\n", rank, n);
    for (long i = 0; i < n; i++) {
	kmr_dump_opaque(kv[i].k.p, kv[i].klen, kbuf, sizeof(kbuf));
	kmr_dump_opaque(kv[i].v.p, kv[i].vlen, vbuf, sizeof(vbuf));
	printf("[%03d] k[%d]=%s;v[%d]=%s\n", rank,
	       kv[i].klen, kbuf, kv[i].vlen, vbuf);

	if (i > 0) {
	    struct kmr_kv_box b0 = kv[0];
	    switch (kvs->c.key_data) {
	    case KMR_KV_BAD:
		assert(kvs->c.key_data != KMR_KV_BAD);
		break;
	    case KMR_KV_INTEGER:
		assert(kv[i].klen == b0.klen
		       && kv[i].k.i == b0.k.i);
		break;
	    case KMR_KV_FLOAT8:
		assert(kv[i].klen == b0.klen
		       && kv[i].k.d == b0.k.d);
		break;
	    case KMR_KV_OPAQUE:
	    case KMR_KV_CSTRING:
	    case KMR_KV_POINTER_OWNED:
	    case KMR_KV_POINTER_UNMANAGED:
		assert(kv[i].klen == b0.klen
		       && memcmp(kv[i].k.p, b0.k.p, (size_t)b0.klen) == 0);
		break;
	    default:
		assert(0);
		break;
	    }
	}
    }
    char buf[48];
    int cc;
    snprintf(buf, 48, "%ld_*_%s", n, kv[0].v.p);
    int len = (int)(strlen(buf) + 1);
    struct kmr_kv_box akv = {.klen = kv[0].klen,
			     .vlen = len,
			     .k.p = kv[0].k.p,
			     .v.p = buf};
    cc = kmr_add_kv(kvo, akv);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

static int
checkkeyvaluefn(const struct kmr_kv_box kv0,
		const KMR_KVS *kvs0, KMR_KVS *kvo, void *p, const long i)
{
    KMR *mr = kvs0->c.mr;
    char buf[1024];
    int cc;
    int x;
    char gomi;
    cc = sscanf((&((char *)kv0.k.p)[3]), "%d%c", &x, &gomi);
    assert(cc == 1);
    snprintf(buf, sizeof(buf), "%d_*_newvalue%d", mr->nprocs, x);
    assert(strlen(kv0.v.p) == strlen(buf));
    size_t len = strlen(buf);
    assert(strncmp((kv0.v.p), buf, len) == 0);
    return MPI_SUCCESS;
}

/* Tests simplest map/shuffle/reduce sequence. */

static void
simple0(int nprocs, int rank)
{
    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("simple0...\n");}
    fflush(0);

    int cc;

    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);

    /* Put five pairs. */

    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) printf("ADD\n");
    fflush(0);

    int N = 5;

    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_map_on_rank_zero(kvs0, 0, kmr_noopt, addfivekeysfn);
    assert(cc == MPI_SUCCESS);
    kmr_dump_kvs(kvs0, 0);

    long cnt0;
    kmr_get_element_count(kvs0, &cnt0);
    printf("[%03d] cnt0 = %ld\n", rank, cnt0);
    assert(cnt0 == N);

    {
	long histo[MAX(nprocs, 5)];
	double var[4];
	for (int i = 0; i < MAX(nprocs, 5); i++) {histo[i] = 0;}
	kmr_histogram_count_by_ranks(kvs0, histo, var, 0);
	if (rank == 0) {
	    printf(("[%03d] histo: %ld %ld %ld %ld %ld...\n"
		    "[%03d] ave/var/min/max: %f %f %ld %ld\n"),
		   rank, histo[0], histo[1], histo[2], histo[3], histo[4],
		   rank, var[0], var[1], (long)var[2], (long)var[3]);
	}
    }

    /* Replicate pairs to all ranks. */

    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) printf("REPLICATE\n");
    fflush(0);

    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_replicate(kvs0, kvs1, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    kmr_dump_kvs(kvs1, 0);

    long cnt1;
    kmr_get_element_count(kvs1, &cnt1);
    printf("[%03d] cnt0 = %ld\n", rank, cnt1);
    assert(cnt1 == (N * nprocs));

    {
	long histo[MAX(nprocs, 5)];
	double var[4];
	for (int i = 0; i < MAX(nprocs, 5); i++) {histo[i] = 0;}
	kmr_histogram_count_by_ranks(kvs1, histo, var, 0);
	if (rank == 0) {
	    printf(("[%03d] histo: %ld %ld %ld %ld %ld...\n"
		    "[%03d] ave/var/min/max/: %f %f %ld %ld\n"),
		   rank, histo[0], histo[1], histo[2], histo[3], histo[4],
		   rank, var[0], var[1], (long)var[2], (long)var[3]);
	}
    }

    /* Map pairs. */

    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) printf("MAP\n");
    fflush(0);

    KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_map(kvs1, kvs2, 0, kmr_noopt, replacevaluefn);
    assert(cc == MPI_SUCCESS);

    long cnt2;
    kmr_get_element_count(kvs2, &cnt2);
    assert(cnt2 == (N * nprocs));

    /* Collect pairs by theirs keys. */

    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) printf("SHUFFLE\n");
    fflush(0);

    KMR_KVS *kvs3 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_shuffle(kvs2, kvs3, kmr_noopt);
    assert(cc == MPI_SUCCESS);

    long cnt3;
    kmr_get_element_count(kvs3, &cnt3);
    assert(cnt3 == (N * nprocs));

    /* Reduce collected pairs. */

    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) printf("REDUCE\n");
    fflush(0);

    KMR_KVS *kvs4 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_reduce(kvs3, kvs4, 0, kmr_noopt, aggregatevaluesfn);
    assert(cc == MPI_SUCCESS);

    kmr_dump_kvs(kvs4, 0);

    long cnt4;
    kmr_get_element_count(kvs4, &cnt4);
    assert(cnt4 == N);

    /* Gather pairs to rank0. */

    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) printf("GATHER\n");
    fflush(0);

    KMR_KVS *kvs5 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    struct kmr_option opt = {.rank_zero = 1};
    cc = kmr_replicate(kvs4, kvs5, opt);
    assert(cc == MPI_SUCCESS);
    kmr_dump_kvs(kvs5, 0);

    long cnt5;
    kmr_get_element_count(kvs5, &cnt5);
    assert(cnt5 == N);

    cc = kmr_map(kvs5, 0, 0, kmr_noopt, checkkeyvaluefn);
    assert(cc == MPI_SUCCESS);

    kmr_free_context(mr);
}

/* Tests on empty KVS. */

static void
simple1(int nprocs, int rank)
{
    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) printf("simple1...\n");
    fflush(0);

    int cc;

    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);

    /* Make empty KVS. */

    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    kmr_add_kv_done(kvs0);

    long cnt0;
    kmr_get_element_count(kvs0, &cnt0);
    assert(cnt0 == 0);

    /* Replicate. */

    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_replicate(kvs0, kvs1, kmr_noopt);
    assert(cc == MPI_SUCCESS);

    /* Map. */

    KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_map(kvs1, kvs2, 0, kmr_noopt, 0);
    assert(cc == MPI_SUCCESS);

    /* Shuffle. */

    KMR_KVS *kvs3 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_shuffle(kvs2, kvs3, kmr_noopt);
    assert(cc == MPI_SUCCESS);

    /* Reduce. */

    KMR_KVS *kvs4 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_reduce(kvs3, kvs4, 0, kmr_noopt, 0);
    assert(cc == MPI_SUCCESS);

    /* Save/Restore. */

    void *data = 0;
    size_t sz = 0;
    cc = kmr_save_kvs(kvs4, &data, &sz, kmr_noopt);
    assert(cc == MPI_SUCCESS && data != 0 && sz != 0);
    cc = kmr_free_kvs(kvs4);
    assert(cc == MPI_SUCCESS);

    KMR_KVS *kvs5 = kmr_create_kvs(mr, KMR_KV_BAD, KMR_KV_BAD);
    cc = kmr_restore_kvs(kvs5, data, sz, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    free(data);
    cc = kmr_free_kvs(kvs5);
    assert(cc == MPI_SUCCESS);

    kmr_free_context(mr);
}

int
main(int argc, char *argv[])
{
    int nprocs, rank, thlv;
    /*MPI_Init(&argc, &argv);*/
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &thlv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    kmr_init();

    simple0(nprocs, rank);
    simple1(nprocs, rank);

    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) printf("OK\n");
    fflush(0);

    kmr_fin();

    MPI_Finalize();

    return 0;
}
