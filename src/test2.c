/* test2.c (2014-02-04) */

/* Check memory leakage by displaying "ps". */

/* Run it with "mpirun -np n a.out", and watch output. */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include "kmr.h"

static double
wtime()
{
    static struct timeval tv0 = {.tv_sec = 0};
    struct timeval tv;
    int cc;
    cc = gettimeofday(&tv, 0);
    assert(cc == 0);
    if (tv0.tv_sec == 0) {
	tv0 = tv;
	assert(tv0.tv_sec != 0);
    }
    double dt = ((double)(tv.tv_sec - tv0.tv_sec)
		 + ((double)(tv.tv_usec - tv0.tv_usec) * 1e-6));
    return dt;
}

/* Puts 200 key-value pairs to output KVO.  It is a map-function.  It
   runs only on rank0.  Inputs (KV0 and KVS0) are dummy. */

static int
addkeysfn(const struct kmr_kv_box kv0,
	  const KMR_KVS *kvs0, KMR_KVS *kvo, void *p, const long ind)
{
    assert(kvs0 == 0 && kv0.klen == 0 && kv0.vlen == 0 && kvo != 0);
    char k[80];
    char v[80];
    int cc;
    for (int i = 0; i < 200; i++) {
	snprintf(k, 80, "key%d", i);
	snprintf(v, 80, "value%d", i);
	struct kmr_kv_box kv = {
	    .klen = (int)(strlen(k) + 1),
	    .vlen = (int)(strlen(v) + 1),
	    .k.p = k,
	    .v.p = v
	};
	cc = kmr_add_kv(kvo, kv);
	assert(cc == MPI_SUCCESS);
    }
    return MPI_SUCCESS;
}

static int
replacevaluefn(const struct kmr_kv_box kv0,
	       const KMR_KVS *kvs0, KMR_KVS *kvo, void *p,
	       const long i)
{
    assert(kvs0 != 0 && kvo != 0);
    int cc, x;
    char gomi;
    cc = sscanf((&((char *)kv0.k.p)[3]), "%d%c", &x, &gomi);
    assert(cc == 1);
    char v[80];
    snprintf(v, 10, "newvalue%d", x);
    struct kmr_kv_box kv = {.klen = kv0.klen,
			    .vlen = (int)(strlen(v) + 1),
			    .k.p = kv0.k.p,
			    .v.p = v
    };
    cc = kmr_add_kv(kvo, kv);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

static int
emptyreducefn(const struct kmr_kv_box kv[], const long n,
	      const KMR_KVS *kvs, KMR_KVS *kvo, void *p)
{
    return MPI_SUCCESS;
}

/* Do KMR operations many times. */

static void
simple0(int nprocs, int rank)
{
    int cc;

    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);

    double t0, t1;
    t0 = wtime();

    for (int i = 0; i < 10000; i++) {

	/* Check timeout. */

	t1 = wtime();
	KMR_KVS *to0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
	if (rank == 0) {
	    struct kmr_kv_box kv = {
		.klen = (int)sizeof(long),
		.vlen = (int)sizeof(long),
		.k.i = 0,
		.v.i = ((t1 - t0) > 20.0)
	    };
	    cc = kmr_add_kv(to0, kv);
	    assert(cc == MPI_SUCCESS);
	}
	cc = kmr_add_kv_done(to0);
	assert(cc == MPI_SUCCESS);
	KMR_KVS *to1 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
	cc = kmr_replicate(to0, to1, kmr_noopt);
	assert(cc == MPI_SUCCESS);
	struct kmr_kv_box tok = {.klen = (int)sizeof(long), .k.p = 0,
				 .vlen = 0, .v.p = 0};
	struct kmr_kv_box tov;
	cc = kmr_find_key(to1, tok, &tov);
	assert(cc == MPI_SUCCESS);
	cc = kmr_free_kvs(to1);
	assert(cc == MPI_SUCCESS);
	if (tov.v.i) {
	    if (rank == 0) {
		printf("loops %d\n", i);
	    }
	    break;
	}

	/* Put some pairs. */

	KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	cc = kmr_map_on_rank_zero(kvs0, 0, kmr_noopt, addkeysfn);
	assert(cc == MPI_SUCCESS);

	/* Replicate pairs to all ranks. */

	KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	cc = kmr_replicate(kvs0, kvs1, kmr_noopt);
	assert(cc == MPI_SUCCESS);

	/* Map pairs. */

	KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	cc = kmr_map(kvs1, kvs2, 0, kmr_noopt, replacevaluefn);
	assert(cc == MPI_SUCCESS);

	/* Collect pairs by theirs keys. */

	KMR_KVS *kvs3 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	cc = kmr_shuffle(kvs2, kvs3, kmr_noopt);
	assert(cc == MPI_SUCCESS);

	/* Reduce collected pairs. */

	KMR_KVS *kvs4 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	cc = kmr_reduce(kvs3, kvs4, 0, kmr_noopt, emptyreducefn);
	assert(cc == MPI_SUCCESS);

	cc = kmr_free_kvs(kvs4);
	assert(cc == MPI_SUCCESS);
    }

    cc = kmr_free_context(mr);
    assert(cc == MPI_SUCCESS);
}

int
main(int argc, char *argv[])
{
    char cmd[256];
    int pid = getpid();
    int N = 8;

    int nprocs, rank, thlv;
    /*MPI_Init(&argc, &argv);*/
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &thlv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    kmr_init();

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {printf("Check leakage by observing heap size.\n");}
    if (rank == 0) {printf("Watch VSZ changes (loops %d times)...\n", N);}
    if (rank == 0) {printf("(Each loop will take approx. 20 sec).\n");}
    fflush(0);
    usleep(50 * 1000);
    MPI_Barrier(MPI_COMM_WORLD);

    for (int i = 0; i < N; i++) {
	simple0(nprocs, rank);

	MPI_Barrier(MPI_COMM_WORLD);
	if (rank == 0) {
	    snprintf(cmd, sizeof(cmd), "ps l %d", pid);
	    system(cmd);
	}
	fflush(0);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) printf("OK\n");
    fflush(0);

    kmr_fin();

    MPI_Finalize();

    return 0;
}
