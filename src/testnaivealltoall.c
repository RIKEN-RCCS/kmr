/* testnaivealltoall.c */

/* Test naive alltoall.  Naive alltoall is for large messages.  It
   tests with data sizes of 1 MB to 1 GB, by forcing the maximum
   message size to 1 MB. */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <assert.h>
#ifdef _OPENMP
#include <omp.h>
#endif

#include "kmr.h"
#include "kmrimpl.h"

void
fill_by_randoms(void *v, size_t size)
{
    unsigned short *p = v;
    size_t n = (size / sizeof(unsigned short));
    for (size_t i = 0; i < n; i++) {
	p[i] = (unsigned short)rand();
    }
}

void
simple0(int nprocs, int rank)
{
    long SIZE_LIMIT = (8 * 1024 * 1024);
    char k[256];
    char v[1024 * 1024];

    int cc;

    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);

    if (1) {
	const int count = 1024;
	const size_t loosecount = (size_t)(count + 200);
	size_t sizes[2] = {1024, (1024 * 1024)};
	struct kmr_kv_box boxes0[loosecount];
	struct kmr_kv_box boxes1[loosecount];

	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {printf("Checking kmr_shuffle...\n");}
	fflush(0);
	usleep(50 * 1000);

	for (size_t vsize = sizes[0]; vsize <= sizes[1]; vsize <<= 1) {
	    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	    for (int i = 0; i < count; i++) {
		snprintf(k, sizeof(k), "key/%d/%d", rank, i);
		fill_by_randoms(v, vsize);
		struct kmr_kv_box kv = {
                  .klen = (int)(strlen(k) + 1), .vlen = (int)vsize,
                  .k.p = k, .v.p = v
		};
		cc = kmr_add_kv(kvs0, kv);
		assert(cc == MPI_SUCCESS);
	    }
	    kmr_add_kv_done(kvs0);

	    struct kmr_option opt = kmr_noopt;
	    opt.inspect = 1;

	    const int N = 5;
	    double tt[10][2][2];

	    for (int i = 0; i < N; i++) {
		mr->atoa_size_limit = 0;
		KMR_KVS *o0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
		tt[i][0][0] = kmr_wtime();
		cc = kmr_shuffle(kvs0, o0, opt);
		assert(cc == MPI_SUCCESS);
		tt[i][0][1] = kmr_wtime();

		mr->atoa_size_limit = SIZE_LIMIT;
		KMR_KVS *o1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
		tt[i][1][0] = kmr_wtime();
		cc = kmr_shuffle(kvs0, o1, opt);
		assert(cc == MPI_SUCCESS);
		tt[i][1][1] = kmr_wtime();

		/* Check data.  Note that data elements are
		   deterministically ordered. */

                long localcount0;
                long localcount1;
                kmr_local_element_count(o0, &localcount0);
                kmr_local_element_count(o0, &localcount1);
                if (!(localcount0 == localcount1)) {
                    printf("ERROR: entry count mismatch (%zd vs %zd)\n",
                           localcount0, localcount1);
                    assert(localcount0 == localcount1);
                    abort();
                }
                size_t localcount = (size_t)localcount0;
                assert(localcount <= loosecount);
		kmr_retrieve_kv_box_entries(o0, boxes0, (long)localcount);
		kmr_retrieve_kv_box_entries(o1, boxes1, (long)localcount);
		for (size_t j = 0; j < localcount; j++) {
		    if (!(boxes0[j].klen == boxes1[j].klen)) {
                        printf("ERROR: key length mismatch\n");
			assert(boxes0[j].klen == boxes1[j].klen);
			abort();
		    }
		    if (!(boxes0[j].vlen == boxes1[j].vlen)) {
			printf("ERROR: value length mismatch\n");
			assert(boxes0[j].vlen == boxes1[j].vlen);
			abort();
		    }
		    if (!((size_t)boxes0[j].vlen == vsize)) {
			printf("ERROR: value length wrong\n");
			assert((size_t)boxes0[j].vlen == vsize);
			abort();
		    }
		    size_t klen = (size_t)boxes0[j].klen;
		    size_t vlen = (size_t)boxes0[j].vlen;
		    cc = memcmp(boxes0[j].k.p, boxes1[j].k.p, klen);
		    if (cc != 0) {
			printf("ERROR: key contents wrong\n");
			assert(memcmp(boxes0[j].k.p, boxes1[j].k.p, klen) == 0);
			abort();
		    }
		    cc = memcmp(boxes0[j].v.p, boxes1[j].v.p, vlen);
		    if (cc != 0) {
			printf("ERROR: value contents wrong\n");
			assert(memcmp(boxes0[j].v.p, boxes1[j].v.p, vlen) == 0);
			abort();
		    }
		}

		kmr_free_kvs(o0);
		kmr_free_kvs(o1);
	    }

	    kmr_free_kvs(kvs0);
	    if (mr->rank == 0) {
		for (int i = 0; i < N; i++) {
		    printf("%zd %f #a\n", vsize, (tt[i][0][1] - tt[i][0][0]));
		}
		for (int i = 0; i < N; i++) {
		    printf("%zd %f #n\n", vsize, (tt[i][1][1] - tt[i][1][0]));
		}
		fflush(0);
	    }
	}
    }

    kmr_free_context(mr);
}

int
main(int argc, char *argv[])
{
    int nprocs, rank, lev;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &lev);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    kmr_init();

    if (rank == 0) {printf("CHECK NAIVE ALLTOALL IMPLEMENTATION\n");}
    fflush(0);

    simple0(nprocs, rank);

    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("OK\n");}
    fflush(0);

    kmr_fin();

    MPI_Finalize();
    return 0;
}
