/* test4.c (2014-02-04) */

/* Check master-slave mappers. */

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

static int
copykvfn(struct kmr_kv_box kv, const KMR_KVS *kvi,
	 KMR_KVS *kvo, void *p, const long ii)
{
    printf("mapfn(k=%s, v=%s)\n", kv.k.p, kv.v.p);
    fflush(0);
    kmr_add_kv(kvo, kv);
    return MPI_SUCCESS;
}

static void
simple0(int nprocs, int rank)
{
    int cc;

    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    mr->trace_map_spawn = 1;
    mr->spawn_max_processes = 4;

    if (1) {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {printf("Checking kmr_map_ms...\n");}
	fflush(0);
	usleep(50 * 1000);

	KMR_KVS *kvs00 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	if (mr->rank == 0) {
	    char k[256];
	    char v[256];
	    for (int i = 0; i < 20; i++) {
		snprintf(k, sizeof(k), "key%d", i);
		snprintf(v, sizeof(v), "value%d", i);
		kmr_add_string(kvs00, k, v);
	    }
	}
	kmr_add_kv_done(kvs00);
	KMR_KVS *kvs01 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	do {
	    struct kmr_option opt = kmr_noopt;
	    opt.nothreading = 1;
	    cc = kmr_map_ms(kvs00, kvs01, 0, opt, copykvfn);
	} while (cc == MPI_ERR_ROOT);
	/*kmr_dump_kvs(kvs01, 0);*/
	if (mr->rank == 0) {
	    assert(kvs01->c.element_count == 20);
	} else {
	    assert(kvs01->c.element_count == 0);
	}
	kmr_free_kvs(kvs01);
    }

    kmr_free_context(mr);
}

static void
simple1(int nprocs, int rank)
{
    int cc;

    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    mr->trace_map_spawn = 1;
    mr->spawn_max_processes = 4;

    if (1) {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {printf("Checking kmr_map_ms_commands...\n");}
	fflush(0);
	usleep(50 * 1000);

	KMR_KVS *kvs00 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	if (mr->rank == 0) {
	    char k[256];
	    char v[256];
	    snprintf(k, sizeof(k), "key");
	    snprintf(v, sizeof(v), "./a.out a0 a1 a2");
	    for (int i = 0; i < 4; i++) {
		kmr_add_string(kvs00, k, v);
	    }
	}
	kmr_add_kv_done(kvs00);
	KMR_KVS *kvs01 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	do {
	    struct kmr_option mopt = kmr_noopt;
	    mopt.nothreading = 1;
	    struct kmr_spawn_option sopt = {.separator_space = 1};
	    cc = kmr_map_ms_commands(kvs00, kvs01, 0,
				     mopt, sopt, copykvfn);
	} while (cc == MPI_ERR_ROOT);
	/*kmr_dump_kvs(kvs01, 0);*/
	if (mr->rank == 0) {
	    assert(kvs01->c.element_count == 4);
	} else {
	    assert(kvs01->c.element_count == 0);
	}
	kmr_free_kvs(kvs01);
    }

    kmr_free_context(mr);
}

int
main(int argc, char *argv[])
{
    if (argc == 1 || argc == 2) {
	int nprocs, rank, lev;
	MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &lev);
	MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	kmr_init();

	if (rank == 0) {printf("CHECK MS MAPPER\n");}
	fflush(0);

	simple0(nprocs, rank);
	simple1(nprocs, rank);

	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {printf("OK\n");}
	fflush(0);

	kmr_fin();

	MPI_Finalize();
    } else {
	printf("a process runs: %s %s %s %s\n",
	       argv[0], argv[1], argv[2], argv[3]);
	fflush(0);
	sleep(3);
	printf("a process done.\n");
	fflush(0);
    }
    return 0;
}
