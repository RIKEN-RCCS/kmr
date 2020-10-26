/* test4.c (2014-02-04) */

/* Check master-worker mappers. */

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
    mr->trace_map_ms = 1;

    if (1) {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {
	    printf("\n");
	    printf("Checking kmr_map_ms...\n");
	}
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
simple1(int nprocs, int rank, _Bool forkexec)
{
    int cc;

    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    mr->trace_map_ms = 1;
    mr->map_ms_abort_on_signal = 1;
    mr->map_ms_use_exec = forkexec;

    if (1) {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {
	    if (forkexec) {
		printf("\n");
		printf("Checking kmr_map_ms_commands (by fork-exec)...\n");
	    } else {
		printf("\n");
		printf("Checking kmr_map_ms_commands (by system(3C)...\n");
	    }
	}
	fflush(0);
	usleep(50 * 1000);

	KMR_KVS *kvs00 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	if (mr->rank == 0) {
	    for (int i = 0; i < 20; i++) {
		char key[] = "key";
		size_t klen = sizeof(key);
		char val_s[] = "echo start a subprocess.; "
		    "/bin/sleep 3; "
		    "echo a process done.";
		char val_e[] = "sh\0-c\0echo start a subprocess.; "
		    "/bin/sleep 3; "
		    "echo a process done.";
		char *val = (forkexec ? val_e : val_s);
		size_t vlen = (forkexec ? sizeof(val_e) : sizeof(val_s));
		assert(klen <= INT_MAX && vlen <= INT_MAX);
		struct kmr_kv_box kv;
		kv.klen = (int)klen;
		kv.k.p = key;
		kv.vlen = (int)vlen;
		kv.v.p = val;
		int ccx = kmr_add_kv(kvs00, kv);
		assert(ccx == MPI_SUCCESS);
	    }
	}
	kmr_add_kv_done(kvs00);
	KMR_KVS *kvs01 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	do {
	    struct kmr_option mopt = kmr_noopt;
	    mopt.nothreading = 1;
	    struct kmr_spawn_option sopt = {.separator_space = 0};
	    cc = kmr_map_ms_commands(kvs00, kvs01, 0,
				     mopt, sopt, copykvfn);
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
	simple1(nprocs, rank, 0);
	simple1(nprocs, rank, 1);

	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {printf("OK\n");}
	fflush(0);

	kmr_fin();

	MPI_Finalize();
    } else {
	/* This part is not used now.  The test invoked this program
	   in a subprocess.  Now, the test invokes a shell script. */

	printf("a process runs: %s %s %s %s\n",
	       argv[0], argv[1], argv[2], argv[3]);
	fflush(0);
	sleep(3);
	printf("a process done.\n");
	fflush(0);
    }
    return 0;
}
