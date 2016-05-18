/* test5.c (2014-02-04) */

/* Check spawning mappers. */

/* Run it with four or more dynamic processes. */

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

_Bool skipmpiwork = 0;

static int
empty_map_fn_seq(const struct kmr_kv_box kv, const KMR_KVS *kvi,
		 KMR_KVS *kvo, void *p, long ii)
{
    fflush(0);
    usleep(50 * 1000);
    printf("test5:empty-map-fn[%d]: called.\n", (int)ii);
    fflush(0);
    sleep(3);
    return MPI_SUCCESS;
}

static int
empty_map_fn_mpi_noreply(const struct kmr_kv_box kv, const KMR_KVS *kvi,
			 KMR_KVS *kvo, void *p, long ii)
{
    KMR *mr = kmr_get_context_of_kvs(kvi);
    MPI_Comm *pp = kmr_get_spawner_communicator(mr, ii);
    MPI_Comm ic = *pp;
    fflush(0);
    usleep(50 * 1000);
    if (sizeof(int) != sizeof(void *) && sizeof(ic) == sizeof(void *)) {
	printf("test5:empty-map-fn[%d]: sleeping 12 sec (icomm=%p)...\n",
	       (int)ii, (void *)ic);
    } else {
	printf("test5:empty-map-fn[%d]: sleeping 12 sec (icomm=%d)...\n",
	       (int)ii, (int)ic);
    }
    fflush(0);
    sleep(12);
    return MPI_SUCCESS;
}

static int
empty_map_fn_mpi_with_reply(const struct kmr_kv_box kv, const KMR_KVS *kvi,
			    KMR_KVS *kvo, void *p, long ii)
{
    KMR *mr = kmr_get_context_of_kvs(kvi);
    MPI_Comm *pp = kmr_get_spawner_communicator(mr, ii);
    MPI_Comm ic = *pp;
    fflush(0);
    usleep(50 * 1000);
    if (sizeof(int) != sizeof(void *) && sizeof(ic) == sizeof(void *)) {
	printf("test5:empty-map-fn[%d]: called (icomm=%p).\n",
	       (int)ii, (void *)ic);
    } else {
	printf("test5:empty-map-fn[%d]: called (icomm=%d).\n",
	       (int)ii, (int)ic);
    }
    fflush(0);
    return MPI_SUCCESS;
}

static void
simple0(int nprocs, int rank)
{
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    mr->trace_map_spawn = 1;
    mr->spawn_max_processes = 4;

    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);

    if (1) {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {
	    printf("\n");
	    printf("** CHECK kmr_map_via_spawn WITH RETURNING KVS.\n");
	    printf("** Spawn 2-rank work 4 times"
		   " using %d dynamic processes.\n",
		   mr->spawn_max_processes);
	}
	fflush(0);
	usleep(50 * 1000);

	KMR_KVS *kvs00 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	if (mr->rank == 0) {
	    char kbuf[256];
	    char vbuf[256];
	    snprintf(kbuf, sizeof(kbuf), "key");
	    snprintf(vbuf, sizeof(vbuf),
		     "maxprocs=2 %s ./a.out mpi returnkvs a0 a1 a2",
		     "info0=value0 info1=value1 info2=value2");
	    struct kmr_kv_box nkv = {
		.klen = (int)(strlen(kbuf) + 1),
		.vlen = (int)(strlen(vbuf) + 1),
		.k.p = kbuf,
		.v.p = vbuf};
	    for (int i = 0; i < 4; i++) {
		kmr_add_kv(kvs00, nkv);
	    }
	}
	kmr_add_kv_done(kvs00);
	KMR_KVS *kvs01 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	struct kmr_spawn_option opt = {.reply_each = 1,
				       .separator_space = 1};
	kmr_map_via_spawn(kvs00, kvs01, 0,
			  MPI_INFO_NULL, opt, kmr_receive_kvs_from_spawned_fn);
	/*kmr_dump_kvs(kvs01, 0);*/
	if (mr->rank == 0) {
	    assert(kvs01->c.element_count == 32);
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
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    mr->trace_map_spawn = 1;
    mr->spawn_max_processes = 4;

    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);

    if (1) {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {
	    printf("\n");
	    printf("** CHECK kmr_map_via_spawn WITH WAITING IN MAP-FN.\n");
	    printf("** Spawn 2-rank work 4 times"
		   " using %d dynamic processes.\n",
		   mr->spawn_max_processes);
	}
	fflush(0);
	usleep(50 * 1000);

	KMR_KVS *kvs00 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	if (mr->rank == 0) {
	    char kbuf[256];
	    char vbuf[256];
	    snprintf(kbuf, sizeof(kbuf), "key");
	    snprintf(vbuf, sizeof(vbuf),
		     "maxprocs=2 ./a.out mpi noreply a0 a1 a2");
	    struct kmr_kv_box nkv = {
		.klen = (int)(strlen(kbuf) + 1),
		.vlen = (int)(strlen(vbuf) + 1),
		.k.p = kbuf,
		.v.p = vbuf};
	    for (int i = 0; i < 4; i++) {
		kmr_add_kv(kvs00, nkv);
	    }
	}
	kmr_add_kv_done(kvs00);
	KMR_KVS *kvs01 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	struct kmr_spawn_option opt = {.separator_space = 1};
	kmr_map_via_spawn(kvs00, kvs01, 0,
			  MPI_INFO_NULL, opt, empty_map_fn_mpi_noreply);
	kmr_free_kvs(kvs01);
    }

    if (1) {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {
	    printf("\n");
	    printf("** CHECK kmr_map_via_spawn WITH REPLY (EACH).\n");
	    printf("** Spawn 2-rank work 4 times"
		   " using %d dynamic processes.\n",
		   mr->spawn_max_processes);
	}
	fflush(0);
	usleep(50 * 1000);

	KMR_KVS *kvs10 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	if (mr->rank == 0) {
	    char kbuf[256];
	    char vbuf[256];
	    snprintf(kbuf, sizeof(kbuf), "key");
	    snprintf(vbuf, sizeof(vbuf),
		     "maxprocs=2 ./a.out mpi eachreply a0 a1 a2");
	    struct kmr_kv_box nkv = {
		.klen = (int)(strlen(kbuf) + 1),
		.vlen = (int)(strlen(vbuf) + 1),
		.k.p = kbuf,
		.v.p = vbuf};
	    for (int i = 0; i < 4; i++) {
		kmr_add_kv(kvs10, nkv);
	    }
	}
	kmr_add_kv_done(kvs10);
	KMR_KVS *kvs11 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	struct kmr_spawn_option opt = {.reply_each = 1,
				       .separator_space = 1};
	kmr_map_via_spawn(kvs10, kvs11, 0,
			  MPI_INFO_NULL, opt, empty_map_fn_mpi_with_reply);
	kmr_free_kvs(kvs11);
    }

    if (1) {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {
	    printf("\n");
	    printf("** CHECK kmr_map_via_spawn WITH REPLY (ROOT).\n");
	    printf("Spawn 2-rank work 4 times"
		   " using %d dynamic processes.\n",
		   mr->spawn_max_processes);
	}
	fflush(0);
	usleep(50 * 1000);

	KMR_KVS *kvs20 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	if (mr->rank == 0) {
	    char kbuf[256];
	    char vbuf[256];
	    snprintf(kbuf, sizeof(kbuf), "key");
	    snprintf(vbuf, sizeof(vbuf),
		     "maxprocs=2 ./a.out mpi rootreply a0 a1 a2");
	    struct kmr_kv_box nkv = {
		.klen = (int)(strlen(kbuf) + 1),
		.vlen = (int)(strlen(vbuf) + 1),
		.k.p = kbuf,
		.v.p = vbuf};
	    for (int i = 0; i < 4; i++) {
		kmr_add_kv(kvs20, nkv);
	    }
	}
	kmr_add_kv_done(kvs20);
	KMR_KVS *kvs21 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	struct kmr_spawn_option opt = {.reply_root = 1,
				       .separator_space = 1};
	kmr_map_via_spawn(kvs20, kvs21, 0,
			  MPI_INFO_NULL, opt, empty_map_fn_mpi_with_reply);
	kmr_free_kvs(kvs21);
    }

    kmr_free_context(mr);
}

static void
simple2(int nprocs, int rank)
{
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    mr->trace_map_spawn = 1;
    mr->spawn_max_processes = 4;

    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);

    if (1) {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {
	    printf("\n");
	    printf("** CHECK kmr_map_processes (SERIAL)...\n");
	    printf("** Spawn 2 serial processes 4 times.\n");
	}
	fflush(0);
	usleep(50 * 1000);

	KMR_KVS *kvs10 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	if (mr->rank == 0) {
	    char kbuf[256];
	    char vbuf[256];
	    snprintf(kbuf, sizeof(kbuf), "key");
	    snprintf(vbuf, sizeof(vbuf),
		     "maxprocs=2 ./a.out seq noreply a0 a1 a2");
	    struct kmr_kv_box nkv = {
		.klen = (int)(strlen(kbuf) + 1),
		.vlen = (int)(strlen(vbuf) + 1),
		.k.p = kbuf,
		.v.p = vbuf};
	    for (int i = 0; i < 8; i++) {
		kmr_add_kv(kvs10, nkv);
	    }
	}
	kmr_add_kv_done(kvs10);
	KMR_KVS *kvs11 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	struct kmr_spawn_option opt = {.separator_space = 1};
	kmr_map_processes(1, kvs10, kvs11, 0, MPI_INFO_NULL, opt,
			  empty_map_fn_seq);
	kmr_free_kvs(kvs11);
    }

    if (!skipmpiwork) {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {
	    printf("\n");
	    printf("** CHECK kmr_map_processes (MPI)...\n");
	    printf("** Spawn 2-rank work 4 times"
		   " using %d dynamic processes.\n",
		   mr->spawn_max_processes);
	    printf("** THIS TEST MAY BLOCK INDEFINITELY"
		   " ON SOME IMPLEMENTATIONS OF MPI.\n");
	    printf("** THEN, RUN THIS TEST WITH a.out 0"
		   " TO SKIP THIS PART.\n");
	}
	fflush(0);
	usleep(50 * 1000);

	KMR_KVS *kvs00 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	if (mr->rank == 0) {
	    char kbuf[256];
	    char vbuf[256];
	    snprintf(kbuf, sizeof(kbuf), "key");
	    snprintf(vbuf, sizeof(vbuf),
		     "maxprocs=2 ./a.out mpi noreply a0 a1 a2");
	    struct kmr_kv_box nkv = {
		.klen = (int)(strlen(kbuf) + 1),
		.vlen = (int)(strlen(vbuf) + 1),
		.k.p = kbuf,
		.v.p = vbuf};
	    for (int i = 0; i < 4; i++) {
		kmr_add_kv(kvs00, nkv);
	    }
	}
	kmr_add_kv_done(kvs00);
	KMR_KVS *kvs01 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	struct kmr_spawn_option opt = {.separator_space = 1};
	kmr_map_processes(0, kvs00, kvs01, 0, MPI_INFO_NULL, opt,
			  empty_map_fn_seq);
	kmr_free_kvs(kvs01);
    }

    kmr_free_context(mr);
}

static int
spawned(int argc, char *argv[])
{
    /* SPAWNED CHILD */

    assert(strcmp(argv[1], "seq") == 0 || strcmp(argv[1], "mpi") == 0);
    if (strcmp(argv[1], "seq") == 0) {
	printf("test5:spawned(serial): started (%s,%s,%s).\n",
	       argv[0], argv[1], argv[3]);
	printf("test5:spawned(serial): sleeping 3 sec...\n");
	fflush(0);
	sleep(3);
	printf("test5:spawned(serial): exits.\n");
	fflush(0);
    } else if (strcmp(argv[1], "mpi") == 0) {
	int nprocs, rank, lev;
	MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &lev);
	MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	MPI_Comm parent;
	MPI_Comm_get_parent(&parent);
	assert(parent != MPI_COMM_NULL);

	int peer_nprocs;
	MPI_Comm_remote_size(parent, &peer_nprocs);
	assert(peer_nprocs == 1);
	printf("test5:spawned(mpi;rank=%d/%d): started (%s,%s,%s).\n",
	       rank, nprocs, argv[0], argv[1], argv[2]);
	printf("test5:spawned(mpi;rank=%d/%d): sleeping 3 sec...\n",
	       rank, nprocs);
	fflush(0);
	sleep(3);

	assert((strcmp(argv[2], "noreply") == 0)
	       || (strcmp(argv[2], "eachreply") == 0)
	       || (strcmp(argv[2], "rootreply") == 0)
	       || (strcmp(argv[2], "returnkvs") == 0));
	if (strcmp(argv[2], "noreply") == 0) {
	    /* NO REPLY */
	    printf("test5:spawned(mpi;rank=%d/%d):"
		   " no reply.\n",
		   rank, nprocs);
	} else if (strcmp(argv[2], "eachreply") == 0) {
	    /* EACH REPLY */
	    printf("test5:spawned(mpi;rank=%d/%d):"
		   " sending a reply.\n",
		   rank, nprocs);
	    int peer = 0;
	    MPI_Send(0, 0, MPI_BYTE, peer,
		     KMR_TAG_SPAWN_REPLY, parent);
	} else if (strcmp(argv[2], "rootreply") == 0) {
	    /* ROOT REPLY */
	    if (rank == 0) {
		printf("test5:spawned(mpi;rank=%d/%d):"
		       " sending a root reply.\n",
		       rank, nprocs);
		int peer = 0;
		MPI_Send(0, 0, MPI_BYTE, peer,
			 KMR_TAG_SPAWN_REPLY, parent);
	    }
	} else if (strcmp(argv[2], "returnkvs") == 0) {
	    printf("test5:spawned(mpi;rank=%d/%d):"
		   " sending a kvs.\n",
		   rank, nprocs);
	    KMR *mr = kmr_create_dummy_context();
	    kmr_reply_to_spawner(mr);
	    KMR_KVS *kvs00 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	    for (int i = 0; i < 4; i++) {
		char k[40];
		char v[40];
		snprintf(k, sizeof(k), "k%d", i);
		snprintf(v, sizeof(v), "v%d", i);
		kmr_add_string(kvs00, k, v);
	    }
	    kmr_add_kv_done(kvs00);
	    kmr_send_kvs_to_spawner(mr, kvs00);
	} else {
	    /* NO REPLY */
	    assert(0);
	}

	printf("test5:spawned(mpi;rank=%d/%d):"
	       " call MPI_Comm_free (could block)...\n",
	       rank, nprocs);
	fflush(0);
	MPI_Comm_free(&parent);
	printf("test5:spawned(mpi;rank=%d/%d):"
	       " MPI_Comm_free done.\n",
	       rank, nprocs);
	fflush(0);
	printf("test5:spawned(mpi;rank=%d/%d):"
	       " call MPI_Finalize...\n",
	       rank, nprocs);
	MPI_Finalize();
	printf("test5:spawned(mpi;rank=%d/%d):"
	       " MPI_Finalize done; exits.\n",
	       rank, nprocs);
	fflush(0);
    }

    return 0;
}

int
main(int argc, char *argv[])
{
    if (argc == 1 || argc == 2) {
	/* SPAWNER */

	int nprocs, rank, lev;
	MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &lev);
	MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	kmr_init();

	skipmpiwork = (argc == 2 && argv[1][0] == '0');

	if (rank == 0) {
	    printf("Check spawning mapper.\n");
	    printf("Running this test needs 4 or more"
		   " dynamic processes.\n");
	}
	fflush(0);

	simple0(nprocs, rank);
	simple1(nprocs, rank);
	simple2(nprocs, rank);

	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {printf("OK\n");}
	fflush(0);

	kmr_fin();

	MPI_Finalize();
    } else {
	spawned(argc, argv);
    }
    return 0;
}
