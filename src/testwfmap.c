/* testwfmap.c (2016-07-14) */

/* Check simple workflow operations.  Run this with (nprocs>=32+1). */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <strings.h>
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

/* When MINIMINITEST=1, run tests in cases with very small processes
   (with nprocs>=3 or nprocs>=5).  It is for development. */

#define MINIMINITEST (1)

const int MINNPROCS = (32+1);

/* Configuration of Processes (Case 0). */

int lanes0vector[4][20] = {
    {2,2, 0},
    {4,4, 4,4, 0},
    {2,2,2,2, 2,2,2,2, 2,2,2,2, 2,2,2,2, /*(remainings are spares)*/ 0},
    {0}
};

int *lanes0[4] = {
    lanes0vector[0],
    lanes0vector[1],
    lanes0vector[2],
    0
};

/* Configuration of Processes (Case 1).  A different way of describing
   the same configuration as above. */

char *lanes1[] = {
    "0.0.0:2",
    "0.0.1:2",
    "0.0.2:2",
    "0.0.3:2",
    "0.1.0:2",
    "0.1.1:2",
    "0.1.2:2",
    "0.1.3:2",
    "1.0.0:2",
    "1.0.1:2",
    "1.0.2:2",
    "1.0.3:2",
    "1.1.0:2",
    "1.1.1:2",
    "1.1.2:2",
    "1.1.3:2",
    0};

static int
iindex(int e, int *vector, int length)
{
    for (int i = 0; i < length; i++) {
	if (vector[i] == e) {
	    return i;
	}
    }
    return -1;
}

/* Runs degenerated test cases.  CASE: TESTCASE=0 to 5: It just checks
   the workflow control (not execute binaries).  It needs nprocs=3 for
   TESTCASE=0..3, and nprocs=5 for TESTCASE=4..5.  CASE: TESTCASE=6: It
   needs nprocs=3. */

static void
minimini(int TESTCASE, int nprocs, int rank)
{
    int masterrank = (nprocs - 1);
    int root = 0;

    assert(0 <= TESTCASE && TESTCASE <= 9);

    int cc;

    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {
	printf("MINI RUN (case %d)...\n", TESTCASE);
	fflush(0);
    }

    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    assert(mr != 0);
    if (TESTCASE <= 5) {
	/* Disable the spawning library. */ 
	mr->swf_spawner_library = strdup("NONE.so");
    }
    mr->trace_map_spawn = 0;

    char *lanesdesc0[] = {
	"0:2",
	0
    };
    char *lanesdesc1[] = {
	"0.0:2",
	0
    };
    char *lanesdesc2[] = {
	"0.0.0:2",
	0
    };
    char *lanesdesc3[] = {
	"0.0.0.0:2",
	0
    };
    char *lanesdesc4[] = {
	"0.0:2",
	"0.1:2",
	0
    };
    char *lanesdesc5[] = {
	"0:1",
	"1:1",
	"2:1",
	"3:1",
	0
    };
    char *lanesdesc6[] = {
	"0:2",
	0
    };
    char **lanesdesclist[10] = {
	lanesdesc0, lanesdesc1, lanesdesc2, lanesdesc3, lanesdesc4,
	lanesdesc5, lanesdesc6, lanesdesc0, lanesdesc0, lanesdesc0
    };

    char **lanesdesc = lanesdesclist[TESTCASE];

    if (rank == 0) {
	printf("Lane description of tests:\n");
	char **d;
	d = lanesdesc;
	while (*d != 0) {
	    printf("%s\n", *d);
	    d++;
	}
	fflush(0);
    }

    MPI_Comm splitcomms[4];
    cc = kmr_split_swf_lanes(mr, splitcomms, root, lanesdesc, 1);
    assert(cc == MPI_SUCCESS);

    mr->swf_debug_master = 1;
    cc = kmr_init_swf(mr, splitcomms, masterrank);
    assert(cc == MPI_SUCCESS);

    if (rank == masterrank) {
	printf("Test runs using the following lanes:\n");
	fflush(0);
    }
    kmr_dump_swf_lanes(mr);

    mr->trace_map_spawn = 0;
    kmr_set_swf_verbosity(mr, 2);
    cc = kmr_detach_swf_workers(mr);
    assert(cc == MPI_SUCCESS);

    /* ONLY THE MASTER RANK COMES HERE. */

    assert(rank == masterrank);

    if (1) {
	usleep(50 * 1000);
	printf("Minimini 10 works.  Check the trace log...\n");
	fflush(0);
	usleep(50 * 1000);

	KMR_KVS *kvs00 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	{
	    char kbuf[256];
	    char vbuf[256];
	    for (int i = 0; i < 10; i++) {

		/* Make key. */

		if (TESTCASE == 0) {
		    snprintf(kbuf, sizeof(kbuf), "0");
		} else if (TESTCASE == 1) {
		    if (i % 3 == 0) {
			snprintf(kbuf, sizeof(kbuf), "0");
		    } else {
			snprintf(kbuf, sizeof(kbuf), "0.0");
		    }
		} else if (TESTCASE == 2) {
		    if (i % 3 == 0) {
			snprintf(kbuf, sizeof(kbuf), "0");
		    } else {
			snprintf(kbuf, sizeof(kbuf), "0.0.0");
		    }
		} else if (TESTCASE == 3) {
		    if (i % 3 == 0) {
			snprintf(kbuf, sizeof(kbuf), "0");
		    } else {
			snprintf(kbuf, sizeof(kbuf), "0.0.0.0");
		    }
		} else if (TESTCASE == 4) {
		    if (i % 3 == 0) {
			snprintf(kbuf, sizeof(kbuf), "0");
		    } else if (i % 2 == 0) {
			snprintf(kbuf, sizeof(kbuf), "0.0");
		    } else {
			snprintf(kbuf, sizeof(kbuf), "0.1");
		    }
		} else if (TESTCASE == 5) {
		    snprintf(kbuf, sizeof(kbuf), "%d", (i % 4));
		} else if (TESTCASE == 6) {
		    snprintf(kbuf, sizeof(kbuf), "0");
		} else {
		    snprintf(kbuf, sizeof(kbuf), "0");
		}

		/* Make value. */

		if (TESTCASE <= 4) {
		    snprintf(vbuf, sizeof(vbuf),
			     ("maxprocs=2 ./a.out work! index=%d lane=%s"
			      " blah blah blah"), i, kbuf);
		} else if (TESTCASE == 5) {
		    snprintf(vbuf, sizeof(vbuf),
			     ("maxprocs=1 ./a.out work! index=%d lane=%s"
			      " blah blah blah"), i, kbuf);
		} else if (TESTCASE == 6) {
		    snprintf(vbuf, sizeof(vbuf),
			     ("maxprocs=2 ./a.out work! index=%d lane=%s"
			      " blah blah blah"), i, kbuf);
		} else {
		    snprintf(vbuf, sizeof(vbuf),
			     ("maxprocs=2 ./a.out work! index=%d lane=%s"
			      " blah blah blah"), i, kbuf);
		}

		struct kmr_kv_box nkv = {
		    .klen = (int)(strlen(kbuf) + 1),
		    .vlen = (int)(strlen(vbuf) + 1),
		    .k.p = kbuf,
		    .v.p = vbuf};
		kmr_add_kv(kvs00, nkv);
	    }
	}
	kmr_add_kv_done(kvs00);
	KMR_KVS *kvs01 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	struct kmr_spawn_option opt = {.separator_space = 1};
	mr->trace_map_spawn = 1;
	mr->swf_record_history = 1;
	kmr_map_swf(kvs00, kvs01, (void *)0, opt, 0);
	kmr_free_kvs(kvs00);
	kmr_free_kvs(kvs01);

	if (0) {
	    kmr_dump_swf_history(mr);
	}

	printf("Minimini 10 works done.\n");
	fflush(0);
    }

    MPI_Barrier(MPI_COMM_SELF);

    cc = kmr_stop_swf_workers(mr);
    assert(cc == MPI_SUCCESS);
    cc = kmr_free_context(mr);
    assert(cc == MPI_SUCCESS);

    kmr_fin();
    MPI_Finalize();
}

static void
simple0(int nprocs, int rank)
{
    int masterrank = (nprocs - 1);
    int root = 0;

    int cc;

    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {
	printf("CHECK WORKFLOW-MAPPER...\n");
	printf("Run this test with 33 or more processes.\n");
	fflush(0);
    }

    if (nprocs < MINNPROCS) {
	if (rank == 0) {
	    printf("THIS TEST NEEDS 32+1 OR MORE PROCESSES.\n");
	    fflush(0);
	}
	usleep(50 * 1000);
	MPI_Finalize();
	exit(1);
    }

    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    assert(mr != 0);
    mr->trace_map_spawn = 0;

    MPI_Comm splitcomms[4];

    if (0) {
	cc = kmr_split_swf_lanes_a(mr, splitcomms, root, lanes0, 1);
	assert(cc == MPI_SUCCESS);
    } else {
	cc = kmr_split_swf_lanes(mr, splitcomms, root, lanes1, 1);
	assert(cc == MPI_SUCCESS);
    }

    sleep(3);

    mr->swf_debug_master = 1;
    cc = kmr_init_swf(mr, splitcomms, masterrank);
    assert(cc == MPI_SUCCESS);

    if (rank == masterrank) {
	printf("Test runs using the following lanes:\n");
	fflush(0);
    }
    kmr_dump_swf_lanes(mr);

    sleep(3);

    printf("[%05d] Detaching workers...\n", rank);
    fflush(0);

    mr->trace_map_spawn = 0;
    cc = kmr_detach_swf_workers(mr);
    assert(cc == MPI_SUCCESS);

    sleep(3);

    /* ONLY THE MASTER RANK COMES HERE. */

    assert(rank == masterrank);

    /* This part is to check trace using the eyes. */

    if (1) {
	usleep(50 * 1000);
	printf("Simple 100 works.  Check the trace log...\n");
	fflush(0);
	usleep(50 * 1000);

	int level0 = 2;
	int level1 = 2;
	int level2 = 4;
	int n_lanes = (level0 * level1 * level2);

	KMR_KVS *kvs00 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	{
	    char kbuf[256];
	    char vbuf[256];
	    for (int i = 0; i < 100; i++) {
		int lane = (rand() % n_lanes);
		int index1 = ((lane / level2) % level1);
		int index0 = (lane / (level1 * level2));
		snprintf(kbuf, sizeof(kbuf),
			 "%d.%d.*", index0, index1);
		snprintf(vbuf, sizeof(vbuf),
			 ("maxprocs=2 ./a.out work! index=%d lane=%s"
			  " blah blah blah"), i, kbuf);
		struct kmr_kv_box nkv = {
		    .klen = (int)(strlen(kbuf) + 1),
		    .vlen = (int)(strlen(vbuf) + 1),
		    .k.p = kbuf,
		    .v.p = vbuf};
		kmr_add_kv(kvs00, nkv);
	    }
	}
	kmr_add_kv_done(kvs00);
	KMR_KVS *kvs01 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	struct kmr_spawn_option opt = {.separator_space = 1};
	mr->trace_map_spawn = 1;
	mr->swf_record_history = 1;
	kmr_map_swf(kvs00, kvs01, (void *)0, opt, 0);
	kmr_free_kvs(kvs00);
	kmr_free_kvs(kvs01);

	if (0) {
	    kmr_dump_swf_history(mr);
	}

	int count = 100;
	int history[count];
	kmr_dump_swf_order_history(mr, history, (size_t)count);

	if (0) {
	    usleep(50 * 1000);
	    printf("History (finish ordering of work-items):\n");
	    for (int i = 0; i < count; i++) {
		if (i == 0) {
		    printf("%d", history[i]);
		} else if ((i % 15) == 0) {
		    printf(",\n");
		    printf("%d", history[i]);
		} else {
		    printf(",%d", history[i]);
		}
	    }
	    printf("\n");
	    fflush(0);
	}

	printf("Simple 100 works done.\n");
	fflush(0);
    }

    /* This part checks the ordering contraint by assertions. */

    if (0) {
	usleep(50 * 1000);
	printf("Simple 400 works...\n");
	fflush(0);
	usleep(50 * 1000);

	/* Let S as SETSIZE=10, G as GROUPS=4, L as ITERATION=10.
	   (GROUPS is fixed by the lane configuration).  Each (S)th
	   element barriers elements in the same group.  That is, the
	   barrier is the (G*S*i+S-1)th element, and (G*S*i'+j)th
	   elements finish before the barrier and (G*S*i''+j)th
	   elements start after the barrier, where 0<=j<S-1, 0<=i<L,
	   i'<i, i''>i. */

	int SETSIZE = 10;
	int ITERATION = 10;
	int GROUPS = 4;
	assert(GROUPS == 4);
	KMR_KVS *kvs00 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	{
	    char kbuf[256];
	    char vbuf[256];
	    /* A set of works for 4 groups, for loop repeated 10 times. */
	    for (int iteration = 0; iteration < ITERATION; iteration++) {
		for (int group = 0; group < 4; group++) {
		    int a = (group / 2);
		    int b = (group % 2);
		    /* (SETSIZE-1) works in (a.b.*). */
		    for (int i = 0; i < (SETSIZE - 1); i++) {
			snprintf(kbuf, sizeof(kbuf), "%d.%d.*", a, b);
			snprintf(vbuf, sizeof(vbuf),
				 "maxprocs=2 ./a.out simple0 blah blah");
			struct kmr_kv_box nkv = {
			    .klen = (int)(strlen(kbuf) + 1),
			    .vlen = (int)(strlen(vbuf) + 1),
			    .k.p = kbuf,
			    .v.p = vbuf};
			kmr_add_kv(kvs00, nkv);
		    }
		    /* 1 work in (a.b) which barriers previous set. */
		    snprintf(kbuf, sizeof(kbuf), "%d.%d", a, b);
		    snprintf(vbuf, sizeof(vbuf),
			     ("maxprocs=8 ./a.out simple0"
			      " iteration=%d blah blah"),
			     iteration);
		    struct kmr_kv_box nkv = {
			.klen = (int)(strlen(kbuf) + 1),
			.vlen = (int)(strlen(vbuf) + 1),
			.k.p = kbuf,
			.v.p = vbuf};
		    kmr_add_kv(kvs00, nkv);
		}
	    }
	}
	kmr_add_kv_done(kvs00);
	KMR_KVS *kvs01 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	struct kmr_spawn_option opt = {.separator_space = 1};
	mr->trace_map_spawn = 0;
	mr->swf_record_history = 1;
	kmr_map_swf(kvs00, kvs01, (void *)0, opt, 0);
	kmr_free_kvs(kvs00);
	kmr_free_kvs(kvs01);

	if (0) {
	    kmr_dump_swf_history(mr);
	}

	int count = (4 * SETSIZE * 10);
	int history[count];
	kmr_dump_swf_order_history(mr, history, (size_t)count);

	if (0) {
	    usleep(50 * 1000);
	    printf("History (finish ordering of work-items):\n");
	    for (int i = 0; i < count; i++) {
		if (i == 0) {
		    printf("%d", history[i]);
		} else if ((i % 15) == 0) {
		    printf(",\n");
		    printf("%d", history[i]);
		} else {
		    printf(",%d", history[i]);
		}
	    }
	    printf("\n");
	    fflush(0);
	}

	/* Check workflow ordering constraint. */

	if (1) {
	    printf("Checking ordering constraint...\n");
	    fflush(0);
	    for (int group = 0; group < 4; group++) {
		for (int i = 0; i < ITERATION; i++) {
		    int barrier = (4*SETSIZE*i+SETSIZE-1);
		    int pos0 = iindex(barrier, history, count);
		    assert(pos0 != -1);
		    for (int i1 = 0; i1 < i; i1++) {
			for (int j = 0; j < (SETSIZE - 1); j++) {
			    int prea = (4*SETSIZE*i1+j);
			    int pos1 = iindex(prea, history, count);
			    assert(pos1 != -1);
			    assert(pos1 < pos0);
			}
		    }
		    for (int i2 = (i + 1); i2 < ITERATION; i2++) {
			for (int j = 0; j < (SETSIZE - 1); j++) {
			    int post = (4*SETSIZE*i2+j);
			    int pos2 = iindex(post, history, count);
			    assert(pos2 != -1);
			    assert(pos0 < pos2);
			}
		    }
		}
	    }
	}

	printf("Simple 400 works done.\n");
	fflush(0);
    }

    MPI_Barrier(MPI_COMM_SELF);

    cc = kmr_stop_swf_workers(mr);
    assert(cc == MPI_SUCCESS);
    cc = kmr_free_context(mr);
    assert(cc == MPI_SUCCESS);

    kmr_fin();
    MPI_Finalize();
}

int
main(int argc, char *argv[])
{
#define digitp(X) ('0' <= (X) && (X) <= '9')

#if 0
    /*AHO*/ printf("AHOAHO argc=%d\n", argc); fflush(0);
    /*AHO*/ printf("AHOAHO argv[0]=%s\n", argv[0]); fflush(0);
    /*AHO*/ printf("AHOAHO argv[1]=%s\n", argv[1]); fflush(0);
    /*AHO*/ printf("AHOAHO argv[1]=%s strcmp(argv[1], work!)=%d\n",
		   argv[1], (strcmp(argv[1], "work!") == 0));
    fflush(0);
    sleep(3);
#endif

    if (argc == 1 || (argc == 2 && argv[1][1] == 0 && digitp(argv[1][0]))) {

	/* MAIN RUN (Argument: None or a single digit). */

	int testcase = ((argc == 2) ? (argv[1][0] - '0') : 0);
	assert(0 <= testcase && testcase <= 9);

	int nprocs, rank, thlv;
	MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &thlv);
	MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	if (rank == 0) {
#ifdef NDEBUG
	    _Bool assertion = 0;
#else
	    _Bool assertion = 1;
#endif
	    if (!assertion) {
		kmr_error(0, "Assertion disabled; recompile this test");
		return 1;
	    }
	}

	kmr_init();

	if (!MINIMINITEST) {
	    if (rank == 0) {
		if (nprocs < MINNPROCS) {
		    fprintf(stderr, "RUN THIS WITH 33 PROCS OR MORE.\n");
		    fflush(0);
		    abort();
		    return 1;
		}
	    }
	    simple0(nprocs, rank);
	} else {
	    minimini(testcase, nprocs, rank);
	}

	usleep(50 * 1000);
	{printf("OK\n");}
	fflush(0);

	return 0;
    } else if (argc >= 2 && strcmp(argv[1], "work!") == 0) {

	/* RUN INVOKED BY WORKERS (Argument: "work!"). */

	{printf("A work started\n"); fflush(0);}

	int thlv;
	MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &thlv);
	int nprocs, rank;
	MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	{printf("A work with nprocs=%d rank=%d\n", nprocs, rank); fflush(0);}

	MPI_Finalize();
	exit(0);
	return 0;
    } else {
	fprintf(stderr, "RUN WITH BAD ARGUMENTS.\n");
	fflush(0);
	abort();
	return 1;
    }
}
