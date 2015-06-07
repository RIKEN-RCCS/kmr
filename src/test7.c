/* test7.c (2014-02-04) */

/* Test CKPT running Word-Count.  It injects a fault at the entry of a
   selected function when an alarm-signal triggers during a run of a
   job.  An alarm-signal is set at about a half of the elapse time of
   a run of a function.  For the purpose of setting an alarm, each
   step is repeated 3-times, the 1st for a dry-run, the 2nd for time
   measurement, and the 3rd for an actual run.  Word-count ranks the
   words by their occurrence count in the "LICENSE" file.  Copy the
   file in the current directory and run it. */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <sys/mman.h>
#include "kmr.h"
#include "kmrimpl.h"

#define MAX(a,b) (((a)>(b))?(a):(b))
#define MIN(a,b) (((a)<(b))?(a):(b))

struct fault_point {
    int rank;
    int step;
    int location;
} faulty = {.rank = -1};

intptr_t fault_locations[] = {
    [0] = (intptr_t) kmr_add_kv_done,
    [1] = (intptr_t) kmr_ckpt_remove_ckpt_all,
    [2] = (intptr_t) kmr_ckpt_remove_nprocs,
    [3] = (intptr_t) kmr_ckpt_restore_ckpt,
    [4] = (intptr_t) kmr_ckpt_add_ckptdata,
    [5] = (intptr_t) kmr_ckpt_add_ckptlog,
    [6] = (intptr_t) kmr_ckpt_progress_skip,
    [7] = (intptr_t) kmr_ckpt_create_ckpt,
    [8] = (intptr_t) kmr_ckpt_set_doneflag,
    [9] = (intptr_t) kmr_ckpt_finish_ckpt,
    [10] = (intptr_t) kmr_ckpt_save_ckpt,
    [11] = (intptr_t) kmr_ckpt_remove_ckpt,
    [12] = (intptr_t) kmr_ckpt_data_exist,
    [13] = (intptr_t) kmr_ckpt_forward_ckpt,
    [14] = (intptr_t) kmr_ckpt_collect_kv,
    [15] = (intptr_t) kmr_ckpt_set_dontrestoreflag,
    [16] = (intptr_t) kmr_ckpt_assign_io_counter,
    [17] = (intptr_t) kmr_ckpt_init_ctx,
    [18] = (intptr_t) kmr_map9,
    [19] = (intptr_t) kmr_reduce9
};

#define ISALPHA(X) (('a' <= X && X <= 'z') || ('A' <= X && X <= 'Z'))

static int
read_words_from_a_file(const struct kmr_kv_box kv0,
		       const KMR_KVS *kvi, KMR_KVS *kvo, void *p, const long i)
{
    char b[25];
    assert(kvi == 0 && kv0.klen == 0 && kv0.vlen == 0 && kvo != 0);
    FILE *f = fopen("LICENSE1", "r");
    if (f == 0) {
	perror("fopen(LICENSE1)");
	MPI_Abort(MPI_COMM_WORLD, 1);
	return 0;
    }
    int j = 0;
    for (;;) {
	assert((size_t)j <= (sizeof(b) - 1));
	int cc = getc(f);
	if ((cc == EOF || !ISALPHA(cc) || (j == (sizeof(b) - 1))) && j != 0) {
	    b[j] = 0;
	    struct kmr_kv_box kv = {
		.klen = (j + 1), .k.p = b,
		.vlen = sizeof(long), .v.i = 1};
	    kmr_add_kv(kvo, kv);
	    j = 0;
	}
	if (cc == EOF) {
	    break;
	}
	if (ISALPHA(cc)) {
	    b[j] = (char)cc;
	    j++;
	}
    }
    fclose(f);
    return MPI_SUCCESS;
}

static int
print_top_five(const struct kmr_kv_box kv0,
		  const KMR_KVS *kvi, KMR_KVS *kvo, void *p, const long i)
{
    int rank = kvi->c.mr->rank;
    if (rank == 0 && i < 5) {
	printf("#%s=%d\n", kv0.v.p, (int)(0 - kv0.k.i));
	fflush(0);
    }
    return MPI_SUCCESS;
}

static int
sum_counts_for_a_word(const struct kmr_kv_box kv[], const long n,
		      const KMR_KVS *kvs, KMR_KVS *kvo, void *p)
{
    long c = 0;
    for (long i = 0; i < n; i++) {
	c -= kv[i].v.i;
    }
    struct kmr_kv_box nkv = {
	.klen = kv[0].klen,
	.k.p = kv[0].k.p,
	.vlen = sizeof(long),
	.v.i = c};
    kmr_add_kv(kvo, nkv);
    return MPI_SUCCESS;
}

static void
fault_inj(void)
{
    int cc;

    long pg = sysconf(_SC_PAGESIZE);
    if (pg == -1) {
	perror("sysconf(_SC_PAGESIZE)");
	MPI_Abort(MPI_COMM_WORLD, 1);
	return;
    }
    assert(pg == 4096 || pg == 8192 || pg == (2 * 1024 * 1024));

    int n = (int)(sizeof(fault_locations) / sizeof(*fault_locations));
    intptr_t pmax = (intptr_t)fault_locations[0];
    intptr_t pmin = (intptr_t)fault_locations[0];
    for (int i = 0; i < n; i++) {
	pmax = MAX(pmax, (intptr_t)fault_locations[i]);
	pmin = MIN(pmin, (intptr_t)fault_locations[i]);
    }
    pmin = (pmin & ~(pg - 1));
    pmax = ((pmax + pg - 1) & ~(pg - 1));

    cc = mprotect((void *)pmin, (size_t)(pmax - pmin),
		  (PROT_READ|PROT_WRITE|PROT_EXEC));
    if (cc != 0) {perror("mprotect()"); assert(cc == 0);}

    intptr_t f0 = fault_locations[faulty.location];

#if (defined(__sparc_v9__) && defined(__arch64__))
    unsigned int *p = (unsigned int *)f0;
    *p = 0;
#elif (defined(__x86_64__))
    char *p = (char *)f0;
    *p = (char)0xcc;
#else
#define TRAPPING_ONLY_ON_AMD64_AND_SPARC 0
    assert(TRAPPING_ONLY_ON_AMD64_AND_SPARC);
    MPI_Abort(MPI_COMM_WORLD, 1);
    return;
#endif
}

static inline void
timerdiv(struct timeval *tv0, struct timeval *tv1, int n)
{
    struct timeval tv2 = {.tv_sec = (tv0->tv_sec / n),
			  .tv_usec = (tv0->tv_usec / n)};
    struct timeval tv3 = {.tv_sec = 0,
			  .tv_usec = ((tv0->tv_sec % n) * 1000000)};
    timeradd(&tv2, &tv3, tv1);
}

static struct timeval tvzero = {.tv_sec = 0, .tv_usec = 0};
static struct timeval tv0;

static inline struct timeval
next_timer(int divider)
{
    int cc;
    if (divider == 0) {
	cc = gettimeofday(&tv0, 0);
	assert(cc == 0);
	return tvzero;
    } else {
	assert(tv0.tv_sec != 0);
	struct timeval tv1;
	cc = gettimeofday(&tv1, 0);
	assert(cc == 0);
	struct timeval tv2;
	timersub(&tv1, &tv0, &tv2);
	struct timeval tv3;
	timerdiv(&tv2, &tv3, divider);
	tv0 = tvzero;
	return tv3;
    }
}

static void
set_fault_inj(struct timeval tv)
{
    int cc;
    struct itimerval iv = {.it_interval = tvzero, .it_value = tv};
    cc = setitimer(ITIMER_REAL, &iv, 0);
    if (cc == -1) {
	perror("setitimer(ITIMER_REAL, &iv, 0)");
	MPI_Abort(MPI_COMM_WORLD, 1);
	return;
    }
    assert(cc == 0);
}

static KMR_KVS *
copy_kvs(KMR_KVS *kvi)
{
    int cc;
    KMR *mr = kvi->c.mr;
    KMR_KVS *kvo = kmr_create_kvs(mr, kvi->c.key_data, kvi->c.value_data);
    struct kmr_option opt = {.inspect = 1};
    cc = kmr_map(kvi, kvo, 0, opt, kmr_add_identity_fn);
    assert(cc == MPI_SUCCESS);
    return kvo;
}

int
main(int argc, char **argv)
{
    int cc;

    int nprocs, rank;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0) {
	if (argc != 4) {
	    fprintf(stderr, "USAGE: mpirun a.out"
		    " rank step location\n"
		    "step is a program step, location is a faulting routine.\n"
		    "rank=-1 for run though.\n");
	    fflush(0);
	    MPI_Abort(MPI_COMM_WORLD, 1);
	    return 1;
	}
	faulty.rank = atoi(argv[1]);
	faulty.step = atoi(argv[2]);
	faulty.location = atoi(argv[3]);
    }

    MPI_Bcast(&faulty, sizeof(struct fault_point), MPI_BYTE, 0,
	      MPI_COMM_WORLD);

    int n = (int)(sizeof(fault_locations) / sizeof(*fault_locations));
    assert(-1 <= faulty.rank && faulty.rank < nprocs);
    assert(0 <= faulty.step && faulty.step <= 4);
    assert(0 <= faulty.location && faulty.location < n);

    kmr_init();
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);

    {
	struct sigaction sa;
	memset(&sa, 0, sizeof(sa));
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = (SA_RESTART|SA_ONSTACK);
	sa.sa_handler = fault_inj;
	cc = sigaction(SIGALRM, &sa, 0);
	if (cc == -1) {
	    perror("sigaction(SIGALRM, &sa, 0)");
	    MPI_Abort(MPI_COMM_WORLD, 1);
	    return 0;
	}
    }

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {printf("Inflating LICENSE file...\n");}

    if (rank == 0) {
	cc = system("rm -f LICENSE1 LICENSE2");
	if (cc == -1) {
	    perror("system(rm -f LICENSE1 LICENSE2)");
	    MPI_Abort(MPI_COMM_WORLD, 1);
	    return 0;
	}
	cc = system("cp LICENSE LICENSE1");
	if (cc == -1) {
	    perror("system(cp LICENSE LICENSE1)");
	    MPI_Abort(MPI_COMM_WORLD, 1);
	    return 0;
	}
	for (int i = 0; i < 3; i++) {
	    cc = system("cat >LICENSE2"
			" LICENSE1 LICENSE1 LICENSE1 LICENSE1 LICENSE1"
			" LICENSE1 LICENSE1 LICENSE1 LICENSE1 LICENSE1");
	    if (cc == -1) {
		perror("system(cat >LICENSE1 LICENSE...10)");
		MPI_Abort(MPI_COMM_WORLD, 1);
		return 0;
	    }
	    cc = system("mv LICENSE2 LICENSE1");
	    if (cc == -1) {
		perror("system(mv LICENSE2 LICENSE1)");
		MPI_Abort(MPI_COMM_WORLD, 1);
		return 0;
	    }
	}
    }

    if (rank == 0) {printf("Ranking words...\n");}

    /* MAP STEP. */

    KMR_KVS *kvs0 = 0;

    {
	const int STEP = 0;

	if (rank == 0) {printf("map#0...\n");}

	KMR_KVS *kvs0a = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
	kmr_map_once(kvs0a, 0, kmr_noopt, 0, read_words_from_a_file);
	kmr_free_kvs(kvs0a);

	KMR_KVS *kvs0b = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
	next_timer(0);
	kmr_map_once(kvs0b, 0, kmr_noopt, 0, read_words_from_a_file);
	struct timeval tv = next_timer(2);
	kmr_free_kvs(kvs0b);

	KMR_KVS *kvs0c = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
	if (STEP == faulty.step && rank == faulty.rank) {
	    set_fault_inj(tv);
	}
	kmr_map_once(kvs0c, 0, kmr_noopt, 0, read_words_from_a_file);
	kvs0 = kvs0c;
    }

    /* SHUFFLE STEP. */

    KMR_KVS *kvs1 = 0;

    {
	const int STEP = 1;

	if (rank == 0) {printf("shuffle#1...\n");}

	KMR_KVS *kvs0a = copy_kvs(kvs0);
	KMR_KVS *kvs1a = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
	kmr_shuffle(kvs0a, kvs1a, kmr_noopt);
	kmr_free_kvs(kvs1a);

	KMR_KVS *kvs0b = copy_kvs(kvs0);
	KMR_KVS *kvs1b = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
	next_timer(0);
	kmr_shuffle(kvs0b, kvs1b, kmr_noopt);
	struct timeval tv = next_timer(2);
	kmr_free_kvs(kvs1b);

	KMR_KVS *kvs0c = copy_kvs(kvs0);
	KMR_KVS *kvs1c = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
	if (STEP == faulty.step && rank == faulty.rank) {
	    set_fault_inj(tv);
	}
	kmr_shuffle(kvs0c, kvs1c, kmr_noopt);
	kmr_free_kvs(kvs0);
	kvs1 = kvs1c;
    }

    /* REDUCE STEP. */

    KMR_KVS *kvs2 = 0;

    {
	const int STEP = 2;

	if (rank == 0) {printf("reduce#2...\n");}

	KMR_KVS *kvs1a = copy_kvs(kvs1);
	KMR_KVS *kvs2a = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
	kmr_reduce(kvs1a, kvs2a, 0, kmr_noopt, sum_counts_for_a_word);
	kmr_free_kvs(kvs2a);

	KMR_KVS *kvs1b = copy_kvs(kvs1);
	KMR_KVS *kvs2b = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
	next_timer(0);
	kmr_reduce(kvs1b, kvs2b, 0, kmr_noopt, sum_counts_for_a_word);
	struct timeval tv = next_timer(2);
	kmr_free_kvs(kvs2b);

	KMR_KVS *kvs1c = copy_kvs(kvs1);
	KMR_KVS *kvs2c = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
	if (STEP == faulty.step && rank == faulty.rank) {
	    set_fault_inj(tv);
	}
	kmr_reduce(kvs1c, kvs2c, 0, kmr_noopt, sum_counts_for_a_word);
	kmr_free_kvs(kvs1);
	kvs2 = kvs2c;
    }

    /* MAP#2 STEP. */

    KMR_KVS *kvs3 = 0;

    {
	const int STEP = 3;

	if (rank == 0) {printf("map#3...\n");}

	KMR_KVS *kvs2a = copy_kvs(kvs2);
	KMR_KVS *kvs3a = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	kmr_reverse(kvs2a, kvs3a, kmr_noopt);
	kmr_free_kvs(kvs3a);

	KMR_KVS *kvs2b = copy_kvs(kvs2);
	KMR_KVS *kvs3b = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	next_timer(0);
	kmr_reverse(kvs2b, kvs3b, kmr_noopt);
	struct timeval tv = next_timer(2);
	kmr_free_kvs(kvs3b);

	KMR_KVS *kvs2c = copy_kvs(kvs2);
	KMR_KVS *kvs3c = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	if (STEP == faulty.step && rank == faulty.rank) {
	    set_fault_inj(tv);
	}
	kmr_reverse(kvs2c, kvs3c, kmr_noopt);
	kmr_free_kvs(kvs2);
	kvs3 = kvs3c;
    }

    /* SORT STEP. */

    KMR_KVS *kvs4 = 0;

    {
	const int STEP = 4;

	if (rank == 0) {printf("sort#4...\n");}

	KMR_KVS *kvs3a = copy_kvs(kvs3);
	KMR_KVS *kvs4a = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	kmr_sort(kvs3a, kvs4a, kmr_noopt);
	kmr_free_kvs(kvs4a);

	KMR_KVS *kvs3b = copy_kvs(kvs3);
	KMR_KVS *kvs4b = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	next_timer(0);
	kmr_sort(kvs3b, kvs4b, kmr_noopt);
	struct timeval tv = next_timer(2);
	kmr_free_kvs(kvs4b);

	KMR_KVS *kvs3c = copy_kvs(kvs3);
	KMR_KVS *kvs4c = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	if (STEP == faulty.step && rank == faulty.rank) {
	    set_fault_inj(tv);
	}
	kmr_sort(kvs3c, kvs4c, kmr_noopt);
	kmr_free_kvs(kvs3);
	kvs4 = kvs4c;
    }

    kmr_map(kvs4, 0, 0, kmr_noopt, print_top_five);

    kmr_free_context(mr);
    kmr_fin();
    MPI_Finalize();
    return 0;
}
