/* test6.c (2014-02-04) */

/* Check CKPT. */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <ctype.h>

#include "kmr.h"
#include "kmrimpl.h"
#include "kmrckpt.h"

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

/* Put "inputfile" data into kvo.
   "inputfile" is plain text file.
   Wordcount read file program. */
#define LINELEN 2048
static int
readfilefn(const struct kmr_kv_box kv0,
	   const KMR_KVS *kvs0, KMR_KVS *kvo, void *p, const long i_)
{
    FILE *fds;
    int cc;
    char line[LINELEN];
    struct kmr_kv_box akv;

    fds = fopen(p, "r");
    if (fds == NULL) {
        perror("fopen");
	return -1;
    }
    assert(fds != NULL);
    while (fgets(line, sizeof(line), fds) != NULL) {
	char *wtop = NULL, *cp = line;
	int len = (int)strlen(line);
	while(cp - line < len) {
	    while(*cp == ' ')
		cp++;			// skip space
	    if (*cp == '\n' || *cp == '\0')
		break;			// end of line or buffer end.
	    wtop = cp;
	    while(*cp != ' ' && *cp != '\n' && *cp != '\0')
		cp++;
	    *cp = '\0';
//	    printf("%s 1\n", wtop);

	    akv.klen = (int)strlen(wtop) + 1;
	    akv.vlen = (int)sizeof(long);
	    akv.k.p = wtop;
	    akv.v.i = 1;
	    cc = kmr_add_kv(kvo, akv);
	    assert(cc == MPI_SUCCESS);
	    cp++;
	}
    }
    fclose(fds);
    return MPI_SUCCESS;
}

/* Change upper-case to lower-case.
   Map sample program. */
static int
lowercasefn(const struct kmr_kv_box kv0,
	       const KMR_KVS *kvs0, KMR_KVS *kvo, void *p, const long i)
{
    int cc;
    char *wp;
    char buf[256];
    struct kmr_kv_box akv;

    assert(kvs0 != 0 && kvo != 0);
    memset(buf, 0, sizeof(buf));
    strncpy(buf, kv0.k.p, (size_t)kv0.klen);
    wp = buf;
    while(*wp) {
	int c;
	c = (int)*wp;
      	if (isalpha(c) && isupper(c)) {
	    *wp = (char)tolower(c);
	}
	wp++;
    }

    akv.klen = kv0.klen;
    akv.vlen = kv0.vlen;
    akv.k.p = buf;
    akv.v.i = kv0.v.i;
    cc = kmr_add_kv(kvo, akv);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/* Wordcount reduce program. */
static int
countvaluesfn(const struct kmr_kv_box kv[], const long n,
		  const KMR_KVS *kvs, KMR_KVS *kvo, void *p)
{
    int cc;
    int i;
    long sum = 0;
    char prevkey[256];

    memset(prevkey, 0, sizeof(prevkey));
    for (i = 0; i < n; i++) {
//        printf("key, vlue = %s, %ld\n", kv[i].k.p, kv[i].v.i);
	if(strlen(prevkey) == 0) {
	    memset(prevkey, sizeof(prevkey), 0);
	    strncpy(prevkey, kv[i].k.p, (size_t)kv[i].klen);
	    sum = kv[i].v.i;
	    continue;
	}
	if (strcmp(prevkey, kv[i].k.p) != 0) {
	    /* key changed */
//            printf("put key, vlue = %s, %ld\n", kv[i].k.p, sum);
	    struct kmr_kv_box akv = {.klen = (int)(strlen(prevkey) + 1),
				     .vlen = (int)sizeof(long),
				     .k.p = prevkey,
				     .v.i = sum};
	    cc = kmr_add_kv(kvo, akv);
	    assert(cc == MPI_SUCCESS);

	    memset(prevkey, 0, sizeof(prevkey));
	    strncpy(prevkey, kv[i].k.p, (size_t)kv[i].klen);
	    sum = kv[i].v.i;
	} else {
	    sum += kv[i].v.i;
	}
    }
    if (strlen(prevkey) > 0) {
//	printf("put key, vlue = %s, %ld\n", kv[n-1].k.p, sum);
	struct kmr_kv_box akv = {.klen = (int)(strlen(prevkey) + 1),
				 .vlen = (int)sizeof(long),
				 .k.p = prevkey,
				 .v.i = sum};
	cc = kmr_add_kv(kvo, akv);
	assert(cc == MPI_SUCCESS);
    }
    return 0;
}

/* Print key value.
   key is string, value is long. */
static int
printkvfn(const struct kmr_kv_box kv0,
	  const KMR_KVS *kvs0, KMR_KVS *kvo, void *p, const long i)
{
    printf("%s %ld\n", kv0.k.p, kv0.v.i);
    return MPI_SUCCESS;
}

/* Test MapReduce sample */
static void
simple0(int nprocs, int rank, char *file)
{
    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("simple0...\n");}
    fflush(0);

    if (file == NULL) {
	fprintf(stderr, "Error. Please specify input file for word count.\n");
	exit(-1);
    }

    int cc;
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    //mr->mapper_park_size = 10;

    if (rank == 0) printf("ADD\n");
    fflush(0);

    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    //cc = kmr_map_on_rank_zero(kvs0, file, kmr_noopt, readfilefn);
    cc = kmr_map_once(kvs0, file, kmr_noopt, 0, readfilefn);
    assert(cc == MPI_SUCCESS);

    if (rank == 0) printf("MAP\n");
    fflush(0);

    /* Map pairs. */
    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    cc = kmr_map(kvs0, kvs1, 0, kmr_noopt, lowercasefn);
    assert(cc == MPI_SUCCESS);

    //    kmr_dump_kvs(kvs1, 0);

    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) printf("SHUFFLE\n");
    fflush(0);

    KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    cc = kmr_shuffle(kvs1, kvs2, kmr_noopt);
    assert(cc == MPI_SUCCESS);

    /* Reduce collected pairs. */
    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) printf("REDUCE\n");
    fflush(0);

    KMR_KVS *kvs3 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    cc = kmr_reduce(kvs2, kvs3, 0, kmr_noopt, countvaluesfn);
    assert(cc == MPI_SUCCESS);

    /* Gather pairs to rank0. */
    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) printf("GATHER\n");
    fflush(0);

    KMR_KVS *kvs4 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    struct kmr_option opt = {.rank_zero = 1};
    cc = kmr_replicate(kvs3, kvs4, opt);
    assert(cc == MPI_SUCCESS);

    cc = kmr_map(kvs4, 0, 0, opt, printkvfn);
    assert(cc == MPI_SUCCESS);

//  kmr_free_kvs(kvs4);
    kmr_free_context(mr);
}

// Test checkpoint special function.
static void
simple1(int nprocs, int rank)
{
    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("simple1...\n");}
    fflush(0);

    int cc;
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);

    /* Put five pairs. */
    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) printf("ADD\n");
    fflush(0);

    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_map_on_rank_zero(kvs0, 0, kmr_noopt, addfivekeysfn);
    assert(cc == MPI_SUCCESS);
    //    kmr_dump_kvs(kvs0, 0);

    /*
    kmr_revival_kvs(kvs0);
    cc = kmr_map_on_rank_zero(kvs0, 0, kmr_noopt, addfivekeysfn);
    kmr_dump_kvs(kvs0, 0);
    */

    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_map_on_rank_zero(kvs1, 0, kmr_noopt, addfivekeysfn);
    assert(cc == MPI_SUCCESS);
    //    kmr_dump_kvs(kvs1, 0);

    kmr_ckpt_collect_kv(kvs0, kvs1);
    printf("KVS0\n");
    kmr_dump_kvs(kvs0, 0);

    kmr_add_kv_done(kvs1);
    printf("KVS1\n");
    kmr_dump_kvs(kvs1, 0);

    kmr_free_kvs(kvs0);
    kmr_free_kvs(kvs1);
    kmr_free_context(mr);
}

/* Test MapReduce sample
   using pre-created kvs. */
static void
simple2(int nprocs, int rank, char *file)
{
    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("simple2...\n");}
    fflush(0);

    if (file == NULL) {
	fprintf(stderr, "Error. Please specify input file for word count.\n");
	exit(-1);
    }

    int cc;
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);

    //mr->mapper_park_size = 10;

    if (rank == 0) printf("ADD\n");
    fflush(0);

    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    KMR_KVS *kvs3 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    KMR_KVS *kvs4 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);

    cc = kmr_map_on_rank_zero(kvs4, file, kmr_noopt, readfilefn);
    assert(cc == MPI_SUCCESS);

    if (rank == 0) printf("MAP\n");
    fflush(0);

    /* Map pairs. */
    cc = kmr_map(kvs4, kvs3, 0, kmr_noopt, lowercasefn);
    assert(cc == MPI_SUCCESS);

    //    kmr_dump_kvs(kvs1, 0);

    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) printf("SHUFFLE\n");
    fflush(0);

    cc = kmr_shuffle(kvs3, kvs2, kmr_noopt);
    assert(cc == MPI_SUCCESS);

    /* Reduce collected pairs. */
    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) printf("REDUCE\n");
    fflush(0);

    cc = kmr_reduce(kvs2, kvs1, 0, kmr_noopt, countvaluesfn);
    assert(cc == MPI_SUCCESS);

    /* Gather pairs to rank0. */
    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) printf("GATHER\n");
    fflush(0);

    struct kmr_option opt = {.rank_zero = 1};
    cc = kmr_replicate(kvs1, kvs0, opt);
    assert(cc == MPI_SUCCESS);

    cc = kmr_map(kvs0, 0, 0, opt, printkvfn);
    assert(cc == MPI_SUCCESS);

//  kmr_free_kvs(kvs4);
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

    simple0(nprocs, rank, argv[1]);
//    simple1(nprocs, rank);
//    simple2(nprocs, rank, argv[1]);

    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) printf("OK\n");
    fflush(0);

    kmr_fin();

    MPI_Finalize();

    return 0;
}
