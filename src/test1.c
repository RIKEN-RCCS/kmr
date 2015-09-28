/* test1.c (2014-02-04) */

/* Check KMR basic operations.  It includes similar tests as test0.c,
   but with many key-value pairs. */

/* Run it with "mpirun -np n a.out". */

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

/* Puts many key-value pairs to output KVO. */

static int
addsomekeys0(const struct kmr_kv_box kv0,
	     const KMR_KVS *kvs0, KMR_KVS *kvo, void *p, const long i_)
{
    assert(kvs0 == 0 && kv0.klen == 0 && kv0.vlen == 0 && kvo != 0);
    int N = *((int *)p);
    char k[80];
    char v[80];
    int cc;
    for (int i = 0; i < N; i++) {
	snprintf(k, 80, "key%d", i);
	snprintf(v, 80, "value%d", i);
	struct kmr_kv_box kv = {
	    .klen = (int)(strlen(k) + 1), .vlen = (int)(strlen(v) + 1),
	    .k.p = k, .v.p = v};
	cc = kmr_add_kv(kvo, kv);
	assert(cc == MPI_SUCCESS);
    }
    return MPI_SUCCESS;
}

static int
replacevalues0(const struct kmr_kv_box kv0,
	       const KMR_KVS *kvs0, KMR_KVS *kvo, void *p, const long i)
{
    char buf[1024];
    assert(kvs0 != 0 && kvo != 0);
    int cc, x;
    char gomi;
    cc = sscanf((&((char *)kv0.k.p)[3]), "%d%c", &x, &gomi);
    assert(cc == 1);
    snprintf(buf, sizeof(buf), "newvalue%d", x);
    int vlen = (int)(strlen(buf) + 1);
    struct kmr_kv_box kv = {.klen = kv0.klen,
			    .vlen = vlen,
			    .k.p = kv0.k.p,
			    .v.p = buf};
    cc = kmr_add_kv(kvo, kv);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/* Aggregates reduction pairs.  It asserts key equality for
   checking. */

static int
aggregatevalues0(const struct kmr_kv_box kv[], const long n,
		 const KMR_KVS *kvs, KMR_KVS *kvo, void *p)
{
    int nprocs, rank;
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    assert(n == nprocs);
    for (int i = 0; i < n; i++) {
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
checkkeyvalues0(const struct kmr_kv_box kv0,
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

/* Tests simplest.  It shrinks the block-size (preset_block_size), to
   test growing buffers. */

static void
simple0(int nprocs, int rank, _Bool pushoff)
{
    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (!pushoff) {
	if (rank == 0) {printf("CHECK SIMPLE OPERATIONS...\n");}
    } else {
	if (rank == 0) {printf("CHECK PUSH-OFF KVS...\n");}
    }
    fflush(0);
    usleep(50 * 1000);

    int cc;

    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    assert(mr != 0);
    mr->pushoff_stat = 1;
    mr->preset_block_size = 750;
    //mr->pushoff_block_size = 1024;

    int N = 1000000;

    /* Put pairs. */

    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) {printf("ADD (%d elements)\n", N);}
    fflush(0);
    usleep(50 * 1000);

    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_map_on_rank_zero(kvs0, &N, kmr_noopt, addsomekeys0);
    assert(cc == MPI_SUCCESS);

    long cnt0;
    cc = kmr_get_element_count(kvs0, &cnt0);
    assert(cc == MPI_SUCCESS);
    assert(cnt0 == N);

    /* Replicate pairs to all ranks. */

    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) {printf("REPLICATE\n");}
    fflush(0);
    usleep(50 * 1000);

    KMR_KVS *kvs1 = kmr_create_kvs(mr, kvs0->c.key_data, kvs0->c.value_data);
    cc = kmr_replicate(kvs0, kvs1, kmr_noopt);
    assert(cc == MPI_SUCCESS);

    long cnt1;
    cc = kmr_get_element_count(kvs1, &cnt1);
    assert(cc == MPI_SUCCESS);
    assert(cnt1 == (N * nprocs));

    /* Pack and Unpack. */

    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) {printf("PACK+UNPACK\n");}
    fflush(0);
    usleep(50 * 1000);

    void *data = 0;
    size_t sz = 0;
    cc = kmr_save_kvs(kvs1, &data, &sz, kmr_noopt);
    assert(cc == MPI_SUCCESS && data != 0 && sz != 0);
    cc = kmr_free_kvs(kvs1);
    assert(cc == MPI_SUCCESS);

    KMR_KVS *kvs1r = kmr_create_kvs(mr, KMR_KV_BAD, KMR_KV_BAD);
    cc = kmr_restore_kvs(kvs1r, data, sz, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    free(data);
    assert((kvs1r->c.key_data == KMR_KV_OPAQUE
	    || kvs1r->c.key_data == KMR_KV_CSTRING)
	   && (kvs1r->c.value_data == KMR_KV_OPAQUE
	       || kvs1r->c.value_data == KMR_KV_CSTRING));

    /* Map pairs. */

    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) {printf("MAP\n");}
    fflush(0);
    usleep(50 * 1000);

    KMR_KVS *kvs2;
    if (!pushoff) {
	kvs2 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    } else {
	kvs2 = kmr_create_pushoff_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE,
				      kmr_noopt,
				      __FILE__, __LINE__, __func__);
    }
    cc = kmr_map(kvs1r, kvs2, 0, kmr_noopt, replacevalues0);
    assert(cc == MPI_SUCCESS);

    long cnt2;
    cc = kmr_get_element_count(kvs2, &cnt2);
    assert(cc == MPI_SUCCESS);
    assert(cnt2 == (N * nprocs));

    /* Collect pairs by theirs keys. */

    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) {printf("SHUFFLE\n");}
    fflush(0);
    usleep(50 * 1000);

    KMR_KVS *kvs3 = kmr_create_kvs(mr, kvs2->c.key_data, kvs2->c.value_data);
    cc = kmr_shuffle(kvs2, kvs3, kmr_noopt);
    assert(cc == MPI_SUCCESS);

#if 0
    if (!pushoff) {
	kvs3 = kmr_create_kvs(mr, kvs2->c.key_data, kvs2->c.value_data);
	cc = kmr_shuffle(kvs2, kvs3, kmr_noopt);
    } else {
	kvs3 = kvs2;
	kvs2 = 0;
	cc = MPI_SUCCESS;
    }
    assert(cc == MPI_SUCCESS);
#endif

    long cnt3;
    cc = kmr_get_element_count(kvs3, &cnt3);
    assert(cc == MPI_SUCCESS);
    assert(cnt3 == (N * nprocs));

    /* Reduce collected pairs. */

    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) {printf("REDUCE\n");}
    fflush(0);
    usleep(50 * 1000);

    KMR_KVS *kvs4 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_reduce(kvs3, kvs4, 0, kmr_noopt, aggregatevalues0);
    assert(cc == MPI_SUCCESS);

    //kmr_dump_kvs(kvs4, 0);

    long cnt4;
    cc = kmr_get_element_count(kvs4, &cnt4);
    assert(cc == MPI_SUCCESS);
    assert(cnt4 == N);

    /* Gather pairs to rank0. */

    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) {printf("GATHER\n");}
    fflush(0);
    usleep(50 * 1000);

    KMR_KVS *kvs5 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    struct kmr_option opt = {.rank_zero = 1};
    cc = kmr_replicate(kvs4, kvs5, opt);
    assert(cc == MPI_SUCCESS);

    long cnt5;
    cc = kmr_get_element_count(kvs5, &cnt5);
    assert(cc == MPI_SUCCESS);
    assert(cnt5 == N);

    /* Check key-value pairs (pairs on rank0 only). */

    cc = kmr_map(kvs5, 0, 0, kmr_noopt, checkkeyvalues0);
    assert(cc == MPI_SUCCESS);

    if (pushoff && mr->pushoff_stat) {
	char *s = "STATISTICS on push-off kvs:\n";
	kmr_print_statistics_on_pushoff(mr, s);
    }

    cc = kmr_free_context(mr);
    assert(cc == MPI_SUCCESS);
}

static int
addkeys1(const struct kmr_kv_box kv0,
	 const KMR_KVS *kvs0, KMR_KVS *kvo, void *p, const long i_)
{
    assert(kvs0 == 0);
    KMR *mr = kvo->c.mr;
    assert(mr->rank == 0);
    long *arg = p;
    int NN = (int)*arg;
    for (int i = 0; i < NN; i++) {
	struct kmr_kv_box kv = {
	    .klen = sizeof(long),
	    .vlen = sizeof(long),
	    .k.i = i,
	    .v.i = i
	};
	kmr_add_kv(kvo, kv);
    }
    return MPI_SUCCESS;
}

static int
checksorted1(const struct kmr_kv_box kv0,
	    const KMR_KVS *kvs0, KMR_KVS *kvo, void *p, const long i)
{
    assert(kv0.k.i == i && kv0.v.i == i);
    return MPI_SUCCESS;
}

/* Tests distribute(). */

static void
simple1(int nprocs, int rank)
{
    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {printf("DISTIBUTE\n");}
    fflush(0);
    usleep(50 * 1000);

    /* Check with 10 keys for each rank. */

    const long MM = 10;
    const long NN = MM * nprocs;

    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);

    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    kmr_map_on_rank_zero(kvs0, (void *)&NN, kmr_noopt, addkeys1);

    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    kmr_distribute(kvs0, kvs1, 1, kmr_noopt);
    //kmr_dump_kvs(kvs1, 0);
    assert(kvs1->c.element_count == MM);

    KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    kmr_sort_small(kvs1, kvs2, kmr_noopt);
    //kmr_dump_kvs(kvs2, 0);

    KMR_KVS *kvs3 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    kmr_distribute(kvs2, kvs3, 0, kmr_noopt);
    //kmr_dump_kvs(kvs3, 0);
    assert(kvs3->c.element_count == MM);

    /* Check key-value pairs (after collecting to rank0). */

    KMR_KVS *kvs4 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    struct kmr_option opt = {.rank_zero = 1};
    kmr_replicate(kvs3, kvs4, opt);

    KMR_KVS *kvs5 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    kmr_sort_locally(kvs4, kvs5, 0, kmr_noopt);

    kmr_map(kvs5, 0, 0, kmr_noopt, checksorted1);

    kmr_free_context(mr);
}

/* Tests on empty KVS. */

static void
simple2(int nprocs, int rank)
{
    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("CHECK OPERATIONS WITH EMPTY KVS...\n");}
    fflush(0);
    usleep(50 * 1000);

    int cc;

    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    assert(mr != 0);

    /* Make empty KVS. */

    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_add_kv_done(kvs0);
    assert(cc == MPI_SUCCESS);

    long cnt0;
    cc = kmr_get_element_count(kvs0, &cnt0);
    assert(cc == MPI_SUCCESS);
    assert(cnt0 == 0);

    /* Replicate. */

    KMR_KVS *kvs1 = kmr_create_kvs(mr, kvs0->c.key_data, kvs0->c.value_data);
    cc = kmr_replicate(kvs0, kvs1, kmr_noopt);
    assert(cc == MPI_SUCCESS);

    /* Map. */

    KMR_KVS *kvs2 = kmr_create_kvs(mr, kvs1->c.key_data, kvs1->c.value_data);
    cc = kmr_map(kvs1, kvs2, 0, kmr_noopt, 0);
    assert(cc == MPI_SUCCESS);

    /* Shuffle. */

    KMR_KVS *kvs3 = kmr_create_kvs(mr, kvs2->c.key_data, kvs2->c.value_data);
    cc = kmr_shuffle(kvs2, kvs3, kmr_noopt);
    assert(cc == MPI_SUCCESS);

    /* Reduce. */

    KMR_KVS *kvs4 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_reduce(kvs3, kvs4, 0, kmr_noopt, 0);
    assert(cc == MPI_SUCCESS);

    /* Sort. */

    KMR_KVS *kvs5 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_sort(kvs4, kvs5, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    KMR_KVS *kvs6 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_sort_locally(kvs5, kvs6, 0, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    cc = kmr_free_kvs(kvs6);
    assert(cc == MPI_SUCCESS);

    /* Concat. */

    KMR_KVS *kvs70 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_add_kv_done(kvs70);
    assert(cc == MPI_SUCCESS);
    KMR_KVS *kvs71 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_add_kv_done(kvs71);
    assert(cc == MPI_SUCCESS);
    KMR_KVS *kvs72 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    KMR_KVS *vec[] = {kvs70, kvs71};
    kmr_concatenate_kvs(vec, 2, kvs72, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    cc = kmr_free_kvs(kvs72);
    assert(cc == MPI_SUCCESS);

    cc = kmr_free_context(mr);
    assert(cc == MPI_SUCCESS);
}

/* Tests on option (property list) loader. */

static void
simple3(int nprocs, int rank)
{
    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("CHECK LOADING OPTIONS (PROPERTY LIST)...\n");}
    fflush(0);
    usleep(50 * 1000);

    static char props[] =
	"# Test file for property loader.\n"
	"key0=value0\n"
	"key1=\n"
	"=value2\n"
	"key3 value3\n"
	"key4\\\n" "    " ":value4\n"
	"ke\\\n" "    " "y5=val\\\n" "    " "ue5\n"
	"ke\\\n" "    " "\\\n" "    " "y6\\\n"
	"=val\\\n" "    " "\\\n" "    " "ue6\n"
	"key\\u0037=\\u0076alue7\n"
	"key#is!anything=value#is!much=more\n";

    static char *pairs[][2] =
	{{"key0", "value0"},
	 {"key1", ""},
	 {"", "value2"},
	 {"key3", "value3"},
	 {"key4", "value4"},
	 {"key5", "value5"},
	 {"key6", "value6"},
	 {"key7", "value7"},
	 {"key#is!anything", "value#is!much=more"}};

    int npairs = (sizeof(pairs) / (sizeof(char *) * 2));

    int cc;

    if (rank == 0) {
	if (1) {
	    system("rm -f properties");
	    FILE *f = fopen("properties", "w");
	    assert(f != 0);
	    size_t cx = fwrite(props, (sizeof(props) - 1), 1, f);
	    assert(cx == 1);
	    cc = fclose(f);
	    assert(cc == 0);
	}

	MPI_Info info;
	MPI_Info_create(&info);

	cc = kmr_load_properties(info, "properties");
	assert(cc == MPI_SUCCESS);

	//kmr_dump_mpi_info("", info);

	/* Decrease count by one, because an empty key is ignore. */

	{
	    int nkeys;
	    cc = MPI_Info_get_nkeys(info, &nkeys);
	    assert(cc == MPI_SUCCESS);
	    /* (SKIP EMPTY KEY AND EMPTY VALUE IN TEST DATA). */
	    assert((npairs - 2) == nkeys);
	    for (int i = 0; i < npairs; i++) {
		if (*pairs[i][0] == 0) {
		    continue;
		}
		if (*pairs[i][1] == 0) {
		    continue;
		}
		char *key = pairs[i][0];
		char value[MPI_MAX_INFO_VAL + 1];
		int vlen;
		int flag;
		cc = MPI_Info_get_valuelen(info, key, &vlen, &flag);
		assert(cc == MPI_SUCCESS && flag != 0);
		assert(vlen <= MPI_MAX_INFO_VAL);
		cc = MPI_Info_get(info, key, MPI_MAX_INFO_VAL, value, &flag);
		assert(cc == MPI_SUCCESS && flag != 0);
		assert(strcmp(pairs[i][1], value) == 0);
	    }
	}

	MPI_Info_free(&info);

	if (1) {
	    system("rm -f properties");
	}
    }
}

/* Tests on sorting for small number of entries. */

static void
simple4(int nprocs, int rank)
{
#define KLEN 10
#define VLEN 90

    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("CHECK SORTER (SMALL DATA)...\n");}
    fflush(0);
    usleep(50 * 1000);

    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    assert(mr != 0);

    int cc;
    char k[KLEN+1];
    char v[VLEN+1];

    struct kmr_option inspect = kmr_noopt;
    inspect.inspect = 1;

    /* Local sort on opaques. */

    {
	int N = 1000 * 1000;

	if (rank == 0) {
	    printf("Checking local sort on opaques N=%d\n", N);
	    printf("Generating random strings...\n");
	    fflush(0);
	}

	KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	for (int i = 0; i < N; i++) {
	    const int len = 100;
	    char bb[128];
	    for (int j = 0; j < (len - 1); j++) {
		long vv = lrand48();
		assert(vv >= 0);
		int c0 = (int)(vv % 26);
		int c1 = ('A' + (c0 - 0));
		bb[j] = (char)c1;
	    }
	    bb[(len - 1)] = 0;
	    struct kmr_kv_box kv = {
		.klen = len,
		.vlen = len,
		.k.p = bb,
		.v.p = bb
	    };
	    cc = kmr_add_kv(kvs0, kv);
	    assert(cc == MPI_SUCCESS);
	}
	cc = kmr_add_kv_done(kvs0);
	assert(cc == MPI_SUCCESS);

	//kmr_dump_kvs(kvs0, 0); fflush(0);

	if (rank == 0) {printf("Sorting (normal)...\n"); fflush(0);}

	KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	cc = kmr_sort_locally(kvs0, kvs1, 0, kmr_noopt);
	assert(cc == MPI_SUCCESS);
	if (rank == 0) {printf("Checking...\n"); fflush(0);}
	kmr_assert_sorted(kvs1, 1, 0, 0);

	if (rank == 0) {printf("Sorting (shuffle)...\n"); fflush(0);}

	KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	cc = kmr_sort_locally(kvs1, kvs2, 1, kmr_noopt);
	assert(cc == MPI_SUCCESS);
	if (rank == 0) {printf("Check...\n"); fflush(0);}
	kmr_assert_sorted(kvs2, 1, 1, 0);

	cc = kmr_free_kvs(kvs2);
	assert(cc == MPI_SUCCESS);
    }

    /* Local sort on integers. */

    {
	int N = 1000 * 1000;

	if (rank == 0) {
	    printf("Checking local sort on integers N=%d\n", N);
	    printf("Generating random numbers...\n");
	    fflush(0);
	}

	KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
	for (int i = 0; i < N; i++) {
	    long vv = ((mrand48() << 32) ^ mrand48());
	    struct kmr_kv_box kv = {
		.klen = sizeof(long),
		.vlen = sizeof(long),
		.k.i = vv,
		.v.i = vv
	    };
	    cc = kmr_add_kv(kvs0, kv);
	    assert(cc == MPI_SUCCESS);
	}
	cc = kmr_add_kv_done(kvs0);
	assert(cc == MPI_SUCCESS);

	if (rank == 0) {printf("Sorting (normal)...\n"); fflush(0);}

	KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
	cc = kmr_sort_locally(kvs0, kvs1, 0, kmr_noopt);
	assert(cc == MPI_SUCCESS);
	//kmr_dump_kvs(kvs1, 0); fflush(0);
	if (rank == 0) {printf("Checking...\n"); fflush(0);}
	kmr_assert_sorted(kvs1, 1, 0, 0);

	if (rank == 0) {printf("Sorting (shuffle)...\n"); fflush(0);}

	KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
	cc = kmr_sort_locally(kvs1, kvs2, 1, kmr_noopt);
	assert(cc == MPI_SUCCESS);
	if (rank == 0) {printf("Checking...\n"); fflush(0);}
	kmr_assert_sorted(kvs2, 1, 1, 0);

	cc = kmr_free_kvs(kvs2);
	assert(cc == MPI_SUCCESS);
    }

    /* Local sort on doubles. */

    {
	int N = 1000 * 1000;

	if (rank == 0) {
	    printf("Checking local sort on doubles N=%d\n", N);
	    printf("Generating random numbers...\n");
	    fflush(0);
	}

	KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_FLOAT8, KMR_KV_FLOAT8);
	for (int i = 0; i < N; i++) {
	    double vv = (drand48() - 0.5) * 10000.0;
	    struct kmr_kv_box kv = {
		.klen = sizeof(double),
		.vlen = sizeof(double),
		.k.d = vv,
		.v.d = vv
	    };
	    cc = kmr_add_kv(kvs0, kv);
	    assert(cc == MPI_SUCCESS);
	}
	cc = kmr_add_kv_done(kvs0);
	assert(cc == MPI_SUCCESS);

	if (rank == 0) {printf("Sorting (normal)...\n"); fflush(0);}

	KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_FLOAT8, KMR_KV_FLOAT8);
	cc = kmr_sort_locally(kvs0, kvs1, 0, kmr_noopt);
	assert(cc == MPI_SUCCESS);
	//kmr_dump_kvs(kvs1, 0); fflush(0);
	if (rank == 0) {printf("Checking...\n"); fflush(0);}
	kmr_assert_sorted(kvs1, 1, 0, 0);

	if (rank == 0) {printf("Sorting (shuffle)...\n"); fflush(0);}

	KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_FLOAT8, KMR_KV_FLOAT8);
	cc = kmr_sort_locally(kvs1, kvs2, 1, kmr_noopt);
	assert(cc == MPI_SUCCESS);
	if (rank == 0) {printf("Checking...\n"); fflush(0);}
	kmr_assert_sorted(kvs2, 1, 1, 0);

	cc = kmr_free_kvs(kvs2);
	assert(cc == MPI_SUCCESS);
    }

    /* Sort short opaque key-values. */

    {
	KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	int N = 1000;
	for (int i = 0; i < N; i++) {
	    memset(k, 0, sizeof(k));
	    memset(v, 0, sizeof(v));
	    snprintf(k, (KLEN+1), "%05d%05d", rank, (N - 1 - i));
	    snprintf(v, (VLEN+1), "value=%d/%d", rank, i);
	    struct kmr_kv_box kv = {
		.klen = KLEN, .vlen = VLEN, .k.p = k, .v.p = v};
	    cc = kmr_add_kv(kvs0, kv);
	    assert(cc == MPI_SUCCESS);
	}
	cc = kmr_add_kv_done(kvs0);
	assert(cc == MPI_SUCCESS);

	KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	cc = kmr_sort_locally(kvs0, kvs1, 0, inspect);
	assert(cc == MPI_SUCCESS);
	//kmr_dump_kvs(kvs1, 0);
	kmr_assert_sorted(kvs1, 1, 0, 0);

	cc = kmr_free_kvs(kvs0);
	assert(cc == MPI_SUCCESS);
	cc = kmr_free_kvs(kvs1);
	assert(cc == MPI_SUCCESS);
    }

    /* (nprocs x nprocs) entries. */

    {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {printf("SORT (int) nprocs x nprocs data...\n");}
	fflush(0);
	usleep(50 * 1000);

	KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	int N = nprocs;
	for (int i = 0; i < N; i++) {
	    snprintf(v, 80, "value=%d/%d", rank, i);
	    struct kmr_kv_box kv = {
		.klen = (int)sizeof(long),
		.vlen = (int)(strlen(v) + 1),
		.k.i = (100 * i),
		.v.p = v};
	    cc = kmr_add_kv(kvs0, kv);
	    assert(cc == MPI_SUCCESS);
	}
	cc = kmr_add_kv_done(kvs0);
	assert(cc == MPI_SUCCESS);

	KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	cc = kmr_sort_small(kvs0, kvs1, inspect);
	assert(cc == MPI_SUCCESS);
	//kmr_dump_kvs(kvs1, 0);
	kmr_assert_sorted(kvs1, 0, 0, 0);

	KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	cc = kmr_sort_large(kvs0, kvs2, inspect);
	assert(cc == MPI_SUCCESS);
	//kmr_dump_kvs(kvs2, 0);
	kmr_assert_sorted(kvs2, 0, 0, 0);

	KMR_KVS *kvs3 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	cc = kmr_sort_by_one(kvs0, kvs3, inspect);
	assert(cc == MPI_SUCCESS);
	kmr_assert_sorted(kvs3, 0, 0, 0);

	cc = kmr_free_kvs(kvs0);
	assert(cc == MPI_SUCCESS);
    }

    /* nprocs entries (on a sole rank). */

    {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {printf("SORT (int) nprocs data...\n");}
	fflush(0);
	usleep(50 * 1000);

	KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	int N = nprocs;
	if (rank == (nprocs - 1)) {
	    for (int i = 0; i < N; i++) {
		snprintf(v, 80, "value=%d/%d", rank, i);
		struct kmr_kv_box kv = {
		    .klen = (int)sizeof(long),
		    .vlen = (int)(strlen(v) + 1),
		    .k.i = (100 * i),
		    .v.p = v};
		cc = kmr_add_kv(kvs0, kv);
		assert(cc == MPI_SUCCESS);
	    }
	}
	cc = kmr_add_kv_done(kvs0);
	assert(cc == MPI_SUCCESS);

	KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	cc = kmr_sort_small(kvs0, kvs1, inspect);
	assert(cc == MPI_SUCCESS);
	//kmr_dump_kvs(kvs1, 0);
	kmr_assert_sorted(kvs1, 0, 0, 0);

	KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	cc = kmr_sort_large(kvs0, kvs2, inspect);
	assert(cc == MPI_SUCCESS);
	//kmr_dump_kvs(kvs2, 0);
	kmr_assert_sorted(kvs2, 0, 0, 0);

	KMR_KVS *kvs3 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	cc = kmr_sort_by_one(kvs0, kvs3, inspect);
	assert(cc == MPI_SUCCESS);
	kmr_assert_sorted(kvs3, 0, 0, 0);

	cc = kmr_free_kvs(kvs0);
	assert(cc == MPI_SUCCESS);
    }

    /* (1/2 x N x nprocs x nprocs) entries. */

    {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {printf("SORT (int) N x nprocs x nprocs data...\n");}
	fflush(0);
	usleep(50 * 1000);

	KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	int N = (100 * nprocs);
	for (int i = 0; i < N; i++) {
	    snprintf(v, 80, "value=%d/%d", rank, i);
	    struct kmr_kv_box kv = {
		.klen = (int)sizeof(long),
		.vlen = (int)(strlen(v) + 1),
		.k.i = (100 * i),
		.v.p = v};
	    cc = kmr_add_kv(kvs0, kv);
	    assert(cc == MPI_SUCCESS);
	}
	cc = kmr_add_kv_done(kvs0);
	assert(cc == MPI_SUCCESS);

	KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	cc = kmr_sort_small(kvs0, kvs1, inspect);
	assert(cc == MPI_SUCCESS);
	//kmr_dump_kvs(kvs1, 0);
	kmr_assert_sorted(kvs1, 0, 0, 0);

	KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	cc = kmr_sort_large(kvs0, kvs2, inspect);
	assert(cc == MPI_SUCCESS);
	//kmr_dump_kvs(kvs2, 0);
	kmr_assert_sorted(kvs2, 0, 0, 0);

	KMR_KVS *kvs3 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	cc = kmr_sort_by_one(kvs0, kvs3, inspect);
	assert(cc == MPI_SUCCESS);
	kmr_assert_sorted(kvs3, 0, 0, 0);

	cc = kmr_free_kvs(kvs0);
	assert(cc == MPI_SUCCESS);
    }

    /* (nprocs x nprocs) entries. */

    {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {printf("SORT (byte array) nprocs x nprocs data...\n");}
	fflush(0);
	usleep(50 * 1000);

	KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	int N = nprocs;
	for (int i = 0; i < N; i++) {
	    memset(k, 0, sizeof(k));
	    memset(v, 0, sizeof(v));
	    snprintf(k, (KLEN+1), "%05d%05d", rank, (N - 1 - i));
	    snprintf(v, (VLEN+1), "value=%d/%d", rank, i);
	    struct kmr_kv_box kv = {
		.klen = KLEN, .vlen = VLEN, .k.p = k, .v.p = v};
	    cc = kmr_add_kv(kvs0, kv);
	    assert(cc == MPI_SUCCESS);
	}
	cc = kmr_add_kv_done(kvs0);
	assert(cc == MPI_SUCCESS);

	KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	cc = kmr_sort_small(kvs0, kvs1, inspect);
	assert(cc == MPI_SUCCESS);
	//kmr_dump_kvs(kvs1, 0);
	kmr_assert_sorted(kvs1, 0, 0, 0);

	KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	cc = kmr_sort_large(kvs0, kvs2, inspect);
	assert(cc == MPI_SUCCESS);
	//kmr_dump_kvs(kvs2, 0);
	kmr_assert_sorted(kvs2, 0, 0, 0);

	KMR_KVS *kvs3 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	cc = kmr_sort_by_one(kvs0, kvs3, inspect);
	assert(cc == MPI_SUCCESS);
	kmr_assert_sorted(kvs3, 0, 0, 0);

	cc = kmr_free_kvs(kvs0);
	assert(cc == MPI_SUCCESS);
    }

    cc = kmr_free_context(mr);
    assert(cc == MPI_SUCCESS);
}

static int
kmr_icmp(const void *a0, const void *a1)
{
    const long *p0 = a0;
    const long *p1 = a1;
    long d = (*p0 - *p1);
    return ((d == 0) ? 0 : ((d < 0) ? -1 : 1));
}

/* Check bsearch utility. */

static void
simple5(int nprocs, int rank)
{
    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("CHECK BSEARCH UTILITY...\n");}
    fflush(0);
    usleep(50 * 1000);

    /* Check with length 20-40. */

#define NN 41
#define A0(I) (10 + 10 * (I))

    long a0[NN];
    for (int i = 0; i < NN; i++) {
	a0[i] = A0(i);
    }

    for (int N = 20; N < NN; N++) {
	long *p0;
	long k;

	k = A0(-1);
	p0 = kmr_bsearch(&k, a0, (size_t)N, sizeof(long), kmr_icmp);
	assert(p0 == &a0[0]);

	for (int i = 0; i < N; i++) {
	    k = A0(i);
	    p0 = kmr_bsearch(&k, a0, (size_t)N, sizeof(long), kmr_icmp);
	    assert(p0 == &a0[i]);
	}

	k = A0(N);
	p0 = kmr_bsearch(&k, a0, (size_t)N, sizeof(long), kmr_icmp);
	assert(p0 == &a0[N]);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);

#undef NN
#undef A0
}

/* Test kmr_map_for_some(). */

static int
addkeys6(const struct kmr_kv_box kv0,
	 const KMR_KVS *kvs0, KMR_KVS *kvo, void *p, const long i_)
{
    long *arg = p;
    int NN = (int)*arg;
    for (int i = 0; i < NN; i++) {
	struct kmr_kv_box kv = {
	    .klen = sizeof(long),
	    .vlen = sizeof(long),
	    .k.i = i,
	    .v.i = i
	};
	kmr_add_kv(kvo, kv);
    }
    return MPI_SUCCESS;
}

static int
addeach6(const struct kmr_kv_box kv,
	 const KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
	 const long index)
{
    long *counter = arg;
    _Pragma("omp critical")
    {
	(*counter)++;
    }
    kmr_add_kv(kvo, kv);
    return MPI_SUCCESS;
}

static int
addnone6(const struct kmr_kv_box kv,
	 const KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
	 const long index)
{
    long *counter = arg;
    _Pragma("omp critical")
    {
	(*counter)++;
    }
    return MPI_SUCCESS;
}

static void
simple6(int nprocs, int rank)
{
    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("CHECK KMR_MAP_FOR_SOME()...\n");}
    fflush(0);
    usleep(50 * 1000);

    struct kmr_option inspect = {.inspect = 1};

    /* Check with 10000 keys. */

    const long NN = 10000;

    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);

    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    kmr_map_once(kvs0, (void *)&NN, kmr_noopt, 0, addkeys6);

    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    kmr_distribute(kvs0, kvs1, 1, kmr_noopt);
    assert(kvs1->c.element_count == NN);

    long c1 = 0;
    kmr_get_element_count(kvs1, &c1);

    long calls2 = 0;
    KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    kmr_map_for_some(kvs1, kvs2, &calls2, inspect, addeach6);
    long c2 = 0;
    kmr_get_element_count(kvs2, &c2);
    kmr_free_kvs(kvs2);

    if (rank == 0) {
	printf("ADD ALL count(kvs2)=%ld for %ld (calls=%ld)\n",
	       c2, c1, calls2);
	fflush(0);
    }
    assert(c2 > 0 && c2 <= c1);

    long calls3 = 0;
    long e1 = kvs1->c.element_count;
    KMR_KVS *kvs3 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    kmr_map_for_some(kvs1, kvs3, &calls3, inspect, addnone6);
    long c3 = 0;
    kmr_get_element_count(kvs3, &c3);
    kmr_free_kvs(kvs3);

    if (rank == 0) {
	printf("ADD NONE count(kvs3)=%ld for %ld (calls=%ld for %ld)\n",
	       c3, c1, calls3, e1);
	fflush(0);
    }
    assert(c3 == 0 && e1 == calls3);

    kmr_free_kvs(kvs1);

    /* Check using KVS after inspecting by kmr_take_one(). */

    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("CHECK KMR_TAKE_ONE()...\n");}
    fflush(0);
    usleep(50 * 1000);

    const long ONE = 1;

    KMR_KVS *kvs4 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    kmr_map_once(kvs4, (void *)&ONE, kmr_noopt, 0, addkeys6);

    struct kmr_kv_box kv;
    kmr_take_one(kvs4, &kv);
    assert(kv.k.i == 0 && kv.v.i == 0);

    long calls4 = 0;
    KMR_KVS *kvs5 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    kmr_map(kvs4, kvs5, &calls4, kmr_noopt, addeach6);

    kmr_free_kvs(kvs5);

    kmr_free_context(mr);
}

extern void kmr_isort(void *a, size_t n, size_t es, int depth);

/* Check isort() utility. */

static void
simple7(int nprocs, int rank)
{
    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("CHECK ISORT UTILITY (local sort)...\n");}
    fflush(0);
    usleep(50 * 1000);

    int setntheads = 1;

#ifdef __K
    char *fastomp = getenv("FLIB_FASTOMP");
    if (!(fastomp != 0 && strcmp(fastomp, "FALSE") == 0)) {
	if (rank == 0) {printf("Set environment variable FLIB_FASTOMP=FALSE"
			       " and run again.  Otherwise,"
			       " omp_set_num_threads() is ignored.\n");}
	fflush(0);
	setntheads = 0;
    }
#endif

    /* Check with length 20. */

    if (1) {
	long a0[20] = {19, 18, 17, 16, 15, 14, 13, 12, 11, 10,
		       9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
	size_t n0 = (sizeof(a0) / sizeof(long));
	for (int i = 0; i < (int)n0; i++) {
	    a0[i] += 1000000000000;
	}

	if (rank == 0) {printf("Sorting data N=%zd\n", n0);}
	fflush(0);

	kmr_isort(a0, n0, sizeof(long), 0);

	for (int i = 0; i < (int)n0; i++) {
	    assert(a0[i] == (i + 1000000000000));
	}

	//if (rank == 0) {printf("OK\n");}
	//fflush(0);
    }

    /* Check with length 3M. */

    if (1) {
	//long n1 = 100000000; /*100M*/
	long n1 = 3000000; /*3M*/
	long *a1 = malloc(sizeof(long) * (size_t)n1);
	if (a1 == 0) {
	    perror("malloc");
	    MPI_Abort(MPI_COMM_WORLD, 1);
	}

	if (rank == 0) {printf("Sorting data N=%zd\n", n1);}
	fflush(0);

	if (rank == 0) {printf("Generating random numbers...\n");}
	fflush(0);

	for (long i = 0; i < n1; i++) {
	    a1[i] = ((((long)rand()) << 31) ^ ((long)rand()));
	}

	if (0) {
	    printf("Problem...\n");
	    for (long i = 0; i < 10; i++) {
		printf("%ld\n", a1[i]);
	    }
	    printf("\n");
	}

	if (rank == 0) {printf("Sorting (threads=1)...\n");}
	fflush(0);

	double t0 = wtime();
	kmr_isort(a1, (size_t)n1, sizeof(long), 0);
	double t1 = wtime();
	if (rank == 0) {printf("time=%f\n", (t1 - t0));}
	fflush(0);

	if (0) {
	    printf("Result...\n");
	    for (long i = 0; i < 10; i++) {
		printf("%ld\n", a1[i]);
	    }
	    printf("\n");
	}

	if (rank == 0) {printf("Checking...\n");}
	long lb = LONG_MIN;
	for (long i = 0; i < n1; i++) {
	    assert(a1[i] >= lb);
	    if (a1[i] > lb) {
		lb = a1[i];
	    }
	}

	//if (rank == 0) {printf("OK\n");}
	//fflush(0);
    }

    if (1) {
#ifdef _OPENMP
	//long n5 = 100000000; /*100M*/
	long n5 = 3000000; /*3M*/
	long *a5 = malloc(sizeof(long) * (size_t)n5);
	if (a5 == 0) {
	    perror("malloc");
	    MPI_Abort(MPI_COMM_WORLD, 1);
	}

	if (rank == 0) {printf("Sorting data N=%zd\n", n5);}

	for (int threads = 1; threads <= 8; threads *= 2) {
	    if (setntheads) {
		omp_set_num_threads(threads);
	    }

	    int threadsused = 0;
#pragma omp parallel
	    {
		threadsused = omp_get_num_threads();
	    }

	    if (rank == 0) {printf("Generating random numbers...\n");}
	    fflush(0);

	    srand(20140204);
	    for (long i = 0; i < n5; i++) {
		a5[i] = ((((long)rand()) << 31) ^ ((long)rand()));
	    }

	    if (rank == 0) {printf("Sorting (threads=%d)...\n", threadsused);}
	    fflush(0);

	    double t0 = wtime();
	    kmr_isort(a5, (size_t)n5, sizeof(long), 5);
	    double t1 = wtime();
	    if (rank == 0) {printf("time=%f\n", (t1 - t0));}
	    fflush(0);

	    if (rank == 0) {printf("Checking...\n");}
	    fflush(0);

	    long lb5 = LONG_MIN;
	    for (long i = 0; i < n5; i++) {
		assert(a5[i] >= lb5);
		if (a5[i] > lb5) {
		    lb5 = a5[i];
		}
	    }

	    //if (rank == 0) {printf("OK\n");}
	    //fflush(0);
	}
#else
	printf("NOT OMP\n");
	fflush(0);
#endif
    }
}

/* Tests kmr_shuffle_leveling_pair_count().  makemanyintegerkeys8()
   generate random pairs.  copynegate8() makes a copy negating the
   value in the KVS for later checking. */

static int
makemanyintegerkeys8(const struct kmr_kv_box kv0,
		     const KMR_KVS *kvs0, KMR_KVS *kvo, void *p, const long i_)
{
    /* (N: Same keys are generated upto N times). */
    int N = 4;
    long MM = *(long *)p;
    KMR *mr = kvo->c.mr;
    int rank = mr->rank;
    int nprocs = mr->nprocs;
    long cnt = (MM * (nprocs - rank) / nprocs);
    for (long i = 0; i < cnt; i++) {
	long j = (i / N);
	struct kmr_kv_box kv = {
	    .klen = sizeof(long),
	    .vlen = sizeof(long),
	    .k.i = (rank + (j * nprocs)),
	    .v.i = (i + 1)
	};
	kmr_add_kv(kvo, kv);
    }
    return MPI_SUCCESS;
}

static int
makemanystringkeys8(const struct kmr_kv_box kv0,
		    const KMR_KVS *kvs0, KMR_KVS *kvo, void *p, const long i_)
{
    /* (N: Same keys are generated upto N times). */
    int N = 4;
    long MM = *(long *)p;
    KMR *mr = kvo->c.mr;
    int rank = mr->rank;
    int nprocs = mr->nprocs;
    long cnt = (MM * (nprocs - rank) / nprocs);
    for (long i = 0; i < cnt; i++) {
	long j = (i / N);
	char k[80];
	snprintf(k, 80, "key%ld", (rank + (j * nprocs)));
	struct kmr_kv_box kv = {
	    .klen = (int)(strlen(k) + 1),
	    .vlen = sizeof(long),
	    .k.p = k,
	    .v.i = (i + 1)
	};
	kmr_add_kv(kvo, kv);
    }
    return MPI_SUCCESS;
}

static int
copynegate8(const struct kmr_kv_box kv0,
	    const KMR_KVS *kvs0, KMR_KVS *kvo, void *p, const long i_)
{
    assert(kv0.v.i > 0);
    struct kmr_kv_box kv = {
	.klen = kv0.klen,
	.vlen = kv0.vlen,
	.k = kv0.k,
	.v.i = (- kv0.v.i),
    };
    kmr_add_kv(kvo, kv);
    return MPI_SUCCESS;
}

static int
comparebycanceling8(const struct kmr_kv_box kv[], const long n,
		    const KMR_KVS *kvs, KMR_KVS *kvo, void *p)
{
    long values[n];
    for (long i = 0; i < n; i++) {
	assert(kv[i].v.i != 0);
	values[i] = kv[i].v.i;
    }
    for (long i = 0; i < n; i++) {
	if (values[i] == 0) {
	    continue;
	} else {
	    assert(i < (n - 1));
	    for (long j = (i + 1); j < n; j++) {
		if (values[i] == (- values[j])) {
		    values[i] = 0;
		    values[j] = 0;
		    break;
		}
		assert(j != (n - 1));
	    }
	}
    }
    for (long i = 0; i < n; i++) {
	assert(values[i] == 0);
    }
    return MPI_SUCCESS;
}

static void
simple8(int nprocs, int rank)
{
    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {printf("PSEUDO-SCAN\n");}
    fflush(0);
    usleep(50 * 1000);

    /* Check with (MM * (nprocs - rank) / nprocs) keys on ranks. */

    const long MM = 100;

    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);

    /* DO ONCE ON INTEGER KEYS AND ONCE ON STRING KEYS. */

    for (int i = 0; i < 2; i++) {
	assert(i == 0 || i == 1);
	kmr_mapfn_t makedatafn = ((i == 0)
				  ? makemanyintegerkeys8
				  : makemanystringkeys8);
	enum kmr_kv_field keyf = ((i == 0)
				  ? KMR_KV_INTEGER
				  : KMR_KV_OPAQUE);

	KMR_KVS *kvs0 = kmr_create_kvs(mr, keyf, KMR_KV_INTEGER);
	kmr_map_once(kvs0, (void *)&MM, kmr_noopt, 0, makedatafn);

	KMR_KVS *kvs1 = kmr_create_kvs(mr, keyf, KMR_KV_INTEGER);
	struct kmr_option inspect = {.inspect = 1};
	kmr_map(kvs0, kvs1, 0, inspect, copynegate8);

	KMR_KVS *kvs2 = kmr_create_kvs(mr, keyf, KMR_KV_INTEGER);
	kmr_shuffle_leveling_pair_count(kvs0, kvs2);

	long counts[nprocs];
	double stat[4];
	kmr_histogram_count_by_ranks(kvs2, counts, stat, 1);

	if (rank == 0) {
	    printf("number of pairs on ranks:\n");
	    for (int r = 0; r < nprocs; r++) {
		printf("%ld", counts[r]);
		if (r == (nprocs - 1)) {
		    printf("\n");
		} else if (r == 10) {
		    printf("\n");
		} else {
		    printf(",");
		}
	    }
	    fflush(0);
	}
	//kmr_dump_kvs(kvs2, 0);

	/* Check the shuffled KVS is a rearrangement of the original KVS. */

	KMR_KVS *kvs3 = kmr_create_kvs(mr, keyf, KMR_KV_INTEGER);
	KMR_KVS *kvsvec[2] = {kvs1, kvs2};
	kmr_concatenate_kvs(kvsvec, 2, kvs3, kmr_noopt);

	KMR_KVS *kvs4 = kmr_create_kvs(mr, keyf, KMR_KV_INTEGER);
	kmr_shuffle(kvs3, kvs4, kmr_noopt);

	kmr_reduce(kvs4, 0, 0, kmr_noopt, comparebycanceling8);
    }

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

    simple0(nprocs, rank, 0);
    simple0(nprocs, rank, 1);
    simple1(nprocs, rank);
    simple2(nprocs, rank);
    simple3(nprocs, rank);
    simple4(nprocs, rank);
    simple5(nprocs, rank);
    simple6(nprocs, rank);
    simple7(nprocs, rank);
    simple8(nprocs, rank);

    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("OK\n");}
    fflush(0);

    kmr_fin();

    MPI_Finalize();

    return 0;
}
