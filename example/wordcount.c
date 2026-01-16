/* Word Count (2014-02-04) */

/* It ranks the words by their occurrence count in the "LICENSE" file.
   Copy the file in the current directory and run it. */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "kmr.h"

#define ISALPHA(X) (('a' <= X && X <= 'z') || ('A' <= X && X <= 'Z'))

static int
read_words_from_a_file(const struct kmr_kv_box kv0,
		       const KMR_KVS *kvi, KMR_KVS *kvo, void *p, const long i)
{
    char b[25];
    assert(kvi == 0 && kv0.klen == 0 && kv0.vlen == 0 && kvo != 0);
    FILE *f = fopen("LICENSE", "r");
    if (f == 0) {
	perror("Cannot open a file \"LICENSE\"; fopen(LICENSE)");
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

int
main(int argc, char **argv)
{
    int nprocs, rank, thlv;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &thlv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    kmr_init();
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {printf("Ranking words...\n");}

    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    kmr_map_once(kvs0, 0, kmr_noopt, 0, read_words_from_a_file);

    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    kmr_shuffle(kvs0, kvs1, kmr_noopt);

    KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    kmr_reduce(kvs1, kvs2, 0, kmr_noopt, sum_counts_for_a_word);

    KMR_KVS *kvs3 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    kmr_reverse(kvs2, kvs3, kmr_noopt);

    KMR_KVS *kvs4 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    kmr_sort(kvs3, kvs4, kmr_noopt);

    kmr_map(kvs4, 0, 0, kmr_noopt, print_top_five);

    kmr_free_context(mr);
    kmr_fin();
    MPI_Finalize();
    return 0;
}
