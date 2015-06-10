/* testmapmp.c (2015-06-05) */

/* Check MPI parappel mapping on key-value pairs defined in kmrmapmp.c.
   Run it with "mpirun -np n a.out".
 */

#include <unistd.h>
#include <mpi.h>
#include "kmr.h"

/* Puts many key-value pairs to output KVO. */
static int
addintkeys0(const struct kmr_kv_box kv0,
			const KMR_KVS *kvs0, KMR_KVS *kvo, void *p, const long i_)
{
    assert(kvs0 == 0 && kv0.klen == 0 && kv0.vlen == 0 && kvo != 0);
    int N = *((int *)p);
    int cc;
    for (int i = 0; i < N; i++) {
		struct kmr_kv_box kv = {
			.klen = sizeof(long), .k.i = i,
			.vlen = sizeof(long), .v.i = i };
		cc = kmr_add_kv(kvo, kv);
		assert(cc == MPI_SUCCESS);
    }
    return MPI_SUCCESS;
}

/* Puts a key-value whose value is number of key multiplyed by key */
static int
multiplykey0(const struct kmr_kv_box kv0,
			 const KMR_KVS *kvi, KMR_KVS *kvo, void *p, const long i_)
{
    MPI_Comm comm = kvi->c.mr->comm;
    int rank = kvi->c.mr->rank;
    int nprocs;
    MPI_Comm_size(comm, &nprocs);
    int max_nprocs = *((int *)p);
    assert(nprocs == max_nprocs);
    int cc;

    int val = -1;
    if (rank == 0) {
		val = (int)kv0.k.i;
    }
    cc = MPI_Bcast(&val, 1, MPI_INT, 0, comm);
    assert(cc == MPI_SUCCESS);
    if (rank != 0) {
		assert(val != -1);
		assert(val == (int)kv0.k.i);
	}

    struct kmr_kv_box kv = {
		.klen = sizeof(long), .k.i = val,
		.vlen = sizeof(long), .v.i = val * val };
    cc = kmr_add_kv(kvo, kv);
    assert(cc == MPI_SUCCESS);

    return MPI_SUCCESS;
}

/* Verify the answer. */
static int
verify0(const struct kmr_kv_box kv0,
		const KMR_KVS *kvs0, KMR_KVS *kvo, void *p, const long i_)
{
    if (kv0.k.i * kv0.k.i != kv0.v.i) {
		/* if the answer is not correct, set a key-value */
		struct kmr_kv_box kv = {
			.klen = sizeof(long), .k.i = 0,
			.vlen = sizeof(long), .v.i = 0 };
		int cc = kmr_add_kv(kvo, kv);
		assert(cc == MPI_SUCCESS);
    }
    return MPI_SUCCESS;
}

static void
test_map_multiprocess0(int rank)
{
    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("CHECK KMR_MAP_MULTIPROCESS...\n");}
    fflush(0);
    usleep(50 * 1000);

    int cc;
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    assert(mr != 0);

    /* Put key-value pairs. */
    int N = 10;

    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) {printf("ADD (%d elements)\n", N);}
    fflush(0);
    usleep(50 * 1000);

    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    cc = kmr_map_on_rank_zero(kvs0, &N, kmr_noopt, addintkeys0);
    assert(cc == MPI_SUCCESS);

    long cnt0;
    cc = kmr_get_element_count(kvs0, &cnt0);
    assert(cc == MPI_SUCCESS);
    assert(cnt0 == N);

    /* Multipy key using kmr_map_multiprocess  */
    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) {printf("call KMR_MAP_MULTIPROCESS\n");}
    fflush(0);
    usleep(50 * 1000);

    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    int max_nprocs = 2;
    cc = kmr_map_multiprocess(kvs0, kvs1, &max_nprocs, kmr_noopt, max_nprocs,
							  multiplykey0);
    assert(cc == MPI_SUCCESS);

    /* Verify the answer */
    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) {printf("VERIFY THE ANSWER\n");}
    fflush(0);
    usleep(50 * 1000);

    KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    cc = kmr_map(kvs1, kvs2, 0, kmr_noopt, verify0);
    assert(cc == MPI_SUCCESS);

    long cnt2;
    cc = kmr_get_element_count(kvs2, &cnt2);
    assert(cc == MPI_SUCCESS);
    assert(cnt2 == 0);
}

int
main(int argc, char **argv)
{
    int nprocs, rank, thlv;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &thlv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    kmr_init();

    test_map_multiprocess0(rank);

    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("OK\n");}
    fflush(0);

    kmr_fin();
    MPI_Finalize();
    return 0;
}
