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
    assert(nprocs <= max_nprocs);
    assert(kvo != 0);
    int comm_comp_res;
    MPI_Comm_compare(comm, kvo->c.mr->comm, &comm_comp_res);
    assert(comm_comp_res == MPI_IDENT);
    assert(rank == kvo->c.mr->rank);
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
verify0(const struct kmr_kv_box *kv, const long n,
        const KMR_KVS *kvi, KMR_KVS *kvo, void *p)
{
    int max_nprocs = *((int *)p);
    _Bool success = 1;
    if (n <= 0 || n > max_nprocs) {
        success = 0;
    }
    for (int i = 0; i < n; i++) {
        if (kv[i].k.i * kv[i].k.i != kv[i].v.i) {
            success = 0;
            break;
        }
    }
    if (!success) {
        /* if the answer is not correct, set a key-value */
        struct kmr_kv_box nkv = {
            .klen = sizeof(long), .k.i = 0,
            .vlen = sizeof(long), .v.i = 0 };
        int cc = kmr_add_kv(kvo, nkv);
        assert(cc == MPI_SUCCESS);
    }
    return MPI_SUCCESS;
}

static void
test_map_multiprocess0(int rank)
{
    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("CHECK KMR_MAP_MULTIPROCESS 0...\n");}
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

    /* Shuffle */
    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) {printf("PERFORM SHUFFLE\n");}
    fflush(0);
    usleep(50 * 1000);

    KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    cc = kmr_shuffle(kvs1, kvs2, kmr_noopt);
    assert(cc == MPI_SUCCESS);

    /* Verify the answer */
    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) {printf("VERIFY THE ANSWER\n");}
    fflush(0);
    usleep(50 * 1000);

    KMR_KVS *kvs3 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    cc = kmr_reduce(kvs2, kvs3, &max_nprocs, kmr_noopt, verify0);
    assert(cc == MPI_SUCCESS);

    long cnt3;
    cc = kmr_get_element_count(kvs3, &cnt3);
    assert(cc == MPI_SUCCESS);
    assert(cnt3 == 0);

    kmr_free_kvs(kvs3);
    kmr_free_context(mr);
}

/* Compares the key sent from root in sub-communicator. */
static int
comparekey1(const struct kmr_kv_box kv0,
            const KMR_KVS *kvi, KMR_KVS *kvo, void *p, const long i_)
{
    MPI_Comm comm = kvi->c.mr->comm;
    int rank = kvi->c.mr->rank;
    int nprocs;
    MPI_Comm_size(comm, &nprocs);
    int max_nprocs = *((int *)p);
    assert(nprocs <= max_nprocs);
    assert(kvo == 0);
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

    return MPI_SUCCESS;
}

/* Same as test_map_multiprocess0, except that the output kvs is 0. */
static void
test_map_multiprocess1(int rank)
{
    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("CHECK KMR_MAP_MULTIPROCESS 1...\n");}
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

    int max_nprocs = 2;
    cc = kmr_map_multiprocess(kvs0, 0, &max_nprocs, kmr_noopt, max_nprocs,
                              comparekey1);
    assert(cc == MPI_SUCCESS);

    kmr_free_context(mr);
}

/* Puts a key-value whose key is 'even' or 'odd' and whose value is
   sum of values whose keys are same ('even' or 'odd') only on rank0
   in kvi->c.mr->comm (split communicator) */
static int
sumvalues0(const struct kmr_kv_box kv0,
           const KMR_KVS *kvi, KMR_KVS *kvo, void *p, const long i_)
{
    MPI_Comm comm = kvi->c.mr->comm;
    int rank = kvi->c.mr->rank;
    int nprocs;
    MPI_Comm_size(comm, &nprocs);
    assert(kvo != 0);
    int comm_comp_res;
    MPI_Comm_compare(comm, kvo->c.mr->comm, &comm_comp_res);
    assert(comm_comp_res == MPI_IDENT);
    assert(rank == kvo->c.mr->rank);
    int nprocs_calc = *((int *)p);
    assert(nprocs_calc == nprocs);
    assert(i_ == 0);
    int cc;

    int val = (int)kv0.v.i;
    int vals[nprocs];
    cc = MPI_Gather(&val, 1, MPI_INT, vals, 1, MPI_INT, 0, comm);
    assert(cc == MPI_SUCCESS);

    if (rank == 0) {
        int sum = 0;
        for (int i = 0; i < nprocs; i++) {
            sum += vals[i];
        }
        struct kmr_kv_box kv = {
            .klen = kv0.klen, .k.p = kv0.k.p,
            .vlen = sizeof(long), .v.i = sum };
        cc = kmr_add_kv(kvo, kv);
        assert(cc == MPI_SUCCESS);
    }

    return MPI_SUCCESS;
}

/* Verify the answer. */
static int
verify_by_key0(const struct kmr_kv_box kv0,
               const KMR_KVS *kvi, KMR_KVS *kvo, void *p, const long i_)
{
    int nprocs = *((int *)p);
    char *key = (char *)kv0.k.p;
    int start_from = 0;
    if (key[0] == 'o') start_from = 1; /* In case of odd number */
    int sum = 0;
    for (int i = start_from; i < nprocs; i += 2) {
        sum += i;
    }

    if (kv0.v.i != sum) {
        /* if the answer is not correct, set a key-value */
        struct kmr_kv_box nkv = {
            .klen = sizeof(long), .k.i = 0,
            .vlen = sizeof(long), .v.i = 0 };
        int cc = kmr_add_kv(kvo, nkv);
        assert(cc == MPI_SUCCESS);
    }
    return MPI_SUCCESS;
}

static void
test_map_multiprocess_by_key0(int rank, int nprocs)
{
    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("CHECK KMR_MAP_MULTIPROCESS_BY_KEY 0...\n");}
    fflush(0);
    usleep(50 * 1000);

    int cc;
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    assert(mr != 0);

    /* Put key-value pairs. */
    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) {printf("ADD (1 element on each rank)\n");}
    fflush(0);
    usleep(50 * 1000);

    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    {
        char *key = (rank % 2 == 0) ? "even" : "odd";
        struct kmr_kv_box kv = {
            .klen = (int)(strlen(key) + 1), .k.p = key,
            .vlen = sizeof(long),           .v.i = rank };
        cc = kmr_add_kv(kvs0, kv);
        assert(cc == MPI_SUCCESS);
    }
    cc = kmr_add_kv_done(kvs0);
    assert(cc == MPI_SUCCESS);

    long cnt0;
    cc = kmr_get_element_count(kvs0, &cnt0);
    assert(cc == MPI_SUCCESS);
    assert(cnt0 == nprocs);

    /* Multipy values using kmr_map_multiprocess_by_key  */
    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) {printf("call KMR_MAP_MULTIPROCESS_BY_KEY\n");}
    fflush(0);
    usleep(50 * 1000);

    int nprocs_sub = nprocs / 2;
    if (nprocs % 2 == 1 && rank % 2 == 0) nprocs_sub += 1;
    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    cc = kmr_map_multiprocess_by_key(kvs0, kvs1, &nprocs_sub, kmr_noopt,
                                     rank, sumvalues0);
    assert(cc == MPI_SUCCESS);

    /* Verify the answer */
    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) {printf("VERIFY THE ANSWER\n");}
    fflush(0);
    usleep(50 * 1000);

    KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    cc = kmr_map(kvs1, kvs2, &nprocs, kmr_noopt, verify_by_key0);
    assert(cc == MPI_SUCCESS);

    long cnt2;
    cc = kmr_get_element_count(kvs2, &cnt2);
    assert(cc == MPI_SUCCESS);
    assert(cnt2 == 0);

    kmr_free_kvs(kvs2);
    kmr_free_context(mr);
}

/* It just call MPI_Gather. The output kvs should be 0. */
static int
gather0(const struct kmr_kv_box kv0,
        const KMR_KVS *kvi, KMR_KVS *kvo, void *p, const long i_)
{
    assert(kvo == 0);
    MPI_Comm comm = kvi->c.mr->comm;
    int nprocs;
    MPI_Comm_size(comm, &nprocs);
    int nprocs_calc = *((int *)p);
    assert(nprocs_calc == nprocs);
    assert(i_ == 0);
    int cc;

    int val = (int)kv0.v.i;
    int vals[nprocs];
    cc = MPI_Gather(&val, 1, MPI_INT, vals, 1, MPI_INT, 0, comm);
    assert(cc == MPI_SUCCESS);

    return MPI_SUCCESS;
}

/* Same as test_map_multiprocess_by_key0, except that the output kvs is 0. */
static void
test_map_multiprocess_by_key1(int rank, int nprocs)
{
    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("CHECK KMR_MAP_MULTIPROCESS_BY_KEY 1...\n");}
    fflush(0);
    usleep(50 * 1000);

    int cc;
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    assert(mr != 0);

    /* Put key-value pairs. */
    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) {printf("ADD (1 element on each rank)\n");}
    fflush(0);
    usleep(50 * 1000);

    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    {
        char *key = (rank % 2 == 0) ? "even" : "odd";
        struct kmr_kv_box kv = {
            .klen = (int)(strlen(key) + 1), .k.p = key,
            .vlen = sizeof(long),           .v.i = rank };
        cc = kmr_add_kv(kvs0, kv);
        assert(cc == MPI_SUCCESS);
    }
    cc = kmr_add_kv_done(kvs0);
    assert(cc == MPI_SUCCESS);

    long cnt0;
    cc = kmr_get_element_count(kvs0, &cnt0);
    assert(cc == MPI_SUCCESS);
    assert(cnt0 == nprocs);

    /* Multipy values using kmr_map_multiprocess_by_key  */
    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) {printf("call KMR_MAP_MULTIPROCESS_BY_KEY\n");}
    fflush(0);
    usleep(50 * 1000);

    int nprocs_sub = nprocs / 2;
    if (nprocs % 2 == 1 && rank % 2 == 0) nprocs_sub += 1;
    cc = kmr_map_multiprocess_by_key(kvs0, 0, &nprocs_sub, kmr_noopt,
                                     rank, gather0);
    assert(cc == MPI_SUCCESS);

    kmr_free_context(mr);
}

/* Same as test_map_multiprocess_by_key0, except that only the
   even-numbered ranks in MPI_COMM_WORLD emit Key-Values.
   (Processes in the odd-numbered sub-communicator are not used) */
static void
test_map_multiprocess_by_key2(int rank, int nprocs)
{
    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("CHECK KMR_MAP_MULTIPROCESS_BY_KEY 2...\n");}
    fflush(0);
    usleep(50 * 1000);

    int cc;
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    assert(mr != 0);

    /* Put key-value pairs. */
    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) {printf("ADD (1 element on each rank)\n");}
    fflush(0);
    usleep(50 * 1000);

    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    if (rank % 2 == 0){
        char *key = "even";
        struct kmr_kv_box kv = {
            .klen = (int)(strlen(key) + 1), .k.p = key,
            .vlen = sizeof(long),           .v.i = rank };
        cc = kmr_add_kv(kvs0, kv);
        assert(cc == MPI_SUCCESS);
    }
    cc = kmr_add_kv_done(kvs0);
    assert(cc == MPI_SUCCESS);

    int nprocs_even = nprocs / 2 + ((nprocs % 2 == 1) ? 1 : 0);
    int nprocs_odd = nprocs / 2;

    long cnt0;
    cc = kmr_get_element_count(kvs0, &cnt0);
    assert(cc == MPI_SUCCESS);
    assert(cnt0 == nprocs_even);

    /* Multipy values using kmr_map_multiprocess_by_key  */
    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) {printf("call KMR_MAP_MULTIPROCESS_BY_KEY\n");}
    fflush(0);
    usleep(50 * 1000);

    int *nprocs_sub = (rank % 2 == 0) ? &nprocs_even : &nprocs_odd;
    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    cc = kmr_map_multiprocess_by_key(kvs0, kvs1, nprocs_sub, kmr_noopt,
                                     rank, sumvalues0);
    assert(cc == MPI_SUCCESS);

    /* Verify the answer */
    MPI_Barrier(mr->comm);
    usleep(50 * 1000);
    if (rank == 0) {printf("VERIFY THE ANSWER\n");}
    fflush(0);
    usleep(50 * 1000);

    KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    cc = kmr_map(kvs1, kvs2, &nprocs, kmr_noopt, verify_by_key0);
    assert(cc == MPI_SUCCESS);

    long cnt2;
    cc = kmr_get_element_count(kvs2, &cnt2);
    assert(cc == MPI_SUCCESS);
    assert(cnt2 == 0);

    kmr_free_kvs(kvs2);
    kmr_free_context(mr);
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
    test_map_multiprocess1(rank);
    test_map_multiprocess_by_key0(rank, nprocs);
    test_map_multiprocess_by_key1(rank, nprocs);
    test_map_multiprocess_by_key2(rank, nprocs);

    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("OK\n");}
    fflush(0);

    kmr_fin();
    MPI_Finalize();
    return 0;
}
