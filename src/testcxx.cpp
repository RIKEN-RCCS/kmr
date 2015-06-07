/* testcxx.cpp (2014-02-04) */

/* Check KMR with C++11 closures.  NEEDS "-std=gnu++0x" for GCC. */

#include <mpi.h>
#include <string>
#include <iostream>
#include <functional>
#include "kmr.h"

using namespace std;

int
main(int argc, char *argv[])
{
    int cc;
    int nprocs, rank, thlv;
    /*MPI_Init(&argc, &argv);*/
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &thlv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    kmr_init();

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {printf("simple0...\n");}

    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);

    //auto f = [] (int x, int y) -> int { int z = x + y; return z + x; };
    //std::function<int (int, int)> x = f;

    cc = kmr_map_on_rank_zero(kvs0, 0, kmr_noopt, ([&] (const struct kmr_kv_box kv0,
						       const KMR_KVS *kvs0_, KMR_KVS *kvo,
						       void *p,
						       const long i) -> int
	{
	    assert(kvs0_ == 0 && kv0.klen == 0 && kv0.vlen == 0 && kvo != 0);
	    struct kmr_kv_box kv[] = {
		/* {.klen,.vlen,.k.p,.v.p} */
		{5, 7, {"key0"}, {"value0"}},
		{5, 7, {"key1"}, {"value1"}},
		{5, 7, {"key2"}, {"value2"}},
		{5, 7, {"key3"}, {"value3"}},
		{5, 7, {"key4"}, {"value4"}}
	    };
	    for (int i = 0; i < 5; i++) {
		int cc_ = kmr_add_kv(kvo, kv[i]);
		assert(cc_ == MPI_SUCCESS);
	    }
	    return MPI_SUCCESS;
	}));
    assert(cc == MPI_SUCCESS);
    kmr_dump_kvs(kvs0, 0);
    kmr_free_kvs(kvs0);

    MPI_Barrier(MPI_COMM_WORLD);

    kmr_fin();

    MPI_Finalize();
    return 0;
}
