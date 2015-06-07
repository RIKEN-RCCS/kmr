/* spawn test program for testing checkpoint restart in 'selective' mode.

   Run this program like this.

   $ KMROPTION=kmrrc mpiexec -machinefile hosts_file -n 3 ./a.out

      The contents of 'hosts_file' is like this
        umekoji0.aics27.riken.jp
	umekoji0.aics27.riken.jp
	umekoji0.aics27.riken.jp
	umekoji0.aics27.riken.jp
	umekoji0.aics27.riken.jp
	umekoji0.aics27.riken.jp
	umekoji0.aics27.riken.jp
	umekoji0.aics27.riken.jp
	umekoji0.aics27.riken.jp
	umekoji0.aics27.riken.jp
	umekoji0.aics27.riken.jp
	umekoji0.aics27.riken.jp
	umekoji0.aics27.riken.jp

   Then press Ctrl-C to interrupt execution of the program.
   There will be checkpoint directories.

   Rerun the program like the previous run.  The program will be
   be resumed from the previous state using the checkpoint files.

   If you can see the following 10 key-value pairs on screen,
   the answer is correct.

	k[8]=0;v[8]=0
	k[8]=9;v[8]=9
	k[8]=4;v[8]=4
	k[8]=1;v[8]=1
	k[8]=8;v[8]=8
	k[8]=5;v[8]=5
	k[8]=6;v[8]=6
	k[8]=2;v[8]=2
	k[8]=3;v[8]=3
	k[8]=7;v[8]=7

   These key-value pairs can be printed on different runs based on
   your interrupt timing.  Be careful to watch the screen.
*/

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <mpi.h>
#include "kmr.h"

#define NUM_COMMANDS 10

static int
gen_cmdkvs(const struct kmr_kv_box kv,
	   const KMR_KVS *kvi, KMR_KVS *kvo, void *p, long i_)
{
    char *cmd1 = "maxprocs=1 /bin/sleep  1";
    char *cmd2 = "maxprocs=1 /bin/sleep  5";
    char *cmd3 = "maxprocs=1 /bin/sleep 10";
    int vlen = (int)strlen(cmd2) + 1;
    for (int i = 0; i < NUM_COMMANDS; i++) {
	char *cmd = NULL;
	if (i % 3 == 0) {
	    cmd = cmd1;
	} else if (i % 3 == 1) {
	    cmd = cmd2;
	} else {
	    cmd = cmd3;
	}
	struct kmr_kv_box nkv = { .klen = sizeof(long),
				  .vlen = vlen * (int)sizeof(char),
				  .k.i  = i,
				  .v.p  = (void *)cmd };
	kmr_add_kv(kvo, nkv);
    }
    return MPI_SUCCESS;
}

static int
output_result(const struct kmr_kv_box kv,
	      const KMR_KVS *kvi, KMR_KVS *kvo, void *p, long i_)
{
    struct kmr_kv_box nkv = { .klen = sizeof(long),
			      .vlen = sizeof(long),
			      .k.i  = kv.k.i,
			      .v.i  = kv.k.i };
    kmr_add_kv(kvo, nkv);
    return MPI_SUCCESS;
}

int
main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    kmr_init();
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {
	fprintf(stderr, "Start\n");
    }
    MPI_Barrier(MPI_COMM_WORLD);

    KMR_KVS *kvs_commands = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    int ret = kmr_map_once(kvs_commands, 0, kmr_noopt, 1, gen_cmdkvs);
    if (ret != MPI_SUCCESS) {
	MPI_Abort(MPI_COMM_WORLD, 1);
    }
    kmr_dump_kvs(kvs_commands, 1);

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {
	fprintf(stderr, "MAP_ONCE DONE\n");
    }
    MPI_Barrier(MPI_COMM_WORLD);

    KMR_KVS *kvs_runcmds = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    ret = kmr_shuffle(kvs_commands, kvs_runcmds, kmr_noopt);
    if (ret != MPI_SUCCESS) {
	MPI_Abort(MPI_COMM_WORLD, 1);
    }
    kmr_dump_kvs(kvs_runcmds, 1);

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {
	fprintf(stderr, "SHUFFLE DONE\n");
    }
    MPI_Barrier(MPI_COMM_WORLD);

    KMR_KVS *kvs_results = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    struct kmr_spawn_option sopt_sepsp = { .separator_space = 1,
					   .take_ckpt = 1 };
    ret = kmr_map_serial_processes(kvs_runcmds, kvs_results, 0, MPI_INFO_NULL,
				   sopt_sepsp, output_result);
    kmr_dump_kvs(kvs_results, 1);
    kmr_free_kvs(kvs_results);

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {
	fprintf(stderr, "MAP_SPAWN DONE\n");
    }
    MPI_Barrier(MPI_COMM_WORLD);

    if (rank == 0) {
	fprintf(stderr, "Finish\n");
    }

    kmr_free_context(mr);
    kmr_fin();
    MPI_Finalize();
}
