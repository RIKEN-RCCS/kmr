/* Copyright (c) 2007-2009, Stanford University
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*     * Redistributions of source code must retain the above copyright
*       notice, this list of conditions and the following disclaimer.
*     * Redistributions in binary form must reproduce the above copyright
*       notice, this list of conditions and the following disclaimer in the
*       documentation and/or other materials provided with the distribution.
*     * Neither the name of Stanford University nor the names of its
*       contributors may be used to endorse or promote products derived from
*       this software without specific prior written permission.
*
* THIS SOFTWARE IS PROVIDED BY STANFORD UNIVERSITY ``AS IS'' AND ANY
* EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL STANFORD UNIVERSITY BE LIABLE FOR ANY
* DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
* (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
* LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
* ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
* SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

/* This is an example in "test" directory in Phoenix-2.0.0, converted
   for KMR (2013-04-24).  REWRITTEN MUCH. */

#include <mpi.h>
#include <stdio.h>
#include <strings.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctype.h>
#include <time.h>
#include <inttypes.h>
#include <sys/time.h>
#if 0
#include "map_reduce.h"
#include "stddefines.h"
#endif
#include "kmr.h"

/* KMR NOTE: (0) Matrix A is row-split, matrix B is replicated.  (1)
   Remove mmap code.  (2) MM_DATA be made global. */

#define MIN(X,Y) (((X) <= (Y)) ? (X) : (Y))

/* ROW is the start and ROWS is the number of rows of a task.  Matrix
   A and C are only on rank0. */

typedef struct {
    int siz;
    int row_block_length;
    int unit_size;
    int row_counter;
    int *A;
    int *B;
    int *C;
} mm_data_t;

mm_data_t mm_data;

typedef struct {
    int row;
    int rows;
    long id;
    /* (part of rows of matrix A or C) */
} mm_mapdata_t;

#define dprintf(...) fprintf(stdout, __VA_ARGS__)

static inline void
get_time(struct timeval *tv)
{
    gettimeofday(tv, 0);
}

static int
mm_setup(const struct kmr_kv_box kv,
	 const KMR_KVS *kvs0, KMR_KVS *kvo, void *p, long i_)
{
    assert((size_t)kv.klen == sizeof(mm_data_t));
    /*memcpy(&mm_data, (void *)kv.k.p, sizeof(mm_data_t));*/
    mm_data_t *mm = (void *)kv.k.p;
    mm_data.siz = mm->siz;
    mm_data.siz = mm->siz;
    mm_data.row_block_length = mm->row_block_length;
    mm_data.unit_size = mm->unit_size;
    size_t datasz = (sizeof(int) * (size_t)(mm_data.siz * mm_data.siz));
    assert((size_t)kv.vlen == datasz);
    if (kvs0->c.mr->rank != 0) {
	mm_data.B = malloc(sizeof(int) * (size_t)(mm_data.siz * mm_data.siz));
	assert(mm_data.B != 0);
    }
    memcpy(mm_data.B, (void *)kv.v.p, datasz);
    return MPI_SUCCESS;
}

/* Assigns rows to each task.  It runs on rank0. */

static int
matrixmult_splitter(int units, int id, KMR_KVS *kvo)
{
    assert(mm_data.siz >= 0
	   && mm_data.unit_size >= 0
	   && mm_data.row_block_length >= 0
	   && mm_data.A != 0 && mm_data.B != 0 && mm_data.C != 0);

    assert(mm_data.row_counter <= mm_data.siz);
    if (mm_data.row_counter >= mm_data.siz) {
	return 0;
    }
    int n = MIN(units, (mm_data.siz - mm_data.row_counter));
    mm_mapdata_t md;
    size_t datasz = (sizeof(int) * (size_t)(mm_data.siz * n));
    int *partA = malloc(datasz);
    assert(partA != 0);
    md.row = mm_data.row_counter;
    md.rows = n;
    md.id = id;
    mm_data.row_counter += n;
    memcpy(partA, (mm_data.A + (mm_data.siz * md.row)), datasz);
    struct kmr_kv_box nkv = {.klen = sizeof(mm_mapdata_t),
			     .vlen = (int)datasz,
			     .k.p = (const char *)&md,
			     .v.p = (const char *)partA};
    kmr_add_kv(kvo, nkv);
    return 1;
}

static int
phoenix_split(const struct kmr_kv_box kv0,
	      const KMR_KVS *kvs0, KMR_KVS *kvo, void *p, long i_)
{
    assert(kvs0 == 0 && kv0.klen == 0 && kv0.vlen == 0 && kvo != 0);
    mm_data.row_counter = 0;
    long id = 0;
    while (matrixmult_splitter(mm_data.unit_size, (int)id, kvo)) {
	id++;
    }
    return MPI_SUCCESS;
}

/* Multiplies the allocated regions of matrix to compute partial sums. */

static int
mm_map(const struct kmr_kv_box kv,
       const KMR_KVS *kvs, KMR_KVS *kvo, void *p, long i_)
{
    mm_mapdata_t *md0 = (mm_mapdata_t *)kv.k.p;
    int *partA = (void *)kv.v.p;
    int n = md0->rows;
    size_t datasz = (sizeof(int) * (size_t)(mm_data.siz * n));
    int *partC = malloc(datasz);
    assert(partC != 0);
    for (int r = 0; r < md0->rows; r++) {
	int *a = (partA + (r * mm_data.siz));
	for (int i = 0; i < mm_data.siz ; i++) {
	    int *b = (mm_data.B + i);
	    int value = 0;
	    for (int j = 0; j < mm_data.siz; j++) {
		value += (a[j] * b[0]);
		b += mm_data.siz;
	    }
	    partC[r * mm_data.siz + i] = value;
	}
    }
    mm_mapdata_t md1;
    md1.row = md0->row;
    md1.rows = n;
    md1.id = md0->id;
    struct kmr_kv_box nkv = {.klen = sizeof(mm_mapdata_t),
			     .vlen = (int)datasz,
			     .k.p = (const char *)&md1,
			     .v.p = (const char *)partC};
    kmr_add_kv(kvo, nkv);
    return MPI_SUCCESS;
}

static int
mm_output(const struct kmr_kv_box kv,
	  const KMR_KVS *kvs, KMR_KVS *kvo, void *p, long i_)
{
    mm_mapdata_t *md0 = (mm_mapdata_t *)kv.k.p;
    int *partC = (void *)kv.v.p;
    int n = md0->rows;
    size_t datasz = (sizeof(int) * (size_t)(mm_data.siz * n));
    memcpy((mm_data.C + (mm_data.siz * md0->row)), partC, datasz);
    return MPI_SUCCESS;
}

int main(int argc, char *argv[])
{
    off_t data_size;
    int cc;

    struct timeval begin, end;

    srand(20120930);

    int nprocs, rank, thlv;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &thlv);
    /*MPI_Init(&argc, &argv);*/
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);

    get_time(&begin);

    if (rank == 0) {
	if (argc <= 1 || argv[1] == 0) {
	    dprintf("USAGE: %s matrix-size [row-block-size] [no-create-files]\n",
		    argv[0]);
	    MPI_Abort(MPI_COMM_WORLD, 1);
	}

	char *fnameA = "matrix_file_A.txt";
	char *fnameB = "matrix_file_B.txt";
	int matrix_len = atoi(argv[1]);
	assert(matrix_len > 0);
	size_t file_size = (sizeof(int) * (size_t)(matrix_len * matrix_len));
	fprintf(stderr, "***** file size is %ld\n", file_size);

	int row_block_length;
	if (argv[2] == 0) {
	    row_block_length = 1;
	} else {
	    row_block_length = atoi(argv[2]);
	    assert(row_block_length > 0);
	}
	int create_files;
	if (argv[3] != 0) {
	    create_files = 1;
	} else {
	    create_files = 0;
	}
	printf("MatrixMult: Side of the matrix is %d\n", matrix_len);
	printf("MatrixMult: Row Block Len is %d\n", row_block_length);
	printf("MatrixMult: Running...\n");

	/* If the matrix files do not exist, create and fill them. */

	if (create_files) {
	    dprintf("Creating files\n");
	    int value = 0;
	    FILE *fileA = fopen(fnameA, "w");
	    assert(fileA != 0);
	    FILE *fileB = fopen(fnameB, "w");
	    assert(fileB != 0);
	    for (int i = 0; i < matrix_len; i++) {
		for (int j = 0; j < matrix_len; j++) {
		    value = (rand() % 11);
		    size_t cx = fwrite(&value, sizeof(int), 1, fileA);
		    assert(cx == 1);
		}
	    }
	    for (int i = 0; i < matrix_len; i++) {
		for (int j = 0; j < matrix_len; j++) {
		    value = (rand() % 11);
		    size_t cx = fwrite(&value, sizeof(int), 1, fileB);
		    assert(cx == 1);
		}
	    }
	    fclose(fileA);
	    fclose(fileB);
	}

	/* Read in the files. */

	struct stat finfoA, finfoB;

	int fdA = open(fnameA, O_RDONLY);
	assert(fdA >= 0);
	cc = fstat(fdA, &finfoA);
	assert(cc == 0);
	int *fdataA = malloc(file_size);
	assert(fdataA != 0);
	ssize_t cx0 = read(fdA, fdataA, file_size);
	assert(cx0 == (ssize_t)file_size);

	int fdB = open(fnameB, O_RDONLY);
	assert(fdB >= 0);
	cc = fstat(fdB, &finfoB);
	assert(cc == 0);
	int *fdataB = malloc(file_size);
	assert(fdataB != 0);
	ssize_t cx1 = read(fdB, fdataB, file_size);
	assert(cx1 == (ssize_t)file_size);

	mm_data.siz = matrix_len;
	mm_data.row_block_length = row_block_length;
	mm_data.unit_size = (int)(sizeof(int)
				  * (size_t)(row_block_length * matrix_len));
	mm_data.A = (int *)fdataA;
	mm_data.B = (int *)fdataB;
	mm_data.C = malloc(sizeof(int) * (size_t)(mm_data.siz * mm_data.siz));
	assert(mm_data.C != 0);

	close(fdA);
	close(fdB);

	data_size = (off_t)file_size;
    }

    /*map_reduce_init());*/

#if 0
    // Setup map reduce args
    map_reduce_args_t map_reduce_args;
    memset(&map_reduce_args, 0, sizeof(map_reduce_args_t));
    map_reduce_args.task_data = &mm_data;
    map_reduce_args.map = mm_map;
    map_reduce_args.reduce = NULL;
    map_reduce_args.splitter = matrixmult_splitter;
    map_reduce_args.locator = matrixmult_locator;
    map_reduce_args.key_cmp = myintcmp;
    map_reduce_args.unit_size = mm_data.unit_size;
    map_reduce_args.partition = NULL; // use default
    map_reduce_args.result = &mm_vals;
    map_reduce_args.data_size = file_size;
    map_reduce_args.L1_cache_size = atoi(GETENV("MR_L1CACHESIZE"));//1024 * 8;
    map_reduce_args.num_map_threads = atoi(GETENV("MR_NUMTHREADS"));//8;
    map_reduce_args.num_reduce_threads = atoi(GETENV("MR_NUMTHREADS"));//16;
    map_reduce_args.num_merge_threads = atoi(GETENV("MR_NUMTHREADS"));//8;
    map_reduce_args.num_procs = atoi(GETENV("MR_NUMPROCS"));//16;
    map_reduce_args.key_match_factor = (float)atof(GETENV("MR_KEYMATCHFACTOR"));//2;
#endif

    /* Enlarge element size of key-value pairs. */

    KMR_KVS *siz0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    if (rank == 0) {
	struct kmr_kv_box nkv = {.klen = sizeof(long),
				 .vlen = sizeof(long),
				 .k.i = 0,
				 .v.i = mm_data.siz};
	kmr_add_kv(siz0, nkv);
    }
    kmr_add_kv_done(siz0);
    KMR_KVS *siz1 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    kmr_replicate(siz0, siz1, kmr_noopt);
    struct kmr_kv_box kv0 = {.klen = sizeof(long), .k.i = 0};
    struct kmr_kv_box kv1;
    cc = kmr_find_key(siz1, kv0, &kv1);
    assert(cc == MPI_SUCCESS);
    if (rank == 0) {
	assert(mm_data.siz == kv1.v.i);
    }
    mm_data.siz = (int)kv1.v.i;
    kmr_free_kvs(siz1);
    mr->preset_block_size = (5 * sizeof(int) * (size_t)(mm_data.siz * mm_data.siz));

    if (rank == 0) {
	fprintf(stderr, "***** data size is %" PRIdPTR "\n", (intptr_t)data_size);
	printf("MatrixMult: Calling MapReduce Scheduler Matrix Multiplication\n");
    }

    get_time(&end);

#ifdef TIMING
    if (rank == 0) {
	fprintf(stderr, "initialize: %u\n", time_diff(&end, &begin));
    }
#endif

    get_time(&begin);
    /*map_reduce(&map_reduce_args);*/

    KMR_KVS *cnf0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    if (rank == 0) {
	size_t datasz = (sizeof(int) * (size_t)(mm_data.siz * mm_data.siz));
	struct kmr_kv_box nkv = {.klen = sizeof(mm_data_t),
				 .vlen = (int)datasz,
				 .k.p = (const char *)&mm_data,
				 .v.p = (const char *)mm_data.B};
	kmr_add_kv(cnf0, nkv);
    }
    kmr_add_kv_done(cnf0);
    KMR_KVS *cnf1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    kmr_replicate(cnf0, cnf1, kmr_noopt);
    kmr_map(cnf1, 0, 0, kmr_noopt, mm_setup);

    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    kmr_map_on_rank_zero(kvs0, 0, kmr_noopt, phoenix_split);
    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    kmr_shuffle(kvs0, kvs1, kmr_noopt);

    KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    kmr_map(kvs1, kvs2, 0, kmr_noopt, mm_map);

    KMR_KVS *kvs3 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    struct kmr_option rankzero = {.rank_zero = 1};
    kmr_replicate(kvs2, kvs3, rankzero);
    kmr_map(kvs3, 0, 0, kmr_noopt, mm_output);

    get_time(&end);

#ifdef TIMING
    if (rank == 0) {
	fprintf(stderr, "library: %u\n", time_diff(&end, &begin));
    }
#endif

    get_time(&begin);

    /*map_reduce_finalize();*/

    //dprintf("\n");
    //dprintf("The length of the final output is %d\n",mm_vals.length);

    if (rank == 0) {
	int sum = 0;
	for (int i = 0; i < (mm_data.siz * mm_data.siz); i++) {
	    sum += mm_data.C[i];
	}
	dprintf("MatrixMult: total sum is %d\n", sum);
	dprintf("MatrixMult: MapReduce Completed\n");
    }

    if (rank == 0) {
	for (int i = 0; i < mm_data.siz; i++) {
	    for (int j = 0; j < mm_data.siz; j++) {
		int v = 0;
		for (int k = 0; k < mm_data.siz; k++) {
		    int a = mm_data.A[i * mm_data.siz + k];
		    int b = mm_data.B[k * mm_data.siz + j];
		    v += (a * b);
		}
		assert(mm_data.C[i * mm_data.siz + j] == v);
	    }
	}
    }

    if (rank == 0) {
	free(mm_data.A);
	free(mm_data.C);
    }
    free(mm_data.B);

    kmr_free_context(mr);

    get_time(&end);

#ifdef TIMING
    if (rank == 0) {
	fprintf(stderr, "finalize: %u\n", time_diff(&end, &begin));
    }
#endif

    MPI_Finalize();
    return 0;
}
