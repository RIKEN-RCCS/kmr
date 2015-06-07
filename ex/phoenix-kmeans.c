/* Copyright (c) 2007-2009, Stanford University
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*      * Redistributions of source code must retain the above copyright
*         notice, this list of conditions and the following disclaimer.
*      * Redistributions in binary form must reproduce the above copyright
*         notice, this list of conditions and the following disclaimer in the
*         documentation and/or other materials provided with the distribution.
*      * Neither the name of Stanford University nor the names of its
*         contributors may be used to endorse or promote products derived from
*         this software without specific prior written permission.
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
   for KMR (2013-04-24).  REWRITTEN MUCH.  */

/* KMR MEMO: Phoenix map-reduce performs the sequence of split, map,
   reduce, and (built-in) merge. */

#include <mpi.h>
#include <stdio.h>
#include <strings.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>
#include <math.h>
#if 0
#include "stddefines.h"
#include "map_reduce.h"
#endif
#include <stdbool.h>
#include "kmr.h"

#define DEF_NUM_POINTS 100000
#define DEF_NUM_MEANS 100
#define DEF_DIM 3
#define DEF_GRID_SIZE 1000

/* Paramters.  These constants copied on all nodes. */

int dim; 	// dimension
int grid_size;	// range (size) in each dimension
int num_points;	// number of vectors
int num_means;	// number of clusters

int unit_size;

/* POINTS is the problem and replicated on all nodes.  MEANS is a
   replicated copy of means, updated at each loop.  CLUSTERS indexes
   in means and is only on rank0. */

int *points = 0; // [num_points][dim]
int *means = 0;	// [num_means][dim]
int *clusters = 0; // [num_points]

/* Position record in splitter. */

int next_point = 0;

/* Count of clusters are modified.  Total is taken on rank0. */

int modified;

/* Range of points of each work, and a copy of CLUSTERS in that
   range.  */

typedef struct {
    int start;
    int length;
    /* and part of clusters data */
} kmeans_mapdata_t;

#define dprintf(...) fprintf(stdout, __VA_ARGS__)

static inline void
get_time(struct timeval *tv)
{
    gettimeofday(tv, 0);
}

/* Prints out the mean values. */

static void
dump_means(int *_, int size)
{
    for (int i = 0; i < size; i++) {
	for (int j = 0; j < dim; j++) {
	    dprintf("%5d ", means[i * dim + j]);
	}
	dprintf("\n");
    }
}

static int
safe_atoi(const char *s)
{
    char gomi;
    int v, cc;
    assert(s != 0 && s[0] != 0);
    cc = sscanf(s, "%d%c", &v, &gomi);
    assert(cc == 1);
    return v;
}

/* Parses the user arguments. */

static void
parse_args(int argc, char **argv)
{
    int c;
    extern char *optarg;
    extern int optind;

    num_points = DEF_NUM_POINTS;
    num_means = DEF_NUM_MEANS;
    dim = DEF_DIM;
    grid_size = DEF_GRID_SIZE;

    while ((c = getopt(argc, argv, "d:c:p:s:")) != EOF) {
	switch (c) {
	    case 'd':
		dim = safe_atoi(optarg);
		break;
	    case 'c':
		num_means = safe_atoi(optarg);
		break;
	    case 'p':
		num_points = safe_atoi(optarg);
		break;
	    case 's':
		grid_size = safe_atoi(optarg);
		break;
	    case '?':
		printf("Usage: %s -d <vector dimension> -c <num clusters> -p <num points> -s <max value>\n", argv[0]);
		exit(1);
	}
    }

    if (dim <= 0 || num_means <= 0 || num_points <= 0 || grid_size <= 0) {
	printf("Illegal argument value. All values must be numeric and greater than 0\n");
	exit(1);
    }

    printf("Dimension = %d\n", dim);
    printf("Number of clusters = %d\n", num_means);
    printf("Number of points = %d\n", num_points);
    printf("Size of each dimension = %d\n", grid_size);
}

static int
setup_config(const struct kmr_kv_box kv,
	     const KMR_KVS *kvs, KMR_KVS *kvo, void *p, long i_)
{
    assert(kvs == 0 && kv.klen == 0 && kv.vlen == 0);
    assert(kvo->c.mr->rank == 0);
    char v[256];

    snprintf(v, 256, "%d", num_points);
    kmr_add_string(kvo, "num_points", v);
    snprintf(v, 256, "%d", num_means);
    kmr_add_string(kvo, "num_means", v);
    snprintf(v, 256, "%d", dim);
    kmr_add_string(kvo, "dim", v);
    snprintf(v, 256, "%d", grid_size);
    kmr_add_string(kvo, "grid_size", v);

    char *k0 = "points";
    int klen0 = (int)(strlen(k0) + 1);
    kmr_add_kv1(kvo, k0, klen0, points,
		((int)sizeof(int) * num_points * dim));

    char *k1 = "means";
    int klen1 = (int)(strlen(k1) + 1);
    kmr_add_kv1(kvo, k1, klen1, means,
		((int)sizeof(int) * num_means * dim));

    return MPI_SUCCESS;
}

static void
copy_config(KMR_KVS *kvs)
{
    const char *s;
    int cc;
    cc = kmr_find_string(kvs, "num_points", &s);
    assert(cc == MPI_SUCCESS);
    num_points = safe_atoi(s);
    cc = kmr_find_string(kvs, "num_means", &s);
    assert(cc == MPI_SUCCESS);
    num_means = safe_atoi(s);
    cc = kmr_find_string(kvs, "dim", &s);
    assert(cc == MPI_SUCCESS);
    dim = safe_atoi(s);
    cc = kmr_find_string(kvs, "grid_size", &s);
    assert(cc == MPI_SUCCESS);
    grid_size = safe_atoi(s);

    if (points != 0) {
	free(points);
    }
    if (means != 0) {
	free(means);
    }

    cc = kmr_find_string(kvs, "points", &s);
    assert(cc == MPI_SUCCESS);
    points = malloc(sizeof(int) * (size_t)(num_points * dim));
    assert(points != 0);
    memcpy(points, s, (sizeof(int) * (size_t)(num_points * dim)));

    cc = kmr_find_string(kvs, "means", &s);
    assert(cc == MPI_SUCCESS);
    means = malloc(sizeof(int) * (size_t)(num_means * dim));
    assert(means != 0);
    memcpy(means, s, (sizeof(int) * (size_t)(num_means * dim)));
}

/* Generates the points. */

static void
generate_points(int *pts, int size)
{
    for (int i = 0; i < size; i++) {
	for (int j = 0; j < dim; j++) {
	    pts[i * dim + j] = (rand() % grid_size);
	}
    }
}

/* Gets the squared distance between two points. */

static inline unsigned int
get_sq_dist(int *v1, int *v2)
{
    unsigned int sum;
    sum = 0;
    for (int i = 0; i < dim; i++) {
	int p1 = v1[i];
	int p2 = v2[i];
	sum += (unsigned int)((p1 - p2) * (p1 - p2));
    }
    return sum;
}

/* Adds vectors. */

static inline void
add_to_sum(int *sum, int *point)
{
    for (int i = 0; i < dim; i++) {
	sum[i] += point[i];
    }
}

/* Divides points to map tasks.  Tasks is represented by a range of
   partion of points along with associated clusters.  UNITS is a chuck
   size (which will fit to the L1 cache size).  It is called until
   returning 0 to split input data to a number of map tasks. */

static int
kmeans_splitter(int units, int id, KMR_KVS *kvo)
/*int kmeans_splitter(void *data_in, int req_units, map_args_t *out)*/
{
    assert(points != 0 && means != 0 && clusters != 0);
    assert(units > 0 && kvo != 0);

    if (next_point >= num_points) {
	return 0;
    }

    int s = next_point;
    int w;
    if ((next_point + units) < num_points) {
	w = units;
    } else {
	w = (num_points - next_point);
    }
    next_point += units;

    size_t sz = (sizeof(kmeans_mapdata_t) + (sizeof(int) * (size_t)w));
    kmeans_mapdata_t *md = malloc(sz);
    assert(md != 0);
    md->start = s;
    md->length = w;
    int *partofclusters = (void *)((char *)md + sizeof(kmeans_mapdata_t));
    memcpy(partofclusters, &clusters[s], (sizeof(int) * (size_t)w));
    struct kmr_kv_box kv = {.klen = (int)sizeof(long),
			    .vlen = (int)sz,
			    .k.i = id,
			    .v.p = (void *)md};
    kmr_add_kv(kvo, kv);
    free(md);
    return 1;
}

static int
phoenix_split(const struct kmr_kv_box kv0,
	      const KMR_KVS *kvs0, KMR_KVS *kvo, void *p, long i_)
{
    assert(kvs0 == 0 && kv0.klen == 0 && kv0.vlen == 0 && kvo != 0);
    const int DEFAULT_CACHE_SIZE = (64 * 1024);
    int chunk_size = (DEFAULT_CACHE_SIZE / unit_size);
    next_point = 0;
    int id = 0;
    while (kmeans_splitter(chunk_size, id, kvo)) {
	id++;
    }
    return MPI_SUCCESS;
}

/* KEY_CMP and LOCATOR are not used. */

/* Casts by dropping constantness. */

#define LOOSE_CAST(X) ((void *)(X))

/* Finds the cluster that is most suitable for a given set of points.
   PARTOFCLUSTERS is a partial copy of CLUSTERS of the range. */

static void
find_clusters(int start, int length, int *partofclusters, KMR_KVS *kvo)
/*void find_clusters(int *points, keyval_t *means, int *clusters, int size)*/
{
    for (int i = 0; i < length; i++) {
	int p = (start + i);
	unsigned int mindist = get_sq_dist(&points[p * dim], &means[0]);
	int minidx = 0;
	for (int j = 1; j < num_means; j++) {
	    unsigned int d = get_sq_dist(&points[p * dim], &means[j * dim]);
	    if (d < mindist) {
		mindist = d;
		minidx = j;
	    }
	}
	if (partofclusters[i] != minidx) {
	    partofclusters[i] = minidx;
	    modified++;
	}
	struct kmr_kv_box nkv = {.klen = (int)sizeof(long),
				 .vlen = (int)(sizeof(int) * (size_t)dim),
				 .k.i = minidx,
				 .v.p = (const char *)(&points[p * dim])};
	kmr_add_kv(kvo, nkv);
    }
}

/* Maps on the tasks.  The value field is the partions of CLUSTERS.
   It takes the second KVS which is used to share the updates to the
   partitions of CLUSUTERS. */

static int
kmeans_map(const struct kmr_kv_box kv,
	   const KMR_KVS *kvs, KMR_KVS *kvo, void *p, long i_)
/*void kmeans_map(map_args_t *args)*/
{
    assert((size_t)kv.klen == sizeof(long)
	   && (size_t)kv.vlen > sizeof(kmeans_mapdata_t));
    kmeans_mapdata_t *md = LOOSE_CAST(kv.v.p);
    int *partofclusters = (void *)((char *)md + sizeof(kmeans_mapdata_t));
    find_clusters(md->start, md->length, partofclusters, kvo);
    KMR_KVS *kvo1 = p;
    struct kmr_kv_box nkv = {.klen = (int)sizeof(kmeans_mapdata_t),
			     .vlen = (int)(sizeof(int) * (size_t)md->length),
			     .k.p = (void *)md,
			     .v.p = (void *)partofclusters};
    kmr_add_kv(kvo1, nkv);
    return MPI_SUCCESS;
}

/* Updates mean point to the average.  A key is an index in the means,
   and values are points near the mean. */

static int
reduce_to_average(const struct kmr_kv_box kv[], const long n,
		  const KMR_KVS *kvs, KMR_KVS *kvo, void *p)
/*void kmeans_reduce(void *key_in, iterator_t *itr)*/
{
    long meanindex = kv[0].k.i;
    int ave[dim];
    for (int i = 0; i < dim; i++) {
	ave[i] = 0;
    }
    for (int i = 0; i < n; i++) {
	assert((size_t)kv[i].vlen == (sizeof(int) * (size_t)dim));
	assert(kv[0].k.i == meanindex);
	int *point = (int *)kv[i].v.p;
	add_to_sum(ave, point);
    }
    for (int i = 0; i < dim; i++) {
	ave[i] = (int)(ave[i] / n);
    }
    struct kmr_kv_box nkv = {.klen = (int)sizeof(long),
			     .vlen = (int)(sizeof(int) * (size_t)dim),
			     .k.i = meanindex,
			     .v.p = (void *)ave};
    kmr_add_kv(kvo, nkv);
    return MPI_SUCCESS;
}

/* Stores the point to the MEANS array. */

static int
store_means(const struct kmr_kv_box kv,
	    const KMR_KVS *kvs, KMR_KVS *kvo, void *p, long i_)
{
    assert((size_t)kv.klen == sizeof(long)
	   && (size_t)kv.vlen == (sizeof(int) * (size_t)dim));
    long meanindex = kv.k.i;
    int *point = (void *)kv.v.p;
    int *m = &means[meanindex * dim];
    for (int i = 0; i < dim; i++) {
	m[i] = point[i];
    }
    return MPI_SUCCESS;
}

static int
store_clusters(const struct kmr_kv_box kv,
	       const KMR_KVS *kvs, KMR_KVS *kvo, void *p, long i_)
{
    kmeans_mapdata_t *md = LOOSE_CAST(kv.k.p);
    assert((size_t)kv.klen == sizeof(kmeans_mapdata_t)
	   && (size_t)kv.vlen == (sizeof(int) * (size_t)md->length));
    int *partofclusters = (void *)kv.v.p;
    for (int i = 0; i < md->length; i++) {
	clusters[md->start + i] = partofclusters[i];
    }
    return MPI_SUCCESS;
}

/* Updates MODIFIED by the ior of the collected values.  It is called
   once on rank0. */

static int
check_modified(const struct kmr_kv_box kv[], const long n,
	       const KMR_KVS *kvs, KMR_KVS *kvo, void *p)
{
    /*assert(n == nprocs);*/
    int m;
    m = 0;
    for (int i = 0; i < n; i++) {
	assert(kv[i].k.i == 0);
	m += (int)kv[i].v.i;
    }
    modified = m;
    return MPI_SUCCESS;
}

int
main(int argc, char **argv)
{
    srand(20120930);

    int nprocs, rank, thlv;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &thlv);
    /*MPI_Init(&argc, &argv);*/
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);

    /* Enlarge element size of key-value pairs. */
    mr->preset_block_size = (6 * 1024 * 1024);

    struct timeval begin, end;
#ifdef TIMING
    unsigned int library_time = 0;
    unsigned int inter_library_time = 0;
#endif

    get_time(&begin);

    if (rank == 0) {
	parse_args(argc, argv);
    }

    if (rank == 0) {
	/* Generate problem points. */
	points = malloc(sizeof(int) * (size_t)(num_points * dim));
	assert(points != 0);
	generate_points(points, num_points);
	/* Generate initial means. */
	means = malloc(sizeof(int) * (size_t)(num_means * dim));
	assert(means != 0);
	generate_points(means, num_means);
    }

    KMR_KVS *cnf0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    kmr_map_on_rank_zero(cnf0, 0, kmr_noopt, setup_config);
    KMR_KVS *cnf1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    kmr_replicate(cnf0, cnf1, kmr_noopt);
    copy_config(cnf1);
    kmr_free_kvs(cnf1);

    unit_size = (int)(sizeof(int) * (size_t)dim);

    clusters = malloc(sizeof(int) * (size_t)num_points);
    assert(clusters != 0);
    memset(clusters, -1, (sizeof(int) * (size_t)num_points));

    modified = 1;

    /*map_reduce_init();*/

#if 0
    /* Setup map reduce args */
    memset(&map_reduce_args, 0, sizeof(map_reduce_args_t));
    map_reduce_args.task_data = &kmeans_data;
    map_reduce_args.map = kmeans_map;
    map_reduce_args.reduce = kmeans_reduce;
    map_reduce_args.splitter = kmeans_splitter;
    map_reduce_args.locator = kmeans_locator;
    map_reduce_args.key_cmp = mykeycmp;
    map_reduce_args.unit_size = kmeans_data.unit_size;
    map_reduce_args.partition = NULL; // use default
    map_reduce_args.result = &kmeans_vals;
    map_reduce_args.data_size = (num_points + num_means) * dim * sizeof(int);
    map_reduce_args.L1_cache_size = atoi(GETENV("MR_L1CACHESIZE"));//1024 * 8;
    map_reduce_args.num_map_threads = atoi(GETENV("MR_NUMTHREADS"));//8;
    map_reduce_args.num_reduce_threads = atoi(GETENV("MR_NUMTHREADS"));//16;
    map_reduce_args.num_merge_threads = atoi(GETENV("MR_NUMTHREADS"));//8;
    map_reduce_args.num_procs = atoi(GETENV("MR_NUMPROCS"));//16;
    map_reduce_args.key_match_factor = (float)atof(GETENV("MR_KEYMATCHFACTOR"));//2;
    map_reduce_args.use_one_queue_per_task = true;
#endif

    if (rank == 0) {
	dprintf("KMeans: Calling MapReduce Scheduler\n");
    }

    get_time(&end);

#ifdef TIMING
    fprintf(stderr, "initialize: %u\n", time_diff(&end, &begin));
#endif

    while (modified > 0) {
	if (rank == 0) {
	    dprintf("number of modified cluster points=%d\n", modified);
	    fflush(stdout);
	}

	modified = 0;

	get_time(&begin);
	/*map_reduce(&map_reduce_args);*/

	KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	kmr_map_on_rank_zero(kvs0, 0, kmr_noopt, phoenix_split);

	KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	kmr_shuffle(kvs0, kvs1, kmr_noopt);

	KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	KMR_KVS *kvs3 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	kmr_map(kvs1, kvs2, (void *)kvs3, kmr_noopt, kmeans_map);
	kmr_add_kv_done(kvs3);

	KMR_KVS *kvs4 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	kmr_shuffle(kvs2, kvs4, kmr_noopt);

	KMR_KVS *kvs5 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	kmr_reduce(kvs4, kvs5, 0, kmr_noopt, reduce_to_average);

	/* Replicate and store MEANS. */

	KMR_KVS *kvs6 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	kmr_replicate(kvs5, kvs6, kmr_noopt);
	kmr_map(kvs6, 0, 0, kmr_noopt, store_means);

	/* Replicate and store CLUSTERS. */

	struct kmr_option rankzero = {.rank_zero = 1};
	KMR_KVS *kvs7 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	kmr_replicate(kvs3, kvs7, rankzero);
	kmr_map(kvs7, 0, 0, kmr_noopt, store_clusters);

	/* Take sum of the modified flag. */

	KMR_KVS *kvs8 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
	struct kmr_kv_box kv = {.klen = sizeof(long),
				.vlen = sizeof(long),
				.k.i = 0,
				.v.i = modified};
	kmr_add_kv(kvs8, kv);
	kmr_add_kv_done(kvs8);
	KMR_KVS *kvs9 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
	kmr_replicate(kvs8, kvs9, kmr_noopt);
	kmr_reduce(kvs9, 0, 0, kmr_noopt, check_modified);

	get_time(&end);

#ifdef TIMING
	library_time += time_diff(&end, &begin);
#endif

#ifdef TIMING
	inter_library_time += time_diff(&end, &end);
#endif
    }

#ifdef TIMING
    if (rank == 0) {
	fprintf(stderr, "library: %u\n", library_time);
	fprintf(stderr, "inter library: %u\n", inter_library_time);
    }
#endif

    get_time(&begin);

    /*map_reduce_finalize();*/

    if (rank == 0) {
	dprintf("\n");
	printf("KMeans: MapReduce Completed\n");

	dprintf("\n\nFinal means:\n");
	dump_means(means, num_means);
    }

    if (rank == 0) {
	dprintf("Cleaning up\n");
    }

    free(points);
    free(means);
    free(clusters);

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
