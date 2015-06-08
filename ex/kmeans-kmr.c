/* K-Means in KMR (2013-09-19) */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <limits.h>
#include <math.h>
#include <mpi.h>
#include "kmr.h"

#define DEF_NUM_ITERATION 10
#define DEF_NUM_POINTS    10000
#define DEF_NUM_MEANS     100
#define DEF_DIM           3
#define DEF_GRID_SIZE     1000

struct kmr_option kmr_inspect = { .inspect = 1 };

typedef struct {
    // number of k-means iteration
    int n_iteration;
    // size of grid
    int grid_size;
    // dimention of points
    int dim;
    // number of points
    int n_points;
    // points
    int *points;
    // number of means
    int n_means;
    // means
    int *means;
} kmeans_t;

static int
measure_time(struct timeval *tv)
{
    MPI_Barrier(MPI_COMM_WORLD);
    if (gettimeofday(tv, NULL) == -1) {
	perror("gettimeofday");
	return -1;
    }
    return 0;
}

static double
calc_time_diff(struct timeval *tv_s, struct timeval *tv_e)
{
    return ((double)tv_e->tv_sec - (double)tv_s->tv_sec)
	+ ((double)tv_e->tv_usec - (double)tv_s->tv_usec) /1000000.0;
}

/* Parse commandline arguments */
static void
parse_args(int argc, char **argv, kmeans_t *kmeans)
{
    int c;
    extern char *optarg;

    while ((c = getopt(argc, argv, "i:d:c:p:s:")) != EOF) {
	switch (c) {
	case 'i':
	    kmeans->n_iteration = atoi(optarg);
	    break;
	case 'd':
	    kmeans->dim = atoi(optarg);
	    break;
	case 'c':
	    kmeans->n_means = atoi(optarg);
	    break;
	case 'p':
	    kmeans->n_points = atoi(optarg);
	    break;
	case 's':
	    kmeans->grid_size = atoi(optarg);
	    break;
	case '?':
	    printf("Usage: %s -i <num iteration> -d <vector dimension> "
		   "-c <num clusters> -p <num points> -s <max value>\n", argv[0]);
	    MPI_Abort(MPI_COMM_WORLD, 1);
	}
    }

    if (kmeans->n_iteration <= 0 || kmeans->dim <= 0 || kmeans->n_means <= 0
	|| kmeans->n_points <= 0 || kmeans->grid_size <= 0) {
	printf("Illegal argument value. All values must be numeric and "
	       "greater than 0\n");
	MPI_Abort(MPI_COMM_WORLD, 1);
    }
}

#if 0
/* Show statistics of Key-Value */
static void
statistics_kv(int iteration, long kv_cnt)
{
    int rank, nprocs;
    long *data;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    if (rank == 0) {
	data = (long *)malloc((size_t)nprocs * sizeof(long));
    } else {
	data = NULL;
    }
    MPI_Gather(&kv_cnt, 1, MPI_LONG, data, 1, MPI_LONG, 0, MPI_COMM_WORLD);

    if (rank == 0) {
	int i, min_idx, max_idx;
	long sum = 0, min = LONG_MAX, max = 0;
	for (i = 0; i < nprocs; i++) {
	    sum += data[i];
	    if (data[i] < min) {
		min = data[i];
		min_idx = i;
	    }
	    if (data[i] > max) {
		max = data[i];
		max_idx = i;
	    }
	}
	double average = (double)sum / nprocs;
	double variance = 0.0;
	for (i = 0; i < nprocs; i++) {
	    variance += ((double)data[i] - average) * ((double)data[i] - average);
	}
	variance /= nprocs;
	double std_dev = sqrt(variance);

#if 0
	printf("Itr[%2d]: KV statistics\n"
	       "\tMin # of KVs is %10ld on Rank[%05d]\n"
	       "\tMax # of KVs is %10ld on Rank[%05d]\n"
	       "\tStd Dev of KVs is %f\n",
	       iteration, min, min_idx, max, max_idx, std_dev);
#endif
	printf("%d,%ld,%ld,%f\n", iteration, min, max, std_dev);

	free(data);
    }
}
#endif

/* Generate the points. */
static void
generate_randoms(int *pts, int size, int max_val)
{
    int i;
    for (i = 0; i < size; i++)
	pts[i] = (rand() % max_val);
}

/* KMR map function
   It copies multi-dimension points to KVS.
*/
static int
generate_points(const struct kmr_kv_box kv,
		const KMR_KVS *kvi, KMR_KVS *kvo, void *p, long i_)
{
    int i;
    kmeans_t *kmeans = (kmeans_t *)p;
    int *points = kmeans->points;

    for (i = 0; i < kmeans->n_points * kmeans->dim; i += kmeans->dim) {
	struct kmr_kv_box nkv = { .klen = (int)sizeof(long),
				  .vlen = kmeans->dim * (int)sizeof(int),
				  .k.i  = i,
				  .v.p  = (void *)&points[i] };
	kmr_add_kv(kvo, nkv);
    }

    return MPI_SUCCESS;
}

/* calculate squared distance
 */
static unsigned int
calc_sq_dist(int *v1, int *v2, int dim)
{
    int i;
    unsigned int sum = 0;
    for (i = 0; i < dim; i++)
	sum += (unsigned int)((v1[i] - v2[i]) * (v1[i] - v2[i]));
    return sum;
}

/* KMR map function
   It calculates cluster of a point stored in kv.
   It emits a Key-Value whose key is cluster id (index of array
   "kmeans.means") and value is the point.
*/
static int
calc_cluster(const struct kmr_kv_box kv,
	     const KMR_KVS *kvi, KMR_KVS *kvo, void *p, long i_)
{
    int i;
    kmeans_t *kmeans = (kmeans_t *)p;
    int dim    = kmeans->dim;
    int *means = kmeans->means;
    int n_means = kmeans->n_means;
    int *point = (int *)kv.v.p;
    int min_idx = 0;
    unsigned int min_dist = calc_sq_dist(point, &means[0], dim);

    for (i = 1; i < n_means; i++) {
	unsigned int dist = calc_sq_dist(point, &means[i * dim], dim);
	if (dist < min_dist) {
	    min_idx  = i;
	    min_dist = dist;
	}
    }
    struct kmr_kv_box nkv = { .klen = (int)sizeof(long),
			      .vlen = dim * (int)sizeof(int),
			      .k.i  = min_idx,
			      .v.p  = (void *)point };
    kmr_add_kv(kvo, nkv);

    return MPI_SUCCESS;
}

/* KMR reduce function
   It calculates center of clusters.
   It emits a Key-Value whose key is cluster id and value is the new center.
*/
static int
update_cluster(const struct kmr_kv_box kv[], const long n,
	       const KMR_KVS *kvi, KMR_KVS *kvo, void *p)
{
    int i, j;
    int cid = (int)kv[0].k.i;
    kmeans_t *kmeans = (kmeans_t *)p;
    int dim = kmeans->dim;
    long sum[dim];
    int average[dim];

    for (i = 0; i < dim; i++)
	sum[i] = 0;
    for (i = 0; i < n; i++) {
	int *point = (int *)kv[i].v.p;
	for (j = 0; j < dim; j++) {
	    sum[j] += point[j];
	}
    }
    for (i = 0; i < dim; i++)
	average[i] = (int)(sum[i] / n);

    struct kmr_kv_box nkv = { .klen = (int)sizeof(long),
			      .vlen = dim * (int)sizeof(int),
			      .k.i  = cid,
			      .v.p  = (void *)average };
    kmr_add_kv(kvo, nkv);

    return MPI_SUCCESS;
}

/* KMR map function
   It copies centers of clusters stored in kv to "kmeans.means" array.
*/
static int
copy_center(const struct kmr_kv_box kv,
	    const KMR_KVS *kvi, KMR_KVS *kvo, void *p, long i_)
{
    int i;
    int cid = (int)kv.k.i;
    int *center = (int *)kv.v.p;
    kmeans_t *kmeans = (kmeans_t *)p;
    int dim = kmeans->dim;
    int *target = &kmeans->means[cid * dim];

    for (i = 0; i < dim; i++)
	target[i] = center[i];

    return MPI_SUCCESS;
}

#if defined(FULL_DEBUG)
/* KMR map function
   It shows points.
*/
static int
show_points(const struct kmr_kv_box kv,
	    const KMR_KVS *kvi, KMR_KVS *kvo, void *p, long i_)
{
    int i;
    char buf[100], buf2[10];
    kmeans_t *kmeans = (kmeans_t *)p;
    int *point = (int *)kv.v.p;

    memset(buf, 0, strlen(buf));
    for (i = 0; i < kmeans->dim; i++) {
	sprintf(buf2, "%d ", point[i]);
	strncat(buf, buf2, strlen(buf2));
    }
    fprintf(stderr, "( %s)\n", buf);

    return MPI_SUCCESS;
}

/* KMR map function
   It shows points with their cluster.
*/
static int
show_points_w_clusters(const struct kmr_kv_box kv,
		       const KMR_KVS *kvi, KMR_KVS *kvo, void *p, long i_)
{
    int i;
    char buf[100], buf2[10];
    kmeans_t *kmeans = (kmeans_t *)p;
    int cid = (int)kv.k.i;
    int *point = (int *)kv.v.p;
    int rank;

    memset(buf, 0, strlen(buf));
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    for (i = 0; i < kmeans->dim; i++) {
	sprintf(buf2, "%d ", point[i]);
	strncat(buf, buf2, strlen(buf2));
    }
    fprintf(stderr, "RANK[%d]: %3d ( %s)\n", rank, cid, buf);

    return MPI_SUCCESS;
}
#endif

#if defined(DEBUG) || defined(FULL_DEBUG)
static void
print_means(int *means, int size, int dim)
{
    int i, j;
    for (i = 0; i < size; i++) {
	int *mean = &means[i * dim];
	printf("( ");
	for (j = 0; j < dim; j++)
	    printf("%d ", mean[j]);
	printf(")\n");
    }
}
#endif

////////////////////////////////////////////////////////////////////////////////

int
main(int argc, char **argv)
{
    kmeans_t kmeans;
    int nprocs, rank;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    kmr_init();
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);

    // Initialize using MPI functions
    srand((unsigned int)((rank + 1) * getpid()));
    if (rank == 0) {
	kmeans.n_iteration = DEF_NUM_ITERATION;
	kmeans.grid_size   = DEF_GRID_SIZE;
	kmeans.dim         = DEF_DIM;
	// TODO decide the number of points assigned to each procs randomly
	kmeans.n_points    = DEF_NUM_POINTS;
	kmeans.n_means     = DEF_NUM_MEANS;

	parse_args(argc, argv, &kmeans);

	printf("#### Configuration ###########################\n");
	printf("Number of processes = %d\n", nprocs);
	printf("Iteration           = %d\n", kmeans.n_iteration);
	printf("Dimension           = %d\n", kmeans.dim);
	printf("Number of clusters  = %d\n", kmeans.n_means);
	printf("Number of points    = %d\n", kmeans.n_points);
	printf("##############################################\n");
    }
    MPI_Bcast(&(kmeans.n_iteration), 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&(kmeans.grid_size), 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&(kmeans.dim), 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&(kmeans.n_points), 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&(kmeans.n_means), 1, MPI_INT, 0, MPI_COMM_WORLD);
    // set initial centers randomly on rank 0
    kmeans.means = (int *)malloc((size_t)kmeans.n_means * (size_t)kmeans.dim * sizeof(int));
    if (rank == 0) {
	generate_randoms(kmeans.means, kmeans.n_means * kmeans.dim,
			 kmeans.grid_size);
    }
    MPI_Bcast(kmeans.means, kmeans.n_means * kmeans.dim, MPI_INT,
	      0, MPI_COMM_WORLD);
    // set points randomly on each rank
    kmeans.points = (int *)malloc((size_t)kmeans.n_points * (size_t)kmeans.dim * sizeof(int));
    generate_randoms(kmeans.points, kmeans.n_points * kmeans.dim,
		     kmeans.grid_size);

    struct timeval tv_ts, tv_te, tv_s, tv_e;
    // measure kernel start time
    if (measure_time(&tv_ts) == -1) {
	MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // kernel //
    int itr = 0;
    while (itr < kmeans.n_iteration) {
	KMR_KVS *kvs_points = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	kmr_map_once(kvs_points, (void *)&kmeans, kmr_noopt, 0, generate_points);
	long kv_cnt;
	kmr_local_element_count(kvs_points, &kv_cnt);
	//statistics_kv(itr + 1, kv_cnt);

	// measure iteration start time
	if (measure_time(&tv_s) == -1) {
	    MPI_Abort(MPI_COMM_WORLD, 1);
	}

#ifdef DEBUG
	if (rank == 0) {
	    printf("Iteration[%2d]: Means\n", itr);
	    print_means(kmeans.means, kmeans.n_means, kmeans.dim);
	}
#endif
#ifdef FULL_DEBUG
	fprintf(stderr, "Iteration[%2d]: Points\n", itr);
	kmr_map(kvs_points, NULL, (void *)&kmeans, kmr_inspect, show_points);
#endif

	// call kmr_map to calculate cluster
	KMR_KVS *kvs_c2p = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	kmr_map(kvs_points, kvs_c2p, (void *)&kmeans, kmr_noopt, calc_cluster);
#ifdef FULL_DEBUG
	fprintf(stderr, "Iteration[%2d]: Map result\n", itr);
	kmr_map(kvs_c2p, NULL, (void *)&kmeans, kmr_inspect,
		show_points_w_clusters);
#endif

	// call kmr_shuffle to gather points which belong to the same cluster
	KMR_KVS *kvs_c2p_s = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	kmr_shuffle(kvs_c2p, kvs_c2p_s, kmr_noopt);
#ifdef FULL_DEBUG
	fprintf(stderr, "Iteration[%2d]: Shuffle result\n", itr);
	kmr_map(kvs_c2p_s, NULL, (void *)&kmeans, kmr_inspect,
		show_points_w_clusters);
#endif

	// call kmr_reduce to update cluster center
	KMR_KVS *kvs_cluster = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	kmr_reduce(kvs_c2p_s, kvs_cluster, (void *)&kmeans, kmr_noopt,
		   update_cluster);
#ifdef FULL_DEBUG
	fprintf(stderr, "Iteration[%2d]: Reduce result\n", itr);
	kmr_map(kvs_cluster, NULL, (void *)&kmeans, kmr_inspect,
		show_points_w_clusters);
#endif

	// cal kmr_replicate to share new cluster centers
	KMR_KVS *kvs_all_clusters =
	    kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	kmr_replicate(kvs_cluster, kvs_all_clusters, kmr_noopt);
#ifdef FULL_DEBUG
	fprintf(stderr, "Iteration[%2d]: Replicate result\n", itr);
	kmr_map(kvs_all_clusters, NULL, (void *)&kmeans, kmr_inspect,
		show_points_w_clusters);
#endif

	// call kmr_map to copy new cluster centers to kmeans.means array.
	kmr_map(kvs_all_clusters, NULL, (void *)&kmeans, kmr_noopt, copy_center);
	itr += 1;

	// measure iteration end time
	if (measure_time(&tv_e) == -1) {
	    MPI_Abort(MPI_COMM_WORLD, 1);
	}
	if (rank == 0) {
	    double time_diff = calc_time_diff(&tv_s, &tv_e);
	    printf("Iteration[%2d]: Elapse time: %f\n", itr, time_diff);
	}
    }

    // measure kernel end time
    if (measure_time(&tv_te) == -1) {
	MPI_Abort(MPI_COMM_WORLD, 1);
    }

    if (rank == 0) {
	double total_time = calc_time_diff(&tv_ts, &tv_te);
	printf("Total elapse time: %f\n", total_time);
	printf("Cluster corrdinates\n");
	for (int i = 0; i < kmeans.n_means * kmeans.dim; i += kmeans.dim) {
	    printf("    ( ");
	    for (int j = 0; j < kmeans.dim; j++) {
		printf("%d, ", kmeans.means[i * kmeans.dim + j]);
	    }
	    printf(")\n");
	}
    }

    kmr_free_context(mr);
    kmr_fin();

    MPI_Finalize();
    return 0;
}
