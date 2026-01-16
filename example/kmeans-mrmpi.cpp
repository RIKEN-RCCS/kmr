/* K-Means in MR-MPI (2013-09-19) */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <limits.h>
#include <math.h>
#include <mpi.h>
#include "mapreduce.h"
#include "keyvalue.h"

#define DEF_NUM_ITERATION 10
#define DEF_NUM_POINTS    10000
#define DEF_NUM_MEANS     100
#define DEF_DIM           3
#define DEF_GRID_SIZE     1000

using namespace MAPREDUCE_NS;

typedef struct {
    // number of k-means iteration
    int n_iteration;
    // size of grid
    int grid_size;
    // dimention of points
    int dim;
    // number of (initial) points
    int n_points;
    // points
    int *points;
    // number of means
    int n_means;
    // means
    int *means;
} kmeans_t;

int
measure_time(struct timeval *tv)
{
    MPI_Barrier(MPI_COMM_WORLD);
    if (gettimeofday(tv, NULL) == -1) {
	perror("gettimeofday");
	return -1;
    }
    return 0;
}

double
calc_time_diff(struct timeval *tv_s, struct timeval *tv_e)
{
    return ((double)tv_e->tv_sec - (double)tv_s->tv_sec)
	+ ((double)tv_e->tv_usec - (double)tv_s->tv_usec) /1000000.0;
}

/* Parse commandline arguments */
void
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

/* Show statistics of Key-Value */
void
statistics_kv(int iteration, long kv_cnt)
{
    int rank, nprocs;
    long *data;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    if (rank == 0) {
	data = (long *)malloc(nprocs * sizeof(long));
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

/* Generate the points. */
void
generate_randoms(int *pts, int size, int max_val)
{
    int i;
    for (i = 0; i < size; i++)
	pts[i] = (rand() % max_val);
}

/* MR-MPI map function
   It copies multi-dimension points to KVS.
*/
void
generate_points(int itask, KeyValue *kv, void *ptr)
{
    int i;
    kmeans_t *kmeans = (kmeans_t *)ptr;
    int *points = kmeans->points;

    for (i = 0; i < kmeans->n_points * kmeans->dim; i += kmeans->dim) {
	kv->add((char *)&i, sizeof(int),
		(char *)&points[i], kmeans->dim * (int)sizeof(int));
    }
}

/* calculate squared distance
 */
unsigned int
calc_sq_dist(int *v1, int *v2, int dim)
{
    int i;
    unsigned int sum = 0;
    for (i = 0; i < dim; i++)
	sum += ((v1[i] - v2[i]) * (v1[i] - v2[i]));
    return sum;
}

/* MR-MPI map function
   It calculates cluster of a point stored in kv.
   It emits a Key-Value whose key is cluster id (index of array
   "kmeans.means") and value is the point.
*/
void
calc_cluster(uint64_t itask, char *key, int keybytes,
	     char *value, int valuebytes, KeyValue *kv, void *ptr)
{
    int i;
    kmeans_t *kmeans = (kmeans_t *)ptr;
    int dim    = kmeans->dim;
    int *means = kmeans->means;
    int n_means = kmeans->n_means;
    int *point = (int *)value;
    int min_idx = 0;
    unsigned int min_dist = calc_sq_dist(point, &means[0], dim);

    for (i = 1; i < n_means; i++) {
	unsigned int dist = calc_sq_dist(point, &means[i * dim], dim);
	if (dist < min_dist) {
	    min_idx  = i;
	    min_dist = dist;
	}
    }
    kv->add((char *)&min_idx, sizeof(int),
	    (char *)point, dim * (int)sizeof(int));
}

/* MR-MPI reduce function
   It calculates center of clusters.
   It emits a Key-Value whose key is cluster id and value is the new center.
*/
void
update_cluster(char *key, int keybytes, char *multivalue, int nvalues,
	       int *valuebytes, KeyValue *kv, void *ptr)
{
    int i, j;
    int cid = *key;
    kmeans_t *kmeans = (kmeans_t *)ptr;
    int *points = (int *)multivalue;
    int dim = kmeans->dim;
    int average[dim];

    for (i = 0; i < dim; i++)
	average[i] = 0;
    for (i = 0; i < nvalues; i++) {
	int *point = &points[i * dim];
	for (j = 0; j < dim; j++) {
	    average[j] += point[j];
	}
    }
    for (i = 0; i < dim; i++)
	average[i] /= nvalues;

    kv->add((char *)&cid, sizeof(int),
	    (char *)average, dim * (int)sizeof(int));
}

/* MR-MPI map function
   It copies centers of clusters stored in kv to "kmeans.means" array.
*/
void
copy_center(uint64_t itask, char *key, int keybytes,
	    char *value, int valuebytes, KeyValue *kv, void *ptr)
{
    int i;
    int cid = *key;
    int *center = (int *)value;
    kmeans_t *kmeans = (kmeans_t *)ptr;
    int dim = kmeans->dim;
    int *target = &kmeans->means[cid * dim];

    for (i = 0; i < dim; i++)
	target[i] = center[i];
}

#if defined(DEBUG) || defined(FULL_DEBUG)
/* MR-MPI scan function
   It shows points.
*/
void
show_points(char *key, int keybytes, char *value, int valuebytes, void *ptr)
{
    int i;
    char buf[100], buf2[10];
    kmeans_t *kmeans = (kmeans_t *)ptr;
    int *point = (int *)value;

    memset(buf, 0, strlen(buf));
    for (i = 0; i < kmeans->dim; i++) {
	sprintf(buf2, "%d ", point[i]);
	strncat(buf, buf2, strlen(buf2));
    }
    fprintf(stderr, "( %s)\n", buf);
}

/* MR-MPI scan function
   It shows points with their cluster.
*/
void
show_points_w_clusters(char *key, int keybytes,
		       char *value, int valuebytes, void *ptr)
{
    int i;
    char buf[100], buf2[10];
    kmeans_t *kmeans = (kmeans_t *)ptr;
    int cid = *key;
    int *point = (int *)value;
    int rank;

    memset(buf, 0, strlen(buf));
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    for (i = 0; i < kmeans->dim; i++) {
	sprintf(buf2, "%d ", point[i]);
	strncat(buf, buf2, strlen(buf2));
    }
    fprintf(stderr, "RANK[%d]: %3d ( %s)\n", rank, cid, buf);
}

/* MR-MPI scan function
   It shows points with their cluster. KMV version.
*/
void
show_points_w_clusters_m(char *key, int keybytes,
			 char *multivalue, int nvalues, int *valuebytes,
			 void *ptr)
{
    int i, j;
    char buf[100], buf2[10];
    kmeans_t *kmeans = (kmeans_t *)ptr;
    int cid = *key;
    int *points = (int *)multivalue;
    int rank;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    for (j = 0; j < nvalues; j++) {
	memset(buf, 0, strlen(buf));
	int *point = &points[j * kmeans->dim];
	for (i = 0; i < kmeans->dim; i++) {
	    sprintf(buf2, "%d ", point[i]);
	    strncat(buf, buf2, strlen(buf2));
	}
	fprintf(stderr, "RANK[%d]: %3d ( %s)\n", rank, cid, buf);
    }
}

void
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

    // Initialize using MPI functions
    srand((rank + 1) * getpid());
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
    kmeans.means = (int *)malloc(kmeans.n_means * kmeans.dim * sizeof(int));
    if (rank == 0) {
	generate_randoms(kmeans.means, kmeans.n_means * kmeans.dim,
			 kmeans.grid_size);
    }
    MPI_Bcast(kmeans.means, kmeans.n_means * kmeans.dim, MPI_INT,
	      0, MPI_COMM_WORLD);
    // set points randomly on each rank
    kmeans.points = (int *)malloc(kmeans.n_points * kmeans.dim * sizeof(int));
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
	MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
	mr->memsize=512;
	mr->outofcore = -1;
	//mr->verbosity = 2;
	//mr->timer = 1;
	mr->map(nprocs, generate_points, (void *)&kmeans);
	statistics_kv(itr + 1, mr->kv->nkv);

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
	mr->scan(show_points, (void *)&kmeans);
#endif

	// call map to calculate cluster
	mr->map(mr, calc_cluster, (void *)&kmeans);
#ifdef FULL_DEBUG
	fprintf(stderr, "Iteration[%2d]: Map result\n", itr);
	mr->scan(show_points_w_clusters, (void *)&kmeans);
#endif

	// call callate (acutually aggregate & convert) to gather points which
	// belong to the same cluster
	mr->aggregate(NULL);
#ifdef FULL_DEBUG
	fprintf(stderr, "Iteration[%2d]: Collate(aggregate) result\n", itr);
	mr->scan(show_points_w_clusters, (void *)&kmeans);
#endif

	// call callate (acutually aggregate & convert) to gather points which
	// belong to the same cluster
	mr->convert();
#ifdef FULL_DEBUG
	fprintf(stderr, "Iteration[%2d]: Collate(convert) result\n", itr);
	mr->scan(show_points_w_clusters_m, (void *)&kmeans);
#endif

	// call reduce to update cluster center
	mr->reduce(update_cluster, (void *)&kmeans);
#ifdef FULL_DEBUG
	fprintf(stderr, "Iteration[%2d]: Reduce result\n", itr);
	mr->scan(show_points_w_clusters, (void *)&kmeans);
#endif

	// cal gather & broadcast to share new cluster centers
	mr->gather(1);
	mr->broadcast(0);
#ifdef FULL_DEBUG
	fprintf(stderr, "Iteration[%2d]: Gather & Broadcast result\n", itr);
	mr->scan(show_points_w_clusters, (void *)&kmeans);
#endif

	// call map to copy new cluster centers to kmeans.means array.
	mr->map(mr, copy_center, (void *)&kmeans);
	delete mr;
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
    }

    MPI_Finalize();
    return 0;
}
