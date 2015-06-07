/* A file read benchmark for testing locality-aware file assignment.

   This program reads files in a specified directory and measures the average
   time for reading two files on each node.

   This program can be run as the follow.

   1. crate a directory and files

      $ mkdir ./data
      $ dd if=/dev/zero of=./data/file000 bs=1M count=100
      $ dd if=/dev/zero of=./data/file001 bs=1M count=100
      ...

   2. run by mpiexec

      $ mpiexec ./a.out ./data

      When '-s' option is given, it performs shuffle instead of locality-
      aware file assignment.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <assert.h>
#include <mpi.h>
#include "kmr.h"

#define PATHLEN 256
#define READBUFSIZ 1048576  /* 1MB */
#define MAXFILES 8192

/* KMR map function
   It creates a kvs whose key is a sequential number starts from 0 and value
   is a file name in the specified directory.
*/
static int
read_files(const struct kmr_kv_box kv,
	   const KMR_KVS *kvi, KMR_KVS *kvo, void *p, long i_)
{
    char *dirname = (char *)p;

    /* Count files */
    int file_cnt = 0;
    DIR *dir = opendir(dirname);
    assert(dir != NULL);
    for (struct dirent *dp = readdir(dir); dp != NULL; dp = readdir(dir)) {
	assert(strlen(dp->d_name) > 0);
	if (dp->d_name[0] == '.') {
	    continue;
	}
	file_cnt += 1;
    }
    closedir(dir);

    /* Load file names */
    char **files = (char **)malloc(sizeof(char *) * (size_t)file_cnt);
    dir = opendir(dirname);
    assert(dir != NULL);
    int fidx = 0;
    for (struct dirent *dp = readdir(dir); dp != NULL; dp = readdir(dir)) {
	assert(strlen(dp->d_name) > 0);
	if (dp->d_name[0] == '.') {
	    continue;
	}
	files[fidx] = (char *)malloc(sizeof(char) * PATHLEN);
	snprintf(files[fidx], PATHLEN, "%s/%s", dirname, dp->d_name);
	fidx += 1;
    }
    closedir(dir);

    /* Create kv */
    for (int i = 0; i < file_cnt; i++) {
	char *first = files[i];
	char *second = files[(i + 1) % file_cnt];
	size_t first_len = strlen(first);
	size_t second_len = strlen(second);
	long vlen = (long)first_len + (long)second_len + 2;
	char *val = (char *)malloc(sizeof(char) * (size_t)vlen);
	strncpy(val, first, first_len);
	val[first_len] = '\0';
	strncpy(&val[first_len + 1], second, second_len);
	val[vlen - 1] = '\0';
	struct kmr_kv_box nkv = { .klen = sizeof(long),
				  .vlen = (int)(vlen * (long)sizeof(char)),
				  .k.i  = i,
				  .v.p  = (void *)val };
	kmr_add_kv(kvo, nkv);
	free(val);
    }

    /* free all */
    for (int i = 0; i < file_cnt; i++) {
	free(files[i]);
    }
    free(files);

    return MPI_SUCCESS;
}

/* KMR map function
   It measures times required for reading a file.
*/
static int
benchmark(const struct kmr_kv_box kv,
	  const KMR_KVS *kvi, KMR_KVS *kvo, void *p, long i_)
{
    double time_diff = 0.0;
    char *filename = (char *)kv.v.p;
    for (char *pp = filename + 1; pp < kv.v.p + kv.vlen; pp++) {
	if (*pp == '\0') {
	    /* run benchmark */
	    char buf[READBUFSIZ];
	    double t1 = MPI_Wtime();
	    FILE *fp = fopen(filename, "r");
	    assert(fp != NULL);
	    size_t siz;
	    do {
		siz = fread(buf, sizeof(char), READBUFSIZ, fp);
	    } while (siz != 0);
	    fclose(fp);
	    double t2 = MPI_Wtime();
	    time_diff += t2 - t1;

	    filename = pp + 1;
	}
    }

    struct kmr_kv_box nkv = { .klen = sizeof(long),
			      .vlen = sizeof(double),
			      .k.i  = 0,
			      .v.d  = time_diff };
    kmr_add_kv(kvo, nkv);
    return MPI_SUCCESS;
}

/* KMR reduce function
   It calculates the average file read time.
*/
static int
summarize(const struct kmr_kv_box kv[], const long n,
	  const KMR_KVS *kvi, KMR_KVS *kvo, void *p)
{
    double sum = 0.0;
    for (long i = 0; i < n; i++) {
	printf("%f\n", kv[i].v.d);
	sum += kv[i].v.d;
    }
    double avg = sum / (double)n;
    printf("Average read time: %f\n", avg);
    fflush(stdout);
    return MPI_SUCCESS;
}

/*
  It return 1 if the specified file path is a directory.
  Otherwise it returns 0.
*/
static _Bool
check_directory(const char *path)
{
    struct stat s;
    int ret = stat(path, &s);
    if (ret != 0) {
	return 0;
    }
    if (S_ISDIR(s.st_mode)) {
	return 1;
    }
    return 0;
}

static void
show_help(int rank)
{
    if (rank == 0) {
	fprintf(stderr, "Specify a directory.\n\n");
	fprintf(stderr, "Usage: ./a.out [-s] DIRECTORY\n");
	fprintf(stderr, "    -s : perform shuffle to distribute files.\n");
    }
}


int
main(int argc, char **argv)
{
    int nprocs, rank;
    _Bool noiolb = 0;
    char *dirname = NULL;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (!(argc == 2 || argc == 3)) {
	show_help(rank);
	MPI_Finalize();
	return 1;
    } else if (argc == 2) {
	dirname = argv[1];
	if (!check_directory(dirname)) {
	    show_help(rank);
	    MPI_Finalize();
	    return 1;
	}
    } else if (argc == 3) {
	int ret = strcmp(argv[1], "-s");
	if (ret == 0) {
	    noiolb = 1;
	}
	dirname = argv[2];
	if (!check_directory(dirname)) {
	    show_help(rank);
	    MPI_Finalize();
	    return 1;
	}
    }

    kmr_init();
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    mr->trace_iolb = 1;
    mr->verbosity = 5;

    KMR_KVS *kvs_infiles = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    kmr_map_once(kvs_infiles, (void *)dirname, kmr_noopt, 1, read_files);

    KMR_KVS *kvs_targets = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    if (noiolb) {
	/* perform shuffle */
	kmr_shuffle(kvs_infiles, kvs_targets, kmr_noopt);
    } else {
	/* perform file distribution based on locality */
	kmr_assign_file(kvs_infiles, kvs_targets, kmr_noopt);
    }

    KMR_KVS *kvs_times = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_FLOAT8);
    kmr_map(kvs_targets, kvs_times, NULL, kmr_noopt, benchmark);

    KMR_KVS *kvs_all_times = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_FLOAT8);
    kmr_shuffle(kvs_times, kvs_all_times, kmr_noopt);

    kmr_reduce(kvs_all_times, NULL, NULL, kmr_noopt, summarize);

    kmr_free_context(mr);
    kmr_fin();
    MPI_Finalize();
    return 0;
}
