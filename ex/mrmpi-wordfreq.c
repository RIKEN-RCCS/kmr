/* ----------------------------------------------------------------------
   MR-MPI = MapReduce-MPI library
   http://www.cs.sandia.gov/~sjplimp/mapreduce.html
   Steve Plimpton, sjplimp@sandia.gov, Sandia National Laboratories

   Copyright (2009) Sandia Corporation.  Under the terms of Contract
   DE-AC04-94AL85000 with Sandia Corporation, the U.S. Government retains
   certain rights in this software.  This software is distributed under
   the modified Berkeley Software Distribution (BSD) License.

   See the README file in the top-level MapReduce directory.
------------------------------------------------------------------------- */

/* This is an example in "examples" directory in MR-MPI (20Jun11),
   converted for KMR (2013-04-24). */

// MapReduce word frequency example in C++
// Syntax: wordfreq file1 dir1 file2 dir2 ...
// (1) read all files and files in dirs
// (2) parse into words separated by whitespace
// (3) count occurrence of each word in all files
// (4) print top 10 words

#include "mpi.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "sys/stat.h"
#if 0
#include "mapreduce.h"
#include "keyvalue.h"
#endif
#include "kmr.h"

/*void fileread(int, char *, KMR_KVS *, void *);*/
/*void sum(char *, int, char *, int, int *, KMR_KVS *, void *);*/
/*int ncompare(char *, int, char *, int);*/
/*void output(uint64_t, char *, int, char *, int, KMR_KVS *, void *);*/

int fileread(const struct kmr_kv_box kv,
	     const KMR_KVS *kvi, KMR_KVS *kvo, void *p,
	     const long index);
int sum(const struct kmr_kv_box kv[], const long n,
	const KMR_KVS *kvi, KMR_KVS *kvo, void *p);
int output(const struct kmr_kv_box kv,
	   const KMR_KVS *kvi, KMR_KVS *kvo, void *p,
	   const long index);

struct Count {
    int n, limit, flag;
};

/* ---------------------------------------------------------------------- */

int main(int argc, char **argv)
{
    /*MPI_Init(&argc, &argv);*/
    int me, nprocs, thlv;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &thlv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &me);

    kmr_init();

    if (argc <= 1) {
	if (me == 0) printf("Syntax: wordfreq file1 file2 ...\n");
	MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /*MapReduce *mr = new MapReduce(MPI_COMM_WORLD);*/
    /*mr->timer = 1;*/
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);

    MPI_Barrier(MPI_COMM_WORLD);
    double tstart = MPI_Wtime();

    int cc;

    struct kmr_option inspect = {.nothreading = 1, .inspect = 1};
    struct kmr_option nothreading = {.nothreading = 1};
    struct kmr_option rankzero = {.rank_zero = 1};

    /*int nwords = mr->map((argc - 1), &argv[1], 0, 1, 0, fileread, NULL);*/
    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    struct kmr_file_option fopt = {
	.each_rank = 0,
	.subdirectories = 1,
	.list_file = 0,
	.shuffle_names = 1};
    cc = kmr_map_file_names(mr, &argv[1], (argc - 1), fopt,
			    kvs0, 0, kmr_noopt, fileread);
    assert(cc == MPI_SUCCESS);

    /* KMR does not count the number of files mapped. */
    int nfiles = 0;
    /*int nfiles = mr->mapfilecount;*/

    long nwords;
    cc = kmr_get_element_count(kvs0, &nwords);
    assert(cc == MPI_SUCCESS);

    /*mr->collate(NULL);*/
    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    cc = kmr_reduce(kvs0, kvs1, 0, kmr_noopt, sum);
    assert(cc == MPI_SUCCESS);

    /*int nunique = mr->reduce(sum, NULL);*/
    KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    cc = kmr_shuffle(kvs1, kvs2, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    KMR_KVS *kvs3 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    cc = kmr_reduce(kvs2, kvs3, 0, kmr_noopt, sum);
    assert(cc == MPI_SUCCESS);

    long nunique = 0;
    cc = kmr_get_element_count(kvs3, &nunique);
    assert(cc == MPI_SUCCESS);

    MPI_Barrier(MPI_COMM_WORLD);
    double tstop = MPI_Wtime();

    /*mr->sort_values(&ncompare);*/
    KMR_KVS *kvs4 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    cc = kmr_sort(kvs3, kvs4, kmr_noopt);
    assert(cc == MPI_SUCCESS);

    struct Count count;
    count.n = 0;
    count.limit = 10;
    count.flag = 0;

    /*mr->map(mr, output, &count);*/
    KMR_KVS *kvs5 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    cc = kmr_map(kvs4, kvs5, &count, nothreading, output);
    assert(cc == MPI_SUCCESS);

    /*mr->gather(1);*/
    KMR_KVS *kvs6 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    cc = kmr_replicate(kvs5, kvs6, rankzero);
    assert(cc == MPI_SUCCESS);

    /*mr->sort_values(ncompare);*/
    KMR_KVS *kvs7 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    cc = kmr_sort(kvs6, kvs7, kmr_noopt);
    assert(cc == MPI_SUCCESS);

    count.n = 0;
    count.limit = 10;
    count.flag = 1;

    /*mr->map(mr, output, &count);*/
    cc = kmr_map(kvs7, 0, &count, inspect, output);
    assert(cc == MPI_SUCCESS);

    kmr_free_kvs(kvs7);

    /*delete mr;*/
    kmr_free_context(mr);

    if (me == 0) {
	printf("%ld total words,  %ld unique words\n", nwords, nunique);
	printf("Time to process %d files on %d procs = %g (secs)\n",
	       nfiles, nprocs, tstop - tstart);
    }

    kmr_fin();

    MPI_Finalize();
}

/* ----------------------------------------------------------------------
   read a file
   for each word in file, emit key = word, value = NULL
------------------------------------------------------------------------- */

/*void fileread(int itask, char *fname, KeyValue *kv, void *ptr)*/

int fileread(const struct kmr_kv_box kv,
	     const KMR_KVS *kvi, KMR_KVS *kvo, void *p,
	     const long index)
{
    int cc;
    const char *fname = kv.k.p;
    struct stat stbuf;
    int flag = stat(fname, &stbuf);
    if (flag < 0) {
	printf("ERROR: Could not query file size\n");
	MPI_Abort(MPI_COMM_WORLD, 1);
    }
    size_t filesize = (size_t)stbuf.st_size;

    FILE *fp = fopen(fname, "r");
    char *text = malloc(filesize + 1);
    if (text == 0) {
	printf("ERROR: malloc failed\n");
	MPI_Abort(MPI_COMM_WORLD, 1);
    }
    size_t nchar = fread(text, 1, filesize, fp);
    if (nchar != filesize) {
	printf("ERROR: fread returned in the middle\n");
	MPI_Abort(MPI_COMM_WORLD, 1);
    }
    text[nchar] = '\0';
    fclose(fp);

    char *whitespace = " .,;()<>-/0123456789\"\014\t\n\f\r\0";
    char *word = strtok(text, whitespace);
    while (word != 0) {
	/*kv->add(word, strlen(word)+1, NULL, 0);*/
	int len = (int)(strlen(word) + 1);
	struct kmr_kv_box akv = {.klen = len,
				 .vlen = sizeof(long),
				 .k.p = word,
				 .v.i = 1};
	cc = kmr_add_kv(kvo, akv);
	assert(cc == MPI_SUCCESS);
	word = strtok(NULL, whitespace);
    }

    free(text);
    return MPI_SUCCESS;
}

/* ----------------------------------------------------------------------
   count word occurrence
   emit key = word, value = # of multi-values
------------------------------------------------------------------------- */

/*void sum(char *key, int keybytes, char *multivalue,
  int nvalues, int *valuebytes, KeyValue *kv, void *ptr)*/

int sum(const struct kmr_kv_box kv[], const long n,
	const KMR_KVS *kvi, KMR_KVS *kvo, void *p)
{
    /*kv->add(key, keybytes,(char *) &nvalues, sizeof(int));*/
    int cc;
    long cnt = 0;
    for (long i = 0; i < n; i++) {
	cnt += kv[i].v.i;
    }
    struct kmr_kv_box akv = {.klen = kv[0].klen,
			     .vlen = sizeof(long),
			     .k.p = kv[0].k.p,
			     .v.i = cnt};
    cc = kmr_add_kv(kvo, akv);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/* ----------------------------------------------------------------------
   compare two counts
   order values by count, largest first
------------------------------------------------------------------------- */

#if 0
int ncompare(char *p1, int len1, char *p2, int len2)
{
    int i1 = *(int *) p1;
    int i2 = *(int *) p2;
    if (i1 > i2) return -1;
    else if (i1 < i2) return 1;
    else return 0;
}
#endif

/* ----------------------------------------------------------------------
   process a word and its count
   depending on flag, emit KV or print it, up to limit
------------------------------------------------------------------------- */

/*void output(uint64_t itask, char *key, int keybytes, char *value,
  int valuebytes, KeyValue *kv, void *ptr)*/

int output(const struct kmr_kv_box kv,
	   const KMR_KVS *kvi, KMR_KVS *kvo, void *p,
	   const long index)
{
    int cc;
    struct Count *count = (struct Count *)p;
    count->n++;
    if (count->n > count->limit) {
	return MPI_SUCCESS;
    }
    /*int n = *(int *)value;*/
    int n = (int)kv.v.i;
    if (count->flag) {
	printf("%d %s\n", n, kv.k.p);
    } else {
	/*kv->add(key, keybytes, (char *)&n, sizeof(int));*/
	struct kmr_kv_box akv = {.klen = kv.klen,
				 .vlen = sizeof(long),
				 .k.p = kv.k.p,
				 .v.i = n};
	cc = kmr_add_kv(kvo, akv);
	assert(cc == MPI_SUCCESS);
    }
    return MPI_SUCCESS;
}
