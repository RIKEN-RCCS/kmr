/* graysort.c (2014-02-04) */

/* GraySort.  THIS IS NOT FOR BENCHMARKING.  See the FAQ for the
   benchmark rules in TeraSort in "http://sortbenchmark.org". */

/* HOW TO RUN: First, run "gensort-1.2/gensort 1000000 aaa" to make a
   data file.  Second, run "mpirun -np n a.out" to sort "aaa" into
   "bbb.nnnnn", where nnnnn are the ranks of the nodes.  Third, run
   "cat bbb.* > bbb" to concatenate outputs.  Then, run
   "gensort-1.2/valsort bbb" for sorted-ness and the checksum. */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#include "kmr.h"

/* GraySort constants. */

#define RECLEN 100
#define KEYLEN 10

static double
wtime()
{
    static struct timeval tv0 = {.tv_sec = 0};
    struct timeval tv;
    int cc;
    cc = gettimeofday(&tv, 0);
    assert(cc == 0);
    if (tv0.tv_sec == 0) {
	tv0 = tv;
	assert(tv0.tv_sec != 0);
    }
    double dt = ((double)(tv.tv_sec - tv0.tv_sec)
		 + ((double)(tv.tv_usec - tv0.tv_usec) * 1e-6));
    return dt;
}

static int
read_records(char *n, KMR_KVS *kvo)
{
    KMR *mr = kvo->c.mr;
    int nprocs = mr->nprocs;
    int rank = mr->rank;
    int cc;
    FILE *f = fopen(n, "r");
    if (f == 0) {
	char ee[80];
	snprintf(ee, 80, "fopen(%s)", n);
	perror(ee);
	MPI_Abort(MPI_COMM_WORLD, 1);
    }
    int fd = fileno(f);
    struct stat s;
    cc = fstat(fd, &s);
    if (cc != 0) {
	char ee[80];
	snprintf(ee, 80, "fstat(%s)", n);
	perror(ee);
	MPI_Abort(MPI_COMM_WORLD, 1);
    }
    assert((s.st_size % RECLEN) == 0);
    long totrecs = (s.st_size / RECLEN);
    long erecs = ((totrecs + nprocs - 1) / nprocs);
    long nrecs = ((rank != (nprocs - 1))
		  ? erecs
		  : (totrecs - (erecs * (nprocs - 1))));
    off_t off = (RECLEN * erecs * rank);
    cc = fseeko(f, off, SEEK_SET);
    if (cc != 0) {
	char ee[80];
	snprintf(ee, 80, "fseek(%s,%zd)", n, off);
	perror(ee);
	MPI_Abort(MPI_COMM_WORLD, 1);
    }
    size_t zz;
    for (long i = 0 ; i < nrecs; i++) {
	char b[RECLEN];
	zz = fread(b, sizeof(b), 1, f);
	if (zz < 1) {
	    char ee[80];
	    int eof = feof(f);
	    assert(eof != 0);
	    snprintf(ee, 80, "fread(%s,%ld)", n, i);
	    perror(ee);
	    MPI_Abort(MPI_COMM_WORLD, 1);
	}
	assert(zz == 1);
	struct kmr_kv_box kv = {
	    .klen = KEYLEN, .vlen = (RECLEN - KEYLEN),
	    .k.p = b, .v.p = &b[KEYLEN]};
	cc = kmr_add_kv(kvo, kv);
	assert(cc == MPI_SUCCESS);
    }
    cc = fclose(f);
    if (cc != 0) {
	char ee[80];
	snprintf(ee, 80, "fclose(%s)", n);
	perror(ee);
	MPI_Abort(MPI_COMM_WORLD, 1);
    }
    cc = kmr_add_kv_done(kvo);
    assert(cc == MPI_SUCCESS);
    long cnt;
    cc = kmr_get_element_count(kvo, &cnt);
    assert(cc == MPI_SUCCESS);
    assert(totrecs == cnt);
    return MPI_SUCCESS;
}

#if 0
static int
sumreducefn(const struct kmr_kv_box kv[], const long n,
	    const KMR_KVS *kvi, KMR_KVS *kvo, void *p)
{
    int rank = kvi->c.mr->rank;
    int cc;
    long v = 0;
    for (long i = 0; i < n; i++) {
	v += kv[i].v.i;
    }
    struct kmr_kv_box kv0 = {
	.klen = sizeof(long), .vlen = sizeof(long),
	.k.i = rank, .v.i = v};
    cc = kmr_add_kv(kvo, kv0);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}
#endif

static int
writekvfn(const struct kmr_kv_box kv0,
	  const KMR_KVS *kvs0, KMR_KVS *kvo, void *p, const long i)
{
    FILE *f = p;
    size_t zz;
    zz = fwrite(kv0.k.p, KEYLEN, 1, f);
    assert(zz == 1);
    zz = fwrite(kv0.v.p, (RECLEN - KEYLEN), 1, f);
    assert(zz == 1);
    return MPI_SUCCESS;
}

static int
write_records(char *n0, KMR_KVS *kvi)
{
    KMR *mr = kvi->c.mr;
    //int nprocs = mr->nprocs;
    int rank = mr->rank;
#if 0
    long cnt = kvi->c.element_count;
    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    struct kmr_kv_box kv0 = {
	.klen = sizeof(long), .vlen = sizeof(long),
	.k.i = rank, .v.i = cnt};
    cc = kmr_add_kv(kvs0, kv0);
    assert(cc == MPI_SUCCESS);
    cc = kmr_add_kv_done(kvs0);
    assert(cc == MPI_SUCCESS);
    /* Entries are sorted. */
    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    cc = kmr_scan(kvs0, kvs1, sumreducefn, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    struct kmr_kv_box kv1;
    cc = kmr_take_one(kvs1, &kv1);
    assert(cc == MPI_SUCCESS);
    long precs = kv1.v.i;
    off_t off = (precs * RECLEN);
#endif
    char n[80];
    snprintf(n, sizeof(n), "%s.%05d", n0, rank);
    int cc;
    FILE *f = fopen(n, "w");
    if (f == 0) {
	char ee[80];
	snprintf(ee, 80, "fopen(%s)", n);
	perror(ee);
	MPI_Abort(MPI_COMM_WORLD, 1);
    }
    struct kmr_option nothreading = {.nothreading = 1};
    cc = kmr_map(kvi, 0, f, nothreading, writekvfn);
    assert(cc == MPI_SUCCESS);
    cc = fclose(f);
    if (cc != 0) {
	char ee[80];
	snprintf(ee, 80, "fclose(%s)", n);
	perror(ee);
	MPI_Abort(MPI_COMM_WORLD, 1);
    }
    return MPI_SUCCESS;
}

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
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    //mr->one_step_sort = 0;
    //mr->preset_block_size = 750;
    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {printf("reading file...\n");}

    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = read_records("aaa", kvs0);
    assert(cc == MPI_SUCCESS);
    if (rank == 0) {printf("count=%ld\n", kvs0->c.element_count);}

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {printf("run sort...\n");}

    MPI_Barrier(MPI_COMM_WORLD);
    double t0 = wtime();
    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_sort_large(kvs0, kvs1, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    MPI_Barrier(MPI_COMM_WORLD);
    double t1 = wtime();

    if (rank == 0) {printf("run sort in %f sec\n", (t1 - t0));}

    printf("count=%ld\n", kvs1->c.element_count);

    cc = write_records("bbb", kvs1);
    assert(cc == MPI_SUCCESS);

    kmr_fin();
    MPI_Finalize();

    return 0;
}
