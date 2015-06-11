/* test3.c (2014-02-04) */

/* Check KMR file operations. */

/* _GNU_SOURCE for getline(). */

#define _GNU_SOURCE

#include <mpi.h>
#include <stdio.h>
#include <unistd.h>
#include <limits.h>
#include "kmr.h"
#include "kmrimpl.h"

const _Bool runall = 1;

static int
FILVAL(int i)
{
    static int filval[] = {907, 911, 919, 929, 937, 941, 947, 953,
			   967, 971, 977, 983, 991, 997};
    int N = sizeof(filval) / sizeof(int);
    return (filval[(i % N)] + (503 + 1) * (i / N));
}

/* Reads unaligned integer (int) value as v[index]. */

static int
INTAT(void *v, int index)
{
    unsigned char *p = v;
    union {int i; unsigned char c[sizeof(int)];} u;
    for (int j = 0; j < (int)sizeof(int); j++) {
	u.c[j] = p[((int)sizeof(int) * index) + j];
    }
    return u.i;
}

/* Tests kmr_read_files_reassemble(). */

static void
simple0(KMR *mr, size_t SEGSZ, int step)
{
    int nprocs = mr->nprocs;
    int rank = mr->rank;

    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("CHECK READ BY REASSEMBLE...\n");}
    fflush(0);
    usleep(50 * 1000);
    MPI_Barrier(MPI_COMM_WORLD);

    int cc;
    char file[80];

    /* (Use different file names for each step, to avoid long NFS
       directory attribute caching (30 seconds)). */

    snprintf(file, sizeof(file), "dat.%d.%d", step, rank);

    /* Make test-data files of SEGSZ on each rank. */

    {
	char *b = malloc(SEGSZ);
	if (b == 0) {
	    perror("malloc");
	    MPI_Abort(MPI_COMM_WORLD, 1);
	}
	int *bx = (int *)b;
	int x = FILVAL(rank);
	for (int i = 0; i < (int)(SEGSZ / sizeof(int)); i++) {
	    bx[i] = x;
	    x *= 13;
	}
	FILE *f = fopen(file, "w");
	assert(f != 0);
	size_t cx = fwrite(&b[0], SEGSZ, 1, f);
	assert(cx == 1);
	cc = fclose(f);
	assert(cc == 0);
	free(b);

	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);
    }

    /* Read by all ranks. */

    if (runall) {
	if (rank == 0) {printf("Read+take by all (nprocs=%d)\n", nprocs);}
	fflush(0);
	usleep(50 * 1000);

	int color = 0;
	void *buffer;
	off_t size;
	buffer = 0;
	size = -1;
	cc = kmr_read_files_reassemble(mr, file, color, 0, -1,
				       &buffer, &size);
	assert(cc == MPI_SUCCESS);
	assert((size_t)size == (SEGSZ * (size_t)nprocs));
	printf("[%05d] read size=%ld (segment-size=%ld); checking...\n",
	       rank, size, SEGSZ);
	fflush(0);
	char *cbuffer = buffer;
	for (int r = 0; r < nprocs; r++) {
	    int *v = (int *)&cbuffer[SEGSZ * (size_t)r];
	    int x = FILVAL(r);
	    for (int i = 0; i < (int)(SEGSZ / sizeof(int)); i++) {
		assert(INTAT(v, i) == x);
		x *= 13;
	    }
	}
	free(buffer);

	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);
    }

    /* Read by one-by-another ranks. */

    if (runall) {
	if (rank == 0) {printf("Read+take by all with 2 colors"
			       " (color by even/odd ranks)\n");}
	fflush(0);
	usleep(50 * 1000);

	int color = (rank % 2);
	int colorset = ((rank % 2) == 0
			? ((nprocs + 1)/ 2)
			: (nprocs / 2));
	void *buffer;
	off_t size;
	buffer = 0;
	size = -1;
	cc = kmr_read_files_reassemble(mr, file, color, 0, -1,
				       &buffer, &size);
	assert(cc == MPI_SUCCESS);
	assert((size_t)size == (SEGSZ * (size_t)colorset));
	printf("[%05d] read size=%ld (color=%d/%d segment-size=%ld);"
	       " checking...\n",
	       rank, size, color, colorset, SEGSZ);
	fflush(0);
	char *cbuffer = buffer;
	int index = 0;
	for (int r = 0; r < nprocs; r++) {
	    if ((r % 2) == color) {
		int *v = (int *)&cbuffer[SEGSZ * (size_t)index];
		int x = FILVAL(r);
		for (int i = 0; i < (int)(SEGSZ / sizeof(int)); i++) {
		    assert(INTAT(v, i) == x);
		    x *= 13;
		}
		index++;
	    }
	}
	free(buffer);

	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);
    }

    /* Read by one-by-another ranks, limitting read size. */

    if (runall) {
	if (rank == 0) {printf("Read+take by all 2 colors"
			       " with offset/size (64K/16K)\n");}
	fflush(0);
	usleep(50 * 1000);

	off_t off2 = (64 * 1024);
	off_t sz2 = (16 * 1024);
	assert((size_t)(off2 + sz2) < SEGSZ);
	int color = (rank % 2);
	int colorset = ((rank % 2) == 0
			? ((nprocs + 1)/ 2)
			: (nprocs / 2));
	void *buffer;
	off_t size;
	buffer = 0;
	size = -1;
	cc = kmr_read_files_reassemble(mr, file, color, off2, sz2,
				       &buffer, &size);
	assert(cc == MPI_SUCCESS);
	assert(size == (sz2 * colorset));
	printf("[%05d] read size=%ld (color=%d/%d off=%ld sz=%ld);"
	       " checking...\n",
	       rank, size, color, colorset, off2, sz2);
	fflush(0);
	char *cbuffer = buffer;
	int index = 0;
	for (int r = 0; r < nprocs; r++) {
	    if ((r % 2) == color) {
		int *v = (int *)&cbuffer[sz2 * index];
		int x = FILVAL(r);
		for (int i = 0; i < (int)((size_t)off2 / sizeof(int)); i++) {
		    x *= 13;
		}
		for (int i = 0; i < (int)((size_t)sz2 / sizeof(int)); i++) {
		    assert(INTAT(v, i) == x);
		    x *= 13;
		}
		index++;
	    }
	}
	free(buffer);

	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);
    }

    /* Read by all ranks but rank=2. */

    if (runall) {
	if (rank == 0) {printf("Read by all but rank=2, take by all\n");}
	fflush(0);
	usleep(50 * 1000);

	int SKIPRANK = 2;
	int nreaders = ((SKIPRANK < nprocs) ? (nprocs - 1) : nprocs);
	char *filename = ((SKIPRANK == rank) ? 0 : file);
	int color = 0;
	void *buffer;
	off_t size;
	buffer = 0;
	size = -1;
	cc = kmr_read_files_reassemble(mr, filename, color, 0, -1,
				       &buffer, &size);
	assert(cc == MPI_SUCCESS);
	assert(buffer != 0);
	assert((size_t)size == (SEGSZ * (size_t)nreaders));
	printf("[%05d] read size=%ld (segment-size=%ld); checking...\n",
	       rank, size, SEGSZ);
	fflush(0);
	char *cbuffer = buffer;
	{
	    int index = 0;
	    for (int r = 0; r < nprocs; r++) {
		if (r != SKIPRANK) {
		    int *v = (int *)&cbuffer[SEGSZ * (size_t)index];
		    int x = FILVAL(r);
		    for (int i = 0; i < (int)(SEGSZ / sizeof(int)); i++) {
			assert(INTAT(v, i) == x);
			x *= 13;
		    }
		    index++;
		}
	    }
	}
	free(buffer);

	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);
    }

    /* Read by all ranks but rank=2, and rank=1 ignore data. */

    if (runall) {
	if (rank == 0) {
	    printf("Read by all but rank=2, take by all but rank=1\n");
	}
	fflush(0);
	usleep(50 * 1000);

	int skiprank = 2;
	int notakerank = 1;
	int nreaders = ((skiprank < nprocs) ? (nprocs - 1) : nprocs);
	char *filename = ((skiprank == rank) ? 0 : file);
	int color = 0;
	void *buffer;
	off_t size;
	buffer = 0;
	size = -1;
	void **buf = ((notakerank == rank) ? 0 : &buffer);
	cc = kmr_read_files_reassemble(mr, filename, color, 0, -1,
				       buf, &size);
	assert(cc == MPI_SUCCESS);
	assert((notakerank == rank)
	       || (size_t)size == (SEGSZ * (size_t)nreaders));
	printf("[%05d] read size=%ld (segment-size=%ld); checking...\n",
	       rank, size, SEGSZ);
	fflush(0);
	char *cbuffer = buffer;
	if (notakerank != rank) {
	    int index = 0;
	    for (int r = 0; r < nprocs; r++) {
		if (r != skiprank) {
		    int *v = (int *)&cbuffer[SEGSZ * (size_t)index];
		    int x = FILVAL(r);
		    for (int i = 0; i < (int)(SEGSZ / sizeof(int)); i++) {
			assert(INTAT(v, i) == x);
			x *= 13;
		    }
		    index++;
		}
	    }
	}
	if (notakerank != rank) {
	    free(buffer);
	}

	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);
    }

    /* Clean up. */

    {
	cc = unlink(file);
	assert(cc == 0);
    }
}

/* Tests kmr_read_file_by_segments(). */

static void
simple1(KMR *mr, size_t SEGSZ, size_t EXTRA, int step)
{
    char file0[80];
    char file1[80];

    int nprocs = mr->nprocs;
    int rank = mr->rank;

    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("CHECK READ BY SEGMENTS...\n");}
    fflush(0);
    usleep(50 * 1000);
    MPI_Barrier(MPI_COMM_WORLD);

    int cc;

    /* (Use different file names for each step, to avoid long NFS
       directory attribute caching (30 seconds)). */

    cc = snprintf(file0, sizeof(file0), "dat.%d.0", step);
    assert((size_t)cc < sizeof(file0));
    cc = snprintf(file1, sizeof(file1), "dat.%d.1", step);
    assert((size_t)cc < sizeof(file1));
    /*size_t SEGSZ = (1024 * 1024);*/
    size_t totsz = ((5 * SEGSZ * (size_t)nprocs) / 2 + EXTRA);

    /* Make two files of ((5/2 * SEGSZ * nprocs) + EXTRA). */

    {
	if (rank == 0) {
	    char *b = malloc(totsz);
	    if (b == 0) {
		perror("malloc");
		MPI_Abort(MPI_COMM_WORLD, 1);
	    }
	    int nn = (int)(totsz / sizeof(int));
	    int *bx = (int *)b;
	    {
		int x = FILVAL(0);
		for (int i = 0; i < nn; i++) {
		    bx[i] = x;
		    x *= 13;
		}
		FILE *f = fopen(file0, "w");
		assert(f != 0);
		size_t cx = fwrite(&b[0], totsz, 1, f);
		assert(cx == 1);
		cc = fclose(f);
		assert(cc == 0);
	    }
	    {
		int x = FILVAL(1);
		for (int i = 0; i < nn; i++) {
		    bx[i] = x;
		    x *= 13;
		}
		FILE *f = fopen(file1, "w");
		assert(f != 0);
		size_t cx = fwrite(&b[0], totsz, 1, f);
		assert(cx == 1);
		cc = fclose(f);
		assert(cc == 0);
	    }
	    free(b);
	}
	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);
    }

    /* Read by all ranks. */

    if (runall) {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {printf("Read+take by all (nprocs=%d)\n", nprocs);}
	fflush(0);
	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);

	int color = 0;
	void *buffer;
	off_t size;
	buffer = 0;
	size = -1;
	cc = kmr_read_file_by_segments(mr, file0, color,
				       &buffer, &size);
	assert(cc == MPI_SUCCESS);
	printf("[%05d] read size=%ld (segsize=%ld);"
	       " checking...\n",
	       rank, size, SEGSZ);
	fflush(0);

	assert((size_t)size == totsz);
	int nn = (int)(totsz / sizeof(int));
	int *bx = (int *)buffer;
	int x = FILVAL(0);
	for (int i = 0; i < nn; i++) {
	    assert(bx[i] == x);
	    x *= 13;
	}
	free(buffer);

	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);
    }

    /* Read by one-by-another ranks. */

    if (runall) {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {printf("Read+take by all for 2 files"
			       " (color by even/odd ranks)\n");}
	fflush(0);
	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);

	char *file = (((rank % 2) == 0) ? file0 : file1);
	int color = ((rank % 2) == 0);
	void *buffer;
	off_t size;
	buffer = 0;
	size = -1;
	cc = kmr_read_file_by_segments(mr, file, color,
				       &buffer, &size);
	assert(cc == MPI_SUCCESS);
	printf("[%05d] read size=%ld (segsize=%ld);"
	       " checking...\n",
	       rank, size, SEGSZ);
	fflush(0);

	assert((size_t)size == totsz);
	int nn = (int)(totsz / sizeof(int));
	int *bx = (int *)buffer;
	int x = (((rank % 2) == 0) ? FILVAL(0) : FILVAL(1));
	for (int i = 0; i < nn; i++) {
	    assert(bx[i] == x);
	    x *= 13;
	}
	free(buffer);

	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);
    }

    /* Read by all ranks but rank=2. */

    if (runall) {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {printf("Read+take by all but rank=2\n");}
	fflush(0);
	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);

	int skiprank = 2;
	char *file = ((skiprank == rank) ? 0 : file0);
	int color = ((skiprank == rank) ? -1 : 0);
	void *buffer;
	off_t size;
	buffer = 0;
	size = -1;
	void **buf = ((skiprank == rank) ? 0 : &buffer);
	cc = kmr_read_file_by_segments(mr, file, color,
				       buf, &size);
	assert(cc == MPI_SUCCESS);
	printf("[%05d] read size=%ld (segsize=%ld);"
	       " checking...\n",
	       rank, size, SEGSZ);
	fflush(0);

	if (skiprank != rank) {
	    assert((size_t)size == totsz);
	    int nn = (int)(totsz / sizeof(int));
	    int *bx = (int *)buffer;
	    int x = FILVAL(0);
	    for (int i = 0; i < nn; i++) {
		assert(bx[i] == x);
		x *= 13;
	    }
	    free(buffer);
	}

	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);
    }

    /* Read by rank=2, and all others take data. */

    if (runall) {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {printf("Read by rank=2, take by all but rank=2\n");}
	fflush(0);
	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);

	int readrank = 2;
	char *file = ((readrank == rank) ? file0 : 0);
	int color = 0;
	void *buffer;
	off_t size;
	buffer = 0;
	size = -1;
	void **buf = ((readrank == rank) ? 0 : &buffer);
	cc = kmr_read_file_by_segments(mr, file, color,
				       buf, &size);
	assert(cc == MPI_SUCCESS);
	printf("[%05d] read size=%ld (segsize=%ld);"
	       " checking...\n",
	       rank, size, SEGSZ);
	fflush(0);

	if (readrank != rank) {
	    assert((size_t)size == totsz);
	    int nn = (int)(totsz / sizeof(int));
	    int *bx = (int *)buffer;
	    int x = FILVAL(0);
	    for (int i = 0; i < nn; i++) {
		assert(bx[i] == x);
		x *= 13;
	    }
	    free(buffer);
	}

	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);
    }

    /* Clean up. */

    {
	if (rank == 0) {
	    cc = unlink(file0);
	    assert(cc == 0);
	    cc = unlink(file1);
	    assert(cc == 0);
	}
    }
}

static int
list_keys(const struct kmr_kv_box kv,
	  const KMR_KVS *kvi, KMR_KVS *kvo, void *p,
	  const long index)
{
    char *buf = p;
    if (*buf != 0) {
	strcat(buf, " ");
    }
    strcat(buf, kv.k.p);
    int cc = kmr_add_kv(kvo, kv);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

static void
simple2(KMR *mr)
{
    int nprocs = mr->nprocs;
    int rank = mr->rank;

    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("CHECK MAP FILE NAMES...\n");}
    fflush(0);
    usleep(50 * 1000);
    MPI_Barrier(MPI_COMM_WORLD);

    int cc;

    char *datx = "dat.x";
    char *names0[] = {"dat.0", "dat.1", "dat.2", "dat.3"};
    int nnames0 = (sizeof(names0) / sizeof(char *));

    char *dir1 = "dir.0";
    char *names1[] = {"dat.0.0", "dat.0.1", "dat.0.2", "dat.0.3"};
    int nnames1 = (sizeof(names1) / sizeof(char *));

    char *dir2 = "dir.0/dir.0.0";
    char *names2[] = {"dat.0.0.0", "dat.0.0.1", "dat.0.0.2", "dat.0.0.3"};
    int nnames2 = (sizeof(names2) / sizeof(char *));

    if (rank == 0) {
	printf("Making test files.\n");
	fflush(0);

	char ss[(8 * 1024)];
	for (int i = 0; i < nnames0; i++) {
	    snprintf(ss, sizeof(ss), "touch %s", names0[i]);
	    system(ss);
	}

	snprintf(ss, sizeof(ss), "mkdir %s", dir1);
	system(ss);

	for (int i = 0; i < nnames1; i++) {
	    snprintf(ss, sizeof(ss), "touch %s/%s", dir1, names1[i]);
	    system(ss);
	}

	snprintf(ss, sizeof(ss), "mkdir %s", dir2);
	system(ss);

	for (int i = 0; i < nnames2; i++) {
	    snprintf(ss, sizeof(ss), "touch %s/%s", dir2, names2[i]);
	    system(ss);
	}

	snprintf(ss, sizeof(ss), "echo '' > %s", datx);
	system(ss);
	for (int i = 0; i < nnames0; i++) {
	    snprintf(ss, sizeof(ss), "echo %s >> %s", names0[i], datx);
	    system(ss);
	}
	snprintf(ss, sizeof(ss), "echo %s >> %s", dir1, datx);
	system(ss);
    }
    usleep(50 * 1000);
    MPI_Barrier(MPI_COMM_WORLD);

    struct kmr_option nothreading = {.nothreading = 1};
    char buf[64 * 1024];

    {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {printf("kmr_map_file_names (no-option).\n");}
	fflush(0);
	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);

	struct kmr_file_option opt0 = kmr_fnoopt;
	KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
	*buf = 0;
	cc = kmr_map_file_names(mr, names0, nnames0, opt0,
				kvs0, buf, nothreading, list_keys);
	assert(cc == MPI_SUCCESS);

	long cnt0;
	cc = kmr_get_element_count(kvs0, &cnt0);
	assert(cc == MPI_SUCCESS);
	assert(cnt0 == 4);
	//printf("[%05d] [%s]\n", rank, buf);
	//fflush(0);

	kmr_free_kvs(kvs0);
	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);
    }

    {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {printf("kmr_map_file_names (each_rank).\n");}
	fflush(0);
	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);

	struct kmr_file_option opt1 = {.each_rank = 1};
	KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
	*buf = 0;
	cc = kmr_map_file_names(mr, names0, nnames0, opt1,
				kvs1, buf, nothreading, list_keys);
	assert(cc == MPI_SUCCESS);

	long cnt1;
	cc = kmr_get_element_count(kvs1, &cnt1);
	assert(cc == MPI_SUCCESS);
	assert(cnt1 == (4 * nprocs));
	//printf("[%05d] [%s]\n", rank, buf);
	//fflush(0);

	kmr_free_kvs(kvs1);
	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);
    }

    {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {printf("kmr_map_file_names (list_file).\n");}
	fflush(0);
	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);

	struct kmr_file_option opt2 = {.list_file = 1};
	KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
	*buf = 0;
	char *na[] = {datx};
	cc = kmr_map_file_names(mr, na, 1, opt2,
				kvs2, buf, nothreading, list_keys);
	assert(cc == MPI_SUCCESS);

	long cnt2;
	cc = kmr_get_element_count(kvs2, &cnt2);
	assert(cc == MPI_SUCCESS);
	assert(cnt2 == 12);
	//printf("[%05d] [%s]\n", rank, buf);
	//fflush(0);

	kmr_free_kvs(kvs2);
	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);
    }

    {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {printf("kmr_map_file_names (subdirs).\n");}
	fflush(0);
	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);

	struct kmr_file_option opt3 = {.subdirectories = 1};
	KMR_KVS *kvs3 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
	*buf = 0;
	char *na[] = {dir1};
	cc = kmr_map_file_names(mr, na, 1, opt3,
				kvs3, buf, nothreading, list_keys);
	assert(cc == MPI_SUCCESS);

	long cnt3;
	cc = kmr_get_element_count(kvs3, &cnt3);
	assert(cc == MPI_SUCCESS);
	assert(cnt3 == 8);
	//printf("[%05d] [%s]\n", rank, buf);
	//fflush(0);

	kmr_free_kvs(kvs3);
	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);
    }

    {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {printf("kmr_map_file_names (enumerate files).\n");}
	fflush(0);
	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);

	struct kmr_file_option opt4 = {.list_file = 1};
	KMR_KVS *kvs4 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
	char *na[] = {datx};
	cc = kmr_map_file_names(mr, na, 1, opt4,
				kvs4, 0, nothreading, 0);
	assert(cc == MPI_SUCCESS);

	long cnt4;
	cc = kmr_get_element_count(kvs4, &cnt4);
	assert(cc == MPI_SUCCESS);
	assert(cnt4 == 12);

	kmr_free_kvs(kvs4);
	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);
    }

    {
	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {printf("kmr_map_file_names (shuffle_names).\n");}
	fflush(0);
	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);

	struct kmr_file_option opt5 = {.list_file = 1, .shuffle_names = 1};
	KMR_KVS *kvs5 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
	*buf = 0;
	char *na[] = {datx};
	cc = kmr_map_file_names(mr, na, 1, opt5,
				kvs5, buf, nothreading, list_keys);
	assert(cc == MPI_SUCCESS);

	long cnt5;
	cc = kmr_get_element_count(kvs5, &cnt5);
	assert(cc == MPI_SUCCESS);
	assert(cnt5 == 12);
	printf("[%05d] [%s]\n", rank, buf);
	fflush(0);

	kmr_free_kvs(kvs5);
	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);
    }

    /* Clean up. */

    {
	if (rank == 0) {
	    for (int i = 0; i < nnames0; i++) {
		cc = unlink(names0[i]);
		assert(cc == 0);
	    }
	    char ss[(8 * 1024)];
	    snprintf(ss, sizeof(ss), "rm -rf %s", dir1);
	    system(ss);
	    cc = unlink(datx);
	    assert(cc == 0);
	}
    }
}

static int
compareline(const struct kmr_kv_box kv[], const long n,
	    const KMR_KVS *kvi, KMR_KVS *kvo, void *arg)
{
    assert(n == 2);
    assert(kv[0].vlen == kv[1].vlen);
    assert(strncmp(kv[0].v.p, kv[1].v.p, (size_t)kv[0].vlen) == 0);
    return MPI_SUCCESS;
}

static void
simple3(KMR *mr)
{
    //int nprocs = mr->nprocs;
    int rank = mr->rank;

    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("CHECK MAP GETLINE...\n");}
    fflush(0);

    int cc;

    /* kmr_map_getline(), once with threads, and once without. */

    for (int i = 0; i < 2; i++) {
	struct kmr_option opt = {.nothreading = ((i == 0) ? 0 : 1)};

	MPI_Barrier(MPI_COMM_WORLD);
	usleep(50 * 1000);
	if (rank == 0) {
	    printf("kmr_map_getline (%s threads).\n",
		   (opt.nothreading ? "without" : "with"));
	}
	fflush(0);
	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);

	FILE *f = fopen("../LICENSE", "r");
	if (f == 0) {
	    perror("fopen(../LICENSE)");
	    MPI_Abort(MPI_COMM_WORLD, 1);
	}

	KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	cc = kmr_map_getline(mr, f, 0, 1, kvs1, 0, opt, kmr_add_identity_fn);
	assert(cc == MPI_SUCCESS);
	assert(kvs1->c.element_count == 25);

	rewind(f);

	KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	char *line = 0;
	size_t linesz = 0;
	ssize_t rc = 0;
	long lineno = 0;
	for (;;) {
	    rc = getline(&line, &linesz, f);
	    if (rc == -1) {
		break;
	    }
	    assert(rc <= INT_MAX);
	    struct kmr_kv_box kv = {
		.klen = (int)sizeof(long),
		.vlen = (int)rc,
		.k.i = lineno,
		.v.p = (char *)line
	    };
	    cc = kmr_add_kv(kvs0, kv);
	    assert(cc == MPI_SUCCESS);
	    lineno++;
	}
	if (ferror(f)) {
	    perror("getline()");
	    MPI_Abort(MPI_COMM_WORLD, 1);
	}
	free(line);
	line = 0;
	kmr_add_kv_done(kvs0);
	assert(kvs0->c.element_count == 25);

	cc = fclose(f);
	assert(cc == 0);
	f = 0;

	KMR_KVS *vec[] = {kvs0, kvs1};
	KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	kmr_concatenate_kvs(vec, 2, kvs2, kmr_noopt);

	cc = kmr_reduce(kvs2, 0, 0, kmr_noopt, compareline);
	assert(cc == MPI_SUCCESS);

	usleep(50 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);
    }
}

int
main(int argc, char *argv[])
{
    int cc;

    int nprocs, rank, thlv;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &thlv);
    /*MPI_Init(&argc, &argv);*/
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    kmr_init();

    if (rank == 0) {
	if (nprocs < 4) {
	    printf("Run this with nprocs>=4.\n");
	    MPI_Abort(MPI_COMM_WORLD, 1);
	    return 0;
	}
    }

    static char props[] = "trace_file_io=1\n";
    if (1) {
	if (rank == 0) {
	    system("rm -f option");
	    FILE *f = fopen("option", "w");
	    assert(f != 0);
	    size_t cx = fwrite(props, (sizeof(props) - 1), 1, f);
	    assert(cx == 1);
	    cc = fclose(f);
	    assert(cc == 0);
	}
	cc = setenv("KMROPTION", "option", 0);
	assert(cc == 0);
    }

    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    assert(mr != 0);
    assert(mr->trace_file_io == 1);

    const size_t SEGSZ0 = (3 * (1024 * 1024) + 512 + 0);
    const size_t SEGSZ1 = (3 * (1024 * 1024) + 512 + 1);
    simple0(mr, SEGSZ0, 0);
    simple0(mr, SEGSZ1, 1);
    const size_t SEGSZ2 = (1024 * 1024);
    const size_t EXTRA0 = (512 + 0);
    const size_t EXTRA1 = (512 + 1);
    simple1(mr, SEGSZ2, EXTRA0, 0);
    simple1(mr, SEGSZ2, EXTRA1, 1);
    simple2(mr);
    simple3(mr);

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {printf("OK\n");}
    fflush(0);

    cc = kmr_free_context(mr);
    assert(cc == MPI_SUCCESS);

    if (1) {
	if (rank == 0) {
	    system("rm -f option");
	}
    }

    kmr_fin();
    MPI_Finalize();
    return 0;
}
