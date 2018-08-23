/* kmrfiles.c (2014-02-04) */
/* Copyright (C) 2012-2018 RIKEN R-CCS */

/** \file kmrfiles.c File Access Support.  This provides mappers
    working on files and directories, especially provides support for
    the file-system configuration on K.  The access practice on the
    file-system on K has affinity to the z-axis of the TOFU network,
    to lessen the disturbance to the communication of the other users.
    Thus, I/O-groups are formed by the nodes on the same z-axis.  To
    respect this access practice, accessing a file should be by nodes
    with particular positions.  The routines defined here ease these
    coupling.  MEMO: This part uses MPI routines directly, because
    messages here are not eight-byte aligned. */

/* _GNU_SOURCE is needed for "pread()" and "getline()".  */

#if defined(__linux__)
#define _GNU_SOURCE
#endif

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <libgen.h>
#include <limits.h>
#include <errno.h>
#include <assert.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/param.h>
#include <sys/stat.h>
#include "kmr.h"
#include "kmrimpl.h"
#include "kmrfefs.h"

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))
#define ABS(X) ((X) < 0 ? -(X) : (X))
#define CEILING(N,D) (((N)+(D)-1)/(D))
#define NEVERHERE 0

/** Read size limit.  Large read at once slows down on K, due to the
    limited amount of the kernel buffer (rumored as 128MB). */

#define CHUNK_LIMIT (16 * 1024 *1024)

static const struct kmr_fefs_stripe_info
kmr_bad_stripe = {.size=0, .count=0, .offset=0};

/** Segment Reading Information of Each Rank.  Entries are stored in
    an array of a group of ranks who participate in the same
    collective read.  An entry is associated to RANK.  COLOR is a
    given color value (COLOR!=-1).  IOGROUP is an I/O-group of a rank.
    INO and SIZE are about a file.  READS is an amount of a read.
    OFFSET holds a byte offset of a read.  STRIPE holds an index in a
    stripe, where STRIPE=-1 when a rank does not perform reads.
    INGESTING and DIGESTING indicates what a rank performs.  HEAD is
    true on a rank which is the minimum in the same color and performs
    reads.  A head rank prints diagnostics/debugging messages. */

struct kmr_file_reader {
    int rank;
    int color;
    int iogroup;
    ino_t ino;
    off_t size;
    off_t reads;
    long offset;
    int stripe;
    _Bool ingesting;
    _Bool digesting;
    _Bool head;
};

/* ================================================================ */

/* Returns an I/O-group (an integer key) of a compute node or an I/O
   node from a TOFU-coordinate.  It projects a TOFU-coordinate to the
   X-Y. */

static int
kmr_iogroup(kmr_k_position_t p)
{
    assert(p[0] < 0x7fff && p[1] < 0x7fff);
    return (p[0] << 16 | p[1]);
}

/* Returns a distance (manhattan-distance) between I/O-groups. */

int
kmr_iogroup_distance(int a0, int a1)
{
    int x0 = ((a0 >> 16) & 0xffff);
    int y0 = (a0 & 0xffff);
    int x1 = ((a1 >> 16) & 0xffff);
    int y1 = (a1 & 0xffff);
    return (ABS(x0 - y0) + ABS(x1 - y1));
}

/** Returns an I/O-group (an integer key) of a compute node. */

int
kmr_iogroup_of_node(KMR *mr)
{
    int cc;
    kmr_k_position_t p;
    cc = kmr_k_node(mr, p);
    assert(cc == MPI_SUCCESS);
    return kmr_iogroup(p);
}

/** Returns an I/O-group (an integer key) of a disk from an OBDIDX of
    Lustre file-system.  It uses magic expressions (x=obdidx/2048) and
    (y=(obdidx%2048)/64). */

int
kmr_iogroup_of_obd(int obdidx)
{
    int x = (obdidx / 2048);
    int y = ((obdidx % 2048) / 64);
    kmr_k_position_t p = {(unsigned short)x, (unsigned short)y, 0, 0};
    return kmr_iogroup(p);
}

static inline void
kmr_assert_file_readers_are_sorted(struct kmr_file_reader *sgv, long n)
{
    for (long i = 1; i < n; i++) {
	assert(sgv[i - 1].rank <= sgv[i].rank);
    }
}

static int
kmr_file_reader_compare(const void *p0, const void *p1)
{
    const struct kmr_file_reader *q0 = p0;
    const struct kmr_file_reader *q1 = p1;
    return (q0->rank - q1->rank);
}

static int
kmr_copyout_file_readers(const struct kmr_kv_box kv[], const long n,
			 const KMR_KVS *kvi, KMR_KVS *kvo, void *p)
{
    assert(n > 0);
    int cc;
    struct kmr_file_reader sgv[n];
    long segment = kv[0].k.i;
    for (long i = 0; i < n; i++) {
	struct kmr_file_reader *a = (struct kmr_file_reader *)kv[i].v.p;
	sgv[i] = *a;
    }
    qsort(sgv, (size_t)n, sizeof(struct kmr_file_reader),
	  kmr_file_reader_compare);
    kmr_assert_file_readers_are_sorted(sgv, n);
    size_t vlen = (sizeof(struct kmr_file_reader) * (size_t)n);
    assert(vlen <= INT_MAX);
    struct kmr_kv_box nkv = {
	.klen = (int)sizeof(long),
	.vlen = (int)vlen,
	.k.i = segment,
	.v.p = (char *)sgv
    };
    cc = kmr_add_kv(kvo, nkv);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/* Shares file segment information, which indicates which part
   (position and size) is read by which rank.  It returns an array of
   information of the related ranks in SGV of COLORSETSIZE entries.
   COLORSETSIZE is set to the number of ranks having the same color.
   The information of inactive ranks have no entries in SGV.  The work
   is independent when the colors differ.  COLORINDEX is an index in
   SGV of the local rank.  COLORINDEX=-1, when the local rank is
   inactive (not perticipating in ingesting nor digesting).  The SGV
   should be freed by callers.  OFFSET and STRIPE information in SGV
   should be filled later. */

static int
kmr_share_segment_information(KMR *mr, char *file, int color,
			      _Bool ingesting, _Bool digesting,
			      off_t offset, off_t bytes,
			      struct kmr_file_reader **sgvq,
			      int *colorsetsizeq, int *colorindexq)
{
    assert(sgvq != 0 && colorsetsizeq != 0);
    assert(sizeof(ino_t) <= sizeof(long));
    int cc;
    int nprocs = mr->nprocs;
    int rank = mr->rank;
    _Bool active = (color != -1);
    assert(active == (ingesting || digesting));

    /* Check accessibility and get read size of a file. */

    ino_t ino = 0;
    off_t fsz = 0;
    off_t reads = 0;
    if (ingesting) {
	struct stat s;
	do {
	    cc = stat(file, &s);
	} while (cc == -1 && errno == EINTR);
	if (cc == 0) {
	    /* Existent and accessible, OK. */
	    if ((s.st_mode & S_IFMT) != S_IFREG) {
		char ee[160];
		snprintf(ee, sizeof(ee),
			 "File (%s) is not a regular file", file);
		kmr_error(mr, ee);
	    }
	    ino = s.st_ino;
	    fsz = s.st_size;
	} else if (errno == ENOENT) {
	    /* Non-existent. */
	    assert(cc == -1);
	    char ee[160];
	    snprintf(ee, sizeof(ee), "File (%s) does not exist", file);
	    kmr_error(mr, ee);
	    ino = 0;
	    fsz = 0;
	} else if (errno == EACCES
		   || errno == ELOOP
		   || errno == ENOLINK
		   || errno == ENOTDIR) {
	    /* Non-existent (Really, inaccessible). */
	    assert(cc == -1);
	    char ee[160];
	    char *m = strerror(errno);
	    snprintf(ee, sizeof(ee), "File (%s) inaccessible: %s", file, m);
	    kmr_error(mr, ee);
	    ino = 0;
	    fsz = 0;
	} else {
	    /* STAT errs. */
	    assert(cc == -1);
	    char ee[160];
	    char *m = strerror(errno);
	    snprintf(ee, sizeof(ee), "stat(%s): %s", file, m);
	    kmr_error(mr, ee);
	    ino = 0;
	    fsz = 0;
	}
	reads = ((bytes == -1) ? (fsz - offset) : bytes);
    } else {
	reads = 0;
    }

    /* Collect read size information of each node. */

    struct kmr_file_reader *sgv = 0;
    int colorsetsize = 0;
    {
	KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	if (active) {
	    struct kmr_file_reader seg = {
		.rank = rank,
		.color = color,
		.iogroup = kmr_iogroup_of_node(mr),
		.ino = ino,
		.size = fsz,
		.reads = reads,
		.offset = 0,
		.stripe = -1,
		.ingesting = ingesting,
		.digesting = digesting,
		.head = 0
	    };
	    struct kmr_kv_box nkv = {
		.klen = (int)sizeof(long),
		.vlen = (int)sizeof(struct kmr_file_reader),
		.k.i = color,
		.v.p = (char *)&seg
	    };
	    cc = kmr_add_kv(kvs0, nkv);
	    assert(cc == MPI_SUCCESS);
	}
	kmr_add_kv_done(kvs0);
	KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	cc = kmr_shuffle(kvs0, kvs1, kmr_noopt);
	assert(cc == MPI_SUCCESS);
	KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	cc = kmr_reduce(kvs1, kvs2, 0, kmr_noopt, kmr_copyout_file_readers);
	assert(cc == MPI_SUCCESS);
	KMR_KVS *kvs3 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	cc = kmr_replicate(kvs2, kvs3, kmr_noopt);
	assert(cc == MPI_SUCCESS);
	if (active) {
	    struct kmr_kv_box key = {
		.klen = (int)sizeof(long),
		.vlen = 0,
		.k.i = color,
		.v.i = 0
	    };
	    struct kmr_kv_box kv;
	    cc = kmr_find_key(kvs3, key, &kv);
	    assert(cc == MPI_SUCCESS);
	    assert((kv.vlen % (int)sizeof(struct kmr_file_reader)) == 0);
	    colorsetsize = (kv.vlen / (int)sizeof(struct kmr_file_reader));
	    size_t sz = (sizeof(struct kmr_file_reader)
			 * (size_t)colorsetsize);
	    sgv = kmr_malloc(sz);
	    memcpy(sgv, kv.v.p, sz);
	} else {
	    colorsetsize = 0;
	    sgv = 0;
	}
	cc = kmr_free_kvs(kvs3);
	assert(cc == MPI_SUCCESS);
	kmr_assert_file_readers_are_sorted(sgv, colorsetsize);
    }

    /* Marks the leader entry. */

    {
	int r0 = nprocs;
	for (int k = 0; k < colorsetsize; k++) {
	    if (sgv[k].ingesting) {
		r0 = MIN(r0, sgv[k].rank);
	    }
	}
	assert(colorsetsize == 0 || r0 < nprocs);
	for (int k = 0; k < colorsetsize; k++) {
	    if (sgv[k].rank == r0) {
		sgv[k].head = 1;
	    }
	}
    }

    /* Find information of the local rank. */

    int colorindex = -1;
    for (int k = 0; k < colorsetsize; k++) {
	if (sgv[k].rank == rank) {
	    colorindex = k;
	    break;
	}
    }
    assert(active == (colorindex != -1));

    *sgvq = sgv;
    *colorsetsizeq = colorsetsize;
    *colorindexq = colorindex;
    return MPI_SUCCESS;
}

/* Takes the maximum repeat count of reads for all the colors.  It
   takes global maximum in all colors because each color works
   independently. */

static int
kmr_take_maximum_loop_count(KMR *mr, off_t reads,
			    struct kmr_fefs_stripe *stripe,
			    long *maxloopsq)
{
    int cc;
    long maxloops = 0;
    const off_t singlestripe = (stripe->s.size * stripe->s.count);
    KMR_KVS *kvs6 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    if (singlestripe != 0) {
	long nloops = CEILING(reads, singlestripe);
	struct kmr_kv_box nkv = {
	    .klen = (int)sizeof(long),
	    .vlen = (int)sizeof(long),
	    .k.i = 0,
	    .v.i = nloops
	};
	cc = kmr_add_kv(kvs6, nkv);
	assert(cc == MPI_SUCCESS);
    }
    kmr_add_kv_done(kvs6);
    KMR_KVS *kvs7 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    cc = kmr_replicate(kvs6, kvs7, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    cc = kmr_reduce_as_one(kvs7, 0, &maxloops, kmr_noopt,
			   kmr_imax_one_fn);
    assert(cc == MPI_SUCCESS);
    assert(maxloops > 0);
    *maxloopsq = maxloops;
    return MPI_SUCCESS;
}

/* Performs reads and gathers data.  REASSEMBLING is just used for
   trace messages.  BASEOFFSET is the position of the start of reads.
   BUFFER is area to collect the result.  TOTALSIZE is the size of the
   data to be returned, and BUFFER should have TOTALSIZE.  TMPBUF is a
   temporary buffer, whose size is STRIPE.SIZE and each single read is
   limited by it.  MAXLOOPS is a loop count.  SGV and COLORSETSIZE are
   information of file readers.  STRIPE is striping information.
   RANKINDEXES holds an index in a stripe of each rank.  Ranks with
   RANKINDEXES=-1 does not perform reads.  */

static int
kmr_read_and_gather(KMR *mr, _Bool reassembling,
		    char *file, int fd, off_t baseoffset,
		    char *buffer, off_t totalsize, char *tmpbuf,
		    struct kmr_fefs_stripe *stripe,
		    long maxloops,
		    struct kmr_file_reader *sgv, int colorsetsize,
		    int colorindex)
{
    int nprocs = mr->nprocs;
    int rank = mr->rank;
    const off_t singlestripe = (stripe->s.size * stripe->s.count);
    _Bool tracing5 = (mr->trace_file_io && (5 <= mr->verbosity));
    _Bool tracing7 = (mr->trace_file_io && (7 <= mr->verbosity));
#if 0
    long *scnts = kmr_malloc(sizeof(long) * (size_t)nprocs);
    long *sdsps = kmr_malloc(sizeof(long) * (size_t)nprocs);
    long *rcnts = kmr_malloc(sizeof(long) * (size_t)nprocs);
    long *rdsps = kmr_malloc(sizeof(long) * (size_t)nprocs);
#endif
    int *scnts = kmr_malloc(sizeof(int) * (size_t)nprocs);
    int *sdsps = kmr_malloc(sizeof(int) * (size_t)nprocs);
    int *rcnts = kmr_malloc(sizeof(int) * (size_t)nprocs);
    int *rdsps = kmr_malloc(sizeof(int) * (size_t)nprocs);
    int cc;
    assert(reassembling || (baseoffset == 0));
    _Bool head = ((colorindex != -1) ? sgv[colorindex].head : 0);

    int sgindex = -1;
    for (int k = 0; k < colorsetsize; k++) {
	if (sgv[k].rank == rank) {
	    sgindex = k;
	    break;
	}
    }
    _Bool ingesting = ((sgindex != -1) && sgv[sgindex].stripe != -1);
    _Bool digesting = ((sgindex != -1) && sgv[sgindex].digesting);

    if (tracing5 && rank == 0) {
	fprintf(stderr, ";;KMR [%05d] file-read: maxloops=%zd\n",
		rank, maxloops);
	fflush(0);
    }
    if (tracing5 && !reassembling && head) {
	int nreaders = 0;
	for (int k = 0; k < colorsetsize; k++) {
	    //int r = sgv[k].rank;
	    if (sgv[k].stripe != -1) {
		nreaders++;
	    }
	}
	for (int k = 0; k < colorsetsize; k++) {
	    int r = sgv[k].rank;
	    if (sgv[k].stripe != -1) {
		fprintf(stderr,
			";;KMR [%05d] file-read:"
			" stripe unit index=%d/%d (color=%d nprocs=%d)\n",
			r, sgv[k].stripe, nreaders,
			sgv[k].color, colorsetsize);
	    }
	}
	fflush(0);
    }

    int ndigestings = 0;
    int segi = -1;
    for (int k = 0; k < colorsetsize; k++) {
	if (sgv[k].rank == rank) {
	    segi = k;
	}
	if (sgv[k].digesting) {
	    ndigestings++;
	}
    }
    assert(colorsetsize == 0 || segi >= 0);

    /* Cycle reading one stripe. */

    for (long i = 0; i < maxloops; i++) {

	/* Read segments locally. */

	off_t start = (i * singlestripe);
	off_t pos0;
	off_t cnt0;
	if (ingesting) {
	    assert(sgindex != -1);
	    pos0 = start + ((long)stripe->s.size * sgv[sgindex].stripe);
	    cnt0 = MAX(MIN((sgv[segi].reads - pos0), stripe->s.size), 0);
	} else {
	    pos0 = 0;
	    cnt0 = 0;
	}

	if (tracing5) {
	    if (cnt0 > 0) {
		fprintf(stderr,
			";;KMR [%05d] file-read: offset=%zd size=%zd"
			" (file=%s)\n",
			rank, (baseoffset + pos0), cnt0, file);
	    } else {
		fprintf(stderr, ";;KMR [%05d] file-read: (noread)\n", rank);
	    }
	    fflush(0);
	}

	if (cnt0 > 0) {
	    for (off_t rx = 0; rx < cnt0;) {
		ssize_t cx1;
		do {
		    off_t chunk = MIN((cnt0 - rx), CHUNK_LIMIT);
		    cx1 = pread(fd, (tmpbuf + rx), (size_t)chunk,
				(baseoffset + pos0 + rx));
		} while (cx1 == -1 && errno == EINTR);
		if (cx1 == -1) {
		    char ee[160];
		    char *m = strerror(errno);
		    snprintf(ee, sizeof(ee), "read(%s): %s", file, m);
		    kmr_error(mr, ee);
		}
		rx += cx1;
	    }
	}

	/* Collect one stripe (TMPBUF to BUFFER). */

	{
	    for (int r = 0; r < nprocs; r++) {
		scnts[r] = 0;
		sdsps[r] = 0;
		rcnts[r] = 0;
		rdsps[r] = 0;
	    }
	    //off_t cnt0roundup = ROUNDUP(cnt0, 8);
	    for (int k = 0; k < colorsetsize; k++) {
		int r = sgv[k].rank;
		long sendsz = (sgv[k].digesting ? cnt0 : 0);
		scnts[r] = (int)sendsz;
		sdsps[r] = 0;
		assert(sendsz <= stripe->s.size);
		assert(sendsz <= INT_MAX);
	    }
	    for (int k = 0; k < colorsetsize; k++) {
		int r = sgv[k].rank;
		off_t cntx;
		off_t offx;
		if (digesting && sgv[k].stripe != -1) {
		    off_t posx = start + ((int)stripe->s.size * sgv[k].stripe);
		    cntx = MAX(MIN((sgv[k].reads - posx), stripe->s.size), 0);
		    offx = ((cntx != 0) ? (start + sgv[k].offset) : 0);
		} else {
		    cntx = 0;
		    offx = 0;
		}
		//off_t cntxroundup = ROUNDUP(cntx, 8);
		rcnts[r] = (int)cntx;
		rdsps[r] = (int)(offx - start);
		assert((offx + cntx) <= totalsize);
		assert(cntx <= INT_MAX && (offx - start) <= INT_MAX);
	    }

	    if (tracing7) {
		for (int r = 0; r < nprocs; r++) {
		    if (scnts[r] != 0 || rcnts[r] != 0) {
			fprintf(stderr,
				(";;KMR [%05d] file-read: data exchange:"
				 " *<->[%05d] send=%d (disp=%d);"
				 " recv=%d disp=%zd\n"),
				rank, r, scnts[r], sdsps[r],
				rcnts[r], (start + rdsps[r]));
		    }
		}
		fflush(0);
	    }

	    char *bufptr = (buffer + start);
	    if (ndigestings == nprocs && !mr->file_io_always_alltoallv) {
#if 0
		cc = kmr_allgatherv(mr, 0, tmpbuf, cnt0,
				    buffer, rcnts, rdsps);
		assert(cc == MPI_SUCCESS);
#endif
		cc = MPI_Allgatherv(tmpbuf, (int)cnt0, MPI_BYTE,
				    bufptr, rcnts, rdsps, MPI_BYTE,
				    mr->comm);
		assert(cc == MPI_SUCCESS);
	    } else {
#if 0
		cc = kmr_alltoallv(mr, tmpbuf, scnts, sdsps,
				   buffer, rcnts, rdsps);
		assert(cc == MPI_SUCCESS);
#endif
		cc = MPI_Alltoallv(tmpbuf, scnts, sdsps, MPI_BYTE,
				   bufptr, rcnts, rdsps, MPI_BYTE,
				   mr->comm);
		assert(cc == MPI_SUCCESS);
	    }
	}
    }
#if 0
    kmr_free(scnts, (sizeof(long) * (size_t)nprocs));
    kmr_free(sdsps, (sizeof(long) * (size_t)nprocs));
    kmr_free(rcnts, (sizeof(long) * (size_t)nprocs));
    kmr_free(rdsps, (sizeof(long) * (size_t)nprocs));
#endif
    kmr_free(scnts, (sizeof(int) * (size_t)nprocs));
    kmr_free(sdsps, (sizeof(int) * (size_t)nprocs));
    kmr_free(rcnts, (sizeof(int) * (size_t)nprocs));
    kmr_free(rdsps, (sizeof(int) * (size_t)nprocs));
    return MPI_SUCCESS;
}

/* Assigns each rank which part of the file to read for reassembling.
   RANKINDEXES is set 0 or -1 for reads or for no reads.  RANKINDEXES
   holds an index to a unit in a stripe, but it is always zero in the
   reassembling case which reads the whole file. */

static int
kmr_assign_ranks_trivially(KMR *mr, char *file,
			   struct kmr_file_reader *sgv, int colorsetsize,
			   _Bool leader,
			   struct kmr_fefs_stripe *stripe)
{
    //int nprocs = mr->nprocs;
    for (int k = 0; k < colorsetsize; k++) {
	if (sgv[k].ingesting) {
	    sgv[k].stripe = 0;
	}
    }
    off_t sz = 0;
    for (int k = 0; k < colorsetsize; k++) {
	if (sgv[k].ingesting) {
	    sgv[k].offset = sz;
	    sz += sgv[k].reads;
	} else {
	    sgv[k].offset = 0;
	}
    }
    return MPI_SUCCESS;
}

/** Reassembles files reading by ranks.  It is intended to reassembles
    a file from files split into segments.  FILE is a file name.  A
    file name can be null, when the rank does not participate reading
    (COLOR=-1).  COLOR groups ranks (be COLOR>=-1).  The files on the
    ranks with the same COLOR are concatenated, where concatenation is
    ordered by the rank-order.  Read is performed for OFFSET and BYTES
    on each file.  BYTES can be -1 to read an entire file.  BUFFER and
    SIZE are set to the malloced buffer and the size on return.  Ranks
    with non-null FILE retrieve a file (ingest), while ranks with
    non-zero BUFFER receive contents (digest).  Ranks with COLOR=-1 do
    not participate in file reading.  REMARK ON K: It reads a
    specified file by each rank, assuming the files reside in specific
    I/O-groups to the ranks. */

int
kmr_read_files_reassemble(KMR *mr, char *file, int color,
			  off_t offset, off_t bytes,
			  void **buffer, off_t *size)
{
    assert((color != -1) == ((file != 0) || (buffer != 0)));
    assert(color >= -1);
    int cc;
    //int nprocs = mr->nprocs;
    //int rank = mr->rank;
    _Bool ingesting = (file != 0);
    _Bool digesting = (buffer != 0);
    _Bool active = (color != -1);

    /* Collect read segment information of each node. */

    struct kmr_file_reader *sgv;
    int colorsetsize;
    int colorindex;
    cc = kmr_share_segment_information(mr, file, color, ingesting, digesting,
				       offset, bytes,
				       &sgv, &colorsetsize, &colorindex);
    off_t reads = ((colorindex != -1) ? sgv[colorindex].reads : 0);
    //_Bool head = ((colorindex != -1) ? sgv[colorindex].head : 0);

    assert(cc == MPI_SUCCESS);
    assert(!active || (sgv != 0 && colorsetsize != 0));
    assert((sgv == 0) == (colorsetsize == 0));

    if (ingesting && (reads < bytes)) {
	char ee[160];
	snprintf(ee, sizeof(ee),
		 "File (%s) too small to read offset=%zd bytes=%zd",
		 file, offset, bytes);
	kmr_error(mr, ee);
    }
    off_t totalsize = 0;
    for (int k = 0; k < colorsetsize; k++) {
	if (sgv[k].ingesting) {
	    totalsize += sgv[k].reads;
	}
    }

    /* Dummy single unit stripe. */

    struct kmr_fefs_stripe stripe = {.s = {
	    .size = (uint32_t)mr->file_io_block_size,
	    .count = 1,
	    .offset = 0}
    };

    cc = kmr_assign_ranks_trivially(mr, file, sgv, colorsetsize, /*head*/ 0,
				    &stripe);
    assert(cc == MPI_SUCCESS);
    assert(ingesting == (sgv[colorindex].stripe != -1));

    /* Determine repeat count of reads. */

    long maxloops = 0;
    cc = kmr_take_maximum_loop_count(mr, reads, &stripe, &maxloops);
    assert(cc == MPI_SUCCESS && maxloops > 0);

    char *b = kmr_malloc((size_t)totalsize);
    {
	char *tmpbuf = kmr_malloc((size_t)stripe.s.size);
	int fd = -1;
	if (ingesting) {
	    do {
		fd = open(file, O_RDONLY, 0);
	    } while (fd == -1 && errno == EINTR);
	    if (fd == -1) {
		char ee[160];
		char *m = strerror(errno);
		snprintf(ee, sizeof(ee), "open(%s): %s", file, m);
		kmr_error(mr, ee);
	    }
	}

	cc = kmr_read_and_gather(mr, 1, file, fd, offset,
				 b, totalsize, tmpbuf, &stripe, maxloops,
				 sgv, colorsetsize, colorindex);
	assert(cc == MPI_SUCCESS);

	if (ingesting) {
	    do {
		cc = close(fd);
	    } while (cc == -1 && errno == EINTR);
	    if (cc != 0) {
		char ee[160];
		char *m = strerror(errno);
		snprintf(ee, sizeof(ee), "close(%s): %s", file, m);
		kmr_error(mr, ee);
	    }
	}
	kmr_free(tmpbuf, (size_t)stripe.s.size);
    }
    if (sgv != 0) {
	kmr_free(sgv, (sizeof(struct kmr_file_reader) * (size_t)colorsetsize));
    }
    if (buffer != 0) {
	*buffer = b;
    } else {
	kmr_free(b, (size_t)totalsize);
    }
    *size = totalsize;
    return MPI_SUCCESS;
}

/* Returns striping information of a file.  It returns fake OBDINDX in
   the case of KMR_FILE_DUMMY_STRIPE to avoid raising an error.  Dummy
   striping allows simple testing without Lustre file-system.  The
   fake OBDIDX should match with the position values of ranks returned
   by kmr_k_node(). */

static int
kmr_get_stripe(KMR *mr, char *file, int colorsetsize, _Bool leader,
	       struct kmr_fefs_stripe *stripe)
{
    char path[PATH_MAX];
    int cc;
    size_t len = strlen(file);
    assert(len < PATH_MAX);
    memcpy(path, file, (len + 1));
    char *d = path;
    char *f = 0;
    for (char *p = &path[len - 1]; p >= path; p--) {
	if (*p == '/') {
	    f = (p + 1);
	    *p = 0;
	    break;
	}
    }
    if (f == 0) {
	/* No directory part in file name. */
	d = ".";
	f = path;
    }
    int errori = 0;
    _Bool dumpi = (mr->trace_file_io && (7 <= mr->verbosity));
    cc = kmr_fefs_get_stripe(d, f, stripe, &errori, dumpi);
    switch (cc) {
    default:
	assert(0 <= cc && cc <=7);
	break;
    case 0:
	/* OK */
	break;
    case 1:
	/* OS unsupported. */
	stripe->s = kmr_bad_stripe;
	break;
    case 2:
	/* malloc fails. */
	stripe->s = kmr_bad_stripe;
	{
	    char ee[160];
	    char *m = strerror(errori);
	    snprintf(ee, sizeof(ee), "malloc(ostdata): %s", m);
	    kmr_error(mr, ee);
	}
	break;
    case 3:
	/* open fails. */
	stripe->s = kmr_bad_stripe;
	{
	    char ee[160];
	    char *m = strerror(errori);
	    snprintf(ee, sizeof(ee), "open(dir=%s): %s", d, m);
	    kmr_error(mr, ee);
	}
	break;
    case 4:
	/* ioctl fails (Not Lustre FS). */
    case 5:
	/* Bad magic. */
    case 6:
	/* Bad version. */
    case 7:
	/* Bad pattern. */
	/* IGNORE ERROR, OK NOT BEING LUSTRE. */
	stripe->s = kmr_bad_stripe;
	break;
    }
    if (stripe->s.count == 0 && mr->file_io_dummy_striping) {
	if (leader) {
	    char ee[160];
	    snprintf(ee, sizeof(ee),
		     ("FILE (%s) ASSIGNED WITH DUMMY STRIPE,"
		      " missing striping information"), file);
	    kmr_warning(mr, 5, ee);
	}
	stripe->s.size = (uint32_t)mr->file_io_block_size;
	stripe->s.count = (uint16_t)MIN(colorsetsize, 20000);
	stripe->s.offset = 0;
	assert((stripe->s.count * 64) < 0x10000);
	for (int i = 0; i < stripe->s.count; i++) {
	    stripe->obdidx[i] = (uint16_t)(i * 64);
	}
    }
    return MPI_SUCCESS;
}

/* Shares striping information. */

static int
kmr_share_striping_information(KMR *mr, char *file, int color,
			       struct kmr_file_reader *sgv, int colorsetsize,
			       _Bool leader,
			       _Bool ingesting, _Bool digesting,
			       struct kmr_fefs_stripe *stripe)
{
    int cc;
    _Bool active = (colorsetsize != 0);
    if (ingesting) {
	cc = kmr_get_stripe(mr, file, colorsetsize, leader, stripe);
	assert(cc == MPI_SUCCESS);
    }
    KMR_KVS *kvs4 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    if (ingesting && leader) {
	size_t vlen = (kmr_fefs_stripe_info_size
		       + (sizeof(uint16_t) * stripe->s.count));
	struct kmr_kv_box nkv = {
	    .klen = (int)sizeof(long),
	    .vlen = (int)vlen,
	    .k.i = color,
	    .v.p = (void *)stripe
	};
	cc = kmr_add_kv(kvs4, nkv);
	assert(cc == MPI_SUCCESS);
    }
    kmr_add_kv_done(kvs4);
    KMR_KVS *kvs5 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    cc = kmr_replicate(kvs4, kvs5, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    if (active && !ingesting) {
	struct kmr_kv_box key = {
	    .klen = (int)sizeof(long),
	    .vlen = 0,
	    .k.i = color,
	    .v.p = 0
	};
	struct kmr_kv_box kv;
	cc = kmr_find_key(kvs5, key, &kv);
	assert(cc == MPI_SUCCESS);
	memcpy(stripe, kv.v.p, (size_t)kv.vlen);
    }
    cc = kmr_free_kvs(kvs5);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/* Assigns each rank a unit in a stripe, and sets RANKINDEXES which
   holds an index to a unit in a stripe for each rank.  Note it may
   fix striping information to meet the number of readers. */

static int
kmr_assign_ranks_to_stripe(KMR *mr, char *file,
			   struct kmr_file_reader *sgv, int colorsetsize,
			   _Bool leader,
			   struct kmr_fefs_stripe *stripe)
{
    _Bool active = (colorsetsize != 0);
    //int nprocs = mr->nprocs;
    if (active) {
	//int nprocs = mr->nprocs;
	int nreaders = 0;
	for (int k = 0; k < colorsetsize; k++) {
	    if (sgv[k].ingesting) {
		nreaders++;
	    }
	}
	assert(nreaders > 0);
	if ((stripe->s.count > nreaders) || (stripe->s.count == 0)) {
	    if ((stripe->s.count == 0) && leader) {
		char ee[160];
		snprintf(ee, sizeof(ee),
			 ("FILE (%s) ASSIGNED TO ARBITRARY READERS,"
			  "  no striping information"), file);
		kmr_warning(mr, 5, ee);
	    }
	    if ((stripe->s.count > nreaders) && leader) {
		char ee[160];
		snprintf(ee, sizeof(ee),
			 ("FILE (%s) ASSIGNED TO ARBITRARY READERS,"
			  " more stripes than ranks"), file);
		kmr_warning(mr, 5, ee);
	    }

	    /* Assign readers arbitrary. */

	    stripe->s.count = (uint16_t)nreaders;
	    int sindex = 0;
	    for (int k = 0; k < colorsetsize; k++) {
		if (sgv[k].ingesting) {
		    //int r = sgv[k].rank;
		    sgv[k].stripe = sindex;
		    sindex++;
		}
	    }
	    assert(sindex == stripe->s.count);
	} else {
	    _Bool assigned[stripe->s.count];
	    int nassigned = 0;
	    for (int i = 0; i < stripe->s.count; i++) {
		assigned[i] = 0;
	    }
	    for (int i = 0; i < stripe->s.count; i++) {
		int iog = kmr_iogroup_of_obd(stripe->obdidx[i]);
		for (int k = 0; k < colorsetsize; k++) {
		    //int r = sgv[k].rank;
		    int g = sgv[k].iogroup;
		    if (sgv[k].ingesting && iog == g && sgv[k].stripe != -1) {
			sgv[k].stripe = i;
			assigned[i] = 1;
			nassigned++;
			break;
		    }
		}
	    }
	    if ((nassigned < stripe->s.count) && leader) {
		char ee[160];
		snprintf(ee, sizeof(ee),
			 ("FILE (%s) ASSIGNED TO ARBITRARY READERS,"
			  " some stripes not covered by ranks"), file);
		kmr_warning(mr, 5, ee);
	    }

	    /* Assign readers to remaining units in a stripe. */

	    if (nassigned < stripe->s.count) {
		for (int i = 0; i < stripe->s.count; i++) {
		    if (!assigned[i]) {
			for (int k = 0; k < colorsetsize; k++) {
			    //int r = sgv[k].rank;
			    if (sgv[k].ingesting && sgv[k].stripe == -1) {
				sgv[k].stripe = i;
				assigned[i] = 1;
				nassigned++;
				break;
			    }
			}
		    }
		}
	    }
	    assert(nassigned == stripe->s.count);
	}
    }
    for (int k = 0; k < colorsetsize; k++) {
	//int r = sgv[k].rank;
	if (sgv[k].stripe != -1) {
	    sgv[k].offset = ((int)stripe->s.size * sgv[k].stripe);
	} else {
	    sgv[k].offset = 0;
	}
    }
    return MPI_SUCCESS;
}

/** Reads one file by segments and reassembles by all-gather.  FILE is
    a file name.  COLOR groups ranks (be COLOR>=-1).  The ranks with
    the same COLOR collaborate to read a file, and thus, they must
    specify the same file (with an identical inode number).  BUFFER
    and SIZE are set to the malloced buffer and the size on return.
    Ranks with non-zero FILE retrieve a file (ingest).  Ranks with
    non-zero BUFFER receive contents (digest).  Ranks with COLOR=-1 do
    not participate in file reading, and then arguments should be
    FILE=0 and BUFFER=0.  */

int
kmr_read_file_by_segments(KMR *mr, char *file, int color,
			  void **buffer, off_t *size)
{
    assert((color != -1) == ((file != 0) || (buffer != 0)));
    assert(color >= -1);
    int cc;
    //const int nprocs = mr->nprocs;
    //const int rank = mr->rank;
    _Bool ingesting = (file != 0);
    _Bool digesting = (buffer != 0);
    _Bool active = (color != -1);

    /* Collect read segment information of each node. */

    struct kmr_file_reader *sgv;
    int colorsetsize;
    int colorindex;
    const off_t offset = 0;
    const int bytes = -1;
    cc = kmr_share_segment_information(mr, file, color, ingesting, digesting,
				       offset, bytes,
				       &sgv, &colorsetsize, &colorindex);
    assert(cc == MPI_SUCCESS);
    assert(active == (sgv != 0) && active == (colorsetsize != 0));
    _Bool head = ((colorindex != -1) ? sgv[colorindex].head : 0);

    /* Check inode number for all the ranks access the same fail. */

    off_t totalsize = 0;
    ino_t ino = 0;
    for (int k = 0; k < colorsetsize; k++) {
	if (sgv[k].ingesting) {
	    if (totalsize == 0) {
		ino = sgv[k].ino;
		totalsize = sgv[k].reads;
		break;
	    }
	}
    }
    for (int k = 0; k < colorsetsize; k++) {
	if (sgv[k].ingesting) {
	    if (ino != sgv[k].ino) {
		char ee[160];
		snprintf(ee, sizeof(ee), "File (%s) with different ino", file);
		kmr_error(mr, ee);
	    }
	    if (totalsize != sgv[k].reads) {
		char ee[160];
		snprintf(ee, sizeof(ee), "File (%s) returns different sizes", file);
		kmr_error(mr, ee);
	    }
	}
    }

    /* Share striping information. */

    struct kmr_fefs_stripe stripe = {.s = kmr_bad_stripe};
    cc = kmr_share_striping_information(mr, file, color,
					sgv, colorsetsize,
					head, ingesting, digesting,
					&stripe);
    assert(cc == MPI_SUCCESS);

    /* Assign ranks to units in a stripe. */

    cc = kmr_assign_ranks_to_stripe(mr, file, sgv, colorsetsize, head,
				    &stripe);
    assert(cc == MPI_SUCCESS);

    /* Determine repeat count of reads. */

    long maxloops = 0;
    cc = kmr_take_maximum_loop_count(mr, totalsize, &stripe, &maxloops);
    assert(cc == MPI_SUCCESS && maxloops > 0);

    char *b = kmr_malloc((size_t)totalsize);
    {
	char *tmpbuf = kmr_malloc((size_t)stripe.s.size);
	int fd = -1;
	if (colorindex != -1 && sgv[colorindex].stripe != -1) {
	    do {
		fd = open(file, O_RDONLY, 0);
	    } while (fd == -1 && errno == EINTR);
	    if (fd == -1) {
		char ee[160];
		char *m = strerror(errno);
		snprintf(ee, sizeof(ee), "open(%s): %s", file, m);
		kmr_error(mr, ee);
	    }
	}

	/* Cycle reading one stripe. */

	cc = kmr_read_and_gather(mr, 0, file, fd, offset,
				 b, totalsize, tmpbuf, &stripe, maxloops,
				 sgv, colorsetsize, colorindex);
	assert(cc == MPI_SUCCESS);

	if (colorindex != -1 && sgv[colorindex].stripe != -1) {
	    do {
		cc = close(fd);
	    } while (cc == -1 && errno == EINTR);
	    if (cc != 0) {
		char ee[160];
		char *m = strerror(errno);
		snprintf(ee, sizeof(ee), "close(%s): %s", file, m);
		kmr_error(mr, ee);
	    }
	}
	kmr_free(tmpbuf, (size_t)stripe.s.size);
    }
    if (sgv != 0) {
	kmr_free(sgv, (sizeof(struct kmr_file_reader) * (size_t)colorsetsize));
    }
    if (buffer != 0) {
	*buffer = b;
    } else {
	kmr_free(b, (size_t)totalsize);
    }
    *size = totalsize;
    return MPI_SUCCESS;
}

/* ================================================================ */

/* (MP-MPI MAP VARIANT 1.  MR_map).  Nothing provided. */

/* (MP-MPI MAP VARIANT 2.  MR_map_file). */

#define COMINGSOON 0

/** Adds file names in a key-value stream KVO.  It checks the file
    name NAMES[i] exists, and adds it for a regular file, or
    enumerates it for a directory. */

int
kmr_file_enumerate(KMR *mr, char *names[], int n, KMR_KVS *kvo,
		   struct kmr_file_option fopt)
{
    assert(!fopt.list_file || fopt.subdirectories);
    for (int i = 0; i < n; i++) {
	char *path = names[i];
	struct stat s;
	int cc;
	do {
	    cc = stat(path, &s);
	} while (cc == -1 && errno == EINTR);
	if (cc == 0) {
	    if (!(S_ISREG(s.st_mode) || S_ISDIR(s.st_mode))) {
		char *m;
		if (S_ISFIFO(s.st_mode)) {
		    m = "fifo special";
		} else if (S_ISCHR(s.st_mode)) {
		    m = "character special";
		} else if (S_ISDIR(s.st_mode)) {
		    m = "directory";
		} else if (S_ISBLK(s.st_mode)) {
		    m = "block special";
		} else if (S_ISREG(s.st_mode)) {
		    m = "regular file";
		} else {
		    m = "unknown";
		}
		char ee[160];
		snprintf(ee, sizeof(ee), "Path (%s): type is %s", path, m);
		kmr_error(mr, ee);
	    } else {
		/* Follow to main work. */
	    }
	} else if (errno == ENOENT) {
	    /* Non-existent. */
	    char ee[160];
	    snprintf(ee, sizeof(ee), "Path (%s): nonexistent", path);
	    kmr_error(mr, ee);
	    assert(errno != ENOENT);
	} else if (errno == EACCES
		   || errno == ELOOP
		   || errno == ENOLINK
		   || errno == ENOTDIR) {
	    /* Non-existent (Really, inaccessible). */
	    char ee[160];
	    char *m = strerror(errno);
	    snprintf(ee, sizeof(ee), "stat(%s): %s", path, m);
	    kmr_error(mr, ee);
	} else {
	    /* STAT errs. */
	    assert(cc == -1);
	    char ee[160];
	    char *m = strerror(errno);
	    snprintf(ee, sizeof(ee), "stat(%s): %s", path, m);
	    kmr_error(mr, ee);
	}

	/* Process file/directory entry. */

	if (S_ISREG(s.st_mode)) {
	    /* Existent and accessible, regular file. */
	    if (fopt.list_file) {
		FILE *f = 0;
		do {
		    f = fopen(path, "r");
		} while (f == 0 && errno == EINTR);
		if (f == 0) {
		    char ee[160];
		    char *e = strerror(errno);
		    snprintf(ee, sizeof(ee), "fopen(%s): %s", path, e);
		    kmr_error(mr, ee);
		}
		char line[MAXPATHLEN];
		while ((fgets(line, sizeof(line), f)) != 0) {
		    char *e = strchr(line, '\n');
		    /* Drop a newline. */
		    if (e != 0) {
			*e = 0;
		    } else {
			char ee[160];
			if (feof(f)) {
			    snprintf(ee, sizeof(ee),
				     ("File (%s) misses newline"
				      " at the end"), path);
			    kmr_warning(mr, 5, ee);
			} else {
			    snprintf(ee, sizeof(ee),
				     "File (%s) misses newline", path);
			    kmr_error(mr, ee);
			}
		    }
		    if (e != line) {
			e--;
			while (e != line && *e == ' ') {
			    e--;
			}
			if (e != line) {
			    e++;
			    assert(*e == 0 || *e == ' ');
			}
			*e = 0;
		    }
		    char *p = line;
		    while (*p != 0 && *p == ' ') {
			p++;
		    }
		    if (*p == 0 || *p == '#') {
			continue;
		    }
		    /* Suppress list_file, recursing is ill. */
		    struct kmr_file_option foptx = fopt;
		    assert(foptx.list_file);
		    foptx.list_file = 0;
		    char *na[] = {p};
		    cc = kmr_file_enumerate(mr, na, 1, kvo, foptx);
		    assert(cc == MPI_SUCCESS);
		}
		if (ferror(f)) {
		    char ee[160];
		    char *e = strerror(fileno(f));
		    snprintf(ee, sizeof(ee), "fgets(%s): %s", path, e);
		    kmr_error(mr, ee);
		}
	    } else {
		size_t len = (strlen(path) + 1);
		struct kmr_kv_box nkv = {.klen = (int)len,
					 .vlen = (int)sizeof(long),
					 .k.p = path,
					 .v.i = 0};
		cc = kmr_add_kv(kvo, nkv);
		assert(cc == MPI_SUCCESS);
	    }
	} else if (S_ISDIR(s.st_mode)) {
	    /* Existent and accessible, directory. */
	    if (fopt.subdirectories) {
		size_t dsz;
		long nmax = pathconf(path, _PC_NAME_MAX);
		if (nmax == -1) {
		    dsz = (64 * 1024);
		} else {
		    dsz = (offsetof(struct dirent, d_name) + (size_t)(nmax + 1));
		}
		DIR *d;
		struct dirent *dp;
		char b[dsz];
		d = opendir(path);
		if (d == 0) {
		    char ee[160];
		    char *m = strerror(errno);
		    snprintf(ee, sizeof(ee), "opendir(%s): %s", path, m);
		    kmr_error(mr, ee);
		}
		errno = 0;
		while ((cc = readdir_r(d, (void *)b, &dp)) == 0) {
		    char subdir[MAXPATHLEN];
		    if (dp == 0) {
			if (errno != 0) {
			    char ee[160];
			    char *m = strerror(errno);
			    snprintf(ee, sizeof(ee),
				     "readdir_r(%s): %s", path, m);
			    kmr_error(mr, ee);
			}
			/* End of records. */
			break;
		    }
		    if (*(dp->d_name) == '.') {
			continue;
		    }
		    cc = snprintf(subdir, sizeof(subdir),
				  "%s/%s", path, dp->d_name);
		    if (cc > ((int)sizeof(subdir) - 1)) {
			char ee[160];
			snprintf(ee, sizeof(ee),
				 "Path (%s): too long", subdir);
			kmr_error(mr, ee);
		    }
		    char *na[] = {subdir};
		    cc = kmr_file_enumerate(mr, na, 1, kvo, fopt);
		    assert(cc == MPI_SUCCESS);
		}
		do {
		    cc = closedir(d);
		} while (cc == -1 && errno == EINTR);
		if (cc != 0) {
		    char ee[160];
		    char *m = strerror(errno);
		    snprintf(ee, sizeof(ee), "closedir(%s): %s", path, m);
		    kmr_error(mr, ee);
		}
	    } else {
		/* Just ignore directories. */
	    }
	} else {
	    assert(NEVERHERE);
	}
    }
    return MPI_SUCCESS;
}

/** Maps on file names.  NAMES specifies N file names.  The
    map-function gets a file name in the key field (the value field is
    integer zero).  File-option EACH_RANK specifies each rank
    independently to enumerate file names, otherwise to work on rank0
    only.  File-option SUBDIRECTORIES specifies to descend to
    subdirectories.  It ignores files/directories whose name starting
    with dots.  File-option LIST_FILE specifies to read contents of
    each file for file names.  File consists of one file name per
    line, and ignores a line beginning with a "#".  Whitespaces are
    trimed at the beginning and the end.  LIST_FILE implies
    SUBDIRECTORIES.  It enumerates names of regular files only.
    File-option SHUFFLE_FILES runs shuffling file names among
    ranks. */

int
kmr_map_file_names(KMR *mr, char **names, int n, struct kmr_file_option fopt,
		   KMR_KVS *kvo, void *arg,
		   struct kmr_option opt, kmr_mapfn_t m)
{
    struct kmr_option kmr_supported = {.nothreading = 1, .keep_open = 1};
    kmr_check_fn_options(mr, kmr_supported, opt, __func__);
    //const int nprocs = mr->nprocs;
    const int rank = mr->rank;
    int cc;
    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
    if (fopt.each_rank || rank == 0) {
	struct kmr_file_option foptx = fopt;
	if (foptx.list_file) {
	    foptx.subdirectories = 1;
	}
	cc = kmr_file_enumerate(mr, names, n, kvs0, foptx);
    } else {
	/*assert(n == 0);*/
    }
    kmr_add_kv_done(kvs0);
    KMR_KVS *kvs1;
    if (fopt.shuffle_names) {
	kvs1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER);
	cc = kmr_shuffle(kvs0, kvs1, kmr_noopt);
	assert(cc == MPI_SUCCESS);
    } else {
	kvs1 = kvs0;
    }
    cc = kmr_map(kvs1, kvo, arg, opt, m);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/* (MP-MPI MAP Variant 3.  MR_map_file_char).  Nothing provided. */

/* (MP-MPI MAP VARIANT 4.  MR_map_file_str). Nothing provided. */

#if 0

/* Maps on file contents.  Mapper first makes chunks of the file
   contents with roughly equal sizes, then maps on them.  It chunks
   files on rank0 only.  NCHUNKS is the number of total chunks.  NAMES
   specifies N file names.  SUBDIRS specifies to descend
   subdirectories.  LISTFILE specifies to read contents of each file
   for file names.  File consists of one file name per line.  DELTAUB
   is the upper bound of the number of bytes skipped before the next
   separator is found.  Note the flag RANK0 does not exist in contrast
   to this mapper.  */

int
kmr_map_file_contents(KMR *mr, int nchunks, char **names, int n,
		      _Bool subdirs, _Bool listfile,
		      char *sep, long deltaub,
		      KMR_KVS *kvo, void *arg, struct kmr_option opt,
		      kmr_mapfn_t m)
{
    assert(COMINGSOON);
    return MPI_SUCCESS;
}

#endif

/* (MP-MPI MAP VARIANT 5.  MR_map_mr).  Nothing provided. */

/* ================================================================ */

static int
kmr_map_getline_threading(KMR *mr, FILE *f, long limit, _Bool largebuffering,
			  KMR_KVS *kvo, void *arg,
			  struct kmr_option opt, kmr_mapfn_t m)
{
    int cc;

    int glineno;
    glineno = 0;

#ifdef _OPENMP
    const _Bool threading = !(mr->single_thread || opt.nothreading);
#endif
    KMR_OMP_PARALLEL_IF_(threading)
    {
	char *line;
	size_t linesz;
	line = 0;
	linesz = 0;
	long lineno;
	ssize_t rc;
	for (;;) {
	    KMR_OMP_CRITICAL_
	    {
		lineno = glineno;
		if (lineno < limit) {
		    rc = getline(&line, &linesz, f);
		    if (rc != -1) {
			glineno++;
		    }
		} else {
		    rc = 0;
		}
	    }

	    if (!(lineno < limit && rc != -1)) {
		break;
	    }

	    assert(rc <= INT_MAX);
	    struct kmr_kv_box kv = {
		.klen = sizeof(long),
		.vlen = (int)rc,
		.k.i = lineno,
		.v.p = line
	    };
	    cc = (*m)(kv, 0, kvo, arg, lineno);
	    if (cc != MPI_SUCCESS) {
		char ee[80];
		snprintf(ee, sizeof(ee),
			 "Map-fn returned with error cc=%d", cc);
		kmr_error(mr, ee);
	    }
	}
	if (rc == -1 && ferror(f)) {
	    char ee[80];
	    char *w = strerror(errno);
	    snprintf(ee, sizeof(ee),
		     "kmr_map_getline: getline at line %ld failed: %s",
		     lineno, w);
	    kmr_error(mr, ee);
	}

	if (line != 0) {
	    free(line);
	}
    }
    return MPI_SUCCESS;
}

static int
kmr_map_getline_nothreading(KMR *mr, FILE *f, long limit, _Bool largebuffering,
			    KMR_KVS *kvo, void *arg,
			    struct kmr_option opt, kmr_mapfn_t m)
{
    int cc;

    char *line = 0;
    size_t linesz = 0;
    long lineno = 0;
    ssize_t rc = 0;
    while ((lineno < limit) && (rc = getline(&line, &linesz, f)) != -1) {
	assert(rc <= INT_MAX);
	struct kmr_kv_box kv = {
	    .klen = sizeof(long),
	    .vlen = (int)rc,
	    .k.i = lineno,
	    .v.p = line
	};
	cc = (*m)(kv, 0, kvo, arg, lineno);
	if (cc != MPI_SUCCESS) {
	    char ee[80];
	    snprintf(ee, sizeof(ee),
		     "Map-fn returned with error cc=%d", cc);
	    kmr_error(mr, ee);
	}
	lineno++;
    }
    if (rc == -1 && ferror(f)) {
	char ee[80];
	char *w = strerror(errno);
	snprintf(ee, sizeof(ee),
		 "kmr_map_getline: getline at line %ld failed: %s",
		 lineno, w);
	kmr_error(mr, ee);
    }

    if (line != 0) {
	free(line);
    }

    return MPI_SUCCESS;
}

/** Calls a map-function M for each line by getline() on an input F.
    A map-function gets a line number in key and a string in value
    (the index argument is the same as the key).  Calls to getline()
    is limited to LIMIT lines (0 for unlimited).  It is multi-threaded
    and the call order is arbitrary.  ARG and OPT are passed verbatim
    to a map-function.  Effective-options: NOTHREADING, KEEP_OPEN,
    TAKE_CKPT.  See struct kmr_option. */

int
kmr_map_getline(KMR *mr, FILE *f, long limit, _Bool largebuffering,
		KMR_KVS *kvo, void *arg,
		struct kmr_option opt, kmr_mapfn_t m)
{
    kmr_assert_kvs_ok(0, kvo, 0, 1);
    struct kmr_option kmr_supported = {.nothreading = 1, .keep_open = 1,
                                       .take_ckpt = 1};
    kmr_check_fn_options(mr, kmr_supported, opt, __func__);

    if (kmr_ckpt_enabled(mr)) {
	if (kmr_ckpt_progress_init(0, kvo, opt)) {
	    if (kvo != 0 && !opt.keep_open) {
		kmr_add_kv_done(kvo);
	    }
	    return MPI_SUCCESS;
	}
    }

    size_t BFSZ = (8 * 1024 * 1024);
    int cc;

    if (f != 0) {
	if (limit == 0) {
	    limit = LONG_MAX;
	}

	char *b = 0;
	if (largebuffering) {
	    b = kmr_malloc(BFSZ);
	    cc = setvbuf(f, b, _IOFBF, BFSZ);
	    if (cc != 0) {
		char ee[80];
		char *w = strerror(errno);
		snprintf(ee, sizeof(ee), "%s: setvbuf failed (ignored): %s",
			 __func__, w);
		kmr_warning(mr, 5, ee);
		kmr_free(b, BFSZ);
		b = 0;
	    }
	}

	if (mr->single_thread || opt.nothreading) {
	    cc = kmr_map_getline_nothreading(mr, f, limit, largebuffering,
					     kvo, arg, opt, m);
	} else {
	    cc = kmr_map_getline_threading(mr, f, limit, largebuffering,
					   kvo, arg, opt, m);
	}

	if (b != 0) {
	    assert(largebuffering);
	    cc = setvbuf(f, 0, _IONBF, 0);
	    if (cc != 0) {
		char ee[80];
		char *w = strerror(errno);
		snprintf(ee, sizeof(ee),
			 "%s: setvbuf() at the end failed (ignored): %s",
			 __func__, w);
		kmr_warning(mr, 5, ee);
	    }
	    /* FREE BUFFER ANYWAY. */
	    kmr_free(b, BFSZ);
	}
    }

    if (kvo != 0 && !opt.keep_open) {
	kmr_add_kv_done(kvo);
    }

    if (kmr_ckpt_enabled(mr)) {
	kmr_ckpt_save_kvo_whole(mr, kvo);
	kmr_ckpt_progress_fin(mr);
    }

    return MPI_SUCCESS;
}

/* Calls a map-function M for each line in a memory buffer.
   (TENTATIVE).  It is equivalent to calling kmr_map_getline() on a
   stream returned by fmemopen().  It calls a mapper with a line
   pointing to the inside the buffer.  */

int
kmr_map_getline_in_memory_(KMR *mr, void *b, size_t sz, long limit,
			   KMR_KVS *kvo, void *arg,
			   struct kmr_option opt, kmr_mapfn_t m)
{
    kmr_assert_kvs_ok(0, kvo, 0, 1);
    struct kmr_option kmr_supported = {.nothreading = 1, .keep_open = 1,
                                       .take_ckpt = 1};
    kmr_check_fn_options(mr, kmr_supported, opt, __func__);
    int cc;

    if (kmr_ckpt_enabled(mr)) {
	if (kmr_ckpt_progress_init(0, kvo, opt)) {
	    if (kvo != 0 && !opt.keep_open) {
		kmr_add_kv_done(kvo);
	    }
	    return MPI_SUCCESS;
	}
    }

    if (limit == 0) {
	limit = LONG_MAX;
    }

    int glineno = 0;
    char *gline = b;
    char * const end = (gline + sz);

#ifdef _OPENMP
    const _Bool threading = !(mr->single_thread || opt.nothreading);
#endif
    KMR_OMP_PARALLEL_IF_(threading)
    {
	char *line = 0;
	size_t linesz = 0;
	long lineno = 0;
	ssize_t rc;
	for (;;) {
	    KMR_OMP_CRITICAL_
	    {
		if (!((gline < end) && (glineno < limit))) {
		    line = 0;
		    linesz = 0;
		    lineno = glineno;
		    rc = -1;
		} else {
		    char *p = gline;
		    while (p < end && *p != '\n') {
			p++;
		    }
		    if (p < end && *p == '\n') {
			p++;
		    }
		    line = gline;
		    linesz = (size_t)(p - gline);
		    lineno = glineno;
		    gline = p;
		    glineno++;
		    rc = (ssize_t)linesz;
		}
	    }

	    if (rc == -1) {
		break;
	    }

	    assert(rc <= INT_MAX);
	    struct kmr_kv_box kv = {
		.klen = sizeof(long),
		.vlen = (int)rc,
		.k.i = lineno,
		.v.p = line
	    };
	    cc = (*m)(kv, 0, kvo, arg, lineno);
	    if (cc != MPI_SUCCESS) {
		char ee[80];
		snprintf(ee, sizeof(ee),
			 "Map-fn returned with error cc=%d", cc);
		kmr_error(mr, ee);
	    }
	}
    }

    if (kvo != 0 && !opt.keep_open) {
	kmr_add_kv_done(kvo);
    }

    if (kmr_ckpt_enabled(mr)) {
	kmr_ckpt_save_kvo_whole(mr, kvo);
	kmr_ckpt_progress_fin(mr);
    }

    return MPI_SUCCESS;
}

/*
Copyright (C) 2012-2018 RIKEN R-CCS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/
