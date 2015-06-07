/* kmratoa.c (2014-02-04) */
/* Copyright (C) 2012-2015 RIKEN AICS */

/** \file kmratoa.c Communication Routines.  KMR makes almost all data
    exchanges through this.  Some exceptions are "kmrmapms.c" and
    "kmrfiles.c".  It provides operations with size_t data length. */

/* Used MPI routines: Alltoall, Alltoallv, Allgather, Allgatherv,
   Allreduce, Gatherv, Exscan. Irecv, Isend, Irsend, Sendrecv,
   Waitall. */

#include <mpi.h>
#include <stdlib.h>
#include <limits.h>
#include <errno.h>
#include <assert.h>
#include "kmr.h"
#include "kmrimpl.h"

#define MAX(a,b) (((a)>(b))?(a):(b))

static int kmr_alltoallv_mpi(KMR *mr, void *sbuf, long *scnts, long *sdsps,
			     void *rbuf, long *rcnts, long *rdsps);
static int kmr_alltoallv_bruck(KMR *mr, void *sbuf, long *scnts, long *sdsps,
			       void *rbuf, long *rcnts, long *rdsps);
static int kmr_alltoall_bruck(KMR *mr, void *sbuf, void *rbuf, int cnt);
static void kmr_atoa_dump_(KMR *mr, void *sbuf, int sz, char *title, int step);

/* Checks if X is power of two/four. */

static inline _Bool
kmr_powerof2_p(int x)
{
    return ((x > 0) && ((x & (x - 1)) == 0));
}

static inline _Bool
kmr_powerof4_p(int x)
{
    return (kmr_powerof2_p(x) && ((x & 0x2aaaaaaa) == 0));
}

/** Calls all-to-all to exchange one long-integer. */

int
kmr_exchange_sizes(KMR *mr, long *sbuf, long *rbuf)
{
    MPI_Comm comm = mr->comm;
    int cc;
    cc = MPI_Alltoall(sbuf, 1, MPI_LONG, rbuf, 1, MPI_LONG, comm);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/** Calls all-gather for collecting one long-integer. */

int
kmr_gather_sizes(KMR *mr, long siz, long *rbuf)
{
    MPI_Comm comm = mr->comm;
    int cc;
    cc = MPI_Allgather(&siz, 1, MPI_LONG, rbuf, 1, MPI_LONG, comm);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/** All-gathers data, or gathers data when RANKZEROONLY. */

int
kmr_allgatherv(KMR *mr, _Bool rankzeroonly, void *sbuf, long scnt,
	       void *rbuf, long *rcnts, long *rdsps)
{
    MPI_Comm comm = mr->comm;
    int nprocs = mr->nprocs;
    int self = mr->rank;
    int *rsz;
    int *rdp;
    if (!rankzeroonly || self == 0) {
	rsz = kmr_malloc(sizeof(int) * (size_t)nprocs);
	rdp = kmr_malloc(sizeof(int) * (size_t)nprocs);
	for (int r = 0; r < nprocs; r++) {
	    assert(INT_MIN <= rcnts[r] && rcnts[r] <= INT_MAX);
	    assert(INT_MIN <= rdsps[r] && rdsps[r] <= INT_MAX);
	    rsz[r] = (int)rcnts[r];
	    rdp[r] = (int)rdsps[r];
	}
    } else {
	rsz = 0;
	rdp = 0;
    }
    int cc;
    if (rankzeroonly) {
	cc = MPI_Gatherv(sbuf, (int)scnt, MPI_BYTE,
			 rbuf, rsz, rdp, MPI_BYTE, 0, comm);
	assert(cc == MPI_SUCCESS);
    } else {
	cc = MPI_Allgatherv(sbuf, (int)scnt, MPI_BYTE,
			    rbuf, rsz, rdp, MPI_BYTE, comm);
	assert(cc == MPI_SUCCESS);
    }
    if (rsz != 0) {
	kmr_free(rsz, (sizeof(int) * (size_t)nprocs));
    }
    if (rdp != 0) {
	kmr_free(rdp, (sizeof(int) * (size_t)nprocs));
    }
    return MPI_SUCCESS;
}

/* ================================================================ */

/** Does all-to-all-v, but it takes arguments of long-integers.
    Setting ATOA_THRESHOLD=0 forces to use MPI all-to-all-v. */

int
kmr_alltoallv(KMR *mr,
	      void *sbuf, long *scnts, long *sdsps,
	      void *rbuf, long *rcnts, long *rdsps)
{
    int nprocs = mr->nprocs;
    int cc;
    if (!kmr_powerof4_p(nprocs) || nprocs == 1 || mr->atoa_threshold == 0) {
	cc = kmr_alltoallv_mpi(mr, sbuf, scnts, sdsps,
			       rbuf, rcnts, rdsps);
	assert(cc == MPI_SUCCESS);
    } else {
	cc = kmr_alltoallv_bruck(mr, sbuf, scnts, sdsps,
				 rbuf, rcnts, rdsps);
	assert(cc == MPI_SUCCESS);
    }
    return MPI_SUCCESS;
}

/* Does all-to-all-v using MPI_Alltoallv.  It takes offsets upto 8 GB
   (we understand it is not enough).  It assumes data is 8-byte
   aligned. */

static int
kmr_alltoallv_mpi(KMR *mr,
		  void *sbuf, long *scnts, long *sdsps,
		  void *rbuf, long *rcnts, long *rdsps)
{
    MPI_Comm comm = mr->comm;
    int nprocs = mr->nprocs;
    int *ssz = kmr_malloc(sizeof(int) * (size_t)nprocs);
    int *sdp = kmr_malloc(sizeof(int) * (size_t)nprocs);
    int *rsz = kmr_malloc(sizeof(int) * (size_t)nprocs);
    int *rdp = kmr_malloc(sizeof(int) * (size_t)nprocs);

    for (int r = 0; r < nprocs; r++) {
	assert(INT_MIN * 8L <= scnts[r] && scnts[r] <= INT_MAX * 8L);
	assert(INT_MIN * 8L <= rcnts[r] && rcnts[r] <= INT_MAX * 8L);
	assert(INT_MIN * 8L <= sdsps[r] && sdsps[r] <= INT_MAX * 8L);
	assert(INT_MIN * 8L <= rdsps[r] && rdsps[r] <= INT_MAX * 8L);
	assert(((scnts[r] & 7) == 0)
	       && ((rcnts[r] & 7) == 0)
	       && ((sdsps[r] & 7) == 0)
	       && ((rdsps[r] & 7) == 0));
	ssz[r] = (int)(scnts[r] / 8L);
	rsz[r] = (int)(rcnts[r] / 8L);
	sdp[r] = (int)(sdsps[r] / 8L);
	rdp[r] = (int)(rdsps[r] / 8L);
    }
    int cc;
    cc = MPI_Alltoallv(sbuf, ssz, sdp, MPI_LONG,
		       rbuf, rsz, rdp, MPI_LONG, comm);
    assert(cc == MPI_SUCCESS);

    kmr_free(ssz, (sizeof(int) * (size_t)nprocs));
    kmr_free(rsz, (sizeof(int) * (size_t)nprocs));
    kmr_free(sdp, (sizeof(int) * (size_t)nprocs));
    kmr_free(rdp, (sizeof(int) * (size_t)nprocs));
    return MPI_SUCCESS;
}

/* Does all-to-all-v using Bruck's all-to-all when message size is
   small (less than ATOA_THRESHOLD). */

static int
kmr_alltoallv_bruck(KMR *mr,
		    void *sbuf, long *scnts, long *sdsps,
		    void *rbuf, long *rcnts, long *rdsps)
{
    MPI_Comm comm = mr->comm;
    int nprocs = mr->nprocs;
    int cc;
    char *sbuf0 = sbuf;
    char *rbuf0 = rbuf;

    long maxcnt = 0;
    for (int i = 0; i < nprocs; i++) {
	maxcnt = MAX(maxcnt, scnts[i]);
    }
    cc = MPI_Allreduce(MPI_IN_PLACE, &maxcnt, 1, MPI_LONG, MPI_MAX, comm);
    assert(cc == MPI_SUCCESS);

    if (maxcnt >= mr->atoa_threshold) {
	cc = kmr_alltoallv_mpi(mr, sbuf, scnts, sdsps,
			       rbuf, rcnts, rdsps);
	assert(cc == MPI_SUCCESS);
	/*cc = MPI_Alltoall(sb, maxcnt, MPI_BYTE, rb, maxcnt,
	  MPI_BYTE, comm);*/
    } else {
	char *sb = kmr_malloc((size_t)(maxcnt * nprocs));
	char *rb = kmr_malloc((size_t)(maxcnt * nprocs));
	for (int i = 0; i < nprocs; i++) {
	    memcpy(&sb[maxcnt * i], &sbuf0[sdsps[i]], (size_t)scnts[i]);
	}
	cc = kmr_alltoall_bruck(mr, sb, rb, (int)maxcnt);
	assert(cc == MPI_SUCCESS);
	for (int i = 0; i < nprocs; i++) {
	    memcpy(&rbuf0[rdsps[i]], &rb[maxcnt * i], (size_t)rcnts[i]);
	}
	kmr_free(sb, (size_t)(maxcnt * nprocs));
	kmr_free(rb, (size_t)(maxcnt * nprocs));
    }
    return MPI_SUCCESS;
}

#if 0
static int
kmr_alltoall_naive(KMR *mr, void *sbuf, void *rbuf, int cnt)
{
    MPI_Comm comm = mr->comm;
    int nprocs = mr->nprocs;
    int rank = mr->rank;
    int tag = KMR_TAG_ATOA;
    MPI_Request *rqs = kmr_malloc(sizeof(MPI_Request) * (size_t)(nprocs * 2));
    int cc;
    char *r = rbuf;
    for (int i = 0; i < nprocs; i++) {
	cc = MPI_Irecv(&r[i * cnt], cnt, MPI_BYTE,
		       i, tag, comm, &rqs[i]);
	assert(cc == MPI_SUCCESS);
    }
    char *s = sbuf;
    int peer;
    peer = (rank % nprocs);
    for (int i = 0; i < nprocs; i++) {
	peer++;
	if (peer >= nprocs) {
	    peer -= nprocs;
	}
	cc = MPI_Irsend(&s[peer * cnt], cnt, MPI_BYTE,
			peer, tag, comm, &rqs[nprocs + peer]);
	assert(cc == MPI_SUCCESS);
    }
    cc = MPI_Waitall((2 * nprocs), rqs, MPI_STATUSES_IGNORE);
    assert(cc == MPI_SUCCESS);
    kmr_free(rqs, (sizeof(MPI_Request) * (size_t)(nprocs * 2)));
    return MPI_SUCCESS;
}
#endif

/* Does all-to-all, using Bruck-like butter-fly pattern. */

static int
kmr_alltoall_bruck(KMR *mr, void *sbuf, void *rbuf, int cnt)
{
#define DUMP_(X0,X1,X2,X3,X4) if (tracing) kmr_atoa_dump_(X0,X1,X2,X3,X4)
    MPI_Comm comm = mr->comm;
    int nprocs = mr->nprocs;
    int rank = mr->rank;
    int tag = KMR_TAG_ATOA;
    _Bool tracing = mr->trace_alltoall;
    assert((nprocs & 3) == 0);
    int nprocs4th = (nprocs / 4);
    int cc;

    int lognprocs = 0;
    while ((1 << lognprocs) < nprocs) {
	lognprocs++;
    }
    assert((1 << lognprocs) == nprocs);

    char *buf0 = kmr_malloc((size_t)(cnt * nprocs));
    char *buf1 = kmr_malloc((size_t)(cnt * nprocs));
    memcpy(buf0, sbuf, (size_t)(cnt * nprocs));

    MPI_Request rqs[6];
    for (int stage = 0; stage < lognprocs; stage += 2) {
	DUMP_(mr, buf0, cnt, "step", stage);
	for (int j = 0; j < nprocs4th; j++) {
	    for (int i = 0; i < 4; i++) {
		void *s = &buf0[cnt * (i + (j * 4))];
		void *r = &buf1[cnt * (nprocs4th * i + j)];
		memcpy(r, s, (size_t)cnt);
	    }
	}
	DUMP_(mr, buf1, cnt, "pack", stage);
	for (int k = 0; k < 4; k++) {
	    int flip = (k << stage);
	    int peer = (rank ^ flip);
	    int baserank = ((rank >> stage) & 3);
	    int basepeer = ((peer >> stage) & 3);
	    if (k == 0) {
		void *s = &buf1[cnt * (baserank * nprocs4th)];
		void *r = &buf0[cnt * (baserank * nprocs4th)];
		memcpy(r, s, (size_t)(cnt * nprocs4th));
	    } else {
		void *s = &buf1[cnt * (basepeer * nprocs4th)];
		void *r = &buf0[cnt * (basepeer * nprocs4th)];
#if 0
		cc = MPI_Sendrecv(s, (cnt * nprocs4th), MPI_BYTE, peer, tag,
				  r, (cnt * nprocs4th), MPI_BYTE, peer, tag,
				  comm, MPI_STATUS_IGNORE);
		assert(cc == MPI_SUCCESS);
#else
		cc = MPI_Isend(s, (cnt * nprocs4th), MPI_BYTE, peer, tag,
			       comm, &rqs[(k - 1) * 2 + 1]);
		assert(cc == MPI_SUCCESS);
		cc = MPI_Irecv(r, (cnt * nprocs4th), MPI_BYTE, peer, tag,
			       comm, &rqs[(k - 1) * 2]);
		assert(cc == MPI_SUCCESS);
#endif
	    }
	}
	cc = MPI_Waitall(6, rqs, MPI_STATUSES_IGNORE);
	assert(cc == MPI_SUCCESS);
	DUMP_(mr, buf0, cnt, "exchange", stage);
    }
    memcpy(rbuf, buf0, (size_t)(cnt * nprocs));
    kmr_free(buf0, (size_t)(cnt * nprocs));
    kmr_free(buf1, (size_t)(cnt * nprocs));
    return MPI_SUCCESS;
}

/* Displays buffer contents (first byte) in the middle of all-to-all.
   It does nothing when the number of ranks is large. */

static void
kmr_atoa_dump_(KMR *mr, void *sbuf, int sz, char *title, int step)
{
    MPI_Comm comm = mr->comm;
    int nprocs = mr->nprocs;
    int rank = mr->rank;
    int cc;
    if (nprocs <= 64) {
	char *xbuf;
	if (rank == 0) {
	    xbuf = malloc((size_t)(sz * nprocs * nprocs));
	    assert(xbuf != 0);
	} else {
	    xbuf = 0;
	}
	cc = MPI_Gather(sbuf, (sz * nprocs), MPI_BYTE,
			xbuf, (sz * nprocs), MPI_BYTE,
			0, comm);
	assert(cc == MPI_SUCCESS);
	if (rank == 0) {
	    fprintf(stderr, ";;KMR %s (%d)\n", title, step);
	    for (int j = 0; j < nprocs; j++) {
		fprintf(stderr, ";;KMR ");
		for (int i = 0; i < nprocs; i++) {
		    fprintf(stderr, "%02x ",
			    (0xff & xbuf[(i * (sz * nprocs)) + (j * sz)]));
		}
		fprintf(stderr, "\n");
	    }
	    fprintf(stderr, ";;KMR\n");
	    fflush(0);
	}
	if (xbuf != 0) {
	    free(xbuf);
	}
	MPI_Barrier(comm);
    }
}

/* ================================================================ */

#if 0
int
kmr_exscan(void *sbuf, void *rbuf, int cnt, MPI_Datatype dt, MPI_Op op,
	   MPI_Comm comm)
{
    const int SCANTAG = 60;
    MPI_Comm comm = kvs->c.mr->comm;
    int nprocs = kvs->c.mr->nprocs;
    int self = kvs->c.mr->rank;
    int cc;
    /*cc = MPI_Exscan(sbuf, rbuf, cnt, dt, op, comm);*/
    for (int stage = 1; stage < nprocs; stage <<= 1) {
	int peer = (self ^ stage);
	if (peer < nprocs) {
	    cc = MPI_Sendrecv(&ssz, 1, MPI_LONG, peer, SCANTAG,
			      &rsz, 1, MPI_LONG, peer, SCANTAG,
			      comm, MPI_STATUS_IGNORE);
	    assert(cc == MPI_SUCCESS);
	    cc = MPI_Sendrecv(sbuf, ssz, MPI_BYTE, peer, SCANTAG,
			      rbuf, rsz, MPI_BYTE, peer, SCANTAG,
			      comm, MPI_STATUS_IGNORE);
	    assert(cc == MPI_SUCCESS);
	    if (self > peer) {
		/* Do not include the first element of segment. */
		if ((self & (stage - 1)) != 0) {
		    kmr_add_kv_vector(kvo, rbuf, rsz);
		}
	    }
	    /* reducevalue*=xbuf */
	    if (commute || self > peer) {
		kmr_add_kv_vector(kvs, rbuf, rsz);
	    } else {
		/* PUT AT FRONT */
		kmr_add_kv_vector(kvs, rbuf, rsz);
	    }
	}
	if (kvs->element_count > threshold) {
	    reduce();
	}
    }
    return MPI_SUCCESS;
}
#endif

/*
Copyright (C) 2012-2015 RIKEN AICS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/
