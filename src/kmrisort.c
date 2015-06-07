/* kmrisort.c (2014-02-04) */
/* Copyright (C) 2012-2015 RIKEN AICS */
/* Copyright (c) 1992, 1993 The Regents of the University of California. */

/* $NetBSD: qsort.c,v 1.15 2008/03/11 18:04:59 rmind Exp $ */

/*-
 * Copyright (c) 1992, 1993
 *	The Regents of the University of California.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/** \file kmrisort.c Sorter on Long Integers.  This sorting is
    NOT-STABLE.  This file is a copy of "qsort.c" from NetBSD-5.1, and
    copyrighted by The Regents of the University of California.  It is
    deoptimized by removing the code to gather equal keys, due to
    not-fast 3-way comparison (that returns -1/0/1) on K. */

#include <mpi.h>
#include <sys/types.h>
#include <assert.h>
#include <errno.h>
#ifdef _OPENMP
#include <omp.h>
#endif
#include "kmr.h"
#include "kmrimpl.h"

#define MAX(a,b) (((a)>(b))?(a):(b))

#if defined(_OPENMP) && (_OPENMP >= 201106) && !defined(KMROMP30)
/* OK. */
#elif defined(_OPENMP)
#pragma message ("Compiler seems not support OMP 3.1")
#else
#pragma message ("Compiler seems not support OMP")
#endif

/* kmr_isort only looks at the first entry (long v).  It should match
   the type of struct kmr_sort_record. */

struct kmr_isort_entry {
    long v;
    char _[1];
};

static inline ptrdiff_t
kmr_min(ptrdiff_t a, intptr_t b) {return ((a < b) ? a : b);}

/* It returns 0 for long, 1 for long-aligned, or 2 for unaligned. */

static inline int
kmr_swaptype(void *a, size_t es)
{
    return ((((size_t)(intptr_t)a % sizeof(long) != 0)
	     || (es % sizeof(long) != 0))
	    ? 2 : (es == sizeof(long) ? 0 : 1));
}

static inline void
kmr_swapbytype(void *a, void *b, size_t sz, int swaptype)
{
    if (swaptype <= 1) {
	long *p = a;
	long *q = b;
	size_t i = (sz / sizeof(long));
	do {
	    long t = *p; *p = *q; *q = t;
	    p++; q++;
	} while (--i > 0);
    } else {
	char *p = a;
	char *q = b;
	size_t i = (sz / sizeof(char));
	do {
	    char t = *p; *p = *q; *q = t;
	    p++; q++;
	} while (--i > 0);
    }
}

static inline void
kmr_vecswap(void *a, void *b, size_t sz, int swaptype)
{
    if (sz > 0) {
	kmr_swapbytype(a, b, sz, swaptype);
    }
}

static inline void
kmr_swap(void *a, void *b, size_t ES, int swaptype)
{
    if (swaptype == 0) {
	long *p = a;
	long *q = b;
	long t = *p; *p = *q; *q = t;
    } else {
	kmr_swapbytype(a, b, ES, swaptype);
    }
}

/* Compares in two-way; it never returns 0.  This variation returns
   POSITIVE on equals. */

static inline int
KMR_CMP2P(void *A, void *B)
{
    struct kmr_isort_entry *a = A;
    struct kmr_isort_entry *b = B;
    /*return ((a->v == b->v) ? 0 : ((a->v < b->v) ? -1 : 1));*/
    return ((a->v < b->v) ? -1 : 1);
}

/* Compares in two-way; it never returns 0.  This variation returns
   NEGATIVE on equals. */

static inline int
KMR_CMP2N(void *A, void *B)
{
    struct kmr_isort_entry *a = A;
    struct kmr_isort_entry *b = B;
    /*return ((a->v == b->v) ? 0 : ((a->v < b->v) ? -1 : 1));*/
    return ((a->v <= b->v) ? -1 : 1);
}

static inline void *
kmr_meddleof(void *a, void *b, void *c)
{
    return (KMR_CMP2P(a, b) < 0
	    ? (KMR_CMP2P(b, c) < 0 ? b : (KMR_CMP2P(a, c) < 0 ? c : a))
	    : (KMR_CMP2P(b, c) > 0 ? b : (KMR_CMP2P(a, c) < 0 ? a : c)));
}

static inline char *
kmr_medianof(char *A, const size_t N, const size_t ES)
{
    char *pm = A + (N / 2) * ES;
    if (N > 7) {
	char *pl = A;
	char *pn = A + (N - 1) * ES;
	if (N > 40) {
	    size_t d = (N / 8) * ES;
	    pl = kmr_meddleof(pl, (pl + d), (pl + 2 * d));
	    pm = kmr_meddleof((pm - d), pm, (pm + d));
	    pn = kmr_meddleof((pn - 2 * d), (pn - d), pn);
	}
	pm = kmr_meddleof(pl, pm, pn);
    }
    return pm;
}

static inline void
kmr_subsort(char *A, size_t N, const size_t ES, int swaptype)
{
    for (char *p0 = A + ES; p0 < A + N * ES; p0 += ES) {
	for (char *p1 = p0; p1 > A && KMR_CMP2N((p1 - ES), p1) > 0; p1 -= ES) {
	    kmr_swap(p1, (p1 - ES), ES, swaptype);
	}
    }
}

static void
kmr_isort0(void *A0, size_t N, const size_t ES, int depth)
{
    char *A = A0;
    /*LOOP:*/
    assert(A != 0);
    int swaptype = kmr_swaptype(A, ES);
    int swapoccur = 0;
    if (N < 10) {
	kmr_subsort(A, N, ES, swaptype);
	return;
    }
    char *pm = kmr_medianof(A, N, ES);
    kmr_swap(A, pm, ES, swaptype);

    char *pa = A + ES;
    char *pb = pa;
    char *pc = A + (N - 1) * ES;
    char *pd = pc;
    for (;;) {
	while (pb <= pc && KMR_CMP2P(pb, A) <= 0) {
	    if (0) {
		/* EQUAL CASE HANDLING. */
		swapoccur = 1;
		kmr_swap(pa, pb, ES, swaptype);
		pa += ES;
	    }
	    pb += ES;
	}
	while (pb <= pc && KMR_CMP2N(pc, A) >= 0) {
	    if (0) {
		/* EQUAL CASE HANDLING. */
		swapoccur = 1;
		kmr_swap(pc, pd, ES, swaptype);
		pd -= ES;
	    }
	    pc -= ES;
	}
	if (pb > pc) {
	    break;
	}
	kmr_swap(pb, pc, ES, swaptype);
	swapoccur = 1;
	pb += ES;
	pc -= ES;
    }
    if (swapoccur == 0) {
	kmr_subsort(A, N, ES, swaptype);
	return;
    }

    char *pn = A + N * ES;

    if (0) {
	/* EQUAL CASE HANDLING. */
	size_t r0 = (size_t)kmr_min(pa - A, pb - pa);
	kmr_vecswap(A, (pb - r0), r0, swaptype);
	size_t r1 = (size_t)kmr_min((pd - pc), (pn - pd - (ptrdiff_t)ES));
	kmr_vecswap(pb, (pn - r1), r1, swaptype);
	assert(r0 == ES);
	assert(r1 == 0);
    } else {
	kmr_swap(A, (pb - ES), ES, swaptype);
    }

    size_t r2 = (size_t)(pb - pa);
    if (r2 > ES) {
#if defined(_OPENMP) && (_OPENMP >= 201106) && !defined(KMROMP30)
	KMR_OMP_TASK_FINAL_(depth == 0)
	    kmr_isort0(A, (r2 / ES), ES, MAX((depth - 1), 0));
#else
	if (depth > 0) {
	    KMR_OMP_TASK_
		kmr_isort0(A, (r2 / ES), ES, MAX((depth - 1), 0));
	} else {
	    kmr_isort0(A, (r2 / ES), ES, MAX((depth - 1), 0));
	}
#endif
    }
    size_t r3 = (size_t)(pd - pc);
    if (r3 > ES) {
#if defined(_OPENMP) && (_OPENMP >= 201106) && !defined(KMROMP30)
	KMR_OMP_TASK_FINAL_(depth == 0)
	    kmr_isort0((pn - r3), (r3 / ES), ES, MAX((depth - 1), 0));
#else
	if (depth > 0) {
	    KMR_OMP_TASK_
		kmr_isort0((pn - r3), (r3 / ES), ES, MAX((depth - 1), 0));
	} else {
	    kmr_isort0((pn - r3), (r3 / ES), ES, MAX((depth - 1), 0));
	}
#endif
    }
#if defined(_OPENMP) && (_OPENMP >= 201106) && !defined(KMROMP30)
    /* (ONLY INTEL-CC ACTUALLY NEEDS TASKWAIT (ICC 13 and 14)). */
    KMR_OMP_TASKWAIT_;
#else
    /*empty*/
#endif
}

/** Sorts by comparator on long integers.  The target array is of
    "struct kmr_isort_entry".  DEPTH tells how deep OMP threading is
    enabled (using tasks), and it is decremented by one each
    recursion.  Zero depth tells a sequential run. */

void
kmr_isort(void *A, const size_t N, const size_t ES, int depth)
{
    KMR_OMP_PARALLEL_
    {
	KMR_OMP_SINGLE_NOWAIT_
	{
	    kmr_isort0(A, N, ES, depth);
	}
    }
}

/*
NOTICE-NOTICE-NOTICE
*/
