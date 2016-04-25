/* kmrtrace.c (2015-12-07) */
/* Copyright (C) 2012-2016 RIKEN AICS */

/** \file kmrtrace.c KMRViz tracing Support. */

#include <mpi.h>
#include <stdint.h>
#include <assert.h>
#include "../config.h"
#include "kmr.h"
#include "kmrimpl.h"
#include "kmrtrace.h"

typedef struct kmr_trace kmr_trace_t;

/* Time getting function used by the tracer */
static inline double
kmr_trace_gettime()
{
    return kmr_wtime() * 10E9;
}

/* Initialize */
static void
kmr_trace_init(kmr_trace_t *kt, MPI_Comm comm)
{
    MPI_Comm_rank(comm, &(kt->rank));
    kt->start_t = kt->end_t = 0.0;
    kt->n = 0;
    kt->head = kt->tail = NULL;
}

/* Finalize */
static void
kmr_trace_fini(kmr_trace_t *kt)
{
    kmr_trace_entry_t * en = kt->head;
    while (en) {
        kmr_trace_entry_t * enn = en->next;
        kmr_free(en, sizeof(kmr_trace_entry_t));
        en = enn;
    }
}

/* Start tracing */
static void
kmr_trace_start(kmr_trace_t *kt)
{
    kt->start_t = kmr_trace_gettime();
}

/* Stop tracing */
static void
kmr_trace_stop(kmr_trace_t *kt)
{
    kt->end_t = kmr_trace_gettime();
}

static int
kmr_trace_count(kmr_trace_entry_t * e1, kmr_trace_entry_t * e2)
{
    if (!e1 || !e2 || e1 == e2) return 0; // to pair with itself
    kmr_trace_entry_t * e = e1;
    int count = 0;
    while (e != e2 && e != NULL) {
        e = e->next;
        count++;
    }
    if (e == e2) {
        return count;
    }
    return 0;
}

/* Dump the trace to a binary file */
static void
kmr_trace_dump_bin(kmr_trace_t * kt, char * filename)
{
    FILE * wp = fopen(filename, "wb");
    if (!wp) {
        fprintf(stderr, "error: fopen: %s (%s)\n", strerror(errno), filename);
    }
    uint32_t endian_checker = KT_ENDIAN_CHECKER;
    if (fwrite(&endian_checker, sizeof(endian_checker), 1, wp) != 1
        || fwrite(&kt->rank, sizeof(kt->rank), 1, wp) != 1
        || fwrite(&kt->start_t, sizeof(kt->start_t), 1, wp) != 1
        || fwrite(&kt->end_t, sizeof(kt->end_t), 1, wp) != 1
        || fwrite(&kt->n, sizeof(kt->n), 1, wp) != 1) {
        fprintf(stderr, "error: fwrite: %s (%s)\n", strerror(errno), filename);
    }
    kmr_trace_entry_t * en = kt->head;
    while (en) {
        kmr_trace_entry_t * enn = en->next;
        int offset = kmr_trace_count(en, en->pair);
        if (fwrite(&en->t, sizeof(en->t), 1, wp) != 1
            || fwrite(&en->kvi_element_count, sizeof(en->kvi_element_count), 1, wp) != 1
            || fwrite(&en->kvo_element_count, sizeof(en->kvo_element_count), 1, wp) != 1
            || fwrite(&en->e, sizeof(en->e), 1, wp) != 1
            || fwrite(&offset, sizeof(offset), 1, wp) != 1) {
            fprintf(stderr, "error: fwrite: %s (%s)\n",
		    strerror(errno), filename);
        }
        en = enn;
    }
    fclose(wp);
    printf("rank %d's trace written to %s\n", kt->rank, filename);
}

/* Dump the trace to a text file */
static void
kmr_trace_dump_txt(kmr_trace_t * kt, char * filename)
{
    FILE * wp = fopen(filename, "wb");
    if (!wp) {
        fprintf(stderr, "error: fopen: %s (%s)\n", strerror(errno), filename);
    }
    double base_t = kt->start_t;
    fprintf(wp, "rank: %d\nstart_t: %.0lf\nend_t: %.0lf\nn: %ld\n",
	    kt->rank, kt->start_t - base_t, kt->end_t - base_t, kt->n);
    kmr_trace_entry_t * en = kt->head;
    while (en) {
        kmr_trace_entry_t * enn = en->next;
        int offset = kmr_trace_count(en, en->pair);
        fprintf(wp, "event %d at t=%.0lf, element count=(%ld,%ld), offset to pair: %d\n",
		en->e, en->t - base_t, en->kvi_element_count,
		en->kvo_element_count, offset);
        en = enn;
    }
    fclose(wp);
    printf("rank %d's trace written to %s\n", kt->rank, filename);
}

/* Dump the trace to files */
static void
kmr_trace_dump(KMR *mr)
{
    char *prefix = 0;
    if (strlen(mr->identifying_name) > 0) {
	prefix = mr->identifying_name;
    } else {
	prefix = "00kt";
    }
    char filename[KT_PATHLEN];
    kmr_trace_t *kt = mr->kvt_ctx;
    sprintf(filename, "%s_rank%d.bin", prefix, kt->rank);
    kmr_trace_dump_bin(kt, filename);
    sprintf(filename, "%s_rank%d.txt", prefix, kt->rank);
    kmr_trace_dump_txt(kt, filename);
}


/** Initialize a trace */
void kmr_trace_initialize(KMR *mr)
{
    /* inialized irrespective of mr->kmrviz_trace */
    mr->kvt_ctx = (kmr_trace_t *)kmr_malloc(sizeof(kmr_trace_t));
    kmr_trace_init(mr->kvt_ctx, mr->comm);
    kmr_trace_start(mr->kvt_ctx);
}

/** Finalize a trace */
void kmr_trace_finalize(KMR *mr)
{
    if (mr->kmrviz_trace) {
        kmr_trace_stop(mr->kvt_ctx);
        kmr_trace_dump(mr);
        kmr_trace_fini(mr->kvt_ctx);
    }
    kmr_free(mr->kvt_ctx, sizeof(kmr_trace_t));
}

/** Add an entry to the trace */
kmr_trace_entry_t *
kmr_trace_add_entry(KMR *mr, kmr_trace_event_t ev, kmr_trace_entry_t *pre,
		    KMR_KVS *kvi, KMR_KVS *kvo)
{
    /* Create */
    kmr_trace_entry_t * en =
	(kmr_trace_entry_t *) kmr_malloc( sizeof(kmr_trace_entry_t) );
    en->t = kmr_trace_gettime();
    en->e = ev;
    if (kvi) {
        en->kvi_element_count = kvi->c.element_count;
    } else {
        en->kvi_element_count = -1;
    }
    if (kvo) {
        en->kvo_element_count = kvo->c.element_count;
    } else {
        en->kvo_element_count = -1;
    }
    en->pair = NULL;
    en->next = NULL;
    /* Append */
    kmr_trace_t *kt = mr->kvt_ctx;
    if (!kt->head) {
        kt->head = kt->tail = en;
    } else {
        kt->tail->next = en;
        kt->tail = en;
    }
    kt->n++;
    /* Pair */
    if (pre) {
        pre->pair = en;
    }
    /* Return */
    return en;
}


/*
Copyright (C) 2012-2016 RIKEN AICS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/
