/* kmrtrace.h (2015-10-14) */
/* Copyright (C) 2012-2016 RIKEN AICS */

#ifndef _KMRTRACE_H
#define _KMRTRACE_H
#pragma once

/** \file kmrtrace.h KMRViz tracing Support. */

#define KT_PATHLEN        512
#define KT_ENDIAN_CHECKER 0xdeadbeef

/** Current supported trace events */
typedef enum {
    KMR_TRACE_EVENT_START,    /* dummy for KMRViz */
    KMR_TRACE_EVENT_END,      /* dummy for KMRViz */
    KMR_TRACE_EVENT_MAP,
    KMR_TRACE_EVENT_MAP_ONCE,
    KMR_TRACE_EVENT_SHUFFLE,
    KMR_TRACE_EVENT_REDUCE,
    KMR_TRACE_EVENT_SORT,
    KMR_TRACE_EVENT_REPLICATE,
} kmr_trace_event_t;

/* A trace entry is a point in time of the trace,
   it includes the time point and the type of event occurring at that time */
typedef struct kmr_trace_entry {
    double t;
    kmr_trace_event_t e;
    long kvi_element_count;
    long kvo_element_count;
    struct kmr_trace_entry * pair;
    struct kmr_trace_entry * next;
} kmr_trace_entry_t;

/* The trace of one processor core (rank) */
struct kmr_trace {
    int rank;
    double start_t;
    double end_t;
    long n;
    kmr_trace_entry_t * head;
    kmr_trace_entry_t * tail;
};

extern void kmr_trace_initialize(KMR *mr);
extern void kmr_trace_finalize(KMR *mr);
extern kmr_trace_entry_t *
kmr_trace_add_entry(KMR *mr, kmr_trace_event_t ev, kmr_trace_entry_t * pre,
		    KMR_KVS * kvi, KMR_KVS * kvo);

/*
Copyright (C) 2012-2016 RIKEN AICS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/

#endif /* _KMRTRACE_H */
