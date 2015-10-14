#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>

#define KMR_TRACE_ENABLE 0

#if KMR_TRACE_ENABLE

typedef enum {
  kmr_trace_event_start, /* trace starts */
  kmr_trace_event_end, /* trace ends */
  kmr_trace_event_map_start,
  kmr_trace_event_map_end,
  kmr_trace_event_shuffle_start,
  kmr_trace_event_shuffle_end,
  kmr_trace_event_reduce_start,
  kmr_trace_event_reduce_end,
  
  kmr_trace_event_mapper_start,
  kmr_trace_event_mapper_end,
  kmr_trace_event_reducer_start,
  kmr_trace_event_reducer_end,
} kmr_trace_event_t;

typedef struct kmr_trace_entry {
  double t;
  kmr_trace_event_t e;
  struct kmr_trace_entry * next;
} kmr_trace_entry_t;

typedef struct kmr_trace {
  int rank;
  double start_t;
  double end_t;
  long n;
  kmr_trace_entry_t * head;
  kmr_trace_entry_t * tail;
} kmr_trace_t;


extern kmr_trace_t KT[1];

static inline double
kmr_gettime() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return ((double) ts.tv_sec) * 10E9 + ((double) ts.tv_nsec);
}

static inline void *
kmr_trace_malloc(size_t sz) {
  return malloc(sz);
}

static inline void
kmr_trace_free(void * p) {
  free(p);
}

static inline void
kmr_trace_init() {
  kmr_trace_t * kt = KT;
  MPI_Comm_rank(MPI_COMM_WORLD, &kt->rank);
  kt->start_t = kt->end_t = 0.0;
  kt->n = 0;
  kt->head = kt->tail = NULL;
}

static inline void
kmr_trace_add_entry(kmr_trace_event_t ev) {
  kmr_trace_entry_t * en = (kmr_trace_entry_t *) kmr_trace_malloc( sizeof(kmr_trace_entry_t) );
  en->t = kmr_gettime();
  en->e = ev;
  en->next = NULL;
  kmr_trace_t * kt = KT;
  if (!kt->head) {
    kt->head = kt->tail = en;
  } else {
    kt->tail->next = en;
    kt->tail = en;
  }
}

static inline void
kmr_trace_init_and_start() {
  kmr_trace_init();
  kmr_trace_add_entry(kmr_trace_event_start);
}

static inline void
kmr_trace_fini() {
  kmr_trace_t * kt = KT;
  kmr_trace_entry_t * en = kt->head;
  while (en) {
    kmr_trace_entry_t * enn = en->next;
    kmr_trace_free(en);
    en = enn;
  }
}

static inline void
kmr_trace_dump_bin(kmr_trace_t * kt, char * filename) {
}

static inline void
kmr_trace_dump_txt(kmr_trace_t * kt, char * filename) {
  FILE * wp = fopen(filename, "wb");
  if (!wp) { 
    fprintf(stderr, "error: fopen: %s (%s)\n", strerror(errno), filename);
  }
  fprintf(wp, "rank: %d\nstart_t: %.0lf\nend_t: %.0lf\n", kt->rank, kt->start_t, kt->end_t);
  kmr_trace_entry_t * en = kt->head;
  while (en) {
    kmr_trace_entry_t * enn = en->next;
    fprintf(wp, "event %d at t=%.0lf\n", en->e, en->t);
    en = enn;
  }
  fclose(wp);
  printf("trace written to %s\n", filename);
}

static inline void
kmr_trace_dump() {
  kmr_trace_t * kt = KT;
  kmr_trace_dump_bin(kt, "00kt.bin");
  kmr_trace_dump_txt(kt, "00kt.txt");
}

#endif