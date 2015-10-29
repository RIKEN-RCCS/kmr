#ifndef _KMRTRACE_H
#define _KMRTRACE_H
#pragma once

#if KMR_TRACE_ENABLE

#include <stdint.h>
#include <time.h>

#define KT_ENDIAN_CHECKER 0xdeadbeef

typedef enum {
  kmr_trace_event_map_start, /* map phase starts */
  kmr_trace_event_map_end, /* map phase ends */
  kmr_trace_event_shuffle_start,
  kmr_trace_event_shuffle_end,
  kmr_trace_event_reduce_start,
  kmr_trace_event_reduce_end,
  kmr_trace_event_map_once_start,
  kmr_trace_event_map_once_end,
  kmr_trace_event_sort_start,
  kmr_trace_event_sort_end,
  /*  
  kmr_trace_event_mapper_start,
  kmr_trace_event_mapper_end,
  kmr_trace_event_reducer_start,
  kmr_trace_event_reducer_end,
  */
} kmr_trace_event_t;

typedef struct kmr_trace_entry {
  double t;
  kmr_trace_event_t e;
  long kvi_element_count, kvo_element_count;
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

/*
void * kmr_trace_malloc(size_t);
void kmr_trace_free(void *);
*/

void
kmr_trace_init() {
  kmr_trace_t * kt = KT;
  MPI_Comm_rank(MPI_COMM_WORLD, &(kt->rank));
  kt->start_t = kt->end_t = 0.0;
  kt->n = 0;
  kt->head = kt->tail = NULL;
}

void
kmr_trace_fini() {
  kmr_trace_t * kt = KT;
  kmr_trace_entry_t * en = kt->head;
  while (en) {
    kmr_trace_entry_t * enn = en->next;
    kmr_free(en, sizeof(en));
    en = enn;
  }
}

void
kmr_trace_start() {
  kmr_trace_t * kt = KT;
  kt->start_t = kmr_gettime();
}

void
kmr_trace_stop() {
  kmr_trace_t * kt = KT;
  kt->end_t = kmr_gettime();
}

void
kmr_trace_add_entry(kmr_trace_event_t ev, KMR_KVS * kvi, KMR_KVS * kvo) {
  kmr_trace_entry_t * en = (kmr_trace_entry_t *) kmr_malloc( sizeof(kmr_trace_entry_t) );
  en->t = kmr_gettime();
  en->e = ev;
  if (kvi)
    en->kvi_element_count = kvi->c.element_count;
  else
    en->kvi_element_count = -1;
  if (kvo)
    en->kvo_element_count = kvo->c.element_count;
  else
    en->kvo_element_count = -1;
  en->next = NULL;
  kmr_trace_t * kt = KT;
  if (!kt->head) {
    kt->head = kt->tail = en;
  } else {
    kt->tail->next = en;
    kt->tail = en;
  }
  kt->n++;
}

static void
kmr_trace_dump_bin(kmr_trace_t * kt, char * filename) {
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
    if (fwrite(&en->t, sizeof(en->t), 1, wp) != 1
        || fwrite(&en->kvi_element_count, sizeof(en->kvi_element_count), 1, wp) != 1
        || fwrite(&en->kvo_element_count, sizeof(en->kvo_element_count), 1, wp) != 1
        || fwrite(&en->e, sizeof(en->e), 1, wp) != 1) {
      fprintf(stderr, "error: fwrite: %s (%s)\n", strerror(errno), filename);
    }
    en = enn;
  }
  fclose(wp);
  printf("rank %d's trace written to %s\n", kt->rank, filename);
}

static void
kmr_trace_dump_txt(kmr_trace_t * kt, char * filename) {
  FILE * wp = fopen(filename, "wb");
  if (!wp) { 
    fprintf(stderr, "error: fopen: %s (%s)\n", strerror(errno), filename);
  }
  double base_t = kt->start_t;
  fprintf(wp, "rank: %d\nstart_t: %.0lf\nend_t: %.0lf\nn: %ld\n", kt->rank, kt->start_t - base_t, kt->end_t - base_t, kt->n);
  kmr_trace_entry_t * en = kt->head;
  while (en) {
    kmr_trace_entry_t * enn = en->next;
    fprintf(wp, "event %d at t=%.0lf, element count=(%ld,%ld)\n", en->e, en->t - base_t, en->kvi_element_count, en->kvo_element_count);
    en = enn;
  }
  fclose(wp);
  printf("rank %d's trace written to %s\n", kt->rank, filename);
}

void
kmr_trace_dump() {
  kmr_trace_t * kt = KT;
  char filename[100];
  sprintf(filename, "00kt_rank%d.bin", kt->rank);
  kmr_trace_dump_bin(kt, filename);
  sprintf(filename, "00kt_rank%d.txt", kt->rank);
  kmr_trace_dump_txt(kt, filename);
}

#endif /* KMR_TRACE_ENABLE */

#endif /* _KMRTRACE_H */
