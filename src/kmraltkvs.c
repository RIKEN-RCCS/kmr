/* kmraltkvs.c (2014-02-04) */
/* Copyright (C) 2012-2018 RIKEN R-CCS */

/** \file kmraltkvs.c Other Key-Value Stream Implementations.  A
    "push-off" key-value stream performs shuffling at key-value
    addition.  It aims at an overlap of communication and computation.
    It includes RDMA-based event notification to tell readiness of MPI
    messages. */

#include <mpi.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <errno.h>
#include <assert.h>
#include <ctype.h>

#include "../config.h"
#include "kmr.h"
#include "kmrimpl.h"

#if (defined(__K) && defined(KMRFASTNOTICE))
#include <mpi-ext.h>
#endif

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))
#define NEVERHERE 0

/* Statistics Timers and Counters. */

#define STAT_TEST_TIME 0
#define STAT_WAIT_TIME 1

#define STAT_RECV_CALLS 0
#define STAT_SEND_CALLS 1
#define STAT_TEST_CALLS 2
#define STAT_WAIT_CALLS 3
#define STAT_TEST0_COUNT 4
#define STAT_TEST1_COUNT 5
#define STAT_WAIT_COUNT 6
#define STAT_SEND_PEND_COUNT 7
#define STAT_RDMA_POLLCQ_CALLS 8
#define STAT_TEST_CLOCKS 9

static int kmr_pushoff_seqno = 0;
static KMR_KVS *kmr_pushoff_movings[64];

/* Areas and Values for Fast-Notice.  See kmr_pushoff_notice(). */

const size_t kmr_pushoff_area_size = (sizeof(int) * 1024);
const int kmr_pushoff_memid = 1;

static volatile int *kmr_pushoff_area = 0;
static uint64_t *kmr_pushoff_addrs = 0;
static int kmr_pushoff_nprocs;
static int kmr_pushoff_rank;

static void kmr_pushoff_notice(KMR *mr, int peer);

/* Flag Written by RDMA.  It is used to check by polling incoming
   messages.  A single place is written by many ranks. */

#define FAST_NOTICE (kmr_pushoff_area[0])

/* RMDA write value (any non-zero value). */

#define PUT_VALUE (((int)'A'<<24)|((int)'H'<<16)|((int)'O'<<8)|((int)'%'))

static inline int
kmr_send_size_of_block(struct kmr_kvs_block *b)
{
    return (int)(offsetof(struct kmr_kvs_block, data)
		 + b->fill_size + kmr_kvs_entry_header);
}

/** Makes a new key-value stream with the specified field data-types.
    It cannot be used with checkpointing.  It allocates by the size of
    the union, which is larger than the necessary for replacement
    later by an on-core KVS at kmr_add_kv_done().  See
    kmr_add_kv_done_pushoff(). */

KMR_KVS *
kmr_create_pushoff_kvs(KMR *mr, enum kmr_kv_field kf, enum kmr_kv_field vf,
		       struct kmr_option opt,
		       const char *file, const int line, const char *func)
{
    assert(mr != 0);
    int cc;
    if (mr->ckpt_enable) {
	kmr_error(mr, ("kmr_create_pushoff_kvs:"
		       " Unable to use it under checkpointing"));
    }
    if ((kf == KMR_KV_POINTER_OWNED) || (kf == KMR_KV_POINTER_UNMANAGED)
	|| (vf == KMR_KV_POINTER_OWNED) || (vf == KMR_KV_POINTER_UNMANAGED)) {
	kmr_error(mr, "kmr_create_pushoff_kvs: Pointers not allowed");
    }

    KMR_KVS *kvs = kmr_malloc(sizeof(KMR_KVS));
    KMR_DEBUGX(memset(kvs, 0, sizeof(KMR_KVS)));
    kvs->o.magic = KMR_KVS_PUSHOFF;
    kvs->o.mr = mr;
    kmr_link_kvs(kvs);
    kvs->o.key_data = kf;
    kvs->o.value_data = vf;
    kvs->o.element_count = 0;
    kvs->o.info_line0.file = file;
    kvs->o.info_line0.func = func;
    kvs->o.info_line0.line = line;

    kvs->o.oncore = 0;
    kvs->o.stowed = 0;
    kvs->o.nogrow = 0;
    kvs->o.sorted = 0;
    kvs->o._uniformly_sized_ = 0;

    int nmovings = (int)(sizeof(kmr_pushoff_movings)
			 / sizeof(kmr_pushoff_movings[0]));
    kvs->o.seqno = (kmr_pushoff_seqno % nmovings);
    kmr_pushoff_seqno++;
    assert(kmr_pushoff_movings[kvs->o.seqno] == 0);
    kmr_pushoff_movings[kvs->o.seqno] = kvs;

    kvs->o.storage = kmr_create_kvs7(mr, kf, vf, opt, file, line, func);

    int nprocs = mr->nprocs;
    size_t vsz = (sizeof(struct kmr_pushoff_buffers) * (size_t)nprocs);
    kvs->o.peers = kmr_malloc(vsz);
    kvs->o.reqs = kmr_malloc(sizeof(MPI_Request) * (size_t)(2 * nprocs));
    kvs->o.indexes = kmr_malloc(sizeof(int) * (size_t)(2 * nprocs));
    kvs->o.statuses = kmr_malloc(sizeof(MPI_Status) * (size_t)(2 * nprocs));
    memset(kvs->o.indexes, 0, (sizeof(int) * (size_t)(2 * nprocs)));
    memset(kvs->o.statuses, 0, (sizeof(MPI_Status) * (size_t)(2 * nprocs)));
    for (int r = 0; r < nprocs; r++) {
	struct kmr_pushoff_buffers *po = &(kvs->o.peers[r]);
	kvs->o.reqs[r] = MPI_REQUEST_NULL;
	kvs->o.reqs[r + nprocs] = MPI_REQUEST_NULL;
	struct kmr_kvs_block *b0 = kmr_malloc(mr->pushoff_block_size);
	kmr_kvs_reset_block(kvs, b0, mr->pushoff_block_size, 0);
	po->adding_point = &(b0->data[0]);
	po->fillbuf = b0;
	po->sendbufs[0] = 0;
	po->sendbufs[1] = 0;
	po->sends = 0;
	po->closed[0] = 0;
	po->closed[1] = 0;
	if (r == mr->rank) {
	    po->recvbuf = 0;
	} else {
	    struct kmr_kvs_block *b1 = kmr_malloc(mr->pushoff_block_size);
	    kmr_kvs_reset_block(kvs, b1, mr->pushoff_block_size, 0);
	    po->recvbuf = b1;
	    int sz = (int)mr->pushoff_block_size;
	    cc = MPI_Irecv(po->recvbuf, sz, MPI_BYTE, r,
			   (KMR_TAG_PUSHOFF + kvs->o.seqno),
			   mr->comm, &kvs->o.reqs[r + nprocs]);
	    assert(cc == MPI_SUCCESS);
	    mr->pushoff_statistics.counts[STAT_RECV_CALLS]++;
	}
    }

    return kvs;
}

int
kmr_free_kvs_pushoff(KMR_KVS *kvs, _Bool deallocate)
{
    KMR *mr = kvs->o.mr;
    int nprocs = mr->nprocs;
    if (kvs->o.storage != 0) {
	kmr_free_kvs(kvs->o.storage);
	kvs->o.storage = 0;
    }
    assert(kvs->o.reqs != 0);
    for (int r = 0; r < nprocs; r++) {
	if (kvs->o.reqs[r] != MPI_REQUEST_NULL) {
	    kmr_error(mr, "kmr_free_kvs: Some send pending");
	}
	if (kvs->o.reqs[r + nprocs] != MPI_REQUEST_NULL) {
	    kmr_error(mr, "kmr_free_kvs: Some receive pending");
	}
    }
    kmr_free(kvs->o.reqs, (sizeof(MPI_Request) * (size_t)(2 * nprocs)));
    kvs->o.reqs = 0;
    kmr_free(kvs->o.indexes, (sizeof(int) * (size_t)(2 * nprocs)));
    kvs->o.indexes = 0;
    kmr_free(kvs->o.statuses, (sizeof(MPI_Status) * (size_t)(2 * nprocs)));
    kvs->o.statuses = 0;
    assert(kvs->o.peers != 0);
    for (int r = 0; r < nprocs; r++) {
	struct kmr_pushoff_buffers *po = &kvs->o.peers[r];
	assert(po->fillbuf == 0);
	assert(po->recvbuf == 0);
	assert(po->sendbufs[0] == 0 && po->sendbufs[1] == 0);
    }
    size_t vsz = (sizeof(struct kmr_pushoff_buffers) * (size_t)nprocs);
    kmr_free(kvs->o.peers, vsz);
    kvs->o.peers = 0;

    if (deallocate) {
	kmr_free(kvs, sizeof(struct kmr_kvs_pushoff));
    }
    return MPI_SUCCESS;
}

/** Links a block for sending.  Calling kmr_pushoff_do_send() starts
    sending. */

static inline void
kmr_pushoff_link_to_send(KMR_KVS *kvs, int peer,
			 struct kmr_pushoff_buffers *po,
			 struct kmr_kvs_block *b)
{
    KMR *mr = kvs->o.mr;
    assert(b->next == 0);
    if (peer == mr->rank) {
	kmr_kvs_insert_block(kvs->o.storage, b);
	assert(po->sendbufs[0] == 0 && po->sendbufs[1] == 0 && po->sends == 0);
    } else {
	if (po->sendbufs[0] != 0) {
	    assert(po->sendbufs[1] != 0);
	    po->sendbufs[1]->next = b;
	    po->sendbufs[1] = b;
	} else {
	    assert(po->sendbufs[1] == 0);
	    po->sendbufs[0] = b;
	    po->sendbufs[1] = b;
	}
	po->sends++;
    }
}

/** Sends the first one in the list of buffered blocks, or it does
    nothing when the pipe is full.  It sends a closing message when
    CLOSING is true and nothing remains to send.  Note MPI operations
    are called inside a mutex (OMP critical). */

static int
kmr_pushoff_do_send(KMR_KVS *kvs, int peer, _Bool closing)
{
    KMR *mr = kvs->o.mr;
    int cc;
    struct kmr_pushoff_buffers *po = &(kvs->o.peers[peer]);

    if (kvs->o.reqs[peer] != MPI_REQUEST_NULL) {
	/* Do nothing, called later when the previous send finishes. */
    } else if (po->sendbufs[0] != 0) {
	assert(peer != mr->rank);
	struct kmr_kvs_block *b = po->sendbufs[0];
	struct kmr_kvs_entry *e = kmr_kvs_adding_point(b);
	kmr_kvs_mark_entry_tail(e);
	int sz = kmr_send_size_of_block(b);
	cc = MPI_Isend(b, sz, MPI_BYTE, peer,
		       (KMR_TAG_PUSHOFF + kvs->o.seqno),
		       mr->comm, &kvs->o.reqs[peer]);
	assert(cc == MPI_SUCCESS);
	if (mr->pushoff_fast_notice) {
	    kmr_pushoff_notice(mr, peer);
	}
	mr->pushoff_statistics.counts[STAT_SEND_CALLS]++;
    } else if (closing) {
	assert(po->sendbufs[0] == 0);
	assert(po->sends == 0);
	if (peer != mr->rank) {
	    /* Send a closing message. */
	    cc = MPI_Isend(0, 0, MPI_BYTE, peer,
			   (KMR_TAG_PUSHOFF + kvs->o.seqno),
			   mr->comm, &kvs->o.reqs[peer]);
	    assert(cc == MPI_SUCCESS);
	    po->closed[0] = 1;
	    if (mr->pushoff_fast_notice) {
		kmr_pushoff_notice(mr, peer);
	    }
	    mr->pushoff_statistics.counts[STAT_SEND_CALLS]++;
	}
    } else {
	assert(!closing && po->sendbufs[0] == 0);
	/* Do nothing, send of the closing message finishes. */
    }
    return MPI_SUCCESS;
}

/* Puts incoming block to the strage.  It is only called with positive
   COUNT.  Note MPI operations are called inside a mutex (OMP
   critical). */

static int
kmr_pushoff_do_recv(KMR_KVS *kvs, int peer)
{
    int cc;
    KMR *mr = kvs->o.mr;
    int nprocs = mr->nprocs;
    assert(peer != mr->rank);
    assert(kvs->o.reqs[peer + nprocs] == MPI_REQUEST_NULL);
    struct kmr_pushoff_buffers *po = &(kvs->o.peers[peer]);
    struct kmr_kvs_block *b = kmr_malloc(mr->pushoff_block_size);
    kmr_kvs_reset_block(kvs, b, mr->pushoff_block_size, 0);
    assert(po->recvbuf == 0);
    po->recvbuf = b;
    int sz = (int)mr->pushoff_block_size;
    cc = MPI_Irecv(b, sz, MPI_BYTE, peer,
		   (KMR_TAG_PUSHOFF + kvs->o.seqno),
		   mr->comm, &kvs->o.reqs[peer + nprocs]);
    assert(cc == MPI_SUCCESS);
    if (mr->pushoff_fast_notice) {
	kmr_pushoff_notice(mr, peer);
    }
    mr->pushoff_statistics.counts[STAT_RECV_CALLS]++;
    return MPI_SUCCESS;
}

/* Checks requests finish.  It returns the number of active requests
   remaining.  CLOSING is true when called from kmr_add_kv_done(). It
   is not inside of a mutex when CLOSING and safe to wait. */

static int
kmr_pushoff_poll(KMR_KVS *kvs, _Bool closing, _Bool block)
{
    KMR *mr = kvs->o.mr;
    int nprocs = mr->nprocs;
    int nprocs2 = (2 * nprocs);
    int cc;

    int remains;
    remains = 0;
    for (int r = 0; r < nprocs2; r++) {
	remains += ((kvs->o.reqs[r] != MPI_REQUEST_NULL) ? 1 : 0);
    }
    if (remains == 0) {
	return remains;
    }
    int hits = 0;
    do {
	if (block) {
	    double t0 = ((!mr->pushoff_stat) ? 0.0 : MPI_Wtime());
	    cc = MPI_Waitsome(nprocs2, kvs->o.reqs, &hits,
			      kvs->o.indexes, kvs->o.statuses);
	    assert(cc == MPI_SUCCESS && hits > 0 && hits != MPI_UNDEFINED);
	    double t1 = ((!mr->pushoff_stat) ? 0.0 : MPI_Wtime());
	    mr->pushoff_statistics.counts[STAT_WAIT_CALLS]++;
	    mr->pushoff_statistics.counts[STAT_WAIT_COUNT] += hits;
	    mr->pushoff_statistics.times[STAT_WAIT_TIME] += (t1 - t0);
	} else {
	    double t0 = ((!mr->pushoff_stat) ? 0.0 : MPI_Wtime());
	    long c0 = kmr_tick();
	    cc = MPI_Testsome(nprocs2, kvs->o.reqs, &hits,
			      kvs->o.indexes, kvs->o.statuses);
	    /* (DO NOT SWAP TWO LINES BELOW (for icc 20140120)). */
	    assert(cc == MPI_SUCCESS);
	    long c1 = kmr_tick();
	    if (hits == MPI_UNDEFINED) {
		hits = 0;
	    }
	    double t1 = ((!mr->pushoff_stat) ? 0.0 : MPI_Wtime());
	    mr->pushoff_statistics.counts[STAT_TEST_CALLS]++;
	    if (!closing) {
		mr->pushoff_statistics.counts[STAT_TEST0_COUNT] += hits;
	    } else {
		mr->pushoff_statistics.counts[STAT_TEST1_COUNT] += hits;
	    }
	    mr->pushoff_statistics.times[STAT_TEST_TIME] += (t1 - t0);
	    mr->pushoff_statistics.counts[STAT_TEST_CLOCKS] += (c1 - c0);
	}
	assert(hits <= remains);
	for (int i = 0; i < hits; i++) {
	    int rank2 = kvs->o.indexes[i];
	    MPI_Status *st= &(kvs->o.statuses[i]);
	    assert(rank2 != MPI_UNDEFINED
		   && kvs->o.reqs[rank2] == MPI_REQUEST_NULL);

	    if (rank2 < nprocs) {
		/* SEND */
		int peer = rank2;
		struct kmr_pushoff_buffers *po = &(kvs->o.peers[peer]);
		struct kmr_kvs_block *b = po->sendbufs[0];
		if (b == 0) {
		    /* No body means a closing message. */
		    assert(closing && po->closed[0] == 1);
		    remains--;
		} else {
		    struct kmr_kvs_block *bn = po->sendbufs[0]->next;
		    if (po->sendbufs[0] == po->sendbufs[1]) {
			assert(bn == 0);
			po->sendbufs[1] = bn;
		    }
		    po->sendbufs[0] = bn;
		    po->sends--;
		    kmr_free(b, mr->pushoff_block_size);
		    kmr_pushoff_do_send(kvs, peer, closing);
		}
	    } else {
		/* RECV */
		int peer = (rank2 - nprocs);
		int count;
		cc = MPI_Get_count(st, MPI_BYTE, &count);
		assert(cc == MPI_SUCCESS);
		assert((size_t)count <= mr->pushoff_block_size);
		struct kmr_pushoff_buffers *po = &(kvs->o.peers[peer]);
		if (count == 0) {
		    /* A closing message. */
		    remains--;
		    struct kmr_kvs_block *b = po->recvbuf;
		    kmr_free(b, mr->pushoff_block_size);
		    po->recvbuf = 0;
		    po->closed[1] = 1;
		} else {
		    /* (b->next) has arbitrary value. */
		    struct kmr_kvs_block *b = po->recvbuf;
		    assert(b->size == mr->pushoff_block_size
			   && b->partial_element_count != 0
			   && b->fill_size != 0
			   && count == kmr_send_size_of_block(b));
		    kmr_kvs_insert_block(kvs->o.storage, b);
		    po->recvbuf = 0;
		    kmr_pushoff_do_recv(kvs, peer);
		}
	    }
	}
    } while ((!closing && hits > 0) || (block && remains > 0));
    if (block) {
	for (int r = 0; r < nprocs2; r++) {
	    assert(kvs->o.reqs[r] == MPI_REQUEST_NULL);
	}
    }
    return remains;
}

static int
kmr_pushoff_poll_all(void)
{
    int nmovings = (int)(sizeof(kmr_pushoff_movings)
			 / sizeof(kmr_pushoff_movings[0]));
    for (int i = 0; i < nmovings; i++) {
	KMR_KVS *kvs = kmr_pushoff_movings[i];
	if (kvs != 0) {
	    KMR *mr = kvs->o.mr;
	    int nprocs = mr->nprocs;
	    int remains = kmr_pushoff_poll(kvs, (kvs->o.stowed), 0);
	    if (remains == 0) {
		int nprocs2 = (2 * nprocs);
		for (int r = 0; r < nprocs2; r++) {
		    assert(kvs->o.reqs[r] == MPI_REQUEST_NULL);
		}
		kmr_pushoff_movings[i] = 0;
	    }
	}
    }

    return MPI_SUCCESS;
}

/** Adds a key-value pair.  It is called from inside a mutex (OMP
    critical).  It first stores a KV into a buffer, and then sends the
    buffer when it gets full.  It sends an empty message as an
    end-of-stream marker.  IT POLLS MESSAGES TOO OFTEN OR TOO
    SELDOM. */

int
kmr_add_kv_pushoff(KMR_KVS *kvs, const struct kmr_kv_box kv)
{
    kmr_assert_kv_sizes(kvs, kv);
    assert(!kvs->o.nogrow);
    KMR *mr = kvs->o.mr;

    int r = kmr_pitch_rank(kv, kvs);
    struct kmr_pushoff_buffers *po = &(kvs->o.peers[r]);

    size_t sz = kmr_kvs_entry_netsize_of_box(kv);
    struct kmr_kvs_block *b0 = po->fillbuf;
    assert(po->adding_point == kmr_kvs_adding_point(b0));
    if (!kmr_kvs_entry_fits_in_block(kvs, b0, sz)) {
	kmr_kvs_mark_entry_tail(po->adding_point);
	kmr_pushoff_link_to_send(kvs, r, po, b0);
	struct kmr_kvs_block *n = kmr_malloc(mr->pushoff_block_size);
	kmr_kvs_reset_block(kvs, n, mr->pushoff_block_size, 0);
	po->fillbuf = n;
	po->adding_point = &(n->data[0]);

	long m0 = mr->pushoff_statistics.counts[STAT_SEND_PEND_COUNT];
	long m1 = MAX(po->sends, m0);
	mr->pushoff_statistics.counts[STAT_SEND_PEND_COUNT] = m1;

	/* TOO SELDOM. */

	if (mr->pushoff_poll_rate == 0) {
	    if (mr->pushoff_fast_notice) {
		FAST_NOTICE = 0;
	    }
	    /*kmr_pushoff_poll(kvs, 0);*/
	    kmr_pushoff_poll_all();
	}
    }

    struct kmr_kvs_entry *e = po->adding_point;
    kmr_poke_kv(e, kv, 0, kvs, 0);
    po->adding_point = kmr_kvs_next_entry(kvs, e);

    struct kmr_kvs_block *b1 = po->fillbuf;
    b1->partial_element_count++;
    b1->fill_size += sz;
    kvs->o.element_count++;

    if (mr->pushoff_fast_notice && FAST_NOTICE) {
	FAST_NOTICE = 0;
	/*kmr_pushoff_poll(kvs, 0);*/
	kmr_pushoff_poll_all();
    }

    if ((mr->pushoff_poll_rate == 1) && (po->sendbufs[0] != 0)) {
	/* TOO OFTEN (BE AVOIDED). */
	/*kmr_pushoff_poll(kvs, 0);*/
	kmr_pushoff_poll_all();
    }

    return MPI_SUCCESS;
}

/** Replaces KVS0 with KVS1.  That is, it moves the structure slots
    from KVS1 to KVS0 and frees KVS1.  (The first one be push-off, and
    the second one be on-core). */

static int
kmr_replace_kvs_components(KMR_KVS *kvs0, KMR_KVS *kvs1)
{
    assert(kvs1->c.magic == KMR_KVS_ONCORE
	   && kvs1->c.ms == 0);
    assert(kvs0->o.mr == kvs1->c.mr);

    kvs0->c.magic = KMR_KVS_ONCORE;
    kvs0->c.key_data = kvs1->c.key_data;
    kvs0->c.value_data = kvs1->c.value_data;
    kvs0->c.element_count = kvs1->c.element_count;
    kvs0->c.oncore = kvs1->c.oncore;

    kvs0->c.storage_netsize = kvs1->c.storage_netsize;
    kvs0->c.block_count = kvs1->c.block_count;
    kvs0->c.first_block = kvs1->c.first_block;

    kvs1->c.element_count = 0;
    kvs1->c.storage_netsize = 0;
    kvs1->c.block_count = 0;
    kvs1->c.first_block = 0;

    kmr_free_kvs(kvs1);

    return MPI_SUCCESS;
}

/** Marks finished adding key-value pairs, called from
    kmr_add_kv_done().  It flushes pending buffers and it is a
    collective operation in effect. */

int
kmr_add_kv_done_pushoff(KMR_KVS *kvs)
{
    KMR *mr = kvs->o.mr;
    int nprocs = mr->nprocs;

    if (kvs->o.stowed) {
	kmr_error(mr, "kmr_add_kv_done: may be called already");
    }

    for (int r = 0; r < nprocs; r++) {
	struct kmr_pushoff_buffers *po = &(kvs->o.peers[r]);
	struct kmr_kvs_block *b0 = po->fillbuf;
	if (b0->partial_element_count > 0) {
	    kmr_kvs_mark_entry_tail(po->adding_point);
	    kmr_pushoff_link_to_send(kvs, r, po, b0);
	    po->fillbuf = 0;
	} else {
	    kmr_free(po->fillbuf, mr->pushoff_block_size);
	    po->fillbuf = 0;
	}
	if (r != mr->rank) {
	    kmr_pushoff_do_send(kvs, r, 1);
	}
    }

    kvs->o.stowed = 1;

    if (!mr->pushoff_hang_out) {
	kmr_pushoff_make_stationary(kvs);
    }

    return MPI_SUCCESS;
}

/* Destructively replaces this kvs by on-core one, after waiting for
   all communication to finish. */

int
kmr_pushoff_make_stationary(KMR_KVS *kvs)
{
    KMR *mr = kvs->o.mr;
    int nprocs = mr->nprocs;

    kmr_pushoff_poll(kvs, 1, 1);
    kmr_pushoff_poll_all();

#if 0
    int remains;
    do {
	remains = kmr_pushoff_poll(kvs, 1, 0);
    } while (remains > 0);
    kmr_pushoff_poll_all();
#endif

    for (int r = 0; r < nprocs; r++) {
	struct kmr_pushoff_buffers *po = &(kvs->o.peers[r]);
	po->adding_point = 0;
	assert(po->fillbuf == 0);
	assert(po->sendbufs[0] == 0 && po->sendbufs[1] == 0);
    }

    KMR_KVS *storage = kvs->o.storage;
    kvs->o.storage = 0;
    kmr_free_kvs_pushoff(kvs, 0);
    kmr_init_kvs_oncore(kvs, mr);
    kmr_replace_kvs_components(kvs, storage);

    long count;
    size_t netsize;
    struct kmr_kvs_block *lastblock;
    count = 0;
    netsize = 0;
    lastblock = 0;
    for (struct kmr_kvs_block *b = kvs->c.first_block; b != 0; b = b->next) {
	count += b->partial_element_count;
	netsize += b->fill_size;
	lastblock = b;
    }
    kvs->c.element_count = count;
    kvs->c.storage_netsize = netsize;
    if (kvs->c.element_count == 0) {
	kvs->c.current_block = 0;
	kvs->c.adding_point = 0;
    } else {
	assert(lastblock != 0);
	kvs->c.current_block = lastblock;
	kvs->c.adding_point = (void *)((char *)&lastblock->data[0]
				       + lastblock->fill_size);
    }

    kvs->c.shuffled_in_pushoff = 1;
    kmr_add_kv_done(kvs);

    return MPI_SUCCESS;
}

void
kmr_print_statistics_on_pushoff(KMR *mr, char *titlestring)
{
    if (mr->pushoff_stat) {
	long *counts = mr->pushoff_statistics.counts;
	double *times = mr->pushoff_statistics.times;
	fprintf(stderr,
		("%s"
		 "[%d] push-off sends=%ld\n"
		 "[%d] push-off recvs=%ld\n"
		 "[%d] push-off tests=%ld hits=%ld time=%f (clocks=%ld)\n"
		 "[%d] push-off waits=%ld hits=%ld time=%f (closing)\n"
		 "[%d] push-off tests hits=(%ld + %ld) (%.2f%% + %.2f%%)\n"
		 "[%d] push-off max-send-pendings=%ld\n"
		 "[%d] push-off cq-polls=%ld\n"),
		titlestring,
		mr->rank,
		counts[STAT_SEND_CALLS],
		mr->rank,
		counts[STAT_RECV_CALLS],
		mr->rank,
		counts[STAT_TEST_CALLS],
		(counts[STAT_TEST0_COUNT] + counts[STAT_TEST1_COUNT]),
		times[STAT_TEST_TIME],
		counts[STAT_TEST_CLOCKS],
		mr->rank,
		counts[STAT_WAIT_CALLS],
		counts[STAT_WAIT_COUNT],
		times[STAT_WAIT_TIME],
		mr->rank,
		counts[STAT_TEST0_COUNT],
		counts[STAT_TEST1_COUNT],
		(100.0 * (double)counts[STAT_TEST0_COUNT]
		 / (double)(counts[STAT_SEND_CALLS]
			    + counts[STAT_RECV_CALLS])),
		(100.0 * (double)counts[STAT_TEST1_COUNT]
		 / (double)(counts[STAT_SEND_CALLS]
			    + counts[STAT_RECV_CALLS])),
		mr->rank,
		counts[STAT_SEND_PEND_COUNT],
		mr->rank,
		counts[STAT_RDMA_POLLCQ_CALLS]);
	fflush(0);
    }
}

/* ================================================================ */

/* FAKE FUNCTIONS FOR FUJITSU-MPI. */

#if (!(defined(__K) && defined(KMRFASTNOTICE)))
#define FJMPI_RDMA_ERROR (0)
#define FJMPI_RDMA_NOTICE (1)
#define FJMPI_RDMA_NIC0 0
#define FJMPI_RDMA_LOCAL_NIC0 0
#define FJMPI_RDMA_REMOTE_NIC0 0
#define FJMPI_RDMA_PATH0 0
struct FJMPI_Rdma_cq {int _;};
static int FJMPI_Rdma_init(void) {return 0;}
static int FJMPI_Rdma_finalize(void) {return 0;}
static uint64_t FJMPI_Rdma_reg_mem(int m, void *b, size_t l) {return 1;}
static int FJMPI_Rdma_dereg_mem(int m) {return 0;}
static uint64_t FJMPI_Rdma_get_remote_addr(int r, int m) {return 1;}
static int FJMPI_Rdma_put(int r, int tag, uint64_t ra, uint64_t la,
			  size_t sz, int f) {return 0;}
static int FJMPI_Rdma_poll_cq(int nic, struct FJMPI_Rdma_cq *cq) {
    return 0;
}
#endif /*__K*/

/** Initializes RDMA for fast-notice.  Fast-notice is RDMA-based event
    notification to tell readiness of MPI messages.  It is only usable
    with communicators having the same processes. */

void
kmr_init_pushoff_fast_notice_(MPI_Comm comm, _Bool verbose)
{
    int cc;

    unsigned int verbosity = (verbose ? 5 : 9);

    int nprocs;
    int rank;
    MPI_Comm_size(comm, &nprocs);
    MPI_Comm_rank(comm, &rank);

    if (kmr_pushoff_area != 0) {
	assert(kmr_pushoff_nprocs == nprocs && kmr_pushoff_rank == rank);
	return;
    }

    if (rank == 0) {
	kmr_warning(0, verbosity, "Initialize pushoff_fast_notice");
    }

#if (!(defined(__K) && defined(KMRFASTNOTICE)))
    if (rank == 0) {
	kmr_warning(0, verbosity, ("Fast-notice needs Fujitsu MPI extension"));
    }
#endif

    cc = FJMPI_Rdma_init();
    assert(cc == 0);

    kmr_pushoff_nprocs = nprocs;
    kmr_pushoff_rank = rank;

    kmr_pushoff_addrs = kmr_malloc(sizeof(uint64_t) * (size_t)nprocs);
    /*kmr_pushoff_area = kmr_malloc(kmr_pushoff_area_size);*/
    size_t malign = (2 * 1024 * 1024);
    cc = posix_memalign((void **)&kmr_pushoff_area, malign,
			kmr_pushoff_area_size);
    if (cc != 0) {
	char ee[80];
	char *m = strerror(errno);
	snprintf(ee, sizeof(ee), "posix_memalign(sz=%zd) failed: %s",
		 kmr_pushoff_area_size, m);
	kmr_error(0, ee);
    }
    assert(kmr_pushoff_area != 0);

    uint64_t a0 = FJMPI_Rdma_reg_mem(kmr_pushoff_memid,
				     (void *)kmr_pushoff_area,
				     kmr_pushoff_area_size);
    assert(a0 != FJMPI_RDMA_ERROR);
    kmr_pushoff_addrs[rank] = a0;

    /* Clear RMDA area. */

    FAST_NOTICE = 0;

    /* Set RMDA source value non-zero. */

    kmr_pushoff_area[1] = (int)PUT_VALUE;
    assert((int)PUT_VALUE != 0);

    for (int r = 0; r < nprocs; r++) {
	if (r == rank) {
	    continue;
	}
	uint64_t a1;
	do {
	    a1 = FJMPI_Rdma_get_remote_addr(r, kmr_pushoff_memid);
	} while (a1 == FJMPI_RDMA_ERROR);
	kmr_pushoff_addrs[r] = a1;
#if 0
	fprintf(stderr, "[%d] rdma addr: b=%p, l=%p r=%p\n",
		r, (void *)kmr_pushoff_area, (void *)a0, (void *)a1);
	fflush(0);
#endif
    }
}

/** Check if fast-notice works.  Check be at immediately after
    initialization. */

void
kmr_check_pushoff_fast_notice_(KMR *mr)
{
    int nprocs = mr->nprocs;
    int rank = mr->rank;

    _Bool check = 1;

#if (!(defined(__K) && defined(KMRFASTNOTICE)))
    check = 0;
#endif

    if (check) {
	int cc;
	assert(kmr_pushoff_area != 0);
	assert(kmr_pushoff_nprocs == nprocs && kmr_pushoff_rank == rank);

	if (rank == 0) {
	    kmr_warning(mr, 5, "Checking fast notification works");
	}
	double t0 = MPI_Wtime();

	int peer = ((rank + 1) % nprocs);
	if (rank == 0) {
	    kmr_pushoff_notice(mr, peer);
	}
	for (;;) {
	    for (int j = 0; j < 1000; j++) {
		if (FAST_NOTICE != 0) {
		    break;
		}
	    }
	    if (FAST_NOTICE != 0) {
		break;
	    }
	    double tm = MPI_Wtime();
	    if ((tm - t0) >= 200.0) {
		break;
	    }
	}
	if (FAST_NOTICE == 0) {
	    kmr_error(mr, "FAST_NOTICE timeout (200 sec)");
	    return;
	}
	if (rank != 0) {
	    kmr_pushoff_notice(mr, peer);
	}
	FAST_NOTICE = 0;

	double t1 = MPI_Wtime();

	/* Try to reclaim CQ entries.  */

	do {
	    struct FJMPI_Rdma_cq cq;
	    cc = FJMPI_Rdma_poll_cq(FJMPI_RDMA_NIC0, &cq);
	    assert(cc == 0 || cc == FJMPI_RDMA_NOTICE);
	} while (cc != 0);

	if (rank == 0) {
	    char ee[80];
	    snprintf(ee, sizeof(ee), "Fast notification works (%f sec)",
		     (t1 - t0));
	    kmr_warning(mr, 5, ee);
	}
    }
}

void
kmr_fin_pushoff_fast_notice_(void)
{
    int cc;

    if (kmr_pushoff_area == 0) {
	return;
    }

    int nprocs = kmr_pushoff_nprocs;
    int rank = kmr_pushoff_rank;

    if (rank == 0) {
	kmr_warning(0, 9, "Finalize pushoff_fast_notice");
    }

    /* Try to reclaim CQ entries.  Not exhaustive. */

    do {
	struct FJMPI_Rdma_cq cq;
	cc = FJMPI_Rdma_poll_cq(FJMPI_RDMA_NIC0, &cq);
	assert(cc == 0 || cc == FJMPI_RDMA_NOTICE);
    } while (cc != 0);

    cc = FJMPI_Rdma_dereg_mem(kmr_pushoff_memid);
    assert(cc == 0);
    cc = FJMPI_Rdma_finalize();
    assert(cc == 0);

    kmr_free((void *)kmr_pushoff_area, kmr_pushoff_area_size);
    kmr_pushoff_area = 0;
    kmr_free(kmr_pushoff_addrs, (sizeof(uint64_t) * (size_t)nprocs));
    kmr_pushoff_addrs = 0;
}

/* Notifies an event to RANK by writing 1 to kmr_pushoff_area[0]. */

static void
kmr_pushoff_notice(KMR *mr, int peer)
{
    assert(mr->pushoff_fast_notice);
    assert(kmr_pushoff_area[1] == PUT_VALUE);
    assert(peer != mr->rank);
    int cc;

    /* Reclaim CQ entries. */

    do {
	struct FJMPI_Rdma_cq cq;
	cc = FJMPI_Rdma_poll_cq(FJMPI_RDMA_NIC0, &cq);
	assert(cc == 0 || cc == FJMPI_RDMA_NOTICE);
	mr->pushoff_statistics.counts[STAT_RDMA_POLLCQ_CALLS]++;
    } while (cc != 0);

    int tag0 = 0x7;
    uint64_t ra = kmr_pushoff_addrs[peer];
    uint64_t la = kmr_pushoff_addrs[mr->rank];
    int flag = (FJMPI_RDMA_LOCAL_NIC0|FJMPI_RDMA_REMOTE_NIC0|FJMPI_RDMA_PATH0);
    cc = FJMPI_Rdma_put(peer, tag0, ra, (la + sizeof(int)), 4, flag);
    assert(cc == 0);
}

/*
Copyright (C) 2012-2018 RIKEN R-CCS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/
