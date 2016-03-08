/* kmrmapms.c (2014-02-04) */
/* Copyright (C) 2012-2016 RIKEN AICS */

/** \file kmrmapms.c Master-Slave Mapping on Key-Value Stream. */

#include <mpi.h>
#include <stddef.h>
#include <unistd.h>
#include <limits.h>
#include <poll.h>
#include <netdb.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <arpa/inet.h>
#include <assert.h>
#ifdef _OPENMP
#include <omp.h>
#endif
#include "kmr.h"
#include "kmrimpl.h"

#define MAX(a,b) (((a)>(b))?(a):(b))
#define MIN(a,b) (((a)<(b))?(a):(b))

static const int kmr_kv_buffer_slack_size = 1024;

/* State of each task of kmr_map_ms(). */

enum {
    KMR_RPC_NONE, KMR_RPC_GOON, KMR_RPC_DONE
};

static inline void
kmr_assert_peer_tag(int tag)
{
    assert(KMR_TAG_PEER_0 <= tag && tag < KMR_TAG_PEER_END);
}

/* Special values of task ID.  Task IDs are non-negative.  A task ID
   is included in an RPC request, which is used both for returning a
   result and for wanting a new task.  KMR_RPC_ID_NONE marks no
   results are returned in the first request from a slave thread.
   KMR_RPC_ID_FIN marks a node has finished with all the slave
   threads. */

#define KMR_RPC_ID_NONE -1
#define KMR_RPC_ID_FIN -2

/** Delivers key-value pairs as requested.  It returns MPI_SUCCESS if
    all done, or MPI_ERR_ROOT otherwise.  It finishes the tasks when
    all nodes have contacted and all slave threads are done.
    Protocol: (1) Receive an RPC request (KMR_TAG_REQ).  A request
    consists of a triple of integers (task-ID, peer-tag, result-size)
    ("int req[3]").  The task-ID encodes some special values.  (2)
    Receive a result if a slave has one.  (3) Return a new task if
    available.  A reply consists of a tuple of integers (task-ID,
    argument-size) ("int ack[2]").  (4) Or, return a "no-tasks"
    indicator by ID=KMR_RPC_ID_NONE.  (5) Count "done" messages by
    ID=KMR_RPC_ID_FIN, which indicates the slave node has finished for
    all slave threads.  The task-ID in an RPC request is
    KMR_RPC_ID_NONE for the first request (meaning that the request
    has no result).  Peer-tags are used in subsequent messages to
    direct reply messages to a requesting thread.  */

static int
kmr_map_master(KMR_KVS *kvi, KMR_KVS *kvo,
	       void *arg, struct kmr_option opt, kmr_mapfn_t m)
{
    KMR *mr = kvi->c.mr;
    if (kmr_fields_pointer_p(kvi)) {
	kmr_error(mr, "kmr_map_ms: cannot handle pointer field types");
    }
    assert(kvo->c.key_data == kvi->c.key_data
	   && kvo->c.value_data == kvi->c.value_data);
    MPI_Comm comm = mr->comm;
    int nprocs = mr->nprocs;
    long cnt = kvi->c.element_count;
    assert(INT_MIN <= cnt && cnt <= INT_MAX);
    struct kmr_map_ms_state *ms = kvi->c.ms;
    char *msstates = &(ms->states[0]);
    if (ms == 0) {
	/* First time. */
	size_t hdsz = offsetof(struct kmr_map_ms_state, states);
	ms = kmr_malloc((hdsz + sizeof(char) * (size_t)cnt));
	kvi->c.ms = ms;
	ms->nodes = 0;
	ms->kicks = 0;
	ms->dones = 0;
	msstates = &(ms->states[0]);
	for (long i = 0; i < cnt; i++) {
	    msstates[i] = KMR_RPC_NONE;
	}
    }
    if (ms->dones == cnt && ms->nodes == (nprocs - 1)) {
	/* Finish the task. */
	if (kvi->c.temporary_data != 0) {
	    kmr_free(kvi->c.temporary_data,
		     (sizeof(struct kmr_kvs_entry *) * (size_t)cnt));
	    kvi->c.temporary_data = 0;
	}
	return MPI_SUCCESS;
    }
    /* Make/remake array of key-value pointers. */
    struct kmr_kvs_entry **ev = kvi->c.temporary_data;
    if (ev == 0) {
	ev = kmr_malloc(sizeof(struct kmr_kvs_entry *) * (size_t)cnt);
	kvi->c.temporary_data = ev;
	kvi->c.current_block = kvi->c.first_block;
	struct kmr_kvs_entry *e = kmr_kvs_first_entry(kvi, kvi->c.first_block);
	for (long i = 0; i < cnt; i++) {
	    assert(e != 0);
	    ev[i] = e;
	    e = kmr_kvs_next(kvi, e, 0);
	}
    }
    /* Wait for one request and process it. */
    assert(ms->dones <= ms->kicks);
    MPI_Status st;
    int cc;
    int req[3];
    cc = MPI_Recv(req, 3, MPI_INT, MPI_ANY_SOURCE, KMR_TAG_REQ, comm, &st);
    assert(cc == MPI_SUCCESS);
    int peer_tag = req[0];
    int peer = st.MPI_SOURCE;
    assert(peer != 0);
    {
	int id = req[1];
	int sz = req[2];
	if (id == KMR_RPC_ID_NONE) {
	    /* Got the first request from a peer, no task results. */
	} else if (id == KMR_RPC_ID_FIN) {
	    /* Got the finishing request from a peer. */
	    ms->nodes++;
	    assert(ms->nodes <= (nprocs - 1));
	} else {
	    /* Receive a task result. */
	    assert(id >= 0);
	    kmr_assert_peer_tag(peer_tag);
	    void *packed = kmr_malloc((size_t)sz);
	    cc = MPI_Recv(packed, sz, MPI_BYTE, peer, peer_tag, comm, &st);
	    assert(cc == MPI_SUCCESS);
	    KMR_KVS *kvx = kmr_create_kvs(mr, KMR_KV_BAD, KMR_KV_BAD);
	    cc = kmr_restore_kvs(kvx, packed, (size_t)sz, kmr_noopt);
	    assert(cc == MPI_SUCCESS);
	    struct kmr_option keepopen = {.keep_open = 1};
	    cc = kmr_map(kvx, kvo, 0, keepopen, kmr_add_identity_fn);
	    assert(cc == MPI_SUCCESS);
	    kmr_free(packed, (size_t)sz);
	    assert(msstates[id] == KMR_RPC_GOON);
	    msstates[id] = KMR_RPC_DONE;
	    ms->dones++;
	}
    }
    if (ms->kicks < cnt) {
	/* Send a new task (cnt is in integer range). */
	int id;
	for (id = 0; id < cnt; id++) {
	    if (msstates[id] == KMR_RPC_NONE) {
		break;
	    }
	}
	assert(id != KMR_RPC_ID_NONE && id != cnt);
	struct kmr_kvs_entry *e = ev[id];
	int sz = (int)kmr_kvs_entry_netsize(e);
	assert(sz > 0);
	int ack[2] = {id, sz};
	cc = MPI_Send(ack, 2, MPI_INT, peer, peer_tag, comm);
	assert(cc == MPI_SUCCESS);
	cc = MPI_Send(e, sz, MPI_BYTE, peer, peer_tag, comm);
	assert(cc == MPI_SUCCESS);
	assert(msstates[id] == KMR_RPC_NONE);
	msstates[id] = KMR_RPC_GOON;
	ms->kicks++;
    } else {
	/* Finish the slave. */
	int ack[2] = {KMR_RPC_ID_NONE, 0};
	cc = MPI_Send(ack, 2, MPI_INT, peer, peer_tag, comm);
	assert(cc == MPI_SUCCESS);
    }
    /* Have more entries. */
    return MPI_ERR_ROOT;
}

/** Asks the master for a task, then calls a map-function.  With
    threading, each thread works independently asking the master for a
    task.  It simply protects MPI send/recv calls by OMP critical
    sections, but their grain sizes are too large for uses of OMP
    critical sections.	*/

static int
kmr_map_slave(KMR_KVS *kvi, KMR_KVS *kvo,
	      void *arg, struct kmr_option opt, kmr_mapfn_t m)
{
    assert(!kmr_fields_pointer_p(kvi)
	   && kvo->c.key_data == kvi->c.key_data
	   && kvo->c.value_data == kvi->c.value_data);
    KMR * const mr = kvi->c.mr;
    const MPI_Comm comm = mr->comm;
    const int rank = mr->rank;
    const enum kmr_kv_field keyf = kmr_unit_sized_or_opaque(kvo->c.key_data);
    const enum kmr_kv_field valf = kmr_unit_sized_or_opaque(kvo->c.value_data);
    assert(rank != 0);
    assert(kvi->c.element_count == 0);
#ifdef _OPENMP
    const _Bool threading = !(mr->single_thread || opt.nothreading);
#endif
    KMR_OMP_PARALLEL_IF_(threading)
    {
	int cc;
	int thr = KMR_OMP_GET_THREAD_NUM();
	MPI_Status st;
	struct kmr_kvs_entry *e = 0;
	int maxsz = 0;
	int peer_tag = KMR_TAG_PEER(thr);
	kmr_assert_peer_tag(peer_tag);
	{
	    /* Make the first request. */
	    int req[3] = {peer_tag, KMR_RPC_ID_NONE, 0};
	    KMR_OMP_CRITICAL_
		cc = MPI_Send(req, 3, MPI_INT, 0, KMR_TAG_REQ, comm);
	    assert(cc == MPI_SUCCESS);
	}
	for (;;) {
	    int ack[2];
	    KMR_OMP_CRITICAL_
		cc = MPI_Recv(ack, 2, MPI_INT, 0, peer_tag, comm, &st);
	    assert(cc == MPI_SUCCESS);
	    int id = ack[0];
	    int sz = ack[1];
	    if (id == KMR_RPC_ID_NONE) {
		break;
	    }
	    assert(id >= 0 && sz > 0);
	    if (sz > maxsz) {
		maxsz = (sz + kmr_kv_buffer_slack_size);
		e = kmr_realloc(e, (size_t)maxsz);
		assert(e != 0);
	    }
	    KMR_OMP_CRITICAL_
		cc = MPI_Recv(e, sz, MPI_BYTE, 0, peer_tag, comm, &st);
	    assert(cc == MPI_SUCCESS);
	    /* Invoke mapper. */
	    KMR_KVS *kvx;
	    KMR_OMP_CRITICAL_
		kvx = kmr_create_kvs(mr, keyf, valf);
	    struct kmr_kv_box kv = kmr_pick_kv(e, kvi);
	    cc = (*m)(kv, kvi, kvx, arg, id);
	    if (cc != MPI_SUCCESS) {
		char ee[80];
		snprintf(ee, sizeof(ee),
			 "Map-fn returned with error cc=%d", cc);
		kmr_error(mr, ee);
	    }
	    kmr_add_kv_done(kvx);
	    void *packed = 0;
	    size_t packsz = 0;
	    cc = kmr_save_kvs(kvx, &packed, &packsz, kmr_noopt);
	    assert(cc == MPI_SUCCESS && packed != 0);
	    /* Send a task result. */
	    assert(packsz <= (size_t)INT_MAX);
	    sz = (int)packsz;
	    int req[3] = {peer_tag, id, sz};
	    KMR_OMP_CRITICAL_
		cc = MPI_Send(req, 3, MPI_INT, 0, KMR_TAG_REQ, comm);
	    assert(cc == MPI_SUCCESS);
	    KMR_OMP_CRITICAL_
		cc = MPI_Send(packed, sz, MPI_BYTE, 0, peer_tag, comm);
	    assert(cc == MPI_SUCCESS);
	    /* Cleanup. */
	    KMR_OMP_CRITICAL_
		cc = kmr_free_kvs(kvx);
	    assert(cc == MPI_SUCCESS);
	    kmr_free(packed, packsz);
	}
	if (e != 0) {
	    kmr_free(e, (size_t)maxsz);
	}
    }
    /* (Threads join). */
    {
	/* Make the finishing request. */
	int cc;
	int req[3] = {0, KMR_RPC_ID_FIN, 0};
	cc = MPI_Send(req, 3, MPI_INT, 0, KMR_TAG_REQ, comm);
	assert(cc == MPI_SUCCESS);
    }
    return MPI_SUCCESS;
}

/** Maps in master-slave mode.  The input key-value stream should be
    empty except on rank0 where the master is running (the contents on
    the slave ranks are ignored).  It consumes the input key-value
    stream.  The master does delivery only.  The master returns
    frequently to give a chance to check-pointing, etc.  The master
    returns immaturely each time one pair is delivered, and those
    returns are marked by MPI_ERR_ROOT indicating more tasks remain.
    In contrast, slaves return only after all tasks done.  The enough
    state to have to keep during kmr_map_ms() for check-pointing is in
    the key-value streams KVI and KVO on the master.  Note that this
    totally diverges from bulk-synchronous execution.  It does not
    accept key-value field types KMR_KV_POINTER_OWNED or
    KMR_KV_POINTER_UNMANAGED.  Effective-options: NOTHREADING,
    KEEP_OPEN.	See struct kmr_option. */

int
kmr_map_ms(KMR_KVS *kvi, KMR_KVS *kvo,
	   void *arg, struct kmr_option opt, kmr_mapfn_t m)
{
    kmr_assert_kvs_ok(kvi, kvo, 1, 0);
    KMR *mr = kvi->c.mr;
    struct kmr_option kmr_supported = {.nothreading = 1, .keep_open = 1};
    kmr_check_fn_options(mr, kmr_supported, opt, __func__);
    int kcdc = kmr_ckpt_disable_ckpt(mr);
    int rank = mr->rank;
    long cnt = kvi->c.element_count;
    assert(INT_MIN <= cnt && cnt <= INT_MAX);
    int ccr;
    int cc;
    if (rank == 0) {
	ccr = kmr_map_master(kvi, kvo, arg, opt, m);
	if (ccr == MPI_SUCCESS) {
	    cc = kmr_add_kv_done(kvo);
	    assert(cc == MPI_SUCCESS);
	    cc = kmr_free_kvs(kvi);
	    assert(cc == MPI_SUCCESS);
	}
    } else {
	ccr = kmr_map_slave(kvi, kvo, arg, opt, m);
	cc = kmr_add_kv_done(kvo);
	assert(cc == MPI_SUCCESS);
	cc = kmr_free_kvs(kvi);
	assert(cc == MPI_SUCCESS);
    }
    kmr_ckpt_enable_ckpt(mr, kcdc);
    return ccr;
}

/* ================================================================ */

/* Mode of Spawning.  KMR_SPAWN_INTERACT indicates spawned processes
   interact with a map-function.  KMR_SPAWN_SERIAL and
   KMR_SPAWN_PARALLEL indicates spawned processes do not interact with
   the parent.  KMR_SPAWN_SERIAL is for sequential programs, for which
   a watch-program "kmrwatch0" replies in place of spawned processes.
   KMR_SPAWN_PARALLEL is for independent MPI programs, which do not
   interact with the parent.  Since independent MPI programs run
   freely, it uses a socket connection by a watch-program "kmrwatch0"
   to detect their ends. */

enum kmr_spawn_mode {
    KMR_SPAWN_INTERACT, KMR_SPAWN_SERIAL, KMR_SPAWN_PARALLEL
};

/** State of each Spawning.  The array of this structure is stored in
    the kmr_spawning structure.  RUNNING indicates the spawned
    processes are running.  N_PROCS is the number of processes to be
    spawned (it equals to the COUNT below).  INDEX is the number of
    processes spawned so far, and COUNT is the number of processes of
    the current spawn.  The range INDEX by COUNT enumerates spawned
    processes, and is used to point in the array of the MPI requests.
    ARGC and ARGV are the argument list.  ABUF (byte array of size
    with ALEN), ARGV0 (pointer array of size with ARGC0) are buffers
    for making command line arguments.  ICOMM is the
    inter-communicator.  WATCH_PORT hold a IP port number for
    watch-programs. */

struct kmr_spawn_state {
    _Bool running;
    int n_procs;
    int index;
    int count;
    int argc;
    char **argv;
    int argc0;
    char **argv0;
    size_t alen;
    char *abuf;
    MPI_Comm icomm;
    int watch_port;
    double timestamp[6];
};

/** State of Spawner.  REPLIES hold receive requests for spawned
    processes.  WATCHES hold sockets used to detect the end of the
    spawned processes.  N_STARTEDS is process count of started, and
    N_RUNNINGS is process count that have not finished. */

struct kmr_spawning {
    char *fn;
    enum kmr_spawn_mode mode;
    int n_spawns;
    int n_spawners;
    int n_processes;
    int usize;
    int spawn_limit;

    int n_starteds;
    int n_runnings;

    struct kmr_spawn_state *spawned;
    struct kmr_kv_box *ev;
    MPI_Request *replies;
    int *watches;

    int watch_listener;
    char watch_host[MAXHOSTNAMELEN + 10];
};

/* Sums integers among all ranks.  It is used to check the number of
   ranks which call spawn. */

static int
kmr_sum_on_all_ranks(KMR *mr, int v, int *sum)
{
    assert(sum != 0);
    int cc;
    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    struct kmr_kv_box nkv = {
	.klen = (int)sizeof(long),
	.vlen = (int)sizeof(long),
	.k.i = 0,
	.v.i = v
    };
    cc = kmr_add_kv(kvs0, nkv);
    assert(cc == MPI_SUCCESS);
    kmr_add_kv_done(kvs0);
    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    cc = kmr_replicate(kvs0, kvs1, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    long count;
    cc = kmr_reduce_as_one(kvs1, 0, &count, kmr_noopt, kmr_isum_one_fn);
    assert(cc == MPI_SUCCESS);
    *sum = (int)count;
    return MPI_SUCCESS;
}

static int
kmr_make_pretty_argument_string(char *s, size_t sz, int argc, char **argv)
{
    int cc;
    size_t cnt = 0;
    for (int i = 0; (i < argc && argv[i] != 0); i++) {
	cc = snprintf(&s[cnt], (sz - cnt), (i == 0 ? "%s" : ",%s"), argv[i]);
	cnt += (size_t)cc;
	if (cnt >= sz) {
	    return 0;
	}
    }
    return 0;
}

int
kmr_map_via_spawn_ff(KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
		     int finfo, struct kmr_spawn_option opt,
		     kmr_mapfn_t mapfn)
{
    MPI_Info info = MPI_Info_f2c(finfo);
    int cc = kmr_map_via_spawn(kvi, kvo, arg, info, opt, mapfn);
    return cc;
}

static inline void
kmr_spawn_info_put(struct kmr_spawn_info *info,
		   struct kmr_spawn_state *s,
		   struct kmr_spawn_option opt, void *arg)
{
    info->maparg = arg;
    info->u.icomm = s->icomm;
    info->icomm_ff = MPI_Comm_c2f(s->icomm);
    info->reply_root = opt.reply_root;
}

static inline void
kmr_spawn_info_get(struct kmr_spawn_info *info,
		   struct kmr_spawn_state *s)
{
    if (info->u.icomm != s->icomm) {
	s->icomm = info->u.icomm;
    } else if (info->icomm_ff != MPI_Comm_c2f(s->icomm)) {
	s->icomm = MPI_Comm_f2c(info->icomm_ff);
    }
}

/* Lists processes to spawn from the key-value entries. */

static int
kmr_list_spawns(struct kmr_spawning *spw, KMR_KVS *kvi, MPI_Info info,
		struct kmr_spawn_option opt)
{
    assert(spw->n_spawns == (int)kvi->c.element_count);
    KMR * const mr = kvi->c.mr;
    _Bool tracing5 = (mr->trace_map_spawn && (5 <= mr->verbosity));
    int cc;

    /* Scan key-value pairs and put them in EV. */

    kvi->c.current_block = kvi->c.first_block;
    struct kmr_kvs_entry *e = kmr_kvs_first_entry(kvi, kvi->c.first_block);
    for (int w = 0; w < spw->n_spawns; w++) {
	spw->ev[w] = kmr_pick_kv(e, kvi);
	e = kmr_kvs_next(kvi, e, 0);
    }

    /* Share the universe evenly by all ranks which call spawn. */

    int nranks;
    cc = kmr_sum_on_all_ranks(mr, ((spw->n_spawns > 0) ? 1 : 0), &nranks);
    assert(cc == MPI_SUCCESS && nranks <= mr->nprocs);
    spw->n_spawners = nranks;
    int *usizep;
    int uflag;
    cc = MPI_Attr_get(MPI_COMM_WORLD, MPI_UNIVERSE_SIZE, &usizep, &uflag);
    if (cc != MPI_SUCCESS || uflag == 0) {
	char ee[80];
	snprintf(ee, sizeof(ee), "%s: MPI lacks universe size", spw->fn);
	kmr_error(mr, ee);
    }
    spw->usize = *usizep;
    if (spw->usize <= mr->nprocs) {
	char ee[80];
	snprintf(ee, sizeof(ee), "%s: no dynamic processes in universe",
		 spw->fn);
	kmr_error(mr, ee);
    }
    int m = spw->usize - mr->nprocs;
    if (spw->n_spawners != 0) {
	m /= spw->n_spawners;
    }
    spw->spawn_limit = ((mr->spawn_max_processes != 0)
		       ? MIN(mr->spawn_max_processes, m)
		       : m);
    if (tracing5) {
	if (spw->n_spawns > 0) {
	    fprintf(stderr,
		    ";;KMR [%05d] %s: universe-size=%d spawn-limit=%d\n",
		    mr->rank, spw->fn, spw->usize, spw->spawn_limit);
	    fflush(0);
	}
    }

    /* Take MAXPROCS from info if defined. */

    int maxprocs = -1;
    {
	char *infoval = kmr_malloc((size_t)(MPI_MAX_INFO_VAL + 1));
	int iflag;
	if (info != MPI_INFO_NULL) {
	    cc = MPI_Info_get(info, "maxprocs", MPI_MAX_INFO_VAL,
			      infoval, &iflag);
	    assert(cc == MPI_SUCCESS);
	} else {
	    iflag = 0;
	}
	if (iflag != 0) {
	    int v;
	    cc = kmr_parse_int(infoval, &v);
	    if (cc == 0 || v < 0) {
		char ee[80];
		snprintf(ee, sizeof(ee), "%s: bad value in info maxprocs=%s",
			 spw->fn, infoval);
		kmr_error(mr, ee);
		maxprocs = -1;
	    } else {
		maxprocs = v;
	    }
	} else {
	    maxprocs = -1;
	}
	kmr_free(infoval, (size_t)(MPI_MAX_INFO_VAL + 1));
    }

    /* Make the arguments to spawn. */

    spw->n_processes = 0;
    for (int w = 0; w < spw->n_spawns; w++) {
	struct kmr_kv_box kv = spw->ev[w];
	struct kmr_spawn_state *s = &(spw->spawned[w]);
	s->running = 0;
	s->n_procs = -1;
	s->index = 0;
	s->count = 0;
	s->argc = 0;
	s->argv = 0;
	s->argc0 = 0;
	s->argv0 = 0;
	s->alen = 0;
	s->abuf = 0;
	s->icomm = MPI_COMM_NULL;
	s->watch_port = -1;

	s->alen = (size_t)kv.vlen;
	s->abuf = kmr_malloc(s->alen);
	memcpy(s->abuf, kv.v.p, (size_t)kv.vlen);
	int maxargc;
	cc = kmr_scan_argv_strings(mr, s->abuf, s->alen,
				   0, &maxargc, 0,
				   opt.separator_space, spw->fn);
	assert(cc == MPI_SUCCESS);
	s->argv0 = kmr_malloc(sizeof(char *) * (size_t)(maxargc + 1));
	memset(s->argv0, 0, (sizeof(char *) * (size_t)(maxargc + 1)));
	cc = kmr_scan_argv_strings(mr, s->abuf, s->alen,
				   (maxargc + 1), &s->argc0, s->argv0,
				   opt.separator_space, spw->fn);
	assert(cc == MPI_SUCCESS);
	assert(maxargc == s->argc0);

	/* Check if the "MAXPROCS=" string in the arguments. */

	if (s->argc0 > 0 && strncmp("maxprocs=", s->argv0[0], 9) == 0) {
	    int v;
	    cc = kmr_parse_int(&s->argv0[0][9], &v);
	    if (cc == 0 || v < 0) {
		char ee[80];
		snprintf(ee, sizeof(ee), "%s: bad maxprocs=%s",
			 spw->fn, s->argv0[0]);
		kmr_error(mr, ee);
	    }
	    s->n_procs = v;
	    s->argc = (s->argc0 - 1);
	    s->argv = (s->argv0 + 1);
	} else {
	    s->n_procs = maxprocs;
	    s->argc = s->argc0;
	    s->argv = s->argv0;
	}

	if (s->argc <= 0) {
	    char ee[80];
	    snprintf(ee, sizeof(ee), "%s: no arguments", spw->fn);
	    kmr_error(mr, ee);
	}
	if (s->n_procs <= 0) {
	    char ee[80];
	    snprintf(ee, sizeof(ee), "%s: maxprocs not specified",
		     spw->fn);
	    kmr_error(mr, ee);
	}
	if (s->n_procs > spw->spawn_limit) {
	    char ee[80];
	    snprintf(ee, sizeof(ee),
		     "%s: maxprocs too large, (maxprocs=%d limit=%d)",
		     spw->fn, s->n_procs, spw->spawn_limit);
	    kmr_error(mr, ee);
	}

	spw->n_processes += s->n_procs;
    }

    return MPI_SUCCESS;
}

static int
kmr_free_comm_with_tracing(KMR *mr, struct kmr_spawning *spw,
			   struct kmr_spawn_state *s)
{
    int cc;
    _Bool tracing5 = (mr->trace_map_spawn && (5 <= mr->verbosity));
    if (s->icomm != MPI_COMM_NULL) {
	if (tracing5) {
	    ptrdiff_t done = (s - spw->spawned);
	    fprintf(stderr, (";;KMR [%05d] %s [%ld]:"
			     " MPI_Comm_free (could block)...\n"),
		    mr->rank, spw->fn, done);
	    fflush(0);
	}

	if (!mr->spawn_disconnect_but_free) {
	    cc = MPI_Comm_free(&(s->icomm));
	} else {
	    cc = MPI_Comm_disconnect(&(s->icomm));
	}
	assert(cc == MPI_SUCCESS);

	if (tracing5) {
	    ptrdiff_t done = (s - spw->spawned);
	    fprintf(stderr, (";;KMR [%05d] %s [%ld]:"
			     " MPI_Comm_free done\n"),
		    mr->rank, spw->fn, done);
	    fflush(0);
	}
    }
    return MPI_SUCCESS;
}

/* Makes a listening socket for a watch-program.  It fills
   WATCH_LISTENER (fd) and WATCH_HOST fields in the array of SPAWNS,
   if successful. */

static int
kmr_listen_to_watch(KMR *mr, struct kmr_spawning *spw, int index)
{
    assert(sizeof(spw->watch_host) >= 46);
    int cc;
    union {
	struct sockaddr sa;
	struct sockaddr_in sa4;
	struct sockaddr_in6 sa6;
	struct sockaddr_storage ss;
    } sa;
    char hostname[MAXHOSTNAMELEN];
    char address[INET6_ADDRSTRLEN];

    /* Not use AF_UNSPEC. */

    int af = mr->spawn_watch_af;
    assert(af == 0 || af == 4 || af == 6);
    char *family = (af == 4 ? "AF_INET" : "AF_INET6");

    assert(spw->watch_listener == -1);
    const int *ports = mr->spawn_watch_port_range;
    assert(ports[0] != 0 || ports[1] == 0);
    for (int port = ports[0]; port <= ports[1]; port++) {
	if (af == 4) {
	    sa.sa.sa_family = AF_INET;
	} else if (af == 0 || af == 6) {
	    sa.sa.sa_family = AF_INET6;
	} else {
	    assert(0);
	}
	int fd = socket(sa.sa.sa_family, SOCK_STREAM, 0);
	if (fd < 0) {
	    char ee[80];
	    char *m = strerror(errno);
	    snprintf(ee, sizeof(ee), "%s: socket(%s) failed: %s",
		     spw->fn, family, m);
	    kmr_error(mr, ee);
	}
	int one = 1;
	cc = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
	if (cc != 0) {
	    char ee[80];
	    char *m = strerror(errno);
	    snprintf(ee, sizeof(ee), "%s: setsockopt(SO_REUSEADDR): %s",
		     spw->fn, m);
	    kmr_warning(mr, 1, ee);
	}

	socklen_t salen;
	if (af == 4) {
	    memset(&sa, 0, sizeof(sa));
	    sa.sa4.sin_family = AF_INET;
	    sa.sa4.sin_addr.s_addr = htonl(INADDR_ANY);
	    sa.sa4.sin_port = htons((uint16_t)port);
	    salen = sizeof(sa.sa4);
	} else if (af == 0 || af == 6) {
	    memset(&sa, 0, sizeof(sa));
	    sa.sa6.sin6_family = AF_INET6;
	    sa.sa6.sin6_addr = in6addr_any;
	    sa.sa6.sin6_port = htons((uint16_t)port);
	    salen = sizeof(sa.sa6);
	} else {
	    assert(0);
	}

	/* NOTE: Linux returns EINVAL for EADDRINUSE in bind. */

	cc = bind(fd, &sa.sa, salen);
	if (cc != 0) {
	    if (errno == EADDRINUSE || errno == EINVAL) {
		cc = close(fd);
		assert(cc == 0);
		continue;
	    } else {
		char ee[80];
		char *m = strerror(errno);
		snprintf(ee, sizeof(ee), "%s: bind(%s, port=%d) failed: %s",
			 spw->fn, family, port, m);
		kmr_error(mr, ee);
	    }
	}

	/* NOTE: Linux may return EADDRINUSE in listen, too. */

	int backlog = spw->spawn_limit;
	cc = listen(fd, backlog);
	if (cc != 0) {
	    if (errno == EADDRINUSE || errno == EINVAL) {
		cc = close(fd);
		assert(cc == 0);
		continue;
	    } else {
		char ee[80];
		char *m = strerror(errno);
		snprintf(ee, sizeof(ee), "%s: listen(%s, port=%d) failed: %s",
			 spw->fn, family, port, m);
		kmr_error(mr, ee);
	    }
	}
	assert(fd != -1);
	spw->watch_listener = fd;
	break;
    }

    int fd = spw->watch_listener;
    if (fd == -1) {
	char ee[80];
	snprintf(ee, sizeof(ee), "%s: no ports to listen to watch-programs",
		 spw->fn);
	kmr_error(mr, ee);
    }

    /* Get address and port number from the socket. */

    memset(&sa, 0, sizeof(sa));
    socklen_t salen = sizeof(sa);
    cc = getsockname(fd, &sa.sa, &salen);
    if (cc != 0) {
	char ee[80];
	char *m = strerror(errno);
	snprintf(ee, sizeof(ee), "%s: getsockname() failed: %s",
		 spw->fn, m);
	kmr_error(mr, ee);
    }

    int port = 0;
    if (sa.sa.sa_family == AF_INET) {
	port = ntohs(sa.sa4.sin_port);
    } else if (sa.sa.sa_family == AF_INET6) {
	port = ntohs(sa.sa6.sin6_port);
    } else {
	char ee[80];
	snprintf(ee, sizeof(ee), "%s: getsockname(): unknown ip family=%d",
		 spw->fn, sa.sa.sa_family);
	kmr_error(mr, ee);
    }
    assert(port != 0);

    if (mr->spawn_watch_host_name != 0) {
	cc = snprintf(hostname, sizeof(hostname),
		      "%s", mr->spawn_watch_host_name);
	assert(cc < (int)sizeof(hostname));
    } else {
	cc = gethostname(hostname, sizeof(hostname));
	if (cc != 0) {
	    char ee[80];
	    char *m = strerror(errno);
	    snprintf(ee, sizeof(ee), "%s: gethostname() failed: %s",
		     spw->fn, m);
	    kmr_error(mr, ee);
	}
    }

    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = AI_ADDRCONFIG;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    if (af == 4) {
	hints.ai_family = AF_INET;
    } else if (af == 6) {
	hints.ai_family = AF_INET6;
    } else if (af == 0) {
	hints.ai_family = (AF_INET6 | AI_V4MAPPED);
    } else {
	assert(0);
    }
    struct addrinfo *addrs = 0;
    cc = getaddrinfo(hostname, 0, &hints, &addrs);
    if (cc != 0) {
	char ee[80];
	const char *m = gai_strerror(cc);
	snprintf(ee, sizeof(ee), "%s: getaddrinfo(%s) failed: %s",
		 spw->fn, hostname, m);
	kmr_error(mr, ee);
    }
    struct addrinfo *p;
    for (p = addrs; p != 0; p = p->ai_next) {
	if (!(p->ai_family == AF_INET || p->ai_family == AF_INET6)) {
	    continue;
	}
	if (af == 4 && p->ai_family != AF_INET) {
	    continue;
	}
	if (af == 6 && p->ai_family != AF_INET6) {
	    continue;
	}
	break;
    }
    if (p == 0) {
	char ee[80];
	snprintf(ee, sizeof(ee), "%s: getaddrinfo(%s): no address for host",
		 spw->fn, hostname);
	kmr_error(mr, ee);
    }

    if (p->ai_family == AF_INET) {
	void *inaddr = &(((struct sockaddr_in *)p->ai_addr)->sin_addr);
	inet_ntop(p->ai_family, inaddr, address, sizeof(address));
    } else if (p->ai_family == AF_INET6) {
	void *inaddr = &(((struct sockaddr_in6 *)p->ai_addr)->sin6_addr);
	inet_ntop(p->ai_family, inaddr, address, sizeof(address));
    } else {
	char ee[80];
	snprintf(ee, sizeof(ee), "%s: getaddrinfo(%s): unknown ip family=%d",
		 spw->fn, hostname, p->ai_family);
	kmr_error(mr, ee);
    }
    freeaddrinfo(addrs);

    assert(0 <= index && index < spw->n_spawns);
    struct kmr_spawn_state *s = &(spw->spawned[index]);
    s->watch_port = port;

    cc = snprintf(spw->watch_host, sizeof(spw->watch_host),
		  "%s", address);
    assert(cc < (int)sizeof(spw->watch_host));

    return MPI_SUCCESS;
}

/* Waits for connections from the watch-programs of all the spawned
   processes.  It works on spawning one by one. */

static int
kmr_accept_on_watch(KMR *mr, struct kmr_spawning *spw, int index)
{
    _Bool tracing5 = (mr->trace_map_spawn && (5 <= mr->verbosity));
    assert(0 <= index && index < spw->n_spawns);
    struct kmr_spawn_state *s = &(spw->spawned[index]);
    assert(s->n_procs > 0);
    union {
	struct sockaddr sa;
	struct sockaddr_in sa4;
	struct sockaddr_in6 sa6;
	struct sockaddr_storage ss;
    } sa;
    int cc;

    assert(spw->watch_listener != -1);
    int fd0 = spw->watch_listener;
    for (int count = 0; count < s->n_procs; count++) {
	for (;;) {
	    nfds_t nfds = 1;
	    struct pollfd fds0, *fds = &fds0;
	    memset(fds, 0, (sizeof(struct pollfd) * nfds));
	    fds[0].fd = fd0;
	    fds[0].events = (POLLIN|POLLPRI);
	    fds[0].revents = 0;

	    assert(mr->spawn_watch_accept_onhold_msec >= (60 * 1000));
	    int msec = mr->spawn_watch_accept_onhold_msec;
	    int nn = poll(fds, nfds, msec);
	    if (nn == 0) {
		char ee[80];
		snprintf(ee, sizeof(ee),
			 "%s: accepting watch-programs timed out"
			 " (msec=%d)", spw->fn, msec);
		kmr_error(mr, ee);
	    } else if (nn < 0 && (errno == EAGAIN || errno == EINTR)) {
		char ee[80];
		char *m = strerror(errno);
		snprintf(ee, sizeof(ee),
			 "%s: poll (for watch-programs) returned: %s",
			 spw->fn, m);
		kmr_warning(mr, 1, ee);
		continue;
	    } else if (nn < 0){
		char ee[80];
		char *m = strerror(errno);
		snprintf(ee, sizeof(ee),
			 "%s: poll (for watch-programs) failed: %s",
			 spw->fn, m);
		kmr_error(mr, ee);
	    }
	    break;
	}

	memset(&sa, 0, sizeof(sa));
	socklen_t salen = sizeof(sa);
	int fd = accept(fd0, &sa.sa, &salen);
	if (fd == -1) {
	    char ee[80];
	    char *m = strerror(errno);
	    snprintf(ee, sizeof(ee),
		     "%s: accept (for watch-programs) failed: %s",
		     spw->fn, m);
	    kmr_error(mr, ee);
	}

	/*setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));*/

	if (tracing5) {
	    char address[INET6_ADDRSTRLEN];
	    //int port = 0;
	    if (sa.sa.sa_family == AF_INET) {
		void *inaddr = &sa.sa4.sin_addr;
		inet_ntop(sa.sa.sa_family, inaddr, address, sizeof(address));
		//port = ntohs(sa.sa4.sin_port);
	    } else if (sa.sa.sa_family == AF_INET6) {
		void *inaddr = &sa.sa6.sin6_addr;
		inet_ntop(sa.sa.sa_family, inaddr, address, sizeof(address));
		//port = ntohs(sa.sa6.sin6_port);
	    } else {
		char ee[80];
		snprintf(ee, sizeof(ee), "%s: accept(): unknown ip family=%d",
			 spw->fn, sa.sa.sa_family);
		kmr_error(mr, ee);
	    }

	    fprintf(stderr, (";;KMR [%05d] %s [%d]:"
			     " accepting a connection of watch-programs"
			     " on port=%d from %s (%d/%d)\n"),
		    mr->rank, spw->fn, index,
		    s->watch_port, address, (count + 1), s->n_procs);
	    fflush(0);
	}

	int val;
	if (count == 0 || mr->spawn_watch_all) {
	    assert((s->index + count) <= spw->n_processes);
	    spw->watches[s->index + count] = fd;
	    /* send 1 when connection is accepted */
	    val = 1;
	} else {
	    val = 0;
	}
	ssize_t wsize = write(fd, &val, sizeof(int));
	if (wsize < 0) {
	    char ee[80];
	    char *m = strerror(errno);
	    snprintf(ee, sizeof(ee),
		     "%s: write (for watch-programs) failed: %s",
		     spw->fn, m);
	    kmr_error(mr, ee);
	}
	assert(wsize == sizeof(int));

	int rval;
	ssize_t rsize = read(fd, &rval, sizeof(int));
	if (rsize < 0) {
	    char ee[80];
	    char *m = strerror(errno);
	    snprintf(ee, sizeof(ee),
		     "%s: read (for watch-programs) failed: %s",
		     spw->fn, m);
	    kmr_error(mr, ee);
	}
	assert(rsize == sizeof(int));
	assert(val == rval);

	if (!(count == 0 || mr->spawn_watch_all)) {
	    cc = close(fd);
	    assert(cc == 0);
	}
    }

    cc = close(spw->watch_listener);
    assert(cc == 0);
    spw->watch_listener = -1;

    return MPI_SUCCESS;
}

static int
kmr_receive_for_reply(KMR *mr, struct kmr_spawning *spw,
		      int w, _Bool replyeach, _Bool replyroot)
{
    assert(0 <= w && w < spw->n_spawns);
    int cc;
    enum kmr_spawn_mode mode = spw->mode;
    struct kmr_spawn_state *s = &(spw->spawned[w]);
    MPI_Request *reqs = spw->replies;
    if (mode == KMR_SPAWN_INTERACT) {
	if (replyeach) {
	    assert(s->index + s->count <= spw->n_processes);
	    for (int rank = 0; rank < s->count; rank++) {
		assert(reqs[s->index + rank] == MPI_REQUEST_NULL);
		cc = MPI_Irecv(0, 0, MPI_BYTE,
			       rank, KMR_TAG_SPAWN_REPLY,
			       s->icomm, &reqs[s->index + rank]);
		assert(cc == MPI_SUCCESS);
		assert(reqs[s->index + rank] != MPI_REQUEST_NULL);
	    }
	} else if (replyroot) {
	    assert(w <= spw->n_processes);
	    int rank = 0;
	    assert(reqs[w] == MPI_REQUEST_NULL);
	    cc = MPI_Irecv(0, 0, MPI_BYTE,
			   rank, KMR_TAG_SPAWN_REPLY,
			   s->icomm, &reqs[w]);
	    assert(cc == MPI_SUCCESS);
	    assert(reqs[w] != MPI_REQUEST_NULL);
	} else {
	    /*nothing*/
	}
    } else if (mode == KMR_SPAWN_SERIAL) {
	assert(replyeach);
	{
	    assert(s->index + s->count <= spw->n_processes);
	    for (int rank = 0; rank < s->count; rank++) {
		assert(reqs[s->index + rank] == MPI_REQUEST_NULL);
		cc = MPI_Irecv(0, 0, MPI_BYTE,
			       rank, KMR_TAG_SPAWN_REPLY,
			       s->icomm, &reqs[s->index + rank]);
		assert(cc == MPI_SUCCESS);
		assert(reqs[s->index + rank] != MPI_REQUEST_NULL);
	    }
	}
    } else {
	assert(mode == KMR_SPAWN_INTERACT || mode == KMR_SPAWN_SERIAL);
    }
    return MPI_SUCCESS;
}

/* Waits for a single reply and checks a finished spawn from the
   request index.  It returns an index of the n-th spawning or -1 if
   nothing has finished.  Or, when no replies are expected, it
   immediately returns a minimum index of the spawns still running.
   The return value is an index of the array SPAWNED.  */

static int
kmr_wait_for_reply(KMR *mr, struct kmr_spawning *spw,
		   struct kmr_spawn_option opt)
{
    _Bool tracing5 = (mr->trace_map_spawn && (5 <= mr->verbosity));
    int cc;
    MPI_Request *reqs = spw->replies;
    if (opt.reply_each) {
	MPI_Status st;
	int index;
	int nwait = spw->n_processes;
	cc = MPI_Waitany(nwait, reqs, &index, &st);
	assert(cc == MPI_SUCCESS && index != MPI_UNDEFINED);
	assert(index < spw->n_processes);
	assert(reqs[index] == MPI_REQUEST_NULL);
	/* Find spawned state from the request index. */
	int done = -1;
	for (int w = 0; w < spw->n_spawns; w++) {
	    struct kmr_spawn_state *s = &spw->spawned[w];
	    if (index < (s->index + s->count)) {
		assert(s->index <= index);
		done = w;
		break;
	    }
	}
	assert(done != -1);
	struct kmr_spawn_state *s = &spw->spawned[done];
	assert(s->running);
	int count = (opt.reply_each ? s->count : 1);
	int nreplies = 0;
	assert((s->index + count) <= spw->n_processes);
	for (int j = 0; j < count; j++) {
	    if (reqs[s->index + j] == MPI_REQUEST_NULL) {
		nreplies++;
	    }
	}

	if (tracing5) {
	    fprintf(stderr, (";;KMR [%05d] %s [%d]: got a reply (%d/%d)\n"),
		    mr->rank, spw->fn, done, nreplies, count);
	    fflush(0);
	}

	_Bool fin = (nreplies == count);
	return (fin ? done : -1);
    } else if (opt.reply_root) {
	MPI_Status st;
	int index;
	int nwait = spw->n_spawns;
	cc = MPI_Waitany(nwait, reqs, &index, &st);
	assert(cc == MPI_SUCCESS && index != MPI_UNDEFINED);
	assert(index <= spw->n_spawns);
	assert(reqs[index] == MPI_REQUEST_NULL);
	int done = index;
	assert(0 <= done && done < spw->n_spawns);
	struct kmr_spawn_state *s = &spw->spawned[done];
	assert(s->running);
	assert(reqs[done] == MPI_REQUEST_NULL);

	if (tracing5) {
	    fprintf(stderr, (";;KMR [%05d] %s [%d]: got a root reply\n"),
		    mr->rank, spw->fn, done);
	    fflush(0);
	}

	return done;
    } else {
	int done = -1;
	for (int w = 0; w < spw->n_spawns; w++) {
	    struct kmr_spawn_state *s = &spw->spawned[w];
	    if (s->running) {
		done = w;
		break;
	    }
	}

	if (tracing5) {
	    fprintf(stderr, (";;KMR [%05d] %s [%d]: (no checks of replies)\n"),
		    mr->rank, spw->fn, done);
	    fflush(0);
	}

	assert(done != -1);
	return done;
    }
}

/* Waits for the end of some spawned process.  It detects the end by
   closure of a socket of the watch-program.  It returns an index of
   the n-th spawning, or -1 if nothing has finished.  It waits for one
   to finish, but it may possibly return with nothing with -1.  (It
   avoids using MPI_STATUS_IGNORE in MPI_Testany() for a bug in some
   versions of Open MPI (around 1.6.3)) */

static int
kmr_wait_for_watch(KMR *mr, struct kmr_spawning *spw,
		   struct kmr_spawn_option _)
{
    _Bool tracing5 = (mr->trace_map_spawn && (5 <= mr->verbosity));
    int cc;
    char garbage[4096];

    int nruns = 0;
    for (int w = 0; w < spw->n_spawns; w++) {
	struct kmr_spawn_state *s = &(spw->spawned[w]);
	if (s->running) {
	    nruns += s->count;
	}
    }
    assert(nruns != 0 && spw->n_runnings == nruns);

    nfds_t nfds = 0;
    for (int i = 0; i < spw->n_processes; i++) {
	if (spw->watches[i] != -1) {
	    nfds++;
	}
    }
    assert(nfds != 0);

    struct pollfd *fds = kmr_malloc(sizeof(struct pollfd) * (size_t)nfds);

    int done = -1;
    for (;;) {
	memset(fds, 0, (sizeof(struct pollfd) * nfds));
	nfds_t fdix = 0;
	for (int i = 0; i < spw->n_processes; i++) {
	    if (spw->watches[i] != -1) {
		assert(fdix < nfds);
		fds[fdix].fd = spw->watches[i];
		fds[fdix].events = (POLLIN|POLLPRI);
		fds[fdix].revents = 0;
		fdix++;
	    }
	}
	assert(fdix == nfds);

	if (tracing5) {
	    fprintf(stderr, (";;KMR [%05d] %s:"
			     " waiting for some watch-programs finish\n"),
		    mr->rank, spw->fn);
	    fflush(0);
	}

	for (;;) {
	    int msec = 1;
	    int nn = poll(fds, nfds, msec);
	    if (nn == 0) {
		int index;
		int ok;
		MPI_Status st;
		MPI_Testany(0, 0, &index, &ok, &st);
		/*kmr_warning(mr, 1,
			    "poll (for watch-programs)"
			    " timed out badly; continuing");*/
		continue;
	    } else if (nn < 0 && (errno == EAGAIN || errno == EINTR)) {
		char ee[80];
		char *m = strerror(errno);
		snprintf(ee, sizeof(ee),
			 ("poll (for watch-programs) interrupted;"
			  " continuing: %s"), m);
		kmr_warning(mr, 1, ee);
		continue;
	    } else if (nn < 0){
		char ee[80];
		char *m = strerror(errno);
		snprintf(ee, sizeof(ee),
			 "%s: poll (for watch-programs) failed: %s",
			 spw->fn, m);
		kmr_error(mr, ee);
	    } else {
		break;
	    }
	}

	int fd = -1;
	for (nfds_t k = 0; k < nfds; k++) {
	    if (fds[k].fd != -1 && fds[k].revents != 0) {
		fd = fds[k].fd;
		break;
	    }
	}
	if (fd == -1) {
	    char ee[80];
	    snprintf(ee, sizeof(ee), "poll (for watch-programs) no FD found");
	    kmr_warning(mr, 1, ee);
	    continue;
	}

	int index = -1;
	for (int w = 0; w < spw->n_spawns; w++) {
	    struct kmr_spawn_state *s = &(spw->spawned[w]);
	    assert((s->index + s->count) <= spw->n_processes);
	    for (int j = 0; j < s->count; j++) {
		if (fd == spw->watches[s->index + j]) {
		    index = (s->index + j);
		    done = w;
		    break;
		}
	    }
	}
	assert(fd != -1 && index != -1 && done != -1);
	assert(0 <= index && index < spw->n_processes);
	assert(0 <= done && done < spw->n_spawns);
	//struct kmr_spawn_state *s = &(spw->spawned[done]);

	ssize_t rr = read(fd, garbage, sizeof(garbage));
	if (rr == 0) {
	    /* Got EOF. */
	    assert(fd == spw->watches[index]);
	    cc = close(fd);
	    assert(cc == 0);
	    spw->watches[index] = -1;
	    break;
	} else if (rr > 0) {
	    /* Read out garbage data. */
	    continue;
	} else if (rr == -1 && (errno == EAGAIN || errno == EINTR)) {
	    char ee[80];
	    char *m = strerror(errno);
	    snprintf(ee, sizeof(ee),
		     "read (for watch-programs) returned: %s", m);
	    kmr_warning(mr, 1, ee);
	    continue;
	} else if (rr == -1) {
	    char ee[80];
	    char *m = strerror(errno);
	    snprintf(ee, sizeof(ee),
		     "%s: read (for watch-programs) failed: %s",
		     spw->fn, m);
	    kmr_error(mr, ee);
	}
    }
    assert(done != -1);

    assert(0 <= done && done < spw->n_spawns);
    struct kmr_spawn_state *s = &(spw->spawned[done]);
    int count = ((mr->spawn_watch_all) ? s->count : 1);
    int nreplies = 0;
    assert((s->index + count) <= spw->n_processes);
    for (int j = 0; j < count; j++) {
	if (spw->watches[s->index + j] == -1) {
	    nreplies++;
	}
    }

    if (tracing5) {
	fprintf(stderr, (";;KMR [%05d] %s [%d]:"
			 " detected a watch done (%d/%d)\n"),
		mr->rank, spw->fn, done, nreplies, count);
	fflush(0);
    }

    _Bool fin = (nreplies == count);
    if (fin) {
	if (s->icomm != MPI_COMM_NULL) {
	    cc = kmr_free_comm_with_tracing(mr, spw, s);
	    assert(cc == MPI_SUCCESS);
	}
    }

    kmr_free(fds, (sizeof(struct pollfd) * (size_t)nfds));
    return (fin ? done : -1);
}

static int
kmr_wait_then_map(KMR *mr, struct kmr_spawning *spw,
		  KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
		  struct kmr_spawn_option opt, kmr_mapfn_t m)
{
    int cc;
    enum kmr_spawn_mode mode = spw->mode;
    int done;
    if (mode == KMR_SPAWN_INTERACT) {
	done = kmr_wait_for_reply(mr, spw, opt);
    } else if (mode == KMR_SPAWN_SERIAL) {
	done = kmr_wait_for_reply(mr, spw, opt);
    } else if (mode == KMR_SPAWN_PARALLEL) {
	done = kmr_wait_for_watch(mr, spw, opt);
    } else {
	assert(0);
	done = -1;
    }
    if (done != -1) {
	assert(0 <= done && done < spw->n_spawns);
	struct kmr_spawn_state *s = &(spw->spawned[done]);
	s->timestamp[3] = MPI_Wtime();
	if (m != 0) {
	    if (mr->spawn_pass_intercomm_in_argument
		&& mode == KMR_SPAWN_INTERACT) {
		assert(mr->spawn_comms != 0);
		assert(mr->spawn_comms[done] == &(s->icomm));
		struct kmr_spawn_info si;
		kmr_spawn_info_put(&si, s, opt, arg);
		cc = (*m)(spw->ev[done], kvi, kvo, &si, done);
		if (cc != MPI_SUCCESS) {
		    char ee[80];
		    snprintf(ee, sizeof(ee),
			     "Map-fn returned with error cc=%d", cc);
		    kmr_error(mr, ee);
		}
		kmr_spawn_info_get(&si, s);
	    } else {
		cc = (*m)(spw->ev[done], kvi, kvo, arg, done);
		if (cc != MPI_SUCCESS) {
		    char ee[80];
		    snprintf(ee, sizeof(ee),
			     "Map-fn returned with error cc=%d", cc);
		    kmr_error(mr, ee);
		}
	    }
	}
	s->timestamp[4] = MPI_Wtime();
	if (s->icomm != MPI_COMM_NULL) {
	    cc = kmr_free_comm_with_tracing(mr, spw, s);
	    assert(cc == MPI_SUCCESS);
	}
	s->timestamp[5] = MPI_Wtime();
	assert(s->running);
	s->running = 0;
	spw->n_runnings -= s->count;
	if (kmr_ckpt_enabled(mr)) {
	    kmr_ckpt_save_kvo_each_add(mr, kvo, done);
	}
    }
    return MPI_SUCCESS;
}

static int
kmr_map_spawned_processes(enum kmr_spawn_mode mode, char *name,
			  KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
			  MPI_Info info,
			  struct kmr_spawn_option opt, kmr_mapfn_t mapfn)
{
    kmr_assert_kvs_ok(kvi, kvo, 1, 0);
    assert(kvi->c.value_data == KMR_KV_OPAQUE
	   || kvi->c.value_data == KMR_KV_CSTRING);
    assert(kvi->c.element_count <= INT_MAX);
    _Bool use_reply = (mode == KMR_SPAWN_INTERACT || mode == KMR_SPAWN_SERIAL);
    _Bool use_watch = (mode != KMR_SPAWN_INTERACT);
    KMR * const mr = kvi->c.mr;
    _Bool tracing5 = (mr->trace_map_spawn && (5 <= mr->verbosity));
    struct kmr_spawning spawning0;
    struct kmr_spawning *spw = &spawning0;
    char magic[20];
    char hostport[MAXHOSTNAMELEN + 10];
    int from = 0;
    int cc;

    if (use_watch) {
	int kcdc = kmr_ckpt_disable_ckpt(mr);
	cc = kmr_install_watch_program(mr, name);
	assert(cc == MPI_SUCCESS);
	kmr_ckpt_enable_ckpt(mr, kcdc);
    }

    int cnt = (int)kvi->c.element_count;
    memset(spw, 0, sizeof(struct kmr_spawning));
    spw->fn = name;
    spw->mode = mode;
    spw->n_spawns = cnt;
    spw->n_starteds = 0;
    spw->n_runnings = 0;
    spw->spawned = kmr_malloc(sizeof(struct kmr_spawn_state) * (size_t)spw->n_spawns);
    spw->ev = kmr_malloc(sizeof(struct kmr_kv_box) * (size_t)spw->n_spawns);
    spw->watch_listener = -1;
    spw->watch_host[0] = 0;

    int kcdc = kmr_ckpt_disable_ckpt(mr);
    cc = kmr_list_spawns(spw, kvi, info, opt);
    assert(cc == MPI_SUCCESS);
    kmr_ckpt_enable_ckpt(mr, kcdc);

    if (kmr_ckpt_enabled(mr)) {
	struct kmr_option kopt = kmr_noopt;
	if (opt.take_ckpt) {
	    kopt.take_ckpt = 1;
	}
	if (kmr_ckpt_progress_init(kvi, kvo, kopt)) {
	    if (kvo != 0) {
		/* No "keep_open" option (!opt.keep_open). */
		kmr_add_kv_done(kvo);
	    }
	    if (1) {
		/* No "inspect" option (!opt.inspect). */
		kmr_free_kvs(kvi);
	    }
	    return MPI_SUCCESS;
	}
	from = (int)kmr_ckpt_first_unprocessed_kv(mr);
	kmr_ckpt_save_kvo_each_init(mr, kvo);
    }

    if (use_reply) {
	assert(spw->replies == 0);
	spw->replies = kmr_malloc(sizeof(MPI_Request) * (size_t)spw->n_processes);
	for (int i = 0; i < spw->n_processes; i++) {
	    spw->replies[i] = MPI_REQUEST_NULL;
	}
    }
    if (mode == KMR_SPAWN_PARALLEL) {
	assert(spw->watches == 0);
	spw->watches = kmr_malloc(sizeof(int) * (size_t)spw->n_processes);
	for (int i = 0; i < spw->n_processes; i++) {
	    spw->watches[i] = -1;
	}
    }
    assert(mr->spawn_comms == 0);
    mr->spawn_size = spw->n_spawns;
    mr->spawn_comms = kmr_malloc(sizeof(MPI_Comm *) * (size_t)spw->n_spawns);
    for (int w = 0; w < spw->n_spawns; w++) {
	mr->spawn_comms[w] = &(spw->spawned[w].icomm);
    }

    int gap;
    if (mr->spawn_gap_msec[0] == 0) {
	gap = 0;
    } else {
	int usz = 0;
	unsigned int v = (unsigned int)spw->usize;
	while (v > 0) {
	    v = (v >> 1);
	    usz++;
	}
	gap = (int)((((long)mr->spawn_gap_msec[1] * usz) / 10)
		    + mr->spawn_gap_msec[0]);
    }

    /* Spawn by each entry. */

    for (int w = from; w < spw->n_spawns; w++) {
	struct kmr_spawn_state *s = &(spw->spawned[w]);

	/* Wait while no more processes are available. */

	if ((spw->n_runnings + s->n_procs) > spw->spawn_limit) {
	    while ((spw->n_runnings + s->n_procs) > spw->spawn_limit) {
		cc = kmr_wait_then_map(mr, spw,
				       kvi, kvo, arg, opt, mapfn);
		assert(cc == MPI_SUCCESS);
	    }
	    if (gap != 0) {
		if (tracing5) {
		    fprintf(stderr,
			    ";;KMR [%05d] %s: sleeping for spawn gap"
			    " (%d msec)\n",
			    mr->rank, spw->fn, gap);
		    fflush(0);
		}
		kmr_msleep(gap, 1);
	    }
	}

	if (mode == KMR_SPAWN_PARALLEL) {
	    cc = kmr_listen_to_watch(mr, spw, w);
	    assert(cc == MPI_SUCCESS);
	} else {
	    cc = snprintf(spw->watch_host, sizeof(spw->watch_host),
			  "0");
	    assert(cc < (int)sizeof(spw->watch_host));
	}

	{
	    char **argv;
	    int argc;
	    if (use_watch) {
		argc = (s->argc + 5);
		argv = kmr_malloc(sizeof(char *) * (size_t)(argc + 1));

		cc = snprintf(hostport, sizeof(hostport),
			      "%s/%d", spw->watch_host, s->watch_port);
		assert(cc < (int)sizeof(hostport));

		unsigned int vv = (unsigned int)random();
		cc = snprintf(magic, sizeof(magic), "%08xN%dV0%s",
			      vv, w, ((mr->trace_map_spawn) ? "T1" : ""));
		assert(cc < (int)sizeof(magic));

		assert(mr->spawn_watch_program != 0);
		argv[0] = mr->spawn_watch_program;
		argv[1] = ((mode == KMR_SPAWN_SERIAL) ? "seq" : "mpi");
		argv[2] = hostport;
		argv[3] = magic;
		argv[4] = "--";
		for (int i = 0; i < s->argc; i++) {
		    argv[5 + i] = s->argv[i];
		}
		argv[(s->argc + 5)] = 0;
	    } else {
		argc = s->argc;
		argv = s->argv;
	    }
	    assert(argv[argc] == 0);

	    if (tracing5) {
		char ee[160];
		kmr_make_pretty_argument_string(ee, sizeof(ee), argc, argv);
		fprintf(stderr, (";;KMR [%05d] %s [%d]: MPI_Comm_spawn"
				 " (maxprocs=%d;%s)\n"),
			mr->rank, spw->fn, w, s->n_procs, ee);
		fflush(0);
	    }

	    s->timestamp[0] = MPI_Wtime();

	    int nspawns;
	    assert(s->icomm == MPI_COMM_NULL);
	    int *ec = kmr_malloc(sizeof(int) * (size_t)s->n_procs);
	    const int root = 0;
	    MPI_Comm spawncomm = MPI_COMM_SELF;
	    cc = MPI_Comm_spawn(argv[0], &(argv[1]), s->n_procs, info,
				root, spawncomm, &s->icomm, ec);
	    assert(cc == MPI_SUCCESS || cc == MPI_ERR_SPAWN);
	    if (cc == MPI_ERR_SPAWN) {
		/* SOFT-case. */
		nspawns = 0;
		for (int r = 0; r < s->n_procs; r++) {
		    if (ec[r] == MPI_SUCCESS) {
			nspawns++;
		    }
		}
	    } else {
		nspawns = s->n_procs;
	    }
	    assert(nspawns > 0);

	    s->timestamp[1] = MPI_Wtime();

	    kmr_free(ec, (sizeof(int) * (size_t)s->n_procs));
	    if (argv != s->argv) {
		kmr_free(argv, (sizeof(char *) * (size_t)(argc + 1)));
	    }
	    argv = 0;

	    s->running = 1;
	    s->index = spw->n_starteds;
	    s->count = nspawns;
	    spw->n_starteds += nspawns;
	    spw->n_runnings += nspawns;
	}

	if (mode == KMR_SPAWN_PARALLEL) {
	    cc = kmr_accept_on_watch(mr, spw, w);
	    assert(cc == MPI_SUCCESS);
	}

	if (mr->spawn_disconnect_early && mode == KMR_SPAWN_PARALLEL) {
	    if (s->icomm != MPI_COMM_NULL) {
		cc = kmr_free_comm_with_tracing(mr, spw, s);
		assert(cc == MPI_SUCCESS);
	    }
	}

	if (mr->spawn_sync_at_startup && s->icomm != MPI_COMM_NULL) {
	    int flag;
	    cc = MPI_Comm_test_inter(s->icomm, &flag);
	    assert(cc == MPI_SUCCESS && flag != 0);
	    int peernprocs;
	    cc = MPI_Comm_remote_size(s->icomm, &peernprocs);
	    assert(cc == MPI_SUCCESS && peernprocs == s->count);
	}

	s->timestamp[2] = MPI_Wtime();

	if (use_reply) {
	    cc = kmr_receive_for_reply(mr, spw, w,
				       opt.reply_each, opt.reply_root);
	    assert(cc == MPI_SUCCESS);
	}
    }

    while (spw->n_runnings > 0) {
	cc = kmr_wait_then_map(mr, spw,
			       kvi, kvo, arg, opt, mapfn);
	assert(cc == MPI_SUCCESS);
    }

    if (tracing5) {
	for (int w = 0; w < spw->n_spawns; w++) {
	    struct kmr_spawn_state *s = &(spw->spawned[w]);
	    fprintf(stderr, (";;KMR [%05d] %s [%d/%d]"
			     " timing:"
			     " spawn=%f setup=%f run=%f mapfn=%f clean=%f"
			     " (msec)\n"),
		    mr->rank, spw->fn, w, spw->n_spawns,
		    ((s->timestamp[1] - s->timestamp[0]) * 1e3),
		    ((s->timestamp[2] - s->timestamp[1]) * 1e3),
		    ((s->timestamp[3] - s->timestamp[2]) * 1e3),
		    ((s->timestamp[4] - s->timestamp[3]) * 1e3),
		    ((s->timestamp[5] - s->timestamp[4]) * 1e3));
	    fflush(0);
	}
    }

    assert(mr->spawn_comms != 0);
    mr->spawn_size = 0;
    kmr_free(mr->spawn_comms, (sizeof(MPI_Comm *) * (size_t)spw->n_spawns));
    mr->spawn_comms = 0;

    for (int w = 0; w < spw->n_spawns; w++) {
	struct kmr_spawn_state *s = &(spw->spawned[w]);
	assert(s->icomm == MPI_COMM_NULL);
	assert(s->abuf != 0);
	kmr_free(s->abuf, s->alen);
	s->abuf = 0;
	assert(s->argv0 != 0);
	kmr_free(s->argv0, (sizeof(char *) * (size_t)(s->argc0 + 1)));
	s->argv0 = 0;
    }

    assert(spw->ev != 0);
    kmr_free(spw->ev, (sizeof(struct kmr_kv_box) * (size_t)spw->n_spawns));
    spw->ev = 0;
    assert(spw->spawned != 0);
    kmr_free(spw->spawned, (sizeof(struct kmr_spawn_state) * (size_t)spw->n_spawns));
    spw->spawned = 0;

    if (use_reply) {
	assert(spw->replies != 0);
	for (int i = 0; i < spw->n_processes; i++) {
	    assert(spw->replies[i] == MPI_REQUEST_NULL);
	}
	kmr_free(spw->replies, (sizeof(MPI_Request) * (size_t)spw->n_processes));
	spw->replies = 0;
    }
    if (mode == KMR_SPAWN_PARALLEL) {
	assert(spw->watches != 0);
	for (int i = 0; i < spw->n_processes; i++) {
	    assert(spw->watches[i] == -1);
	}
	kmr_free(spw->watches, (sizeof(int) * (size_t)spw->n_processes));
	spw->watches = 0;
    }

    assert(spw->watch_listener == -1);

    if (kmr_ckpt_enabled(mr)) {
	kmr_ckpt_save_kvo_each_fin(mr, kvo);
    }

    if (kvo != 0) {
	/* No "keep_open" option (!opt.keep_open). */
	kmr_add_kv_done(kvo);
    }
    if (1) {
	/* No "inspect" option (!opt.inspect). */
	kmr_free_kvs(kvi);
    }

    if (kmr_ckpt_enabled(mr)) {
	kmr_ckpt_progress_fin(mr);
    }

    return MPI_SUCCESS;
}

/** Sends a reply message in the spawned process, which tells it is
    ready to finish and may have some data to send to the spawner in
    kmr_map_via_spawn(). */

int
kmr_reply_to_spawner(KMR *mr)
{
    int cc;
    MPI_Comm ic = MPI_COMM_NULL;
    cc = MPI_Comm_get_parent(&ic);
    assert(cc == MPI_SUCCESS);
    if (ic == MPI_COMM_NULL) {
	kmr_error(mr, ("kmr_reply_to_spawner:"
		       " may be called in a not-spawned process"));
    }
    int peer = 0;
    cc = MPI_Send(0, 0, MPI_BYTE, peer, KMR_TAG_SPAWN_REPLY, ic);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/** Obtains (a reference to) a parent inter-communicator of a spawned
    process.  It is used inside a map-function of kmr_map_via_spawn();
    Pass INDEX the same argument to a map-function.  It returns a
    reference for the side-effect of freeing a communicator in a
    map-function. */

MPI_Comm *
kmr_get_spawner_communicator(KMR *mr, long index)
{
    if (mr->spawn_comms == 0) {
	kmr_error(mr, ("kmr_get_spawner_communicator() be called"
		       " outside of kmr_map_via_spawn()"));
    }
    if (index >= mr->spawn_size) {
	kmr_error(mr, ("kmr_get_spawner_communicator() be called"
		       " with index out of range"));
    }
    MPI_Comm *comm = mr->spawn_comms[index];
    return comm;
}

int
kmr_get_spawner_communicator_ff(KMR *mr, long ii, int *comm)
{
    MPI_Comm *c = kmr_get_spawner_communicator(mr, ii);
    *comm = MPI_Comm_c2f(*c);
    return MPI_SUCCESS;
}

/** Maps on processes started by MPI_Comm_spawn().  It is intended to
    run custom MPI programs which will return a reply as MPI messages.
    Consider other variations to run independent processes, when the
    spawned processes will not interact with the parent:
    kmr_map_processes() or kmr_map_ms_commands().\n The spawner
    (parent) spawns processes specified by key-value pairs.  The key
    part is ignored, and the value part is a list of null-separated
    strings which constitutes a command and arguments.  The option
    SEPARATOR_SPACE changes the separator character to whitespaces.
    If the first string is "maxprocs=n", then the number of processes
    is taken from this string.  Or, an MPI_Info entry "maxprocs" in
    INFO is used, and "maxprocs" is common to all spawns.  It is an
    error if neither is specified.  The multile spawners (more than
    one ranks can have entries to spawn) divide the universe of
    processes evenly among them, and tries to control the number of
    the simultaneously running processes in the range.\n The option
    REPLY_EACH or REPLY_ROOT lets the spawner wait for the reply
    messages from the spawned processes, and then the spawner calls
    the map-function.  A reply message is of the tag
    KMR_TAG_SPAWN_REPLY=500 and length zero, and
    kmr_reply_to_spawner() can be used to send this reply.  When none
    of REPLY_EACH or REPLY_ROOT are specified, the spawner immediately
    calls the map-function one-by-one in the FIFO order (before the
    spawned processes finish).  In that case, no load-balance is
    taken.  The map-function should wait for the spawned processes to
    finish, otherwise, the spawner starts next spawns continuously and
    runs out the processes, which causes the MPI runtime to signal an
    error.\n Communication between the spawned processes and the
    map-function of the spawner is through the inter-communicator.
    The parent inter-communicator of the spawned processes can be
    taken by MPI_Comm_get_parent() as usual.  The inter-communicator
    at the spawner side can be obtained by calling
    kmr_get_spawner_communicator() inside a map-function.\n The INFO
    argument is passed to MPI_Comm_spawn() unchanged.\n NOTE: There is
    no way to check the availability of processes for spawning in the
    MPI specification and MPI implementations. And, the MPI runtime
    signals errors when it runs out the processes.  Thus, it puts a
    sleep (1 sec) in between MPI_Comm_spawn() calls to allow clean-ups
    in the MPI runtime and to avoid timing issues.\n INTERFACE CHANGE:
    Set mr->spawn_pass_intercomm_in_argument=1 to enables the old
    interface, where the map-function MAPFN is called with the
    kmr_spawn_state structure as the general argument.  The argument
    ARG passed to the mapper is stored in the MAPARG slot in the
    kmr_spawn_state structure.
    When TAKE_CKPT option is specified, a checkpoint data file of the
    output key-value stream is saved if both CKPT_ENABLE and
    CKPT_SELECTIVE global options are set. */

int
kmr_map_via_spawn(KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
		  MPI_Info info, struct kmr_spawn_option opt,
		  kmr_mapfn_t mapfn)
{
    int cc = kmr_map_spawned_processes(KMR_SPAWN_INTERACT,
				       "kmr_map_via_spawn",
				       kvi, kvo, arg, info, opt, mapfn);
    return cc;
}

/** Maps on processes started by MPI_Comm_spawn() to run independent
    MPI processes, which will not communicate to the parent.  The
    programs need to be MPI.  It is a variation of
    kmr_map_via_spawn(), and refer to the comment on it for the basic
    usage.  Since the spawned program does not know the parent, there
    is no way to communicate from the spawner.  The map-function is
    called after the processes have exited, so that the map-function
    can check the result files created by the spawned processes.\n
    This function detects the end of spawned processes using a
    watch-program "kmrwatch0", by checking a closure of a socket to
    which "kmrwatch0" connected.\n NOTE THAT THIS OPERATION WILL BLOCK
    INDEFINITELY AND FAIL, DEPENDING ON THE BEHAVIOR OF AN MPI
    IMPLEMENTATION.  It is checked to work with Open MPI (1.6) and
    MPICH2 (1.5), but not with Intel MPI (4.1) and YAMPI2 (GridMPI
    2.1).  It depends on the behavior that MPI_Comm_free() on the
    parent and MPI_Finalize() on the child do not synchronize.  The
    quote of the standard (MPI 2.x) says: "Though collective,
    MPI_Comm_free is anticipated that this operation will normally be
    implemented to be local, ..."  The blocking situation can be
    checked by enabling tracing around calls to MPI_Comm_free() by
    (mr->trace_map_spawn=1).\n NOTE (on MPI spawn implementations):
    Open MPI (1.6) allows to spawn non-MPI processes by passing an
    special MPI_Info.  MPICH2 (1.5) does not allow to spawn non-MPI
    processes, because MPI_Comm_spawn() of the parent and MPI_Init()
    of the child synchronize.  In Intel MPI (4.1) and YAMPI2
    (GridMPI), the calls of MPI_Comm_free() on the parent and
    MPI_Finalize() or MPI_Comm_free() on the child synchronize, and
    thus, they require to call MPI_Comm_free() at an appropriate time
    on the parent.\n  Options REPLY_ROOT and REPLY_EACH have no
    effect.
    When TAKE_CKPT option is specified, a checkpoint data file of the
    output key-value stream is saved if both CKPT_ENABLE and
    CKPT_SELECTIVE global options are set. */

int
kmr_map_parallel_processes(KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
			   MPI_Info info, struct kmr_spawn_option opt,
			   kmr_mapfn_t mapfn)
{
    struct kmr_spawn_option ssopt = opt;
    ssopt.reply_root = 0;
    ssopt.reply_each = 1;
    int cc = kmr_map_spawned_processes(KMR_SPAWN_PARALLEL,
				       "kmr_map_parallel_processes",
				       kvi, kvo, arg, info, ssopt, mapfn);
    return cc;
}

/** Maps on processes started by MPI_Comm_spawn() to run serial
    processes.  This should NOT be used; Use kmr_map_ms_commands(),
    instead.  Fork-execing in kmr_map_ms_commands() is simpler than
    spawning.  See also the comment on kmr_map_via_spawn() and
    kmr_map_parallel_processes().  The map-function is called after
    the processes have exited, thus, there is no way to communicate
    from the map-function.  Instead, the map-function can check the
    result files created by the spawned processes.\n This function
    detects the end of spawned processes using a watch-program
    "kmrwatch0" which sends a reply to the parent in place of the
    serial program.  Options REPLY_ROOT and REPLY_EACH have no
    effect.
    When TAKE_CKPT option is specified, a checkpoint data file of the
    output key-value stream is saved if both CKPT_ENABLE and
    CKPT_SELECTIVE global options are set.  */

int
kmr_map_serial_processes(KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
			 MPI_Info info, struct kmr_spawn_option opt,
			 kmr_mapfn_t mapfn)
{
    struct kmr_spawn_option ssopt = opt;
    ssopt.reply_root = 0;
    ssopt.reply_each = 1;
    int cc = kmr_map_spawned_processes(KMR_SPAWN_SERIAL,
				       "kmr_map_serial_processes",
				       kvi, kvo, arg, info, ssopt, mapfn);
    return cc;
}

/** Maps on processes started by MPI_Comm_spawn() to run independent
    processes.  It either calls kmr_map_parallel_processes() or
    kmr_map_serial_processes() with regard to the NONMPI argument.
    See the comments of kmr_map_parallel_processes() and
    kmr_map_serial_processes(). */

int
kmr_map_processes(_Bool nonmpi, KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
		  MPI_Info info, struct kmr_spawn_option opt,
		  kmr_mapfn_t mapfn)
{
    KMR *mr = kvi->c.mr;
    if (opt.reply_root || opt.reply_each) {
	kmr_error(mr, "kmr_map_processes:"
		  " options REPLY_ROOT/REPLY_EACH not allowed");
    }

    struct kmr_spawn_option ssopt = opt;
    ssopt.reply_root = 0;
    ssopt.reply_each = 1;
    if (nonmpi) {
	int cc = kmr_map_spawned_processes(KMR_SPAWN_SERIAL,
					   "kmr_map_processes",
					   kvi, kvo, arg, info, ssopt, mapfn);
	return cc;
    } else {
	int cc = kmr_map_spawned_processes(KMR_SPAWN_PARALLEL,
					   "kmr_map_processes",
					   kvi, kvo, arg, info, ssopt, mapfn);
	return cc;
    }
}

/* Creates a dummy context in spawned processes.  It only be used to
   make KVS for adding elements. */

KMR *
kmr_create_dummy_context(void)
{
    KMR *mr = kmr_create_context(MPI_COMM_SELF, MPI_INFO_NULL, 0);
    return mr;
}

/** Sends the KVS from a spawned process to the map-function of the
    spawner.  It is paired with kmr_receive_kvs_from_spawned_fn(). */

int
kmr_send_kvs_to_spawner(KMR *mr, KMR_KVS *kvs)
{
    int cc;
    MPI_Comm ic = MPI_COMM_NULL;
    cc = MPI_Comm_get_parent(&ic);
    assert(cc == MPI_SUCCESS);
    if (ic == MPI_COMM_NULL) {
	kmr_error(mr, ("kmr_send_kvs_to_spawner:"
		       " may be called in a not-spawned process"));
    }
    void *data = 0;
    size_t sz = 0;
    cc = kmr_save_kvs(kvs, &data, &sz, kmr_noopt);
    assert(cc == MPI_SUCCESS && data != 0 && sz != 0);
    int siz = (int)sz;
    int peer = 0;
    cc = MPI_Send(&siz, 1, MPI_INT, peer, KMR_TAG_SPAWN_REPLY1, ic);
    assert(cc == MPI_SUCCESS);
    cc = MPI_Send(data, (int)sz, MPI_BYTE, peer, KMR_TAG_SPAWN_REPLY1, ic);
    assert(cc == MPI_SUCCESS);
    free(data);
    return MPI_SUCCESS;
}

/** Collects key-value pairs generated by spawned processes.  It is a
    map-function to be used with kmr_map_via_spawn() with the
    REPLY_EACH option.  The spawned processes call
    kmr_send_kvs_to_spawner() to send generated key-value pairs, and
    this function receives and puts them into KVO.  PROTOCOL: The
    reply consists of one or two messages with the tag
    KMR_TAG_SPAWN_REPLY1=501.  One is the data size, which is followed
    by a marshaled key-value stream when the data size is non-zero. */

int
kmr_receive_kvs_from_spawned_fn(const struct kmr_kv_box kv,
				const KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
				const long index)
{
    _Bool replyeach = 1;
    assert(kvi != 0);
    KMR * const mr = kvi->c.mr;
    MPI_Comm *icommr = kmr_get_spawner_communicator(mr, index);
    assert(icommr != 0);
    MPI_Comm icomm = *icommr;
    int cc;
    int peernprocs;
    cc = MPI_Comm_remote_size(icomm, &peernprocs);
    assert(cc == MPI_SUCCESS);
    int npeers = (replyeach ? peernprocs : 1);
    for (int peerrank = 0; peerrank < npeers; peerrank++) {
	assert(kvo != 0);
	MPI_Status st;
	int sz;
	cc = MPI_Recv(&sz, 1, MPI_INT,
		      peerrank, KMR_TAG_SPAWN_REPLY1,
		      icomm, &st);
	assert(cc == MPI_SUCCESS);
	if (sz == 0) {
	    continue;
	}
	void *data = kmr_malloc((size_t)sz);
	cc = MPI_Recv(data, sz, MPI_BYTE,
		      peerrank, KMR_TAG_SPAWN_REPLY1,
		      icomm, &st);
	assert(cc == MPI_SUCCESS);
	KMR_KVS *kvx = kmr_create_kvs(mr, KMR_KV_BAD, KMR_KV_BAD);
	cc = kmr_restore_kvs(kvx, data, (size_t)sz, kmr_noopt);
	assert(cc == MPI_SUCCESS);
	struct kmr_option keepopen = {.keep_open = 1};
	cc = kmr_map(kvx, kvo, 0, keepopen, kmr_add_identity_fn);
	assert(cc == MPI_SUCCESS);
	kmr_free(data, (size_t)sz);
    }
    return MPI_SUCCESS;
}

struct kmr_map_ms_commands_argument {
    struct kmr_spawn_option opt;
    void *arg;
    kmr_mapfn_t fn;
};

/** Runs commands in kmr_map_ms_commands(). */

static int
kmr_map_ms_fork_exec_command(const struct kmr_kv_box kv,
			     const KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
			     const long index)
{
    char *name = "kmr_map_ms_commands";
    KMR *mr = kvi->c.mr;
    _Bool tracing5 = (mr->trace_map_ms && (5 <= mr->verbosity));
    struct kmr_map_ms_commands_argument *xarg = arg;
    struct kmr_spawn_option opt = xarg->opt;
    int cc;
    char *abuf = kmr_malloc((size_t)kv.vlen);
    memcpy(abuf, kv.v.p, (size_t)kv.vlen);
    int argc;
    char *argv[256];
    const int maxargc = (sizeof(argv) / sizeof(*argv));
    cc = kmr_scan_argv_strings(mr, abuf, (size_t)kv.vlen, maxargc,
			       &argc, argv,
			       opt.separator_space, name);
    assert(cc == MPI_SUCCESS);
    argv[argc] = 0;

    if (tracing5) {
	char ss[160];
	kmr_make_pretty_argument_string(ss, sizeof(ss), argc, argv);
	fprintf(stderr,
		";;KMR [%05d] %s: fork-exec: %s\n",
		mr->rank, name, ss);
	fflush(0);
    }

    int closefds;
    if (mr->keep_fds_at_fork) {
	closefds = 0;
    } else {
	closefds = kmr_getdtablesize(mr);
    }

    int pid = fork();
    if (pid == -1) {
	char ee[80];
	char *m = strerror(errno);
	snprintf(ee, sizeof(ee), "%s: fork() failed: %s",
		 name, m);
	kmr_error(mr, ee);
    } else {
	if (pid == 0) {
	    for (int fd = 3; fd < closefds; fd++) {
		close(fd);
	    }
	    cc = execvp(argv[0], argv);
	    char ss[160];
	    kmr_make_pretty_argument_string(ss, sizeof(ss), argc, argv);
	    if (cc == -1) {
		char ee[80];
		char *m = strerror(errno);
		snprintf(ee, sizeof(ee), "%s: execvp(%s) failed: %s",
			 name, ss, m);
		kmr_error(mr, ee);
	    } else {
		char ee[80];
		snprintf(ee, sizeof(ee), "%s: execvp(%s) returned with cc=%d",
			 name, ss, cc);
		kmr_error(mr, ee);
	    }
	} else {
	    int st;
	    cc = waitpid(pid, &st, 0);
	    if (cc == -1) {
		if (errno == EINTR) {
		    char ee[80];
		    snprintf(ee, sizeof(ee), "%s: waitpid() interrupted",
			     name);
		    kmr_warning(mr, 1, ee);
		} else {
		    char ee[80];
		    char *m = strerror(errno);
		    snprintf(ee, sizeof(ee), "%s: waitpid() failed: %s",
			     name, m);
		    kmr_warning(mr, 1, ee);
		}
	    }
	}
    }

    if (tracing5) {
	char ss[160];
	kmr_make_pretty_argument_string(ss, sizeof(ss), argc, argv);
	fprintf(stderr,
		";;KMR [%05d] %s: fork-exec done: %s\n",
		mr->rank, name, ss);
	fflush(0);
    }

    cc = (*xarg->fn)(kv, kvi, kvo, xarg->arg, index);
    assert(cc == MPI_SUCCESS);

    kmr_free(abuf, (size_t)kv.vlen);
    return MPI_SUCCESS;
}

/** Maps in master-slave mode, specialized to run serial commands.  It
    fork-execs commands specified by key-values, then calls a
    map-function at finishes of the commands.  It takes the commands
    in the same way as kmr_map_via_spawn().  The commands never be MPI
    programs.  It is implemented with kmr_map_ms(); see the comments
    on kmr_map_ms(). */

int
kmr_map_ms_commands(KMR_KVS *kvi, KMR_KVS *kvo,
		    void *arg, struct kmr_option opt,
		    struct kmr_spawn_option sopt, kmr_mapfn_t m)
{
    int cc;
    struct kmr_map_ms_commands_argument xarg = {
	.arg = arg,
	.opt = sopt,
	.fn = m
    };
    cc = kmr_map_ms(kvi, kvo, &xarg, opt, kmr_map_ms_fork_exec_command);
    return cc;
}

/*
Copyright (C) 2012-2016 RIKEN AICS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/
