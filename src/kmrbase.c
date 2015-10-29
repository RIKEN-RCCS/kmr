/* kmrbase.c (2014-02-04) */
/* Copyright (C) 2012-2015 RIKEN AICS */

/** \file kmrbase.c KMR Base Implementation (on-memory operations).
    KMR aims at fast shuffling and scalability, and provides modest
    utilities for programming with map-reduce.  This part implements
    on-memory operations. */

/* NOTE: (1) KMR and KMR_KVS are handled collectively (allocated,
   modified, and freed). */

#include <mpi.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <errno.h>
#include <assert.h>
#include <ctype.h>
#ifdef _OPENMP
#include <omp.h>
#endif
#include "../config.h"
#include "kmr.h"
#include "kmrimpl.h"

#define KMR_TRACE_ENABLE 1

#if KMR_TRACE_ENABLE
#include "kmrtrace.h"
kmr_trace_t KT[1];
#endif

int KMR_API_ID = 0;
const int kmr_version = KMR_H;

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))
#define NEVERHERE 0

/* Default unit of allocation of memory block for key-value pairs.
   See preset_block_size in struct kmr_ctx. */

#define BLOCK_SIZE (64 * 1024 * 1024)

/* Default of the number of entries pooled before calling a
   map-function.  See mapper_park_size in struct kmr_ctx. */

#define MAP_PARK_SIZE (1024)

/* Default block size of a push-off key-value stream.  See See
   pushoff_block_size in struct kmr_ctx. */

#define PUSHOFF_SIZE (64 * 1024)

/* Checks if an end-of-block marker is properly placed.  It only
   checks when a key-value stream consists of a single preallocated
   block. */

static inline void
kmr_assert_on_tail_marker(KMR_KVS *kvs)
{
    if (kvs != 0 && kvs->c.block_count == 1) {
	struct kmr_kvs_block *b = kvs->c.first_block;
	size_t netsz = kvs->c.storage_netsize;
	struct kmr_kvs_entry *e = kmr_kvs_entry_at(kvs, b, netsz);
	assert((((intptr_t)e) & 3) == 0
	       && e->klen == -1 && e->vlen == -1);
    }
}

/* Sets up the environment (nothing currently).  It checks the data
   sizes in C meets the KMR assumptions.  It also checks MPI_LONG has
   8-byte size, that is assumed in all-to-all communication.  It calls
   an OMP function, to initialize the threads environment here. */

int
kmr_init_2(int ignore)
{
    int cc;
    assert(sizeof(long) == sizeof(size_t)
	   && sizeof(long) == sizeof(ssize_t)
	   && sizeof(long) == sizeof(off_t)
	   && sizeof(long) == sizeof(uint64_t)
	   && sizeof(long) >= sizeof(intptr_t)
	   && sizeof(long) >= sizeof(void *));
    assert(kmr_check_alignment(offsetof(struct kmr_kvs_entry, c)));
    assert(kmr_check_alignment(offsetof(struct kmr_kvs_block, data)));
    assert(kmr_check_alignment(offsetof(struct kmr_ntuple_entry, len)));
    assert(sizeof(struct kmr_option) == sizeof(long)
	   && sizeof(struct kmr_file_option) == sizeof(long)
	   && sizeof(struct kmr_spawn_option) == sizeof(long));
    MPI_Aint lb;
    MPI_Aint extent;
    cc = MPI_Type_get_extent(MPI_LONG, &lb, &extent);
    assert(cc == MPI_SUCCESS);
    assert(lb == 0 && extent == 8);

#if 0
    KMR_OMP_PARALLEL_
    {
	int tid = omp_get_thread_num();
	assert(tid >= 0);
    }
#endif

#if KMR_TRACE_ENABLE
    kmr_trace_init();
    kmr_trace_start();
#endif
    
    return MPI_SUCCESS;
}

/** Sets up the environment, and checks the constant definitions in C
    and Fortran are consistent. */

int
kmr_init_ff(int kf, struct kmr_option opt, struct kmr_file_option fopt)
{
    union {struct kmr_option o; unsigned long i;} opt0 = {.o = opt};
    union {struct kmr_file_option o; unsigned long i;} fopt0 = {.o = fopt};
    opt0.o.rank_zero = 0;
    fopt0.o.shuffle_names = 0;
    assert(kf == KMR_KV_POINTER_UNMANAGED
	   && opt.rank_zero && fopt.shuffle_names
	   && opt0.i == 0 && fopt0.i == 0);
    kmr_init_2(0);
    return MPI_SUCCESS;
}

/** Clears the environment. */

int
kmr_fin(void)
{
#if KMR_TRACE_ENABLE
    kmr_trace_stop();
    kmr_trace_dump();
    kmr_trace_fini();
#endif  
    return MPI_SUCCESS;
}

/** Makes a new KMR context (a context has type KMR).  A KMR context
    is a record of common information to all key-value streams.  COMM
    is a communicator for use inside.  It dups the given communicator
    inside, to avoid conflicts with other calls to MPI functions.  MPI
    should be initialized with a thread support level of either
    MPI_THREAD_SERIALIZED or MPI_THREAD_MULTIPLE.  CONF specifies
    configuration options.  It should be freed after a call.  The
    options can differ on each rank, (in this version).  The
    configuration options are first taken from a file with a name
    specified by the environment variable "KMROPTION" on rank0, and
    they are merged with the explicitly given ones.  The KMROPTION
    file has the file format of Java properties (but only in Latin
    characters).  Refer to JDK documents on "java.util.Properties" (on
    "load" method) for the file format.  The explicitly given ones
    have precedence.  IDENTIFYING_NAME is just recorded in the
    context, and has no specific use.  It may be null. */

KMR *
kmr_create_context(const MPI_Comm comm, const MPI_Info conf,
		   const char *identifying_name)
{
    int cc;
    KMR *mr = kmr_malloc(sizeof(struct kmr_ctx));
    KMR_DEBUGX(memset(mr, 0, sizeof(struct kmr_ctx)));

    cc = MPI_Comm_size(comm, &mr->nprocs);
    assert(cc == MPI_SUCCESS);
    cc = MPI_Comm_rank(comm, &mr->rank);
    assert(cc == MPI_SUCCESS);
    cc = MPI_Comm_dup(comm, &mr->comm);
    if (cc != MPI_SUCCESS) {
	kmr_error_mpi(mr, "MPI_Comm_dup", cc);
	MPI_Abort(MPI_COMM_WORLD, 1);
    }

#ifdef _OPENMP
    int omp_thrd = omp_get_thread_limit();
#else
    int omp_thrd = 1;
#endif
    assert(omp_thrd >= 1);

    int mpi_thrd;
    cc = MPI_Query_thread(&mpi_thrd);
    assert(cc == MPI_SUCCESS);
    assert(mpi_thrd == MPI_THREAD_SINGLE
	   || mpi_thrd == MPI_THREAD_FUNNELED
	   || mpi_thrd == MPI_THREAD_SERIALIZED
	   || mpi_thrd == MPI_THREAD_MULTIPLE);
    if (mpi_thrd == MPI_THREAD_SINGLE
	|| mpi_thrd == MPI_THREAD_FUNNELED) {
	if (omp_thrd > 1) {
	    char ee[80];
	    char *s = ((mpi_thrd == MPI_THREAD_SINGLE)
		       ? "MPI_THREAD_SINGLE"
		       : "MPI_THREAD_FUNNELED");
	    snprintf(ee, sizeof(ee), "Thread support of MPI is low: %s", s);
	    kmr_warning(mr, 1, ee);
	}
    }

    mr->kvses.head = 0;
    mr->kvses.tail = 0;

    mr->ckpt_kvs_id_counter = 0;
    mr->ckpt_ctx = 0;
    mr->ckpt_enable = 0;
    mr->ckpt_selective = 0;
    mr->ckpt_no_fsync = 0;

    mr->log_traces = 0;
    mr->atwork = 0;

    mr->spawn_size = 0;
    mr->spawn_comms = 0;

    mr->mapper_park_size = MAP_PARK_SIZE;
    mr->preset_block_size = BLOCK_SIZE;
    mr->malloc_overhead = (int)sizeof(void *);

    mr->atoa_threshold = 512;

    mr->sort_trivial = 100000;
    mr->sort_threshold = 100L;
    mr->sort_sample_factor = 10000;
    mr->sort_threads_depth = 5;

    mr->file_io_block_size = (1024 * 1024);

    mr->pushoff_block_size = PUSHOFF_SIZE;
    mr->pushoff_poll_rate = 0;

#if defined(KMRLIBDIR)
    mr->kmr_installation_path = KMRLIBDIR;
#else
    mr->kmr_installation_path = 0;
#endif
    mr->spawn_watch_program = 0;
    mr->spawn_watch_prefix = 0;
    mr->spawn_watch_host_name = 0;
    mr->spawn_max_processes = 0;
    mr->spawn_watch_af = 4;
    mr->spawn_watch_port_range[0] = 0;
    mr->spawn_watch_port_range[1] = 0;
    mr->spawn_gap_msec[0] = 1000;
    mr->spawn_gap_msec[1] = 10000;
    mr->spawn_watch_accept_onhold_msec = (60 * 1000);

    mr->verbosity = 5;

    mr->onk = 1;
    mr->single_thread = 0;
    mr->one_step_sort = 0;
    mr->step_sync = 0;
    mr->trace_sorting = 0;
    mr->trace_file_io = 0;
    mr->trace_map_ms = 0;
    mr->trace_map_spawn = 0;
    mr->trace_alltoall = 0;
    mr->trace_kmrdp = 0;
    mr->trace_iolb = 0;
    mr->std_abort = 0;
    mr->file_io_dummy_striping = 1;
    mr->file_io_always_alltoallv = 0;
    mr->spawn_sync_at_startup = 0;
    mr->spawn_watch_all = 0;
    mr->spawn_disconnect_early = 0;
    mr->spawn_disconnect_but_free = 0;
    mr->spawn_pass_intercomm_in_argument = 0;
    mr->keep_fds_at_fork = 0;

    mr->mpi_thread_support = (mpi_thrd == MPI_THREAD_SERIALIZED
			      || mpi_thrd == MPI_THREAD_MULTIPLE);

    mr->stop_at_some_check_globally = 0;
    mr->pushoff_hang_out = 0;
    mr->pushoff_fast_notice = 0;
    mr->pushoff_stat = 1;
    memset(&mr->pushoff_statistics, 0, sizeof(mr->pushoff_statistics));

    if (identifying_name != 0) {
	size_t s = strlen(identifying_name);
	assert(s < KMR_JOB_NAME_LEN);
	strncpy(mr->identifying_name, identifying_name, KMR_JOB_NAME_LEN);
	mr->identifying_name[KMR_JOB_NAME_LEN - 1] = 0;
    }

    /* KMR is now usable (with default setting). */

    /* Load and merge MPI infos. */

    cc = MPI_Info_create(&mr->conf);
    assert(cc == MPI_SUCCESS);
    cc = kmr_load_preference(mr, mr->conf);
    assert(cc == MPI_SUCCESS);
    if (conf != MPI_INFO_NULL) {
	cc = kmr_copy_mpi_info(conf, mr->conf);
	assert(cc == MPI_SUCCESS);
    }

    kmr_check_options(mr, mr->conf);

    /* Initialize checkpoint context. */
    kmr_ckpt_create_context(mr);

#if 0 //KMR_TRACE_ENABLE
    kmr_trace_set_rank(mr->rank);
#endif  

    return mr;
}

KMR *
kmr_create_context_world()
{
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, "");
    return mr;
}

KMR *
kmr_create_context_ff(const int fcomm, const int finfo,
		      const char *identifying_name)
{
    MPI_Comm comm = MPI_Comm_f2c(fcomm);
    MPI_Info info = MPI_Info_f2c(finfo);
    KMR *mr = kmr_create_context(comm, info, identifying_name);
    return mr;
}

/** Releases a context created with kmr_create_context(). */

int
kmr_free_context(KMR *mr)
{
    int cc;
    if (mr->kvses.head != 0 || mr->kvses.tail != 0) {
	kmr_warning(mr, 1, "Some key-value streams remain unfreed");
	for (KMR_KVS *p = mr->kvses.head; p != 0; p = p->c.link.next) {
	    if (!KMR_KVS_MAGIC_OK(p->c.magic)) {
		kmr_warning(mr, 1, "- unfreed kvs in bad state");
	    } else if (p->c.magic == KMR_KVS_ONCORE) {
		if (p->c.info_line0.file != 0) {
		    char ee[80];
		    snprintf(ee, 80, "- kvs allocated at %s:%d: %s",
			     p->c.info_line0.file, p->c.info_line0.line,
			     p->c.info_line0.func);
		    kmr_warning(mr, 1, ee);
		}
	    } else {
		kmr_warning(mr, 1, "- unfreed kvs in bad state");
	    }
	}
    }

    /* Free checkpoint context. */

    if (kmr_ckpt_enabled(mr)) {
	MPI_Barrier(mr->comm);
	kmr_ckpt_free_context(mr);
    }

    if (mr->log_traces != 0) {
	cc = fclose(mr->log_traces);
	if (cc == EOF) {
	    char ee[80];
	    char *m = strerror(errno);
	    snprintf(ee, sizeof(ee), "Closing log file failed: %s", m);
	    kmr_warning(mr, 1, ee);
	}
	mr->log_traces = 0;
    }

    cc = MPI_Comm_free(&mr->comm);
    assert(cc == MPI_SUCCESS);
    if (mr->conf != MPI_INFO_NULL) {
	cc = MPI_Info_free(&mr->conf);
	assert(cc == MPI_SUCCESS);
    }

    if (mr->spawn_watch_program != 0) {
	size_t s = (strlen(mr->spawn_watch_program) + 1);
	kmr_free(mr->spawn_watch_program, s);
    }
    assert(mr->spawn_comms == 0);
    /*mr->kmr_installation_path;*/
    /*mr->spawn_watch_prefix;*/
    /*mr->spawn_watch_host_name;*/

    kmr_free(mr, sizeof(struct kmr_ctx));
    return MPI_SUCCESS;
}

KMR *
kmr_get_context_of_kvs(KMR_KVS const *kvs)
{
    KMR *mr = kvs->c.mr;
    return mr;
}

/* Unlinks a key-value stream from a list on a context.  */

static inline void
kmr_unlink_kvs(KMR_KVS *kvs)
{
    KMR *mr = kvs->c.mr;
    KMR_KVS *prev = kvs->c.link.prev;
    KMR_KVS *next = kvs->c.link.next;
    if (prev != 0) {
	prev->c.link.next = next;
    } else {
	assert(mr->kvses.head == kvs);
	mr->kvses.head = next;
    }
    if (next != 0) {
	next->c.link.prev = prev;
    } else {
	assert(mr->kvses.tail == kvs);
	mr->kvses.tail = prev;
    }
}

/** Makes a new key-value stream (type KMR_KVS).  It allocates by the
    size of the union, which may be larger than the necessary. */

static KMR_KVS *
kmr_create_raw_kvs(KMR *mr, const KMR_KVS *_similar)
{
    xassert(mr != 0);
    /*assert(similar->c.mr == mr);*/
    KMR_KVS *kvs = kmr_malloc(sizeof(KMR_KVS));
    KMR_DEBUGX(memset(kvs, 0, sizeof(KMR_KVS)));
    kvs->c.magic = KMR_KVS_ONCORE;
    kvs->c.mr = mr;
    kmr_link_kvs(kvs);

    if (kmr_ckpt_enabled(mr)) {
	mr->ckpt_kvs_id_counter++;
	kvs->c.ckpt_kvs_id = mr->ckpt_kvs_id_counter;
	kvs->c.ckpt_generated_op = 0;
	kvs->c.ckpt_consumed_op = 0;
    }

    kvs->c.key_data = KMR_KV_BAD;
    kvs->c.value_data = KMR_KV_BAD;
    kvs->c.element_count = 0;

    kvs->c.oncore = 1;
    kvs->c.stowed = 0;
    kvs->c.nogrow = 0;
    kvs->c.sorted = 0;
    kvs->c.shuffled_in_pushoff = 0;
    kvs->c._uniformly_sized_ = 0;

    kvs->c.block_size = (mr->preset_block_size - mr->malloc_overhead);
    kvs->c.element_size_limit = (kvs->c.block_size / 4);
    kvs->c.storage_netsize = 0;
    kvs->c.block_count = 0;
    kvs->c.first_block = 0;
    kvs->c.ms = 0;

    /* Transient fields: */

    kvs->c.under_threaded_operation = 0;
    kvs->c.current_block = 0;
    kvs->c.adding_point = 0;
    kvs->c.temporary_data = 0;

    //KMR_OMP_INIT_LOCK(&kvs->c.mutex);

    return kvs;
}

/* Clears the slots of the structure.  It keeps the fields of the a
   link and checkpointing. */

void
kmr_init_kvs_oncore(KMR_KVS *kvs, KMR *mr)
{
    assert(mr != 0);
    kvs->c.magic = KMR_KVS_ONCORE;
    kvs->c.mr = mr;
    /*kmr_link_kvs(kvs);*/

    /*kvs->c.ckpt_kvs_id = 0;*/
    /*kvs->c.ckpt_generated_op = 0;*/
    /*kvs->c.ckpt_consumed_op = 0;*/

    kvs->c.key_data = KMR_KV_BAD;
    kvs->c.value_data = KMR_KV_BAD;
    kvs->c.element_count = 0;

    kvs->c.oncore = 1;
    kvs->c.stowed = 0;
    kvs->c.nogrow = 0;
    kvs->c.sorted = 0;
    kvs->c.shuffled_in_pushoff = 0;
    kvs->c._uniformly_sized_ = 0;

    kvs->c.block_size = (mr->preset_block_size - mr->malloc_overhead);
    kvs->c.element_size_limit = (kvs->c.block_size / 4);
    kvs->c.storage_netsize = 0;
    kvs->c.block_count = 0;
    kvs->c.first_block = 0;
    kvs->c.ms = 0;

    /* Transient fields: */

    kvs->c.under_threaded_operation = 0;
    kvs->c.current_block = 0;
    kvs->c.adding_point = 0;
    kvs->c.temporary_data = 0;
}

/** Makes a new key-value stream with the specified field
    data-types.  */

KMR_KVS *
kmr_create_kvs7(KMR *mr, enum kmr_kv_field kf, enum kmr_kv_field vf,
		struct kmr_option opt,
		const char *file, const int line, const char *func)
{
    KMR_KVS *kvs = kmr_create_raw_kvs(mr, 0);
    kvs->c.key_data = kf;
    kvs->c.value_data = vf;
    kvs->c.info_line0.file = file;
    kvs->c.info_line0.func = func;
    kvs->c.info_line0.line = line;

    if (kmr_ckpt_enabled(mr)) {
	kmr_ckpt_restore_ckpt(kvs);
    }

    return kvs;
}

/** Moves the contents of the input KVI to the output KVO.  It
    consumes the input KVI.  Calling kmr_map() with a null
    map-function has the same effect.  Effective-options: TAKE_CKPT.
    See struct kmr_option. */

int
kmr_move_kvs(KMR_KVS *kvi, KMR_KVS *kvo, struct kmr_option opt)
{
    assert(kvi != 0 && kvo != 0
	   && kvi->c.magic == KMR_KVS_ONCORE
	   && kvo->c.magic == KMR_KVS_ONCORE
	   && kvi->c.oncore && kvo->c.oncore);
    assert(kvi->c.key_data == kvo->c.key_data
	   && kvi->c.value_data == kvo->c.value_data);
    assert(kvi->c.stowed && !kvo->c.stowed);
    // struct kmr_option kmr_supported = {.take_ckpt = 1};
    // kmr_check_fn_options(kvo->c.mr, kmr_supported, opt, __func__);

    if (kmr_ckpt_enabled(kvo->c.mr)) {
	if (kmr_ckpt_progress_init(kvi, kvo, opt)) {
	    kvi->c.first_block = 0;
	    kvi->c.ms = 0;
	    kmr_free_kvs(kvi);
	    return MPI_SUCCESS;
	}
    }

    /* Copy state. */

    kvo->c.stowed = kvi->c.stowed;
    kvo->c.nogrow = kvi->c.nogrow;
    kvo->c.sorted = kvi->c.sorted;
    kvo->c.element_count = kvi->c.element_count;
    kvo->c.storage_netsize = kvi->c.storage_netsize;
    kvo->c.block_count = kvi->c.block_count;
    kvo->c.first_block = kvi->c.first_block;
    kvo->c.ms = kvi->c.ms;

    if (kmr_ckpt_enabled(kvo->c.mr)) {
	kmr_ckpt_save_kvo_whole(kvo->c.mr, kvo);
    }

    /* Dispose of input. */

    kvi->c.first_block = 0;
    kvi->c.ms = 0;
    int cc = kmr_free_kvs(kvi);
    assert(cc == MPI_SUCCESS);

    if (kmr_ckpt_enabled(kvo->c.mr)) {
	kmr_ckpt_progress_fin(kvo->c.mr);
    }
    return MPI_SUCCESS;
}

static inline int
kmr_free_kvs_oncore(KMR_KVS *kvs)
{
    struct kmr_kvs_block *b = kvs->c.first_block;
    while (b != 0) {
	struct kmr_kvs_block *bn = b->next;
	kmr_free(b, b->size);
	b = bn;
    }
    if (kvs->c.ms != 0) {
	long cnt = kvs->c.element_count;
	size_t sz = (sizeof(struct kmr_map_ms_state)
		     + (sizeof(char) * (size_t)cnt));
	kmr_free(kvs->c.ms, sz);
    }
    if (kvs->c.temporary_data != 0) {
	kmr_free(kvs->c.temporary_data, 0);
    }
    kvs->c.magic = KMR_KVS_BAD;

    /* Delete checkpoint file if exists. */

    if (kmr_ckpt_enabled(kvs->c.mr)) {
	kmr_ckpt_remove_ckpt(kvs);
    }

    kmr_free(kvs, sizeof(struct kmr_kvs_oncore));
    return MPI_SUCCESS;
}

/** Releases a key-value stream (type KMR_KVS).  Normally,
    mapper/shuffler/reducer consumes and frees the input key-value
    stream, and explicit calls are unnecessary.  Here,
    mapper/shuffler/reducer includes kmr_map(),
    kmr_map_on_rank_zero(), kmr_map_ms(), kmr_shuffle(),
    kmr_replicate(), kmr_reduce(), and kmr_reduce_as_one(). */

int
kmr_free_kvs(KMR_KVS *kvs)
{
    if (!KMR_KVS_MAGIC_OK(kvs->c.magic)) {
	kmr_error(0, "kmr_free_kvs: kvs already freed or corrupted");
    }
    kmr_unlink_kvs(kvs);

    int cc;
    if (kvs->c.magic == KMR_KVS_ONCORE) {
	cc = kmr_free_kvs_oncore(kvs);
	return cc;
    } else if (kvs->c.magic == KMR_KVS_PUSHOFF) {
	cc = kmr_free_kvs_pushoff(kvs, 1);
	return cc;
    } else {
	assert((kvs->c.magic == KMR_KVS_ONCORE)
	       || (kvs->c.magic == KMR_KVS_PUSHOFF));
	assert(NEVERHERE);
	return 0;
    }
}

/* ================================================================ */

/* Allocates a new block of storage as a current-block.  When the
   SIZE=1, it allocates a block by the pre-specified block-size, and
   allows it to glow incrementally.  When the SIZE!=1, it allocates a
   block by that size after increasing it for the spaces of a header
   and an end-of-block marker.  It sets the STORAGE_NETSIZE field, and
   places an end-of-block marker, because all key-value pairs should
   fit in the given size.  It accepts zero as a legitimate size.  */

int
kmr_allocate_block(KMR_KVS *kvs, size_t size)
{
    if (size != 1) {
	assert(kvs->c.element_count == 0 && kvs->c.storage_netsize == 0
	       && kvs->c.block_count == 0 && kvs->c.first_block == 0
	       && kvs->c.current_block == 0 && kvs->c.adding_point == 0);
    }
    size_t netsz;
    size_t sz;
    if (size == 0) {
	kvs->c.block_size = 0;
	kvs->c.nogrow = 1;
	return MPI_SUCCESS;
    } else if (size == 1) {
	netsz = 0;
	sz = kvs->c.block_size;
	assert(kvs->c.nogrow == 0);
    } else {
	assert(kvs->c.first_block == 0 && kvs->c.current_block == 0
	       && kvs->c.block_count == 0 && kvs->c.adding_point == 0);
	assert(size >= kmr_kvs_entry_header);
	netsz = size;
	sz = (netsz + kmr_kvs_block_header + kmr_kvs_entry_header);
	kvs->c.block_size = sz;
	kvs->c.storage_netsize = netsz;
	kvs->c.nogrow = 1;
    }
    struct kmr_kvs_block *b = kmr_malloc(sz);
    kmr_kvs_reset_block(kvs, b, sz, netsz);
    kmr_kvs_insert_block(kvs, b);
    return MPI_SUCCESS;
}

/* Adjusts the adding-point to the end for putting key-value pairs at
   once.  It is only called when the whole key-value stream size is
   known in advance. */

static inline void
kmr_kvs_adjust_adding_point(KMR_KVS *kvs)
{
    if (kvs->c.block_count == 0) {
	assert(kvs->c.current_block == 0 && kvs->c.adding_point == 0);
    } else {
	assert(kvs->c.current_block != 0 && kvs->c.adding_point != 0);
	struct kmr_kvs_block *b = kvs->c.current_block;
	assert(kmr_kvs_first_entry(kvs, b) == kvs->c.adding_point);
	kvs->c.adding_point = kmr_kvs_last_entry_limit(kvs, b);
	assert(kvs->c.adding_point == kmr_kvs_adding_point(b));
    }
}

/* Adds a key-value pair.  It is a body of kmr_add_kv(), without a
   mutex.  It modifies kmr_kv_box XKV (when non-null) to return the
   pointers to the opaque fields, when a key or a value is opaque.  It
   does not move actual data when RESERVE_SPACE_ONLY=1. */

static inline int
kmr_add_kv_nomutex(KMR_KVS *kvs, const struct kmr_kv_box kv,
		   struct kmr_kv_box *xkv, _Bool reserve_space_only)
{
    kmr_assert_kv_sizes(kvs, kv);
    assert(!kvs->c.nogrow || kvs->c.storage_netsize != 0);
    KMR *mr = kvs->c.mr;
    int cc;
    size_t sz = kmr_kvs_entry_size_of_box(kvs, kv);
    if (sz > (kvs->c.element_size_limit)) {
	char ee[80];
	snprintf(ee, 80, "key-value too large (size=%zd)", sz);
	kmr_error(mr, ee);
    }
    if (kvs->c.first_block == 0) {
	assert(kvs->c.element_count == 0);
	cc = kmr_allocate_block(kvs, 1);
	assert(cc == MPI_SUCCESS);
    }
    if (!kmr_kvs_entry_fits_in_block(kvs, kvs->c.current_block, sz)) {
	assert(!kvs->c.nogrow);
	kmr_kvs_mark_entry_tail(kvs->c.adding_point);
	cc = kmr_allocate_block(kvs, 1);
	assert(cc == MPI_SUCCESS);
    }
    struct kmr_kvs_entry *e = kvs->c.adding_point;
    kmr_poke_kv(e, kv, xkv, kvs, reserve_space_only);
    if (!kvs->c.nogrow) {
	kvs->c.storage_netsize += kmr_kvs_entry_netsize(e);
    }
    kvs->c.current_block->partial_element_count++;
    kvs->c.current_block->fill_size += kmr_kvs_entry_size(kvs, e);
    kvs->c.adding_point = kmr_kvs_next_entry(kvs, e);
    kvs->c.element_count++;
    return MPI_SUCCESS;
}

/** Adds a key-value pair. (It is with serialization when a
    map-function is threaded). */

int
kmr_add_kv(KMR_KVS *kvs, const struct kmr_kv_box kv)
{
    kmr_assert_kvs_ok(0, kvs, 0, 1);
    int cc;
    if (kvs->c.magic == KMR_KVS_ONCORE) {
	KMR_OMP_CRITICAL_
	{
	    cc = kmr_add_kv_nomutex(kvs, kv, 0, 0);
	}
	return cc;
    } else if (kvs->c.magic == KMR_KVS_PUSHOFF) {
	KMR_OMP_CRITICAL_
	{
	    cc = kmr_add_kv_pushoff(kvs, kv);
	}
	return cc;
    } else {
	assert((kvs->c.magic == KMR_KVS_ONCORE)
	       || (kvs->c.magic == KMR_KVS_PUSHOFF));
	assert(NEVERHERE);
	return 0;
    }
}

/** Adds a key-value pair as given directly by a pointer.  An integer
    or a double be passed by a pointer (thus like &v). */

int
kmr_add_kv1(KMR_KVS *kvs, void *k, int klen, void *v, int vlen)
{
    union kmr_unit_sized xk;
    switch (kvs->c.key_data) {
    case KMR_KV_BAD:
	xassert(kvs->c.key_data != KMR_KV_BAD);
	xk.i = 0;
	break;
    case KMR_KV_INTEGER:
	xk.i = *(long *)k;
	break;
    case KMR_KV_FLOAT8:
	xk.d = *(double *)k;
	break;
    case KMR_KV_OPAQUE:
    case KMR_KV_CSTRING:
    case KMR_KV_POINTER_OWNED:
    case KMR_KV_POINTER_UNMANAGED:
	xk.p = k;
	break;
    default:
	xassert(NEVERHERE);
	xk.i = 0;
	break;
    }

    union kmr_unit_sized xv;
    switch (kvs->c.value_data) {
    case KMR_KV_BAD:
	xassert(kvs->c.value_data != KMR_KV_BAD);
	xv.i = 0;
	break;
    case KMR_KV_INTEGER:
	xv.i = *(long *)v;
	break;
    case KMR_KV_FLOAT8:
	xv.d = *(double *)v;
	break;
    case KMR_KV_OPAQUE:
    case KMR_KV_CSTRING:
    case KMR_KV_POINTER_OWNED:
    case KMR_KV_POINTER_UNMANAGED:
	xv.p = v;
	break;
    default:
	xassert(NEVERHERE);
	xv.i = 0;
	break;
    }

    struct kmr_kv_box kv = {.klen = klen, .vlen = vlen, .k = xk, .v = xv};
    int cc;
    cc = kmr_add_kv(kvs, kv);
    return cc;
}

/** Adds a key-value pair, but only allocates a space and returns the
    pointers to the key and the value parts.  It may enable to create
    a large key/value data directly in the space.  It does not return
    a proper value if a key/value field is not a pointer.  (It cannot
    be used with a "push-off" key-value stream, because its buffer
    will be sent out and late fill-in the buffer causes a race). */

int
kmr_add_kv_space(KMR_KVS *kvs, const struct kmr_kv_box kv,
		 void **keyp, void **valuep)
{
    kmr_assert_kvs_ok(0, kvs, 0, 1);
    assert(kvs->c.magic == KMR_KVS_ONCORE);
    int cc;
    struct kmr_kv_box xkv = {
	.k.p = 0,
	.v.p = 0
    };
    KMR_OMP_CRITICAL_
    {
	cc = kmr_add_kv_nomutex(kvs, kv, &xkv, 1);
    }
    if (keyp != 0) {
	*keyp = (void *)xkv.k.p;
    }
    if (valuep != 0) {
	*valuep = (void *)xkv.v.p;
    }
    return cc;
}

int
kmr_add_kv_quick_(KMR_KVS *kvs, const struct kmr_kv_box kv)
{
    int cc = kmr_add_kv_nomutex(kvs, kv, 0, 0);
    return cc;
}

/** Marks finished adding key-value pairs.  Further addition will be
    prohibited.  Normally, mapper/shuffler/reducer finishes the output
    key-value stream by itself, and explicit calls are unnecessary.
    Here, mapper/shuffler/reducer includes kmr_map(),
    kmr_map_on_rank_zero(), kmr_map_ms(), kmr_shuffle(),
    kmr_replicate(), and kmr_reduce(). */

int
kmr_add_kv_done(KMR_KVS *kvs)
{
    kmr_assert_kvs_ok(0, kvs, 0, 1);
    if (kvs->c.magic == KMR_KVS_ONCORE) {
	if (kvs->c.stowed) {
	    kmr_error(kvs->c.mr, "kmr_add_kv_done: may be called already");
	}
	if (kvs->c.element_count == 0) {
	    assert(kvs->c.current_block == 0 && kvs->c.adding_point == 0);
	} else {
	    assert(kvs->c.current_block != 0 && kvs->c.adding_point != 0);
	    kmr_kvs_mark_entry_tail(kvs->c.adding_point);
	}
	kvs->c.stowed = 1;
	kvs->c.current_block = 0;
	kvs->c.adding_point = 0;
	assert(kvs->c.block_count == 0 || kvs->c.first_block != 0);
    } else if (kvs->c.magic == KMR_KVS_PUSHOFF) {
	kmr_add_kv_done_pushoff(kvs);
    } else {
	assert((kvs->c.magic == KMR_KVS_ONCORE)
	       || (kvs->c.magic == KMR_KVS_PUSHOFF));
	assert(NEVERHERE);
	return 0;
    }
    return MPI_SUCCESS;
}

/** Adds a key-value pair of strings.  The key and value fields should
    be of opaque data. */

int
kmr_add_string(KMR_KVS *kvs, const char *k, const char *v)
{
    if (!((kvs->c.key_data == KMR_KV_OPAQUE
	   || kvs->c.key_data == KMR_KV_CSTRING)
	  && (kvs->c.value_data == KMR_KV_OPAQUE
	      || kvs->c.value_data == KMR_KV_CSTRING))) {
	kmr_error(kvs->c.mr,
		  "key-value data-types need be opaque for strings");
    }
    size_t klen = (strlen(k) + 1);
    size_t vlen = (strlen(v) + 1);
    assert(klen <= INT_MAX && vlen <= INT_MAX);
    struct kmr_kv_box kv;
    kv.klen = (int)klen;
    kv.k.p = k;
    kv.vlen = (int)vlen;
    kv.v.p = v;
    int cc = kmr_add_kv(kvs, kv);
    return cc;
}

/** Adds a given key-value pair unmodified.  It is a map-function. */

int
kmr_add_identity_fn(const struct kmr_kv_box kv,
		    const KMR_KVS *kvi, KMR_KVS *kvo, void *arg, const long i)
{
    kmr_add_kv(kvo, kv);
    return MPI_SUCCESS;
}

/* Packs fields as opaque.  When key or value field is a pointer,
   fields need to be packed to make data-exchanges easy. */

static int
kmr_collapse_as_opaque(KMR_KVS *kvi, KMR_KVS *kvo, _Bool inspectp)
{
    assert(kvi != 0 && kvo != 0);
    assert(kmr_fields_pointer_p(kvi) || kvi->c.block_count > 1);
    int cc;
    cc = kmr_allocate_block(kvo, kvi->c.storage_netsize);
    assert(cc == MPI_SUCCESS);
    struct kmr_option collapse = {.collapse = 1, .inspect = inspectp};
    cc = kmr_map(kvi, kvo, 0, collapse, kmr_add_identity_fn);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/** Packs locally the contents of a key-value stream to a byte array.
    It is used to save or to send a key-value stream.  It returns the
    allocated memory with its size, and it should be freed by the
    user.  It may fail on allocating a buffer, and then it returns
    MPI_ERR_BUFFER.  Its reverse is performed by kmr_restore_kvs(). */

int
kmr_save_kvs(KMR_KVS *kvs, void **dataq, size_t *szq,
	     struct kmr_option opt)
{
    if (kvs == 0) {
	kmr_error_at_site(0, "Null input kvs", 0);
    } else if (!KMR_KVS_MAGIC_OK(kvs->c.magic)) {
	kmr_error_at_site(0, "Bad input kvs (freed or corrupted)", 0);
    }
    assert(kvs->c.magic == KMR_KVS_ONCORE);
    kmr_check_fn_options(kvs->c.mr, kmr_noopt, opt, __func__);
    /*assert(kvs->c.current_block == 0 && kvs->c.adding_point == 0);*/
    if (kvs->c.ms != 0 || kvs->c.temporary_data != 0) {
	kmr_warning(kvs->c.mr, 5,
		    "Some fields in KVS may be lost in saved image");
    }
    int cc;
    if (kmr_fields_pointer_p(kvs) || (kvs->c.block_count > 1)) {
	enum kmr_kv_field keyf = kmr_unit_sized_or_opaque(kvs->c.key_data);
	enum kmr_kv_field valf = kmr_unit_sized_or_opaque(kvs->c.value_data);
	KMR_KVS *kvs1 = kmr_create_kvs(kvs->c.mr, keyf, valf);
	cc = kmr_collapse_as_opaque(kvs, kvs1, 0);
	assert(cc == MPI_SUCCESS);
	assert(!kmr_fields_pointer_p(kvs1) && kvs->c.block_count <= 1);
	cc = kmr_save_kvs(kvs1, dataq, szq, kmr_noopt);
	assert(cc == MPI_SUCCESS);
	kmr_free_kvs(kvs1);
	return MPI_SUCCESS;
    }
    assert(!kmr_fields_pointer_p(kvs));
    size_t netsz = kvs->c.storage_netsize;
    size_t blocksz = (netsz + kmr_kvs_block_header + kmr_kvs_entry_header);
    size_t sz = (sizeof(KMR_KVS) + blocksz);
    unsigned char *b = malloc(sz);
    if (b == 0) {
	return MPI_ERR_BUFFER;
    }
    KMR_KVS *h = (void *)b;
    struct kmr_kvs_block *s = (void *)(b + sizeof(KMR_KVS));
    memcpy(h, kvs, sizeof(KMR_KVS));
    h->c.magic = KMR_KVS_ONCORE_PACKED;
    h->c.mr = 0;
    h->c.link.next = 0;
    h->c.link.prev = 0;
    h->c.block_count = 1;
    h->c.first_block = 0;
    h->c.current_block = 0;
    h->c.adding_point = 0;
    h->c.ms = 0;
    h->c.temporary_data = 0;
    if (kvs->c.block_count == 0) {
	/*nothing*/
    } else if (kvs->c.block_count == 1) {
	memcpy(s, kvs->c.first_block, blocksz);
	s->size = blocksz;
    } else {
	xassert(NEVERHERE);
    }
    *dataq = b;
    *szq = sz;
    return MPI_SUCCESS;
}

/** Unpacks locally the contents of a key-value stream from a byte
    array.  It is a reverse of kmr_save_kvs().  */

int
kmr_restore_kvs(KMR_KVS *kvo, void *data, size_t sz_,
		struct kmr_option opt)
{
    assert(kvo != 0 && kvo->c.magic == KMR_KVS_ONCORE);
    kmr_check_fn_options(kvo->c.mr, kmr_noopt, opt, __func__);
    int cc;
    unsigned char *b = data;
    KMR_KVS *h = (void *)b;
    unsigned char *s = (b + sizeof(KMR_KVS));
    if (h->c.magic != KMR_KVS_ONCORE_PACKED) {
	kmr_warning(kvo->c.mr, 1, "Bad packed data, magic mismatch");
	return MPI_ERR_TYPE;
    }
    size_t netsz = h->c.storage_netsize;
    size_t blocksz = (netsz + kmr_kvs_block_header + kmr_kvs_entry_header);
    cc = kmr_allocate_block(kvo, netsz);
    assert(cc == MPI_SUCCESS);
    if (netsz != 0) {
	memcpy(kvo->c.first_block, s, blocksz);
    }
    kvo->c.key_data = h->c.key_data;
    kvo->c.value_data = h->c.value_data;
    assert(kvo->c.sorted == 0);
    kvo->c.element_count = h->c.element_count;
    kmr_kvs_adjust_adding_point(kvo);
    kmr_add_kv_done(kvo);
    return MPI_SUCCESS;
}

/* ================================================================ */

/* Calls a map-function on entries in aggregate.  EV holds EVCNT
   entries.  MAPCOUNT is a counter of mapped entries from the
   beginning. */

static inline int
kmr_map_parked(struct kmr_kv_box *ev, long evcnt, long mapcount,
	       _Bool k_reclaim, _Bool v_reclaim,
	       KMR_KVS *kvi, KMR_KVS *kvo, kmr_mapfn_t m,
	       void *arg, struct kmr_option opt)
{
    int cc;
    KMR *mr = kvi->c.mr;
    long cnt = kvi->c.element_count;
    if (mr->single_thread || opt.nothreading) {
	for (long i = 0; i < evcnt; i++) {
	    double t0 = ((mr->log_traces == 0) ? 0.0 : MPI_Wtime());
	    cc = (*m)(ev[i], kvi, kvo, arg, (mapcount + i));
	    double t1 = ((mr->log_traces == 0) ? 0.0 : MPI_Wtime());
	    if (cc != MPI_SUCCESS) {
		char ee[80];
		snprintf(ee, sizeof(ee),
			 "Map-fn returned with error cc=%d", cc);
		kmr_error(mr, ee);
	    }
	    if (mr->log_traces != 0) {
		kmr_log_map(mr, kvi, &ev[i], (mapcount + 1), cnt,
			    m, (t1 - t0));
	    }
	}
    } else {
	if (kvo != 0) {
	    kvo->c.under_threaded_operation = 1;
	}
	KMR_OMP_PARALLEL_FOR_
	    for (long i = 0; i < evcnt; i++) {
		double t0 = ((mr->log_traces == 0) ? 0.0 : MPI_Wtime());
		int ccx = (*m)(ev[i], kvi, kvo, arg, (mapcount + i));
		double t1 = ((mr->log_traces == 0) ? 0.0 : MPI_Wtime());
		if (ccx != MPI_SUCCESS) {
		    char ee[80];
		    snprintf(ee, sizeof(ee),
			     "Map-fn returned with error cc=%d", ccx);
		    kmr_error(mr, ee);
		}
		if (mr->log_traces != 0) {
		    kmr_log_map(mr, kvi, &ev[i], (mapcount + 1), cnt,
				m, (t1 - t0));
		}
	    }
	if (kvo != 0) {
	    kvo->c.under_threaded_operation = 0;
	}
    }
    for (long i = 0; i < evcnt; i++) {
	if (k_reclaim) {
	    kmr_free((void *)ev[i].k.p, (size_t)ev[i].klen);
	}
	if (v_reclaim) {
	    kmr_free((void *)ev[i].v.p, (size_t)ev[i].vlen);
	}
    }
    return MPI_SUCCESS;
}

/** Maps by skipping the number of entries.  It calls a map-function
    on entries from FROM, skipping by STRIDE, up to LIMIT
    non-inclusive.  See kmr_map(). */

int
kmr_map_skipping(long from, long stride, long limit,
		 _Bool stop_when_some_added,
		 KMR_KVS *kvi, KMR_KVS *kvo,
		 void *arg, struct kmr_option opt, kmr_mapfn_t m)
{
    kmr_assert_kvs_ok(kvi, kvo, 1, 0);
    assert(from >= 0 && stride > 0 && limit >= 0);
    assert(kvi->c.current_block == 0);
    limit = ((limit != 0) ? limit : LONG_MAX);
    KMR *mr = kvi->c.mr;

    if (kmr_ckpt_enabled(mr)) {
	if (kmr_ckpt_progress_init(kvi, kvo, opt)){
	    if (kvo != 0 && !opt.keep_open) {
		kmr_add_kv_done(kvo);
	    }
	    if (!opt.inspect) {
		kmr_free_kvs(kvi);
	    }
	    return MPI_SUCCESS;
	}
	from = kmr_ckpt_first_unprocessed_kv(mr);
	kmr_ckpt_save_kvo_block_init(mr, kvo);
    }

    int cc;

    if (mr->step_sync) {
	cc = MPI_Barrier(mr->comm);
	assert(MPI_SUCCESS);
    }
    if (kvo != 0 && opt.collapse) {
	assert(!kmr_fields_pointer_p(kvo));
    }
    _Bool k_reclaim = (!opt.inspect && (kmr_key_pointer_p(kvi)));
    _Bool v_reclaim = (!opt.inspect && (kmr_value_pointer_p(kvi)));
    long evsz = mr->mapper_park_size;
    struct kmr_kv_box *
	ev = kmr_malloc((sizeof(struct kmr_kv_box) * (size_t)evsz));
    long evcnt = 0;
    long mapcount = 0;
    long nextindex = from;
    long index = 0;
    kvi->c.current_block = kvi->c.first_block;
    while (index < kvi->c.element_count) {
	assert(kvi->c.current_block != 0);
	struct kmr_kvs_block *b = kvi->c.current_block;
	struct kmr_kvs_entry *e = kmr_kvs_first_entry(kvi, b);
	for (int i = 0; i < b->partial_element_count; i++) {
	    assert(e != 0);
	    if (index == nextindex && index < limit) {
		ev[evcnt++] = kmr_pick_kv(e, kvi);
		nextindex = (index + stride);
		if (k_reclaim) {
		    union kmr_unit_sized *w = kmr_point_key(e);
		    w->p = 0;
		}
		if (v_reclaim) {
		    union kmr_unit_sized *w = kmr_point_value(e);
		    w->p = 0;
		}
	    } else {
		if (k_reclaim) {
		    union kmr_unit_sized *w = kmr_point_key(e);
		    kmr_free((void *)w->p, (size_t)e->klen);
		    w->p = 0;
		}
		if (v_reclaim) {
		    union kmr_unit_sized *w = kmr_point_value(e);
		    kmr_free((void *)w->p, (size_t)e->vlen);
		    w->p = 0;
		}
	    }
	    if (evcnt >= evsz) {
		cc = kmr_map_parked(ev, evcnt, mapcount, k_reclaim, v_reclaim,
				    kvi, kvo, m, arg, opt);
		assert(cc == MPI_SUCCESS);

		if (kmr_ckpt_enabled(mr)) {
		    kmr_ckpt_save_kvo_block_add(mr, kvo, evcnt);
		}

		mapcount += evcnt;
		evcnt = 0;

		if (stop_when_some_added) {
		    _Bool done;
		    if (mr->stop_at_some_check_globally) {
			done = 0;
		    } else {
			done = (kvo->c.element_count != 0);
		    }
		    if (done) {
			/* Fake as if go to the end. */
			index = (kvi->c.element_count - 1);
			while (b->next != 0) {
			    b = b->next;
			}
		    }
		}
	    }
	    e = kmr_kvs_next(kvi, e, 1);
	    index++;
	}
	kvi->c.current_block = b->next;
    }
    assert(kvi->c.current_block == 0);
    if (evcnt > 0) {
	cc = kmr_map_parked(ev, evcnt, mapcount, k_reclaim, v_reclaim,
			    kvi, kvo, m, arg, opt);
	assert(cc == MPI_SUCCESS);

	if (kmr_ckpt_enabled(mr)) {
	    kmr_ckpt_save_kvo_block_add(mr, kvo, evcnt);
	}

	mapcount += evcnt;
	evcnt = 0;
    }
    if (kvo != 0 && !opt.keep_open) {
	kmr_add_kv_done(kvo);
    }

    if (kmr_ckpt_enabled(mr)) {
	kmr_ckpt_save_kvo_block_fin(mr, kvo);
    }

    if (!opt.inspect) {
	kmr_free_kvs(kvi);
    }
    if (ev != 0) {
	kmr_free(ev, (sizeof(struct kmr_kv_box) * (size_t)evsz));
    }

    if (kmr_ckpt_enabled(mr)) {
	kmr_ckpt_progress_fin(mr);
    }
    return MPI_SUCCESS;
}

/** Maps simply.  It consumes the input key-value stream KVI unless
    INSPECT option is marked.  The output key-value stream KVO can be
    null, but in that case, a map-function cannot add key-value pairs.
    The pointer ARG is just passed to a map-function as a general
    argument, where accesses to it should be race-free, since a
    map-function is called by threads by default.  M is the
    map-function.  See the description on the type ::kmr_mapfn_t.  It
    copeis the contents of the input KVI to the output KVO, when a
    map-function is null.  During processing, it first makes an array
    pointing to the key-value entries in each data block, and works on
    it for ease threading/parallelization.  Effective-options:
    NOTHREADING, INSPECT, KEEP_OPEN, COLLAPSE, TAKE_CKPT.
    See struct kmr_option. */

int
kmr_map9(_Bool stop_when_some_added,
	 KMR_KVS *kvi, KMR_KVS *kvo,
	 void *arg, struct kmr_option opt, kmr_mapfn_t m,
	 const char *file, const int line, const char *func)
{
#if KMR_TRACE_ENABLE
    kmr_trace_add_entry(kmr_trace_event_map_start, kvi, kvo);
#endif
    int cc;
    kmr_assert_kvs_ok(kvi, kvo, 1, 0);
    KMR *mr = kvi->c.mr;
    struct kmr_option kmr_supported = {.nothreading = 1, .inspect = 1,
                                       .keep_open = 1, .collapse = 1,
                                       .take_ckpt = 1};
    kmr_check_fn_options(mr, kmr_supported, opt, __func__);
    struct kmr_code_line info;
    if (mr->atwork == 0) {
	info.file = file;
	info.func = func;
	info.line = line;
	mr->atwork = &info;
    }
    if (m != 0) {
	cc = kmr_map_skipping(0, 1, 0, stop_when_some_added,
			      kvi, kvo, arg, opt, m);
    } else {
	assert(!opt.inspect && !opt.keep_open);
	cc = kmr_move_kvs(kvi, kvo, opt);
    }
    if (mr->atwork == &info) {
	mr->atwork = 0;
    }
#if KMR_TRACE_ENABLE
    kmr_trace_add_entry(kmr_trace_event_map_end, kvi, kvo);
#endif    
    return cc;
}

/** Maps sequentially with rank by rank for debugging.  See
    kmr_map. */

int
kmr_map_rank_by_rank(KMR_KVS *kvi, KMR_KVS *kvo,
		     void *arg, struct kmr_option opt, kmr_mapfn_t m)
{
    KMR *mr = kvi->c.mr;
    int nprocs = mr->nprocs;
    int cc;
    if (mr->rank != 0) {
	cc = MPI_Recv(0, 0, MPI_INT, (mr->rank - 1),
		      KMR_TAG_MAP_BY_RANK, mr->comm, MPI_STATUS_IGNORE);
	assert(cc == MPI_SUCCESS);
    }
    cc = kmr_map(kvi, kvo, arg, opt, m);
    assert(cc == MPI_SUCCESS);
    fflush(0);
    if (mr->rank != (nprocs - 1)) {
	usleep(1 * 1000);
	cc = MPI_Send(0, 0, MPI_INT, (mr->rank + 1),
		      KMR_TAG_MAP_BY_RANK, mr->comm);
	assert(cc == MPI_SUCCESS);
    }
    return MPI_SUCCESS;
}

/** Extracts a single key-value pair locally in the key-value stream
    KVI.  It is an error when zero or more than one entries are in the
    KVI.  It does not consume the input KVS (INSPECT IMPLIED).  The
    returned key-value entry must be used before freeing the input
    KVS, when it points to an opaque data. */

int
kmr_take_one(KMR_KVS *kvi, struct kmr_kv_box *kv)
{
    kmr_assert_kvs_ok(kvi, 0, 1, 0);
    assert(kvi->c.current_block == 0);
    KMR *mr = kvi->c.mr;
    kvi->c.current_block = kvi->c.first_block;
    if (kvi->c.element_count == 1) {
	assert(kvi->c.current_block != 0);
	struct kmr_kvs_block *b = kvi->c.current_block;
	struct kmr_kvs_entry *e = kmr_kvs_first_entry(kvi, b);
	assert(b->partial_element_count == 1);
	assert(e != 0);
	*kv = kmr_pick_kv(e, kvi);
	kvi->c.current_block = 0;
	return MPI_SUCCESS;
    } else {
	if (kvi->c.element_count == 0) {
	    kmr_warning(mr, 1, "kmr_take_one for no entries");
	    /*return MPI_ERR_COUNT;*/
	} else {
	    kmr_warning(mr, 1, "kmr_take_one for multiple entries");
	    /*return MPI_ERR_COUNT;*/
	}
	MPI_Abort(MPI_COMM_WORLD, 1);
	return MPI_SUCCESS;
    }
}

/** Maps once.  It calls a map-function once with a dummy key-value
    stream and a dummy key-value pair.  See kmr_map().
    Effective-options: KEEP_OPEN, TAKE_CKPT.  See struct kmr_option. */

int
kmr_map_once(KMR_KVS *kvo, void *arg, struct kmr_option opt,
	     _Bool rank_zero_only, kmr_mapfn_t m)
{
#if KMR_TRACE_ENABLE
    kmr_trace_add_entry(kmr_trace_event_map_once_start, NULL, kvo);
#endif
    kmr_assert_kvs_ok(0, kvo, 0, 1);
    KMR *mr = kvo->c.mr;
    struct kmr_option kmr_supported = {.keep_open = 1, .take_ckpt = 1};
    kmr_check_fn_options(mr, kmr_supported, opt, __func__);
    int rank = mr->rank;
    int cc;

    if (kmr_ckpt_enabled(mr)) {
	if (kmr_ckpt_progress_init(0, kvo, opt)) {
	    kmr_add_kv_done(kvo);
	    return MPI_SUCCESS;
	}
    }

    if (!rank_zero_only || rank == 0) {
	struct kmr_kv_box kv = {.klen = 0, .vlen = 0, .k.i = 0, .v.i = 0};
	cc = (*m)(kv, 0, kvo, arg, 0);
	if (cc != MPI_SUCCESS) {
	    char ee[80];
	    snprintf(ee, sizeof(ee),
		     "Map-fn returned with error cc=%d", cc);
	    kmr_error(mr, ee);
	}
    }
    if (!opt.keep_open) {
	cc = kmr_add_kv_done(kvo);
	assert(cc == MPI_SUCCESS);
    }

    if (kmr_ckpt_enabled(mr)) {
	kmr_ckpt_save_kvo_whole(mr, kvo);
	kmr_ckpt_progress_fin(mr);
    }

#if KMR_TRACE_ENABLE
    kmr_trace_add_entry(kmr_trace_event_map_once_end, NULL, kvo);
#endif
    return MPI_SUCCESS;
}

/** Maps on rank0 only.  It calls a map-function once with a dummy
    key-value stream and a dummy key-value pair.  It is used to avoid
    low-level conditionals like (myrank==0).  See kmr_map().
    Effective-options: KEEP_OPEN, TAKE_CKPT.  See struct kmr_option. */

int
kmr_map_on_rank_zero(KMR_KVS *kvo, void *arg, struct kmr_option opt,
		     kmr_mapfn_t m)
{
    int cc;
    cc = kmr_map_once(kvo, arg, opt, 1, m);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/* ================================================================ */

#if 1

/* Hashes.  (MurmurHash3; http://code.google.com/p/smhasher). */

static inline unsigned long
kmr_hash_key_opaque(const unsigned char *p, int n)
{
#define ROT(X,N) ((X) << (N) | (X) >> (64-(N)))
#define KEY(V) (k = (V), k *= 0x87c37b91114253d5UL, \
		k = ROT(k, 31), k *= 0x4cf5ad432745937fUL)
#define MIX() (h ^= k, h = ROT(h, 31), h = h * 5 + 0xe6546b64)
#define FIN() (h ^= (h >> 33), h *= 0xff51afd7ed558ccdUL, h ^= (h >> 33), \
	       h *= 0xc4ceb9fe1a85ec53UL, h ^= (h >> 33))
    unsigned long h = 0x85ebca6bUL; /*c2b2ae35*/
    unsigned long k;
    const unsigned long *v = (void *)p;
    int n8 = (n / 8);
    int rn = (n - (8 * n8));
    const unsigned char *r = &p[(8 * n8)];
    for (int i = 0; i < n8; i++) {
	KEY(v[i]);
	MIX();
    }
    union {unsigned long i; unsigned char c[8];} u = {.i = 0UL};
    for (int i = 0; i < rn; i++) {
	u.c[i] = r[i];
    }
    KEY(u.i);
    MIX();
    FIN();
    return h;
#undef ROT
#undef KEY
#undef MIX
#undef FIN
}

#elif 0

/* (java 1.2 hash). */

static inline unsigned long
kmr_hash_key_opaque(const unsigned char *p, int n)
{
    unsigned long hash = 0;
    for (i = 0 ; i < n ; i++) {
	hash *= 31;
	hash += p[i];
    }
    return hash;
}

#else

/* (java 1.0 hash). */

static inline unsigned long
kmr_hash_key_opaque(const unsigned char *p, int n)
{
    unsigned long hash = 0;
    int m, k, i;
    if (n <= 15) {
	m = 1;
	for (i = 0 ; i < n ; i++) {
	    hash += (p[i] * m);
	    m *= 37;
	}
    } else {
	m = 1;
	k = n / 8;
	for (i = 0 ; i < n ; i += k) {
	    hash += (p[i] * m);
	    m *= 39;
	}
    }
    return hash;
}

#endif

/* Makes an integer key for a key-value pair for shuffling.  It
   returns the long bit representation for float.  This hash is OK for
   the key_as_rank option.  (kmr_hash_key() is for shuffling, and
   kmr_stable_key() is for sorting). */

static inline signed long
kmr_hash_key(const KMR_KVS *kvs, const struct kmr_kv_box kv)
{
    switch (kvs->c.key_data) {
    case KMR_KV_BAD:
	xassert(kvs->c.key_data != KMR_KV_BAD);
	return -1;
    case KMR_KV_OPAQUE:
    case KMR_KV_CSTRING:
	return (signed long)kmr_hash_key_opaque((void *)kv.k.p, kv.klen);
    case KMR_KV_INTEGER:
	return kv.k.i;
    case KMR_KV_FLOAT8:
	return kv.k.i;
    case KMR_KV_POINTER_OWNED:
    case KMR_KV_POINTER_UNMANAGED:
	xassert(kvs->c.key_data != KMR_KV_POINTER_OWNED
		&& kvs->c.key_data != KMR_KV_POINTER_UNMANAGED);
	return -1;
    default:
	xassert(NEVERHERE);
	return -1;
    }
}

/* Returns an integer of the first 8 bytes, shifted to the right by
   one-bit to make it positive.  It is consistent with the ordering by
   memcmp(). */

static inline long
kmr_stable_key_opaque(const struct kmr_kv_box kv)
{
    unsigned char *p = (unsigned char *)kv.k.p;
    int n = kv.klen;
    unsigned long hash = 0;
    for (int i = 0; i < (int)sizeof(long); i++) {
	unsigned char v = ((i < n) ? p[i] : 0);
	hash = ((hash << 8) + v);
    }
    return (long)(hash >> 1);
}

/* Makes an integer key for a key-value pair for sorting.  It returns
   a signed value for comparing integers.  It is consistent with the
   ordering by memcmp() for opaque keys.  (kmr_hash_key() is for
   shuffling, and kmr_stable_key() is for sorting). */

signed long
kmr_stable_key(const struct kmr_kv_box kv, const KMR_KVS *kvs)
{
    switch (kvs->c.key_data) {
    case KMR_KV_BAD:
	xassert(kvs->c.key_data != KMR_KV_BAD);
	return -1;
    case KMR_KV_OPAQUE:
    case KMR_KV_CSTRING:
	return kmr_stable_key_opaque(kv);
    case KMR_KV_INTEGER:
	return kv.k.i;
    case KMR_KV_FLOAT8:
    {
	long v0 = kv.k.i;
	long v1 = ((v0 >= 0L) ? v0 : ((-v0) | (1L << 63)));
	/*assert(v0 >= 0 || v1 < 0);*/
	return v1;
    }
    case KMR_KV_POINTER_OWNED:
    case KMR_KV_POINTER_UNMANAGED:
	xassert(kvs->c.key_data != KMR_KV_POINTER_OWNED
		&& kvs->c.key_data != KMR_KV_POINTER_UNMANAGED);
	return -1;
    default:
	xassert(NEVERHERE);
	return -1;
    }
}

/* Determines a rank to which this key-value entry is directed.  It is
   bases on the hashed keys. */

int
kmr_pitch_rank(const struct kmr_kv_box kv, KMR_KVS *kvs)
{
    unsigned int nprocs = (unsigned int)kvs->c.mr->nprocs;
    unsigned long v = (unsigned long)kmr_hash_key(kvs, kv);
    unsigned int h = (((v >> 32) ^ v) & ((1L << 32) - 1));
    return (int)(h % nprocs);
}

/* Compares in three-way, returning -1, 0, or 1. */

#define KMR_CMP3(X, Y) (((X) == (Y)) ? 0 : ((X) < (Y)) ? -1 : 1)

/* Compares byte strings lexicographically.  This is compatible to
   memcmp() when the lengths are equal, for the terasort
   requirement. */

static inline int
kmr_compare_lexicographically(const unsigned char *p, const int plen,
			      const unsigned char *q, const int qlen)
{
    int s = MIN(plen, qlen);
#if 0
    for (int i = 0; i < s; i++) {
	if (p[i] != q[i]) {
	    return (p[i] - q[i]);
	}
    }
#endif
    int cc = memcmp(p, q, (size_t)s);
    if (cc != 0) {
	return cc;
    } else {
	return (plen - qlen);
    }
}

/* Compares keys lexicographically as byte strings. */

static int
kmr_compare_opaque(const struct kmr_kv_box *p,
		   const struct kmr_kv_box *q)
{
    return kmr_compare_lexicographically((unsigned char *)p->k.p, p->klen,
					 (unsigned char *)q->k.p, q->klen);
}

static int
kmr_compare_integer(const struct kmr_kv_box *p0,
		    const struct kmr_kv_box *p1)
{
    return KMR_CMP3(p0->k.i, p1->k.i);
}

static int
kmr_compare_float8(const struct kmr_kv_box *p0,
		   const struct kmr_kv_box *p1)
{
    return KMR_CMP3(p0->k.d, p1->k.d);
}

/* Compares keys lexicographically as byte strings. */

static int
kmr_compare_record_opaque(const struct kmr_keyed_record *p0,
			  const struct kmr_keyed_record *p1)
{
    struct kmr_kv_box b0 = kmr_pick_kv2(p0->e, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    struct kmr_kv_box b1 = kmr_pick_kv2(p1->e, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    return kmr_compare_lexicographically((unsigned char *)b0.k.p, b0.klen,
					 (unsigned char *)b1.k.p, b1.klen);
}

/* (UNUSED) Sorting uses the key part of the record and it does not
   peek in the kmr_kv_box. */

static int
kmr_compare_record_integer_(const struct kmr_keyed_record *p0,
			    const struct kmr_keyed_record *p1)
{
    assert(NEVERHERE);
    struct kmr_kv_box b0 = kmr_pick_kv2(p0->e, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    struct kmr_kv_box b1 = kmr_pick_kv2(p1->e, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    long v0 = b0.k.i;
    long v1 = b1.k.i;
    return KMR_CMP3(v0, v1);
}

/* (UNUSED) Sorting uses the key part of the record and it does not
   peek in the kmr_kv_box. */

static int
kmr_compare_record_float8_(const struct kmr_keyed_record *p0,
			   const struct kmr_keyed_record *p1)
{
    assert(NEVERHERE);
    struct kmr_kv_box b0 = kmr_pick_kv2(p0->e, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    struct kmr_kv_box b1 = kmr_pick_kv2(p1->e, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    double v0 = b0.k.d;
    double v1 = b1.k.d;
    return KMR_CMP3(v0, v1);
}

/* Returns an appropriate comparator for kmr_kv_box. */

kmr_sorter_t
kmr_choose_sorter(const KMR_KVS *kvs)
{
    switch (kvs->c.key_data) {
    case KMR_KV_BAD:
	xassert(kvs->c.key_data != KMR_KV_BAD);
	return 0;
    case KMR_KV_INTEGER:
	return kmr_compare_integer;
    case KMR_KV_FLOAT8:
	return kmr_compare_float8;
    case KMR_KV_OPAQUE:
    case KMR_KV_CSTRING:
    case KMR_KV_POINTER_OWNED:
    case KMR_KV_POINTER_UNMANAGED:
	return kmr_compare_opaque;
    default:
	xassert(NEVERHERE);
	return 0;
    }
}

/* Returns an appropriate comparator for keyed-records.  It is only
   called for KMR_KV_OPAQUE. */

static kmr_record_sorter_t
kmr_choose_record_sorter(const KMR_KVS *kvs)
{
    switch (kvs->c.key_data) {
    case KMR_KV_BAD:
	xassert(kvs->c.key_data != KMR_KV_BAD);
	return 0;
    case KMR_KV_INTEGER:
	assert(NEVERHERE);
	return kmr_compare_record_integer_;
    case KMR_KV_FLOAT8:
	assert(NEVERHERE);
	return kmr_compare_record_float8_;
    case KMR_KV_OPAQUE:
    case KMR_KV_CSTRING:
    case KMR_KV_POINTER_OWNED:
    case KMR_KV_POINTER_UNMANAGED:
	return kmr_compare_record_opaque;
    default:
	xassert(NEVERHERE);
	return 0;
    }
}

#if 0

/* Copies the entries as keyed-records with hashed keys for
   shuffling. */

static int
kmr_copy_record_shuffle_fn(const struct kmr_kv_box kv,
			   const KMR_KVS *kvi, KMR_KVS *kvo, void *p,
			   const long i)
{
    return MPI_SUCCESS;
}

/* Copies the entries as keyed-records with stable keys for
   sorting. */

static int
kmr_copy_record_sorting_fn(const struct kmr_kv_box kv,
			   const KMR_KVS *kvi, KMR_KVS *kvo, void *p,
			   const long i)
{
    return MPI_SUCCESS;
}

#endif

/** Compares the key field of keyed-records for qsort/bsearch. */

static int
kmr_icmp(const void *a0, const void *a1)
{
    const struct kmr_keyed_record *p0 = a0;
    const struct kmr_keyed_record *p1 = a1;
    long v0 = p0->v;
    long v1 = p1->v;
    return KMR_CMP3(v0, v1);
}

static int
kmr_sort_locally_lo(KMR_KVS *kvi, KMR_KVS *kvo, _Bool shuffling,
		    _Bool ranking, struct kmr_option opt)
{
#if KMR_TRACE_ENABLE
    kmr_trace_add_entry(kmr_trace_event_sort_start, kvi, kvo);
#endif  
    kmr_assert_kvs_ok(kvi, kvo, 1, 1);
    assert(kmr_shuffle_compatible_p(kvo, kvi));
    KMR *mr = kvi->c.mr;
    if (kmr_ckpt_enabled(mr)) {
	if (kmr_ckpt_progress_init(kvi, kvo, opt)) {
	    kmr_add_kv_done(kvo);
	    kvo->c.sorted = 1;
	    if (!opt.inspect) {
		kmr_free_kvs(kvi);
	    }
	    return MPI_SUCCESS;
	}
    }
    _Bool twostep = !mr->one_step_sort;
    _Bool primekey = ((kvi->c.key_data == KMR_KV_INTEGER)
		      || (kvi->c.key_data == KMR_KV_FLOAT8));
    double timestamp[5];
    int cc;
    long cnt = kvi->c.element_count;
    timestamp[0] = MPI_Wtime();
    size_t evsz = (sizeof(struct kmr_keyed_record) * (size_t)cnt);
    struct kmr_keyed_record *ev = kmr_malloc(evsz);
#if 0
    struct kmr_option inspect = {
	.inspect = 1,
	.nothreading = opt.nothreading
    };
    if (shuffling) {
	cc = kmr_map(kvi, 0, ev, inspect, kmr_copy_record_shuffle_fn);
	assert(cc == MPI_SUCCESS);
    } else {
	cc = kmr_map(kvi, 0, ev, inspect, kmr_copy_record_sorting_fn);
	assert(cc == MPI_SUCCESS);
    }
#else
    cc = kmr_retrieve_keyed_records(kvi, ev, cnt, shuffling, ranking);
    assert(cc == MPI_SUCCESS);
#endif
    timestamp[1] = MPI_Wtime();
    if (shuffling || twostep || primekey) {
	if (mr->single_thread || opt.nothreading) {
	    qsort(ev, (size_t)cnt, sizeof(struct kmr_keyed_record), kmr_icmp);
	} else {
	    kmr_isort(ev, (size_t)cnt, sizeof(struct kmr_keyed_record),
		      mr->sort_threads_depth);
	}
    }
    timestamp[2] = MPI_Wtime();
    if (!shuffling && !primekey) {
	/* Sort the array sorted by hashed keys, again by true keys. */
	long *runs = kmr_malloc(sizeof(long) * (size_t)cnt);
	long nruns = 0;
	if (twostep) {
	    long i = 0;
	    while (i < cnt) {
		do {
		    i++;
		    if (i == cnt) {
			break;
		    }
		    cc = KMR_CMP3(ev[i - 1].v, ev[i].v);
		} while (cc == 0);
		assert(nruns < cnt);
		runs[nruns] = i;
		nruns++;
	    }
	    assert(i == cnt && (cnt == 0 || runs[nruns - 1] == cnt));
	} else {
	    nruns = (cnt == 0 ? 0 : 1);
	    runs[0] = cnt;
	}
	kmr_record_sorter_t cmp1 = kmr_choose_record_sorter(kvi);
	if (mr->single_thread || opt.nothreading) {
	    for (long k = 0; k < nruns; k++) {
		long j = (k == 0 ? 0 : runs[k - 1]);
		long i = runs[k];
		assert(j < i);
		if ((i - j) > 1) {
		    qsort(&ev[j], (size_t)(i - j),
			  sizeof(struct kmr_keyed_record),
			  (kmr_qsorter_t)cmp1);
		}
	    }
	} else {
	    KMR_OMP_PARALLEL_FOR_
		for (long k = 0; k < nruns; k++) {
		    long j = (k == 0 ? 0 : runs[k - 1]);
		    long i = runs[k];
		    assert(j < i);
		    if ((i - j) > 1) {
			qsort(&ev[j], (size_t)(i - j),
			      sizeof(struct kmr_keyed_record),
			      (kmr_qsorter_t)cmp1);
		    }
		}
	}
	kmr_free(runs, (sizeof(long) * (size_t)cnt));
    }
    timestamp[3] = MPI_Wtime();
    size_t sz = kvi->c.storage_netsize;
    cc = kmr_allocate_block(kvo, sz);
    assert(cc == MPI_SUCCESS);
    /*NEED-THREADING*/
    for (long i = 0 ; i < cnt; i++) {
	struct kmr_kv_box kv = kmr_pick_kv(ev[i].e, kvi);
	kmr_add_kv_nomutex(kvo, kv, 0, 0);
    }
    timestamp[4] = MPI_Wtime();
    assert(sz == 0 || kmr_kvs_entry_tail_p(kvo->c.adding_point));
    assert(sz == 0 || kvo->c.block_count == 1);
    kmr_add_kv_done(kvo);
    kmr_assert_on_tail_marker(kvo);
    kvo->c.sorted = 1;
    kmr_free(ev, evsz);
    _Bool tracing = mr->trace_sorting;
    if (tracing && (5 <= mr->verbosity)) {
	fprintf(stderr, (";;KMR [%05d] kmr_sort_locally"
			 " time=(%f %f %f %f) (msec)\n"),
		mr->rank,
		((timestamp[1] - timestamp[0]) * 1e3),
		((timestamp[2] - timestamp[1]) * 1e3),
		((timestamp[3] - timestamp[2]) * 1e3),
		((timestamp[4] - timestamp[3]) * 1e3));
	fflush(0);
    }
    if (kmr_ckpt_enabled(mr)) {
	kmr_ckpt_save_kvo_whole(mr, kvo);
    }
    if (!opt.inspect) {
	kmr_free_kvs(kvi);
    }
    if (kmr_ckpt_enabled(mr)) {
	kmr_ckpt_progress_fin(mr);
    }
#if KMR_TRACE_ENABLE
    kmr_trace_add_entry(kmr_trace_event_sort_end, kvi, kvo);
#endif  
    return MPI_SUCCESS;
}

/** Reorders key-value pairs in a single rank.  It sorts pairs when
    SHUFFLING is false, or gathers pairs with the same hashed keys
    adjacent when SHUFFLING is true.  It only respects for not
    ordering but just equality when shuffling.  The sort-keys for
    shuffling are destination ranks for shuffling (taking a modulo of
    the hashed key with nprocs).  As a sorting, it is NOT-STABLE due
    to quick-sort used inside.  It converts pointer keys and values to
    opaque ones for sending.\n
    Sorting on a key-value stream is by memcmp(), unless the keys are
    integer or floating-point numbers (ordering on integers and
    memcmp() are different).  Sorting on non-numbers is performed in
    two steps: the first step sorts by the integer rankings, and the
    second by the specified comparator.  And thus, the comparator is
    required to have a corresponding generator of integer rankings.
    It consumes the input key-value stream.  Effective-options:
    NOTHREADING, INSPECT, KEY_AS_RANK. */

int
kmr_sort_locally(KMR_KVS *kvi, KMR_KVS *kvo, _Bool shuffling,
		 struct kmr_option opt)
{
    kmr_assert_kvs_ok(kvi, kvo, 1, 1);
    assert(kmr_shuffle_compatible_p(kvo, kvi));
    struct kmr_option kmr_supported = {.nothreading = 1, .inspect = 1,
                                       .key_as_rank = 1};
    kmr_check_fn_options(kvi->c.mr, kmr_supported, opt, __func__);
    _Bool ranking = opt.key_as_rank;
    kmr_sort_locally_lo(kvi, kvo, shuffling, ranking, opt);
    return MPI_SUCCESS;
}

/* ================================================================ */

/* Counts the number of entries in the key-value stream.  If
   BOUND_IN_BLOCK is true, it counts only ones in a single data
   block. */

static inline long
kmr_count_entries(KMR_KVS *kvs, _Bool bound_in_block)
{
    kvs->c.current_block = kvs->c.first_block;
    struct kmr_kvs_entry *e = kmr_kvs_first_entry(kvs, kvs->c.first_block);
    long cnt = 0;
    while (e != 0) {
	/*struct kmr_kv_box kv = kmr_pick_kv(e, kvs);*/
	/*printf("entry %d %d %s %s\n", kv.klen, kv.vlen, kv.k.p, kv.v.p);*/
	cnt++;
	e = kmr_kvs_next(kvs, e, bound_in_block);
    }
    return cnt;
}

/** Shuffles key-value pairs to the appropriate destination ranks.  It
    first sorts pairs by the destination ranks of the keys, and then
    exchanges pairs with all-to-all communication.  It converts
    pointer keys and values to opaque ones for sending during the
    sorting stage.  Note that the key-value pairs are sorted by the
    hash-values prior to exchange.  Effective-options: INSPECT,
    KEY_AS_RANK, TAKE_CKPT.  See struct kmr_option. */

int
kmr_shuffle(KMR_KVS *kvi, KMR_KVS *kvo, struct kmr_option opt)
{
#if KMR_TRACE_ENABLE
    kmr_trace_add_entry(kmr_trace_event_shuffle_start, kvi, kvo);
#endif
    kmr_assert_kvs_ok(kvi, kvo, 1, 1);
    assert(kmr_shuffle_compatible_p(kvo, kvi));
    KMR *mr = kvi->c.mr;
    struct kmr_option kmr_supported = {.inspect = 1, .key_as_rank = 1,
                                       .take_ckpt = 1};
    kmr_check_fn_options(mr, kmr_supported, opt, __func__);
    _Bool ranking = opt.key_as_rank;

    /* SKIP SHUFFLING IF MARKED AS SHUFFLED. */

    if (kvi->c.magic == KMR_KVS_PUSHOFF) {
	kmr_pushoff_make_stationary(kvi);
    }
    if (kvi->c.shuffled_in_pushoff) {
	assert(!mr->ckpt_enable);
	int cc = kmr_move_kvs(kvi, kvo, opt);
	return cc;
    }

    if (kmr_ckpt_enabled(mr)) {
	if (kmr_ckpt_progress_init(kvi, kvo, opt)) {
	    kmr_add_kv_done(kvo);
	    if (!opt.inspect) {
		kmr_free_kvs(kvi);
	    }
	    return MPI_SUCCESS;
	}
    }
    int kcdc = kmr_ckpt_disable_ckpt(mr);

    /* Sort for shuffling. */

    enum kmr_kv_field keyf = kmr_unit_sized_or_opaque(kvi->c.key_data);
    enum kmr_kv_field valf = kmr_unit_sized_or_opaque(kvi->c.value_data);
    struct kmr_option n_opt = opt;
    n_opt.inspect = 1;
    KMR_KVS *kvs1 = kmr_create_kvs(mr, keyf, valf);
    kmr_sort_locally_lo(kvi, kvs1, 1, ranking, n_opt);
    assert(kvs1->c.stowed);
    /*kmr_dump_kvs(kvs1, 0);*/
    /*kmr_guess_communication_pattern_(kvs1, opt);*/
    assert(!kmr_fields_pointer_p(kvs1));
    assert(kvs1->c.block_count <= 1);

    int cc;
    int nprocs = mr->nprocs;
    long cnt = kvs1->c.element_count;
    long *ssz = kmr_malloc(sizeof(long) * (size_t)nprocs);
    long *sdp = kmr_malloc(sizeof(long) * (size_t)nprocs);
    long *rsz = kmr_malloc(sizeof(long) * (size_t)nprocs);
    long *rdp = kmr_malloc(sizeof(long) * (size_t)nprocs);
    for (int r = 0; r < nprocs; r++) {
	ssz[r] = 0;
	rsz[r] = 0;
    }
    int rank = 0;
    assert(kvs1->c.current_block == 0);
    kvs1->c.current_block = kvs1->c.first_block;
    struct kmr_kvs_entry *e = kmr_kvs_first_entry(kvs1, kvs1->c.first_block);
    for (long i = 0; i < cnt; i++) {
	assert(e != 0);
	struct kmr_kv_box kv = kmr_pick_kv(e, kvs1);
	int r = (ranking ? (int)kv.k.i : kmr_pitch_rank(kv, kvs1));
	assert(0 <= r && r < nprocs);
	if (ranking && !(0 <= r && r < nprocs)) {
	    kmr_error(mr, "key entries are not ranks");
	}
	if (r < rank) {
	    kmr_error(mr, "key-value entries are not sorted (internal error)");
	}
	ssz[r] += (long)kmr_kvs_entry_netsize(e);
	rank = r;
	e = kmr_kvs_next(kvs1, e, 0);
    }
    /* Exchange send-receive counts. */
    cc = kmr_exchange_sizes(mr, ssz, rsz);
    assert(cc == MPI_SUCCESS);
    long sendsz = 0;
    long recvsz = 0;
    for (int r = 0; r < nprocs; r++) {
	sdp[r] = sendsz;
	sendsz += ssz[r];
	rdp[r] = recvsz;
	recvsz += rsz[r];
    }
    cc = kmr_allocate_block(kvo, (size_t)recvsz);
    assert(cc == MPI_SUCCESS);
    struct kmr_kvs_block *sb = kvs1->c.first_block;
    struct kmr_kvs_entry *sbuf = kmr_kvs_first_entry(kvs1, sb);
    struct kmr_kvs_block *rb = kvo->c.first_block;
    struct kmr_kvs_entry *rbuf = kmr_kvs_first_entry(kvo, rb);
    cc = kmr_alltoallv(mr, sbuf, ssz, sdp, rbuf, rsz, rdp);
    assert(cc == MPI_SUCCESS);
    long ocnt = kmr_count_entries(kvo, 1);
    assert(kvo->c.sorted == 0);
    kvo->c.element_count = ocnt;
    if (recvsz != 0) {
	assert(kvo->c.block_count == 1);
	rb->partial_element_count = ocnt;
	rb->fill_size = (size_t)recvsz;
    }
    kmr_kvs_adjust_adding_point(kvo);
    kmr_add_kv_done(kvo);

    kmr_ckpt_enable_ckpt(mr, kcdc);
    if (kmr_ckpt_enabled(mr)) {
	kmr_ckpt_save_kvo_whole(mr, kvo);
    }

    if (!opt.inspect) {
	kmr_free_kvs(kvi);
    }
    assert(kvo->c.element_count == 0 || kvo->c.storage_netsize != 0);
    xassert(!kmr_fields_pointer_p(kvo));
    kmr_free_kvs(kvs1);
    kmr_free(ssz, (sizeof(long) * (size_t)nprocs));
    kmr_free(sdp, (sizeof(long) * (size_t)nprocs));
    kmr_free(rsz, (sizeof(long) * (size_t)nprocs));
    kmr_free(rdp, (sizeof(long) * (size_t)nprocs));
    if (kmr_ckpt_enabled(mr)) {
	kmr_ckpt_progress_fin(mr);
    }
#if KMR_TRACE_ENABLE
    kmr_trace_add_entry(kmr_trace_event_shuffle_end, kvi, kvo);
#endif
    return MPI_SUCCESS;
}

/** Replicates key-value pairs to be visible on all ranks, that is, it
    has the effect of bcast or all-gather.  It gathers pairs on rank0
    only by the option RANK_ZERO.  It moves stably, keeping the
    ordering of ranks and the ordering of local key-value pairs.
    Effective-options: INSPECT, RANK_ZERO, TAKE_CKPT.
    See struct kmr_option. */

int
kmr_replicate(KMR_KVS *kvi, KMR_KVS *kvo, struct kmr_option opt)
{
    kmr_assert_kvs_ok(kvi, kvo, 1, 1);
    KMR *mr = kvi->c.mr;
    struct kmr_option kmr_supported = {.inspect = 1, .rank_zero = 1,
                                       .take_ckpt = 1};
    kmr_check_fn_options(mr, kmr_supported, opt, __func__);
    int nprocs = mr->nprocs;
    int rank = mr->rank;
    int cc;
    KMR_KVS *kvs1;
    if (kmr_fields_pointer_p(kvi) || kvi->c.block_count > 1) {
	enum kmr_kv_field keyf = kvi->c.key_data;
	enum kmr_kv_field valf = kvi->c.value_data;
	kvs1 = kmr_create_kvs(mr, keyf, valf);

	int kcdc = kmr_ckpt_disable_ckpt(mr);
	cc = kmr_collapse_as_opaque(kvi, kvs1, 1);
	assert(cc == MPI_SUCCESS);
	kmr_ckpt_enable_ckpt(mr, kcdc);
    } else {
	kvs1 = kvi;
    }
    kmr_assert_on_tail_marker(kvs1);
    assert(kvs1->c.block_count <= 1);

    if (kmr_ckpt_enabled(mr)) {
	if (kmr_ckpt_progress_init(kvi, kvo, opt)) {
	    kmr_add_kv_done(kvo);
	    if (kvs1 != kvi) {
		cc = kmr_free_kvs(kvs1);
		assert(cc == MPI_SUCCESS);
	    }
	    if (!opt.inspect) {
		cc = kmr_free_kvs(kvi);
		assert(cc == MPI_SUCCESS);
	    }
	    return MPI_SUCCESS;
	}
    }

    long *rsz = kmr_malloc(sizeof(long) * (size_t)nprocs);
    long *rdp = kmr_malloc(sizeof(long) * (size_t)nprocs);
    /* Exchange send-receive counts. */
    long ssz = (long)kvs1->c.storage_netsize;
    cc = kmr_gather_sizes(mr, ssz, rsz);
    assert(cc == MPI_SUCCESS);
    long recvsz = 0;
    if (!opt.rank_zero || rank == 0) {
	for (int r = 0; r < nprocs; r++) {
	    rdp[r] = recvsz;
	    recvsz += rsz[r];
	}
    }
    if (!(kvo->c.key_data == kvs1->c.key_data
	  && kvo->c.value_data == kvs1->c.value_data)) {
	kmr_error(mr, "key-data or value-data types mismatch");
    }
    cc = kmr_allocate_block(kvo, (size_t)recvsz);
    assert(cc == MPI_SUCCESS);
    struct kmr_kvs_block *sb = kvs1->c.first_block;
    struct kmr_kvs_entry *sbuf = kmr_kvs_first_entry(kvs1, sb);
    struct kmr_kvs_block *rb = kvo->c.first_block;
    struct kmr_kvs_entry *rbuf = kmr_kvs_first_entry(kvo, rb);
    cc = kmr_allgatherv(mr, opt.rank_zero, sbuf, ssz, rbuf, rsz, rdp);
    assert(cc == MPI_SUCCESS);
    assert(kvo->c.element_count == 0);
    long ocnt = kmr_count_entries(kvo, 1);
    kvo->c.element_count = ocnt;
    if (recvsz != 0) {
	rb->partial_element_count = ocnt;
	rb->fill_size = (size_t)recvsz;
    }
    kmr_kvs_adjust_adding_point(kvo);
    cc = kmr_add_kv_done(kvo);
    assert(cc == MPI_SUCCESS);
    kmr_assert_on_tail_marker(kvo);
    assert(kvo->c.element_count == 0 || kvo->c.storage_netsize != 0);

    if (kmr_ckpt_enabled(mr)) {
	kmr_ckpt_save_kvo_whole(mr, kvo);
    }

    if (kvs1 != kvi) {
	cc = kmr_free_kvs(kvs1);
	assert(cc == MPI_SUCCESS);
    }
    if (!opt.inspect) {
	cc = kmr_free_kvs(kvi);
	assert(cc == MPI_SUCCESS);
    }
    kmr_free(rsz, (sizeof(long) * (size_t)nprocs));
    kmr_free(rdp, (sizeof(long) * (size_t)nprocs));

    if (kmr_ckpt_enabled(mr)) {
	kmr_ckpt_progress_fin(mr);
    }
    return MPI_SUCCESS;
}

/* ================================================================ */

/* Copies the entires as the keyed-records.  The keyed-records hould
   hashed keys for sorting. */

static int
kmr_copy_record_fn(const struct kmr_kv_box kv,
		   const KMR_KVS *kvi, KMR_KVS *kvo, void *p,
		   const long i)
{
    struct kmr_kv_box *ev = p;
    ev[i] = kv;
    return MPI_SUCCESS;
}

/* Reduces key-value pairs.  The version "nothreading" uses less
   memory for sort-records than the version "threading". */

static int
kmr_reduce_nothreading(KMR_KVS *kvi, KMR_KVS *kvo,
		       void *arg, struct kmr_option opt, kmr_redfn_t r)
{
    xassert(kvi->c.current_block == 0);
    kmr_sorter_t cmp = kmr_choose_sorter(kvi);
    KMR *mr = kvi->c.mr;
    long cnt = kvi->c.element_count;
    int cc;
    struct kmr_kv_box *ev = 0;
    long evsz = 0;
    kvi->c.current_block = kvi->c.first_block;
    struct kmr_kvs_entry *e = kmr_kvs_first_entry(kvi, kvi->c.first_block);
    long index = 0;
    long redcount = 0;

    if (kmr_ckpt_enabled(mr)) {
	if (kmr_ckpt_progress_init(kvi, kvo, opt)) {
	    if (kvo != 0) {
		kmr_add_kv_done(kvo);
	    }
	    return MPI_SUCCESS;
	}
	long start_from = kmr_ckpt_first_unprocessed_kv(mr);
	while (index < start_from) {
	    e = kmr_kvs_next(kvi, e, 1);
	    index++;
	}
	kmr_ckpt_save_kvo_block_init(mr, kvo);
    }

    for (;;) {
	struct kmr_kvs_entry *ej = e;
	long n = 0;
	while (e != 0) {
	    assert(index < cnt);
	    e = kmr_kvs_next(kvi, e, 1);
	    n++;
	    index++;
	    if (e == 0) {
		break;
	    }
	    //struct kmr_keyed_record s0 = {.v = 0, .e = kmr_pick_kv(ej, kvi)};
	    //struct kmr_keyed_record s1 = {.v = 0, .e = kmr_pick_kv(e, kvi)};
	    struct kmr_kv_box kv0 = kmr_pick_kv(ej, kvi);
	    struct kmr_kv_box kv1 = kmr_pick_kv(e, kvi);
	    cc = (*cmp)(&kv0, &kv1);
	    if (cc != 0) {
		break;
	    }
	}
	if (n == 0) {
	    assert(ej == 0 && e == 0);
	    break;
	}
	assert(n > 0);
	if (n > evsz) {
	    evsz = n;
	    ev = kmr_realloc(ev, (sizeof(struct kmr_kv_box) * (size_t)evsz));
	    assert(ev != 0);
	}
	e = ej;
	for (long i = 0; i < n; i++) {
	    assert(e != 0);
	    ev[i] = kmr_pick_kv(e, kvi);
	    e = kmr_kvs_next(kvi, e, 1);
	}

	double t0 = ((mr->log_traces == 0) ? 0.0 : MPI_Wtime());
	cc = (*r)(ev, n, kvi, kvo, arg);
	double t1 = ((mr->log_traces == 0) ? 0.0 : MPI_Wtime());
	if (cc != MPI_SUCCESS) {
	    char ee[80];
	    snprintf(ee, sizeof(ee),
		     "Reduce-fn returned with error cc=%d", cc);
	    kmr_error(mr, ee);
	}
	if (mr->log_traces != 0) {
	    kmr_log_reduce(mr, kvi, ev, n, r, (t1 - t0));
	}

	if (kmr_ckpt_enabled(mr)) {
	    kmr_ckpt_save_kvo_block_add(mr, kvo, n);
	}

	redcount += n;
    }
    assert(index == cnt);
    if (kmr_ckpt_enabled(mr)) {
	kmr_ckpt_save_kvo_block_fin(mr, kvo);
    }
    if (kvo != 0) {
	kmr_add_kv_done(kvo);
    }
    if (ev != 0) {
	kmr_free(ev, (sizeof(struct kmr_kv_box) * (size_t)evsz));
    }
    return MPI_SUCCESS;
}

static int
kmr_reduce_threading(_Bool stop_when_some_added,
		     KMR_KVS *kvi, KMR_KVS *kvo,
		     void *arg, struct kmr_option opt, kmr_redfn_t r)
{
    int cc;
    if (kmr_ckpt_enabled(kvi->c.mr)) {
	if (kmr_ckpt_progress_init(kvi, kvo, opt)) {
	    if (kvo != 0) {
		kmr_add_kv_done(kvo);
	    }
	    return MPI_SUCCESS;
	}
    }
    struct kmr_option inspect = {
	.inspect = 1,
	.nothreading = opt.nothreading
    };
    assert(kvi->c.current_block == 0);
    long cnt = kvi->c.element_count;
    struct kmr_kv_box *
	ev = kmr_malloc(sizeof(struct kmr_kv_box) * (size_t)cnt);
    int kcdc = kmr_ckpt_disable_ckpt(kvi->c.mr);
    cc = kmr_map(kvi, 0, ev, inspect, kmr_copy_record_fn);
    assert(cc == MPI_SUCCESS);
    kmr_ckpt_enable_ckpt(kvi->c.mr, kcdc);

    kmr_sorter_t cmp = kmr_choose_sorter(kvi);
    long *runs = kmr_malloc(sizeof(long) * (size_t)cnt);
    long nruns = 0;
    {
	long i = 0;
#if 0
	if (kmr_ckpt_enabled(kvi->c.mr)) {
	    i = kmr_ckpt_first_unprocessed_kv(kvi->c.mr);
	    kmr_ckpt_save_kvo_block_init(kvi->c.mr, kvo);
	}
#endif
	while (i < cnt) {
	    do {
		i++;
		if (i == cnt) {
		    break;
		}
		cc = (*cmp)(&ev[i - 1], &ev[i]);
		assert(cc <= 0);
	    } while (cc == 0);
	    assert(nruns < cnt);
	    runs[nruns] = i;
	    nruns++;
	}
	assert(i == cnt && (cnt == 0 || runs[nruns - 1] == cnt));
    }
    {
	if (kvo != 0) {
	    kvo->c.under_threaded_operation = 1;
	}
	KMR *mr = kvi->c.mr;
	_Bool skip = 0;
	KMR_OMP_PARALLEL_FOR_
	    for (long k = 0; k < nruns; k++) {
		/* (Access to stop is sloppy). */
		if (!skip) {
		    long j = (k == 0 ? 0 : runs[k - 1]);
		    long i = runs[k];
		    assert(j < i);
		    double t0 = ((mr->log_traces == 0) ? 0.0 : MPI_Wtime());
		    int ccx = (*r)(&ev[j], (i - j), kvi, kvo, arg);
		    double t1 = ((mr->log_traces == 0) ? 0.0 : MPI_Wtime());
		    if (ccx != MPI_SUCCESS) {
			char ee[80];
			snprintf(ee, sizeof(ee),
				 "Reduce-fn returned with error cc=%d", ccx);
			kmr_error(mr, ee);
		    }
		    if (mr->log_traces != 0) {
			kmr_log_reduce(mr, kvi, ev, (i - j), r, (t1 - t0));
		    }
#if 0
		    if (kmr_ckpt_enabled(mr)) {
			KMR_OMP_CRITICAL_
			{
			    kmr_ckpt_save_kvo_block_add(mr, kvo, (i - j));
			}
		    }
#endif
		    if (stop_when_some_added) {
			_Bool done;
			if (mr->stop_at_some_check_globally) {
			    done = 0;
			} else {
			    done = (kvo->c.element_count != 0);
			}
			if (done) {
			    KMR_OMP_CRITICAL_
			    {
				skip = 1;
			    }
			}
		    }
		}
	    }
	if (kvo != 0) {
	    kvo->c.under_threaded_operation = 0;
	}
    }
    if (kmr_ckpt_enabled(kvi->c.mr)) {
#if 0
	kmr_ckpt_save_kvo_block_fin(mr, kvo);
#endif
	kmr_ckpt_save_kvo_whole(kvi->c.mr, kvo);
    }
    if (kvo != 0) {
	kmr_add_kv_done(kvo);
    }
    kmr_free(runs, (sizeof(long) * (size_t)cnt));
    kmr_free(ev, (sizeof(struct kmr_kv_box) * (size_t)cnt));
    return MPI_SUCCESS;
}

/** Reduces key-value pairs.  It does not include shuffling, and thus,
    it requires being preceded by shuffling.  Or, it works on local
    data (as a local combiner), if it is not preceded by shuffling.
    It always consumes the input key-value stream KVI.  An output
    key-value stream KVO can be null.  It passes an array of key-value
    pairs to a reduce-function whose keys are all equal (equality is
    by bits).  The pointer ARG is just passed to a reduce-function as
    a general argument, where accesses to it should be race-free,
    since a reduce-function is called by threads by default.  R is a
    reduce-function.  See the description on the type ::kmr_redfn_t.
    A reduce-function may see a different input key-value stream
    (internally created one) instead of the one given.  During
    reduction, it first scans adjacent equal keys, then calls a given
    reduce-function.  Effective-options: NOTHREADING, INSPECT, TAKE_CKPT.
    See struct kmr_option. */

int
kmr_reduce9(_Bool stop_when_some_added,
	    KMR_KVS *kvi, KMR_KVS *kvo,
	    void *arg, struct kmr_option opt, kmr_redfn_t r,
	    const char *file, const int line, const char *func)
{
#if KMR_TRACE_ENABLE
    kmr_trace_add_entry(kmr_trace_event_reduce_start, kvi, kvo);
#endif    
    kmr_assert_kvs_ok(kvi, kvo, 1, 0);
    KMR *mr = kvi->c.mr;
    struct kmr_option kmr_supported = {.nothreading = 1, .inspect = 1,
                                       .take_ckpt = 1};
    kmr_check_fn_options(mr, kmr_supported, opt, __func__);
    struct kmr_option i_opt = kmr_copy_options_i_part(opt);
    struct kmr_option o_opt = kmr_copy_options_o_part(opt);

    if (kmr_ckpt_enabled(mr)) {
	kmr_ckpt_lock_start(mr);
    }

    /* Sort for reduction. */

    enum kmr_kv_field keyf = kmr_unit_sized_or_opaque(kvi->c.key_data);
    enum kmr_kv_field valf = kmr_unit_sized_or_opaque(kvi->c.value_data);
    KMR_KVS *kvs1 = kmr_create_kvs(mr, keyf, valf);

    /* Make checkpoint for kvi and kvs1. */

    kmr_sort_locally_lo(kvi, kvs1, 0, 0, i_opt);
    KMR_DEBUGX(kmr_assert_sorted(kvs1, 1, 0, 0));

    /* Make checkpoint for kvs1 and kvo. */

    struct kmr_code_line info;
    if (mr->atwork == 0) {
	info.file = file;
	info.func = func;
	info.line = line;
	mr->atwork = &info;
    }
    int cc;
    if (mr->single_thread || opt.nothreading) {
	cc = kmr_reduce_nothreading(kvs1, kvo, arg, o_opt, r);
    } else {
	cc = kmr_reduce_threading(stop_when_some_added,
				  kvs1, kvo, arg, o_opt, r);
    }
    if (mr->atwork == &info) {
	mr->atwork = 0;
    }

    //kmr_assert_on_tail_marker(kvo);
    kmr_assert_on_tail_marker(kvs1);
    kmr_free_kvs(kvs1);

    if (kmr_ckpt_enabled(mr)) {
	kmr_ckpt_progress_fin(mr);
	kmr_ckpt_lock_finish(mr);
    }

#if KMR_TRACE_ENABLE
    kmr_trace_add_entry(kmr_trace_event_reduce_end, kvi, kvo);
#endif    
    return cc;
}

/** Calls a reduce-function once as if all key-value pairs had the
    same key.  See kmr_reduce().  Effective-options: INSPECT, TAKE_CKPT.
    See struct kmr_option. */

int
kmr_reduce_as_one(KMR_KVS *kvi, KMR_KVS *kvo,
		  void *arg, struct kmr_option opt, kmr_redfn_t r)
{
    kmr_assert_kvs_ok(kvi, kvo, 1, 0);
    KMR *mr = kvi->c.mr;
    assert(kvi->c.current_block == 0);
    struct kmr_option kmr_supported = {.inspect = 1, .take_ckpt = 1};
    kmr_check_fn_options(mr, kmr_supported, opt, __func__);

    if (kmr_ckpt_enabled(mr)) {
	if (kmr_ckpt_progress_init(kvi, kvo, opt)) {
	    if (kvo != 0) {
		kmr_add_kv_done(kvo);
	    }
	    if (!opt.inspect) {
		kmr_free_kvs(kvi);
	    }
	    return MPI_SUCCESS;
	}
    }
    int kcdc = kmr_ckpt_disable_ckpt(mr);

    long cnt = kvi->c.element_count;
    struct kmr_kv_box *
	ev = kmr_malloc(sizeof(struct kmr_kv_box) * (size_t)cnt);

    if (cnt > 0) {
	int cc;

	struct kmr_option inspect = {.inspect = 1};
	cc = kmr_map(kvi, 0, ev, inspect, kmr_copy_record_fn);
	assert(cc == MPI_SUCCESS);

	cc = (*r)(&ev[0], cnt, kvi, kvo, arg);
	if (cc != MPI_SUCCESS) {
	    char ee[80];
	    snprintf(ee, sizeof(ee),
		     "Reduce-fn returned with error cc=%d", cc);
	    kmr_error(mr, ee);
	}
    }

    kmr_ckpt_enable_ckpt(mr, kcdc);
    if (kmr_ckpt_enabled(mr)) {
	kmr_ckpt_save_kvo_whole(mr, kvo);
    }

    if (kvo != 0) {
	kmr_add_kv_done(kvo);
    }
    kmr_free(ev, (sizeof(struct kmr_kv_box) * (size_t)cnt));
    if (!opt.inspect) {
	kmr_free_kvs(kvi);
    }

    if (kmr_ckpt_enabled(mr)) {
	kmr_ckpt_progress_fin(mr);
    }
    return MPI_SUCCESS;
}

/* ================================================================ */

/** Concatenates a number of KVSes to one.  Inputs are consumed.  (It
    is fast because the key-value data is stored internally as a list
    of data blocks, and this routine just links them).  Note that
    concatenating KVS can in effect be performed by consecutive calls
    to kmr_map() with the KEEP_OPEN option using the same output KVS.
    Effective-options: none. */

int
kmr_concatenate_kvs(KMR_KVS *kvs[], int nkvs, KMR_KVS *kvo,
		    struct kmr_option opt)
{
    for (int i = 0; i < nkvs; i++) {
	kmr_assert_i_kvs_ok(kvs[i], 1);
    }
    kmr_assert_o_kvs_ok(kvo, 1);
    if (kvo->c.element_count > 0) {
	KMR *mr = kvo->c.mr;
	kmr_error(mr, "kmr_concatenate_kvs: Output kvs has entries");
    }
    kmr_check_fn_options(kvo->c.mr, kmr_noopt, opt, __func__);

    struct kmr_kvs_block *storage = 0;
    long elements = 0;
    size_t netsize = 0;
    long blocks = 0;

    struct kmr_kvs_block *p = 0;
    for (int i = 0; i < nkvs; i++) {
	elements += kvs[i]->c.element_count;
	netsize += kvs[i]->c.storage_netsize;
	blocks += kvs[i]->c.block_count;

	struct kmr_kvs_block *bb = kvs[i]->c.first_block;
	if (bb != 0) {
	    kvs[i]->c.first_block = 0;
	    if (p == 0) {
		assert(storage == 0);
		p = bb;
		storage = bb;
	    } else {
		assert(blocks != 0 && p->next == 0);
		p->next = bb;
	    }
	}
	if (p != 0) {
	    while (p->next != 0) {
		p = p->next;
	    }
	}
	kmr_free_kvs(kvs[i]);
    }

    kvo->c.first_block = storage;
    kvo->c.element_count = elements;
    kvo->c.storage_netsize = netsize;
    kvo->c.block_count = blocks;

    /*kmr_add_kv_done(kvo);*/
    kvo->c.stowed = 1;
    kvo->c.current_block = 0;
    kvo->c.adding_point = 0;
    assert(kvo->c.block_count == 0 || kvo->c.first_block != 0);

    return MPI_SUCCESS;
}

/** Finds the last entry of a key-value stream.  It returns null when
    a key-value stream is empty.  It sequentially scans all the
    entries and slow. */

struct kmr_kvs_entry *
kmr_find_kvs_last_entry(KMR_KVS *kvs)
{
    kmr_assert_kvs_ok(kvs, 0, 1, 0);
    assert(kvs->c.magic == KMR_KVS_ONCORE);
#if 0
    long cnt = kvs->c.element_count;
    kvs->c.current_block = kvs->c.first_block;
    struct kmr_kvs_entry *e = kmr_kvs_first_entry(kvs, kvs->c.first_block);
    struct kmr_kvs_entry *o = 0;
    for (long i = 0; i < cnt && e != 0; i++) {
	o = e;
	e = kmr_kvs_next(kvs, e, 0);
    }
    kvs->c.current_block = 0;
    return o;
#else
    if (kvs->c.element_count == 0) {
	return 0;
    } else {
	struct kmr_kvs_block *b;
	for (b = kvs->c.first_block; b->next != 0; b = b->next);
	kvs->c.current_block = b;
	struct kmr_kvs_entry *e;
	struct kmr_kvs_entry *o;
	e = kmr_kvs_first_entry(kvs, b);
	o = 0;
	long cnt = b->partial_element_count;
	for (long i = 0; i < cnt && e != 0; i++) {
	    o = e;
	    e = kmr_kvs_next(kvs, e, 1);
	}
	kvs->c.current_block = 0;
	return o;
    }
#endif
}

/** Fills local key-value entries in an array for inspection.  The
    returned pointers point to the inside of the KVS.  The array EV
    should be as large as N.  It implies inspect. */

int
kmr_retrieve_kvs_entries(KMR_KVS *kvs, struct kmr_kvs_entry **ev, long n)
{
    kmr_assert_kvs_ok(kvs, 0, 1, 0);
    assert(kvs->c.magic == KMR_KVS_ONCORE);
    long cnt = MIN(n, kvs->c.element_count);
    kvs->c.current_block = kvs->c.first_block;
    struct kmr_kvs_entry *e = kmr_kvs_first_entry(kvs, kvs->c.first_block);
    for (long i = 0; i < cnt && e != 0; i++) {
	ev[i] = e;
	e = kmr_kvs_next(kvs, e, 0);
    }
    kvs->c.current_block = 0;
    return MPI_SUCCESS;
}

/** Fills keyed records in an array for sorting.  The array EV should
    be as large as N.  It implies inspect. */

int
kmr_retrieve_keyed_records(KMR_KVS *kvs, struct kmr_keyed_record *ev,
			   long n, _Bool shuffling, _Bool ranking)
{
    kmr_assert_kvs_ok(kvs, 0, 1, 0);
    assert(kvs->c.magic == KMR_KVS_ONCORE);
    long cnt = MIN(n, kvs->c.element_count);
    kvs->c.current_block = kvs->c.first_block;
    struct kmr_kvs_entry *e = kmr_kvs_first_entry(kvs, kvs->c.first_block);
    for (long i = 0; i < cnt && e != 0; i++) {
	struct kmr_kv_box kv = kmr_pick_kv(e, kvs);
	if (shuffling) {
	    ev[i].v = (ranking ? kv.k.i : kmr_pitch_rank(kv, kvs));
	    ev[i].e = e;
	} else {
	    ev[i].v = kmr_stable_key(kv, kvs);
	    ev[i].e = e;
	}
	e = kmr_kvs_next(kvs, e, 0);
    }
    kvs->c.current_block = 0;
    return MPI_SUCCESS;
}

/** Returns a minimum byte size of the field: 8 for INTEGER and
    FLOAT8, 0 for others. */

int
kmr_legal_minimum_field_size(KMR *mr, enum kmr_kv_field f)
{
    switch (f) {
    case KMR_KV_BAD:
	kmr_error(mr, "kmr_legal_minimum_field_size: Bad field");
	return 0;
    case KMR_KV_INTEGER:
	return sizeof(long);
    case KMR_KV_FLOAT8:
	return sizeof(double);
    case KMR_KV_OPAQUE:
    case KMR_KV_CSTRING:
    case KMR_KV_POINTER_OWNED:
    case KMR_KV_POINTER_UNMANAGED:
	return 0;
    default:
	kmr_error(mr, "kmr_legal_minimum_field_size: Bad field");
	return 0;
    }
}

/** Scans every key-value with a reduce-function locally
    (independently on each rank).  It works in the order in the KVS.
    It ignores differences of the keys.  It gets the start value from
    CARRYIN and puts the final value to CARRYOUT.  The output has the
    same number of entries as the input.  The carry-in and carry-out
    have one entry.  The carry-out can be null.  The reduce-function
    is called on each key-value pair as the right operand with the
    previous value as the left operand, and it should output a single
    value.  The key part of the output is ignored and a pair is stored
    under the original key. */

int
kmr_scan_locally(KMR_KVS *kvi, KMR_KVS *carryin,
		 KMR_KVS *kvo, KMR_KVS *carryout, kmr_redfn_t r)
{
    int cc;
    KMR *mr = kvo->c.mr;
    enum kmr_kv_field keyf = kvi->c.key_data;
    enum kmr_kv_field valf = kvi->c.value_data;

    long cnt = kvi->c.element_count;
    size_t evsz = (sizeof(struct kmr_keyed_record) * (size_t)cnt);
    struct kmr_keyed_record *ev = kmr_malloc(evsz);
    cc = kmr_retrieve_keyed_records(kvi, ev, cnt, 0, 0);
    assert(cc == MPI_SUCCESS);

    KMR_KVS *lastvalue = carryin;
    for (long i = 0; i < cnt; i++) {
	struct kmr_kv_box kv;
	cc = kmr_take_one(lastvalue, &kv);
	assert(cc == MPI_SUCCESS);
	struct kmr_kv_box bx[2];
	bx[0] = kv;
	bx[1] = kmr_pick_kv(ev[i].e, kvi);
	KMR_KVS *xs = kmr_create_kvs(mr, keyf, valf);
	cc = (*r)(bx, 2, kvi, xs, 0);
	if (cc != MPI_SUCCESS) {
	    char ee[80];
	    snprintf(ee, sizeof(ee),
		     "Reduce-fn returned with error cc=%d", cc);
	    kmr_error(mr, ee);
	}
	cc = kmr_add_kv_done(xs);
	assert(cc == MPI_SUCCESS);
	/* Put the last value as it is a non-inclusive scan. */
	bx[0].klen = bx[1].klen;
	bx[0].k = bx[1].k;
	cc = kmr_add_kv(kvo, bx[0]);
	assert(cc == MPI_SUCCESS);
	kmr_free_kvs(lastvalue);
	lastvalue = xs;
    }
    cc = kmr_add_kv_done(kvo);
    assert(cc == MPI_SUCCESS);

    if (carryout != 0) {
	struct kmr_kv_box kv;
	cc = kmr_take_one(lastvalue, &kv);
	assert(cc == MPI_SUCCESS);
	cc = kmr_add_kv(carryout, kv);
	assert(cc == MPI_SUCCESS);
	cc = kmr_add_kv_done(carryout);
	assert(cc == MPI_SUCCESS);
    }
    kmr_free_kvs(lastvalue);
    kmr_free_kvs(kvi);

    if (ev != 0) {
	kmr_free(ev, evsz);
    }

    return MPI_SUCCESS;
}

/*
Copyright (C) 2012-2015 RIKEN AICS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/
