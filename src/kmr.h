/* kmr.h (2014-02-04) */
/* Copyright (C) 2012-2015 RIKEN AICS */

#ifndef _KMR_H
#define _KMR_H

#define KMR_H  20150622

/** \file kmr.h KMR Interface.  GENERAL NOTES.  (1) The sizes of
    key-value fields are rounded up to 8-byte boundary. */

/** \mainpage KMR
    \htmlinclude kmr-overview.html */

#include <stdio.h>
#include <stddef.h>
#include <inttypes.h>
#include <sys/types.h>
#include <string.h>
#include <assert.h>

/* Poor guess of K or Fujitsu FX10. */
#if defined(__sparc) && defined(__HPC_ACE__)
#define __K 1
#endif

#define KMR_BR0 {
#define KMR_BR1 }
#ifdef __cplusplus
extern "C" KMR_BR0
#endif

#ifdef __cplusplus
typedef unsigned char _Bool;
#endif

/* KMR version number. */

#define KMR_API_ID0(X) KMR_API_ID1(X)
#define KMR_API_ID1(X) kmr_api_ ## X
#define KMR_API_ID KMR_API_ID0(KMR_H)

extern int KMR_API_ID;
extern const int kmr_version;

/* MPI tags used as replies in kmr_map_via_spawn(). */

#define KMR_TAG_SPAWN_REPLY 500
#define KMR_TAG_SPAWN_REPLY1 501

struct kmr_ctx;
union kmr_kvs;

typedef struct kmr_ctx KMR;
typedef union kmr_kvs KMR_KVS;

#define KMR_JOB_NAME_LEN 256

/** Makes a new key-value stream (of type KMR_KVS) with the specified
    field datatypes.  */

#define kmr_create_kvsx(MR,KF,VF,OPT) \
    kmr_create_kvs7((MR), (KF), (VF), (OPT), __FILE__, __LINE__, __func__)

/** Makes a new key-value stream (of type KMR_KVS) with the specified
    field datatypes.  */

#define kmr_create_kvs(MR,KF,VF) \
    kmr_create_kvs7((MR), (KF), (VF), kmr_noopt, __FILE__, __LINE__, __func__)

/** Makes a new key-value stream (of type KMR_KVS).  */

#define kmr_create_kvs_(MR,IGNORE) \
    kmr_create_kvs7((MR), KMR_KV_BAD, KMR_KV_BAD, kmr_noopt, \
		    __FILE__, __LINE__, __func__)

/** Maps simply.  See kmr_map9(). */

#define kmr_map(KVI, KVO, ARG, OPT, M) \
    kmr_map9(0, (KVI), (KVO), (ARG), (OPT), (M), \
	     __FILE__, __LINE__, __func__)

/** Reduces key-value pairs.  See kmr_reduce9(). */

#define kmr_reduce(KVI, KVO, ARG, OPT, R) \
    kmr_reduce9(0, (KVI), (KVO), (ARG), (OPT), (R), \
		__FILE__, __LINE__, __func__)

/* Link of Key-Value Streams. */

struct kmr_kvs_list {
    KMR_KVS *prev, *next;
};

struct kmr_kvs_list_head {
    KMR_KVS *head, *tail;
};

struct kmr_ckpt_ctx;

/** Information of Source Code Line. */

struct kmr_code_line { const char *file; const char *func; int line; };

/** KMR Context.  Structure KMR is a common record of key-value
    streams.  It records a few internal states and many options.

    KVSES is a linked-list recording all active key-value streams.  It
    is used to warn about unfreed key-value streams.

    CKPT_KVS_ID_COUNTER and CKPT_CTX record checkpointing states.

    LOG_TRACES is a file stream, when it is non-null, which records
    times taken by each call to map/reduce-functions.  Note that trace
    routines call MPI_Wtime() in OMP parallel regions (although it may
    be non-threaded).  ATWORK indicates the caller of the current work
    of mapping or reducing (or null if it is not associated), which is
    used in logging traces.

    SPAWN_SIZE and SPAWN_COMMS temporarily holds an array of
    inter-communicators for kmr_map_via_spawn(), so that a
    communicator can be obtained by kmr_get_spawner_communicator() in
    a map-function.

    MAPPER_PARK_SIZE is the number of entries pooled before calling a
    map-function.  Entries are aggregated to try to call a
    map-function with threads.  PRESET_BLOCK_SIZE is the default
    allocation size of a buffer of key-values.  It is used as a
    block-size of key-value streams after trimmed by the amount of the
    malloc overhead.  MALLOC_OVERHEAD (usually an amount of one
    pointer) is reduced from the allocation size, to keep good
    alignment boundary.

    ATOA_THRESHOLD makes the choice of algorithms of all-to-all-v
    communication by the sizes of messages (set to zero to use
    all-to-all-v of MPI).

    SORT_TRIVIAL determines the sorter to run on a single node when
    data size is this value or smaller.  SORT_THRESHOLD determines the
    sorter to use full sampling of a sampling-sort, or pseudo sampling
    when data size is small.  SORT_SAMPLES_FACTOR determines the
    number of samples of a sampling-sort.  SORT_THREADS_DEPTH controls
    the local sorter.  The quick-sort uses Open MP threads until
    recursion depth reaches this value (set to zero for sequential
    run).

    FILE_IO_BLOCK_SIZE is a block size of file reading, used when the
    striping information is not available.

    PUSHOFF_BLOCK_SIZE is a block size of a push-off key-value stream.
    It is a communication block size and should be eqauls on all
    ranks.  PUSHOFF_POLL_RATE gives a hint to a polling interval of a
    push-off key-value stream.

    KMR_INSTALLATION_PATH records the installation path, which is
    taken from the configuration.  SPAWN_WATCH_PROGRAM is a
    watch-program name, which is used in spawning processes which do
    not communicate to the parent.  The variable is a file-path which
    may be set in advance or may be set to one where the watch-program
    is copied (usually in the user's home directory).
    SPAWN_WATCH_PREFIX is a location where a watch-program is to be
    installed (instead of the home directory).  SPAWN_WATCH_HOST_NAME
    is a name of a host-name of a spawner.  It may be set when there
    is a difficulty in connecting a socket.  SPAWN_MAX_PROCESSES
    limits the number of processes simultaneously spawned without
    regard to the universe size.  SPAWN_WATCH_AF is 0, 4, or 6 as the
    preferred IP address format used by the watch-program.
    SPAWN_WATCH_PORT_RANGE[2] is a range of IP port number used by the
    watch-program (values are inclusive).  SPAWN_GAP_MSEC[2] is the
    time given between spawning calls needed by the MPI runtime to
    clean-up the resource management.  The value is scaled to the log
    of the universe size, corresponding the 1st value to 0 processes
    and the 2nd value to 1,000 processes (the default is 1 second to
    one process and 10 seconds for 1,000 processes).
    SPAWN_WATCH_ACCEPT_ONHOLD_MSEC is the time given to wait for the
    watch-program to connect back by a socket.

    VERBOSITY is the verbosity of warning messages; default 5 is good
    for typical use.

    ONK enables the features on K or FX10.  SINGLE_THREAD makes imply
    the nothreading option for mapper/shuffler/reducer.  ONE_STEP_SORT
    disables a prior sorting step which sort on (packed/hashed)
    integer keys in local sorting.  STEP_SYNC is to call a barrier at
    each operation step for debugging.  TRACE_FILE_IO, TRACE_MAP_MS,
    and TRACE_MAP_SPAWN let dump trace output for debugging.
    (TRACE_ALLTOALL lets dump trace output on communication for
    debugging internals).  TRACE_KMRDP lets dump timing information of
    run of KMR-DP.  STD_ABORT lets use abort() instead of MPI_Abort()
    on errors, to let cores dumped on some MPI implementations.
    (FILE_IO_DUMMY_STRIPING is for debugging internals, and assigns
    dummy striping information on not Lustre file-systems).
    (FILE_IO_ALWAYS_ALLTOALLV is for debugging internals).
    SPAWN_DISCONNECT_EARLY (useless) lets the spawner free the
    inter-communicator immediately after spawning.
    SPAWN_DISCONNECT_BUT_FREE lets the spawner use
    MPI_Comm_disconnect() instead of MPI_Comm_free() (It is only used
    with buggy Intel MPI (4.x)).  (SPAWN_PASS_INTERCOMM_IN_ARGUMENT
    changes the behavior to the old API).  MPI_THREAD_SUPPORT records
    the thread support level.  CKPT_ENABLE is a checkpointing enable.
    CKPT_SELECTIVE enables users to specify which kmr functions take
    ckpt files of the output key-value stream.  To take ckpt files
    with this option enabled, users should specify TAKE_CKPT option
    enabled when calling a kmr function.  CKPT_NO_FSYNC does not call
    fsync syscall on writing ckpt files.  Both CKPT_SELECTIVE and
    CKPT_NO_FSYNC should be specified with CKPT_ENABLE.
    STOP_AT_SOME_CHECK_GLOBALLY forces global checking of stop-at-some
    state in mapping (not implemented).  Mapping with stop-at-some
    should be stopped when some key-value is added on any rank, but
    the check is performed only locally by default.  PUSHOFF_HANG_OUT
    makes communication of push-off continue on after a finish of
    mapping/reducing.  PUSHOFF_FAST_NOTICE enables use of RDMA put for
    event notification in push-off key-value streams.  PUSHOFF_STAT
    enables collecting statistics of communication in push-off
    key-value streams.  IDENTIFYING_NAME is just a note. */

struct kmr_ctx {
    MPI_Comm comm;
    int nprocs;
    int rank;
    MPI_Info conf;
    struct kmr_kvs_list_head kvses;

    long ckpt_kvs_id_counter;
    struct kmr_ckpt_ctx *ckpt_ctx;

    FILE *log_traces;
    struct kmr_code_line *atwork;

    int spawn_size;
    MPI_Comm **spawn_comms;

    long mapper_park_size;
    size_t preset_block_size;
    size_t malloc_overhead;

    long atoa_threshold;

    long sort_trivial;
    long sort_threshold;
    long sort_sample_factor;
    int sort_threads_depth;

    long file_io_block_size;

    char *kmr_installation_path;
    char *spawn_watch_program;
    char *spawn_watch_prefix;
    char *spawn_watch_host_name;
    int spawn_max_processes;
    int spawn_watch_af;
    int spawn_watch_port_range[2];
    int spawn_gap_msec[2];
    int spawn_watch_accept_onhold_msec;

    size_t pushoff_block_size;
    int pushoff_poll_rate;

    uint8_t verbosity;

    _Bool onk : 1;
    _Bool single_thread : 1;
    _Bool one_step_sort : 1;
    _Bool step_sync : 1;
    _Bool trace_sorting : 1;
    _Bool trace_file_io : 1;
    _Bool trace_map_ms : 1;
    _Bool trace_map_spawn : 1;
    _Bool trace_alltoall : 1;
    _Bool trace_kmrdp : 1;
    _Bool trace_iolb : 1;
    _Bool std_abort : 1;
    _Bool file_io_dummy_striping : 1;
    _Bool file_io_always_alltoallv : 1;
    _Bool spawn_sync_at_startup : 1;
    _Bool spawn_watch_all : 1;
    _Bool spawn_disconnect_early : 1;
    _Bool spawn_disconnect_but_free : 1;
    _Bool spawn_pass_intercomm_in_argument : 1;
    _Bool keep_fds_at_fork : 1;

    _Bool mpi_thread_support : 1;

    _Bool ckpt_enable : 1;
    _Bool ckpt_selective : 1;
    _Bool ckpt_no_fsync : 1;

    _Bool stop_at_some_check_globally : 1;

    _Bool pushoff_hang_out : 1;
    _Bool pushoff_fast_notice : 1;
    _Bool pushoff_stat : 1;

    struct {
	double times[4];
	long counts[10];
    } pushoff_statistics;

    char identifying_name[KMR_JOB_NAME_LEN];
};

/** Datatypes of Keys or Values.  It indicates the field data of keys
    or values.  KMR_KV_OPAQUE is a variable-sized byte vector, and
    KMR_KV_CSTRING is a non-wide C string, and they are dealt with in
    exactly the same way.  KMR_KV_INTEGER is a long integer, and
    KMR_KV_FLOAT8 is a double.  The datatypes are mostly uninterpreted
    in mapping/reducing, except for in sorting.  There are two other
    types for pointers.  Pointers can be stored as they are (unlike
    opaque data, which are embedded in the field), but converted to
    opaque ones before communication.  KMR_KV_POINTER_OWNED is an
    allocated pointer, and the data will be freed on consuming a
    key-value stream.  KMR_KV_POINTER_UNMANAGED is a pointer to a
    possibly shared data. */

enum kmr_kv_field {
    KMR_KV_BAD,
    KMR_KV_OPAQUE,
    KMR_KV_CSTRING,
    KMR_KV_INTEGER,
    KMR_KV_FLOAT8,
    KMR_KV_POINTER_OWNED,
    KMR_KV_POINTER_UNMANAGED
};

/** Unit-Sized Storage.  A key or a value is passed around as
    unit-sized data.  It is accessed by ".i" for KMR_KV_INTEGER, ".d"
    for KMR_KV_FLOAT8, and ".p" KMR_KV_OPAQUE or other pointer cases.
    It appears in struct kmr_kv_box. */

union kmr_unit_sized {const char *p; long i; double d;};

/* Storage of a Key-Value Entry.  They are stored as key-length;
   value-length; key-content[]; value-content[].  Occupied lengths are
   rounded up to the alignment, thus the size of the entry is:
   (2*sizeof(int)+KMR_ALIGN(klen)+KMR_ALIGN(vlen)).  Flexible-array
   member is avoided for C++. */

struct kmr_kvs_entry {
    int klen;
    int vlen;
    unsigned char c[1];
};

/** Handy Copy of a Key-Value Field.  It holds a pair of a key and a
    value.  The field may either be a copy (for an integer or a real
    value) or a pointer to the storage. */

struct kmr_kv_box {
    int klen;
    int vlen;
    union kmr_unit_sized k;
    union kmr_unit_sized v;
};

#define kmr_kv_cake kmr_kv_box

/** Keyed-Record for Sorting.  A ranking key V is associated with an
    entry for sorting.  An array of this record is held during local
    sorting.  The ranking keys are integers respecting the ordering of
    integers, doubles, and opaque bytes. */

struct kmr_keyed_record {
    long v;
    struct kmr_kvs_entry *e;
};

/** Size of an Entry Header.  It is the size of the length fields of a
    key-value.  It is also the size of the slack space for an
    end-of-block marker, where the end of entries in a block is marded
    by klen=-1 and vlen=-1. */

static const size_t kmr_kvs_entry_header = offsetof(struct kmr_kvs_entry, c);

/* Types of Key-Value Stream.  It is a tag stored in the header.
   KMR_KVS_ONCORE corresponds to struct kmr_kvs_oncore, and
   KMR_KVS_PUSHOFF to struct kmr_kvs_pusoff. (KMR_KVS_STREAM is
   undefined currently).  PACKED is a marker after packing (marshaling
   to byte arrays). */

enum kmr_kvs_magic {
    KMR_KVS_BAD,
    KMR_KVS_ONCORE,
    /*KMR_KVS_STREAM,*/
    KMR_KVS_PUSHOFF,
    KMR_KVS_ONCORE_PACKED
    /*KMR_KVS_STREAM_PACKED*/
};

#define KMR_KVS_MAGIC_OK(X) \
    ((X) == KMR_KVS_ONCORE || (X) == KMR_KVS_ONCORE_PACKED \
     || (X) == KMR_KVS_PUSHOFF)

/** State during kmr_map_ms().  It is kept in a key-value stream only
    during kmr_map_ms() on rank0.  (IT SHOULD BE INCLUDED IN THE STATE
    OF kmr_save_kvs() AND kmr_restore_kvs(), BUT NOT YET).
    Flexible-array member is avoided for C++. */

struct kmr_map_ms_state {
    int nodes;
    int kicks;
    int dones;
    char states[1];
};

/** Key-Value Stream.  MAGIC and MR holds the relevant data.  LINK is
    used to list all allocated key-value streams in the context to
    detect unfreed ones.  KEY_DATA is a datatype of a key field.
    VALUE_DATA is a datatype of a value field.  INFO_LINE0 records the
    location of its allocation in a program.  ELEMENT_COUNT is the
    local count of the number of elements.

    STOWED is set when adding key-value is finished.  ONCORE is always
    true (currently ignored).  NOGROW is set when the buffer is
    preallocated by an enough size.  SORTED indicates the key-value
    stream is locally sorted.  SHUFFLED_IN_PUSHOFF is true when its
    contents are already shuffled.  (UNIFORMLY_SIZED is set when all
    key-values are of the same size.  (currently ignored)).
    (SHUFFLE_TO_SINGLE is set when the shuffling target is a single rank,
    which only checked locally at each rank.  (currently ignored)).

    Fields start from CKPT_ are used only when checkpoint/restart is
    enabled.  CKPT_KVS_ID stores a sequence number starts from 0 which
    is assigned to Key-Value Stream and used to distinguish each other.
    CKPT_GENERATED_OP and CKPT_CONSUMED_OP store sequence numbers of
    operations performed to this Key-Value Stream.  When this Key-Value
    Stream is used as an output, the sequence number of the operation
    is set to CKPT_GENERATED_OP.  When this Key-Value Stream is used as
    an input, the sequence number of the operation is set to
    CKPT_CONSUMED_OP.

    BLOCK_SIZE is an unit of memory allocation invoked at adding a
    key-value pair.  It is PRESET_BLOCK_SIZE in the KMR-context by
    default.  ELEMENT_SIZE_LIMIT restricts the size of one key-value
    pair (for error detection), and it is set to the 1/4 of BLOCK_SIZE
    by default.

    STORAGE_NETSIZE is the local total size used to hold key-value
    pairs.  The size is the sum of kmr_kvs_entry_netsize()
    corresponding to opaque fields, and is much larger than actually
    occupied when the data fields are pointers.  FIRST_BLOCK points to
    the chain of blocks of data.

    MS field holds a state during master-slave mapping.

    A pair of transient fields CURRENT_BLOCK and ADDING_POINT points
    to the current insertion point. */

struct kmr_kvs_oncore {
    enum kmr_kvs_magic magic;
    KMR *mr;
    struct kmr_kvs_list link;
    enum kmr_kv_field key_data;
    enum kmr_kv_field value_data;
    struct kmr_code_line info_line0;
    long element_count;

    _Bool stowed : 1;
    _Bool oncore : 1;
    _Bool nogrow : 1;
    _Bool sorted : 1;
    _Bool shuffled_in_pushoff : 1;
    _Bool _uniformly_sized_ : 1;
    //_Bool _shuffle_to_single_ : 1;

    long ckpt_kvs_id;
    long ckpt_generated_op;
    long ckpt_consumed_op;

    size_t block_size;
    size_t element_size_limit;

    size_t storage_netsize;
    long block_count;
    struct kmr_kvs_block *first_block;

    /* Transient fields: */

    struct kmr_map_ms_state *ms;

    _Bool under_threaded_operation;
    struct kmr_kvs_block *current_block;
    struct kmr_kvs_entry *adding_point;
    void *temporary_data;

    //#ifdef _OPENMP
    //omp_lock_t mutex;
    //#endif /*_OPENMP*/
};

/* Storage of Key-Value Pairs.  The storage is managed as blocks, each
   containing entries terminated by an end-of-block marker.  A marker
   is of klen=-1 and vlen=-1 with no data.  A MARKER IS MANDATORYILY
   PLACED TO CHECK THE END (the early design did not have FILL_SIZE
   field).  SIZE is the whole structure (not just the data part).
   PARTIAL_ELEMENT_COUNT is the count in this block.  FILL_SIZE is a
   next fill-point.  Flexible-array member is avoided for C++. */

struct kmr_kvs_block {
    struct kmr_kvs_block *next;
    size_t size;
    long partial_element_count;
    size_t fill_size;
    struct kmr_kvs_entry data[1];
};

/* Size of a Block Header.  */

static const size_t kmr_kvs_block_header = offsetof(struct kmr_kvs_block, data);

/** Record of Push-Off Key-Value Stream for a Rank.  FILLBUF is for
    fill-in and RECVBUF receiving.  SENDBUFS[2] is a list of blocks,
    where SENDBUFS[0] points to the head and and SENDBUFS[1] to the
    tail. */

struct kmr_pushoff_buffers {
    struct kmr_kvs_entry *adding_point;
    struct kmr_kvs_block *fillbuf;
    struct kmr_kvs_block *recvbuf;
    struct kmr_kvs_block *sendbufs[2];
    int sends;
    _Bool closed[2];
};

/** Key-Value Stream with Shuffling at Addition of Key-Values.
    ELEMENT_COUNT counts local calls of kmr_add_kv(), and it is not
    the result count which is the count of the elements received.
    STORAGE keeps the true contents temporarily, and later it will
    replace this key-value stream.  PEERS and REQS are used
    communication.  REQS is of length (2*nprocs), the first half for
    sends and the second half for receives. */

struct kmr_kvs_pushoff {
    enum kmr_kvs_magic magic;
    KMR *mr;
    struct kmr_kvs_list link;
    enum kmr_kv_field key_data;
    enum kmr_kv_field value_data;
    struct kmr_code_line info_line0;
    long element_count;

    _Bool stowed : 1;
    _Bool oncore : 1;
    _Bool nogrow : 1;
    _Bool sorted : 1;
    _Bool shuffled_in_pushoff : 1;
    _Bool _uniformly_sized_ : 1;

    int seqno;
    KMR_KVS *storage;
    struct kmr_pushoff_buffers *peers;
    MPI_Request *reqs;
    int *indexes;
    MPI_Status *statuses;
};

/** Key-Value Stream (DUMMY); Mandatory Entries. */

struct kmr_kvs_dummy {
    enum kmr_kvs_magic magic;
    KMR *mr;
    struct kmr_kvs_list link;
    enum kmr_kv_field key_data;
    enum kmr_kv_field value_data;
    struct kmr_code_line info_line0;
    long element_count;

    _Bool stowed : 1;
    _Bool oncore : 1;
    _Bool nogrow : 1;
    _Bool sorted : 1;
    _Bool shuffled_in_pushoff : 1;
    _Bool _uniformly_sized_ : 1;
};

/** Key-Value Stream (abstract). */

union kmr_kvs {
    struct kmr_kvs_dummy m;
    struct kmr_kvs_oncore c;
    struct kmr_kvs_pushoff o;
};

/** Options to Mapping, Shuffling, and Reduction.  Options must be the
    same on all ranks.  NOTHREADING tells a mapper and a reducer to
    suppress threading on loops calling a map-function or a
    reduce-function.  It should be set when a map-function or a
    reduce-function is multi-threaded itself.  INSPECT tells a mapper
    that a map-function just inspects the entries, and does not
    consume the input key-value stream.  KEEP_OPEN tells a mapper not
    to close the output key-value stream for further additions of
    key-value pairs.  KEY_AS_RANK tells a shuffler to use a key as a
    rank.  TAKE_CKPT tells kmr functions that support
    Checkpoint/Restart to take checkpoints of the output key-value
    stream when CKPT_SELECTIVE global option is enabled.  RANK_ZERO
    tells kmr_replicate() to gather pairs on rank0 only.  (COLLAPSE
    tells a mapper to convert pointer data to opaque.  Conversions of
    data fields are needed in advance to exchanging data in shuffling.
    It is mainly intended internal use only).  The padding fields make
    the option size as long, for the Fortran binding whose options are
    passed by intergers (SPARC-V9 compilers put bit-fields on
    arguments as 64 bits). */

struct kmr_option {
    _Bool nothreading : 1;
    _Bool inspect : 1;
    _Bool keep_open : 1;
    _Bool key_as_rank : 1;
    _Bool rank_zero : 1;
    _Bool collapse : 1;
    _Bool take_ckpt : 1;
    /*_Bool guess_pattern : 1;*/
    int : 16;
    int : 32;
};

/* Initializers are not by designators for C++ below. */

static const struct kmr_option kmr_noopt = {0, 0, 0, 0, 0, 0, 0};
static const union {struct kmr_option o; unsigned long bits;}
    kmr_optmask = {{1, 1, 1, 1, 1, 1, 1}};

/** Options to Mapping on Files.  See the description on
    kmr_map_file_names() and kmr_map_file_contents() for the meaning.
    The padding fields make the option size as long, for the Fortran
    binding whose options are passed by intergers (SPARC-V9 compilers
    put bit-fields on arguments as 64 bits). */

struct kmr_file_option {
    _Bool each_rank : 1;
    _Bool subdirectories : 1;
    _Bool list_file : 1;
    _Bool shuffle_names : 1;
    int : 16;
    int : 32;
};

/* Initializers are not by designators for C++ below. */

static const struct kmr_file_option kmr_fnoopt = {0, 0, 0, 0};
static const union {struct kmr_file_option o; unsigned long bits;}
    kmr_foptmask = {{1, 1, 1, 1}};

/** Options to Mapping by Spawns.  See the description on
    kmr_map_via_spawn() for the meaning.  The padding fields make the
    option size as long, for the Fortran binding whose options are
    passed by intergers (SPARC-V9 compilers put bit-fields on
    arguments as 64 bits).  ONE_BY_ONE is no longer used (since
    ver.178).  TAKE_CKPT tells kmr functions that support
    Checkpoint/Restart to take checkpoints of the output key-value
    stream when CKPT_SELECTIVE global option is enabled. */

struct kmr_spawn_option {
    _Bool separator_space : 1;
    _Bool reply_each : 1;
    _Bool reply_root : 1;
    _Bool one_by_one : 1;
    _Bool take_ckpt : 1;
    int : 16;
    int : 32;
};

/* Initializers are not by designators for C++ below. */

static const struct kmr_spawn_option kmr_snoopt = {0, 0, 0, 0, 0};
static const union {struct kmr_spawn_option o; unsigned long bits;}
    kmr_soptmask = {{1, 1, 1, 1, 1}};

/** Map-function Type.  A map-function gets a key-value pair as struct
    kmr_kv_box KV.  KVI is the input key-value stream, but it can be
    usually ignored (its potential usage is to check the content type
    of the key and value fields).  KVO is the output key-value stream.
    The pointer ARG is one just passed to kmr_map(), which has no
    specific purpose and is used to pass any argument to a
    map-function.  INDEX is the count of map-function calls, and it
    usually equals to the index of a key-value pair in the input.  It
    is assured distinct, and can be used for race-free accesses to the
    pointer ARG.  */

typedef int (*kmr_mapfn_t)(const struct kmr_kv_box kv,
			   const KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
			   const long index);

/** Reduce-function Type.  A reduce-function gets key-value pairs as
    an array KV of struct kmr_kv_box.  N is the number of key-value
    pairs.  KVI is the the input key-value stream, but it can be
    usually ignored.  KVO is the output key-value stream.  The pointer
    ARG is one just passed to kmr_reduce(), which has no specific
    purpose and is used to pass any argument to a reduce-function.  */

typedef int (*kmr_redfn_t)(const struct kmr_kv_box kv[], const long n,
			   const KMR_KVS *kvi, KMR_KVS *kvo, void *arg);

/** Spawning Info. (OBSOLETE: USE IS ENABLED BY SETTING
    SPAWN_PASS_INTERCOMM_IN_ARGUMENT=1 IN THE MKR STRUCTURE). It is
    passed to a map-function in kmr_map_via_spawn().  MAPARG is the
    pointer argument to kmr_map_via_spawn().  ICOMM is the
    inter-communicator, and ICOMM_FF is for Fortran.  REPLY_ROOT is a
    boolean value of the option REPLY_ROOT.  Union is taken for icomm
    to make it independent from its representation (integer or
    pointer). */

struct kmr_spawn_info {
    void *maparg;
    union {MPI_Comm icomm; long i_;} u;
    int icomm_ff;
    int reply_root;
};

/** N-Tuple.  An n-tuple is a list of opaque values, intended to store
    a list of data items as a key or a value in a key-value stream.
    It is for the utility purpose.  N is the number of entries.
    MARKER can be any value and it is used as a tag for a direct-sum,
    which identifies which key-value stream an n-tuple originally
    belonged.  Use kmr_reset_ntuple() and kmr_put_ntuple() to create
    n-tuples, and kmr_add_ntuple() to add n-tuples in key-value
    stream.  The data items in an n-tuple are a sequence of pairs -- a
    length followed by opaque bytes.  Note the marker and the item
    length be kept the same when n-tuples are used as keys for
    sorting. */

struct kmr_ntuple {
    int marker;
    unsigned short n;
    unsigned short index;
    unsigned short len[1];
};

/** N-Tuple Argument. */

struct kmr_ntuple_entry {
    void *p;
    unsigned short len;
};

/** Sets up the environment.  Currently it does nothing. */

#define kmr_init() kmr_init_2(KMR_API_ID)

extern int kmr_init_2(int ignore);
extern int kmr_fin(void);
extern KMR *kmr_create_context(const MPI_Comm comm, const MPI_Info conf,
			       const char *name);
extern int kmr_free_context(KMR *mr);
extern KMR *kmr_get_context_of_kvs(KMR_KVS const *kvs);

extern KMR_KVS *kmr_create_kvs7(KMR *mr, enum kmr_kv_field k,
				enum kmr_kv_field v,
				struct kmr_option opt,
				const char *, const int, const char *);
extern int kmr_free_kvs(KMR_KVS *kvs);
extern int kmr_move_kvs(KMR_KVS *kvi, KMR_KVS *kvo, struct kmr_option opt);
extern int kmr_concatenate_kvs(KMR_KVS *kvs[], int nkvs, KMR_KVS *kvo,
			       struct kmr_option opt);

extern int kmr_add_kv(KMR_KVS *kvs, const struct kmr_kv_box kv);
extern int kmr_add_kv1(KMR_KVS *kvs, void *k, int klen, void *v, int vlen);
extern int kmr_add_kv_space(KMR_KVS *kvs, const struct kmr_kv_box kv,
			    void **keyp, void **valuep);
extern int kmr_add_kv_quick_(KMR_KVS *kvs, const struct kmr_kv_box kv);
extern int kmr_add_kv_done(KMR_KVS *kvs);
extern int kmr_add_string(KMR_KVS *kvs, const char *k, const char *v);

extern int kmr_map9(_Bool stop_when_some_added,
		    KMR_KVS *kvi, KMR_KVS *kvo,
		    void *arg, struct kmr_option opt, kmr_mapfn_t m,
		    const char *, const int, const char *);
extern int kmr_map_skipping(long from, long stride, long limit,
			    _Bool stop_when_some_added,
			    KMR_KVS *kvi, KMR_KVS *kvo,
			    void *arg, struct kmr_option opt, kmr_mapfn_t m);
extern int kmr_map_once(KMR_KVS *kvo, void *arg, struct kmr_option opt,
			_Bool rank_zero_only, kmr_mapfn_t m);
extern int kmr_map_on_rank_zero(KMR_KVS *kvo, void *arg, struct kmr_option opt,
				kmr_mapfn_t m);
extern int kmr_map_rank_by_rank(KMR_KVS *kvi, KMR_KVS *kvo,
				void *arg, struct kmr_option opt,
				kmr_mapfn_t m);
extern int kmr_map_ms(KMR_KVS *kvi, KMR_KVS *kvo,
		      void *arg, struct kmr_option opt,
		      kmr_mapfn_t m);
extern int kmr_map_ms_commands(KMR_KVS *kvi, KMR_KVS *kvo,
			       void *arg, struct kmr_option opt,
			       struct kmr_spawn_option sopt,
			       kmr_mapfn_t m);
extern int kmr_map_for_some(KMR_KVS *kvi, KMR_KVS *kvo,
			    void *arg, struct kmr_option opt, kmr_mapfn_t m);

extern int kmr_map_via_spawn(KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
			     MPI_Info info, struct kmr_spawn_option opt,
			     kmr_mapfn_t mapfn);
extern int kmr_reply_to_spawner(KMR *mr);
extern MPI_Comm *kmr_get_spawner_communicator(KMR *mr, long index);

extern int kmr_map_processes(_Bool nonmpi,
			     KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
			     MPI_Info info, struct kmr_spawn_option opt,
			     kmr_mapfn_t mapfn);
extern int kmr_map_parallel_processes(KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
				      MPI_Info info,
				      struct kmr_spawn_option opt,
				      kmr_mapfn_t mapfn);
extern int kmr_map_serial_processes(KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
				    MPI_Info info,
				    struct kmr_spawn_option opt,
				    kmr_mapfn_t mapfn);

extern KMR *kmr_create_context_world(void);
extern KMR *kmr_create_dummy_context(void);
extern int kmr_send_kvs_to_spawner(KMR *mr, KMR_KVS *kvs);
extern int kmr_receive_kvs_from_spawned_fn(const struct kmr_kv_box kv,
					   const KMR_KVS *kvi,
					   KMR_KVS *kvo, void *arg,
					   const long index);

#define kmr_sort_a_batch(X0,X1,X2,X3) kmr_sort_locally(X0,X1,X2,X3)

extern int kmr_sort_locally(KMR_KVS *kvi, KMR_KVS *kvo, _Bool shuffling,
			    struct kmr_option opt);

extern int kmr_shuffle(KMR_KVS *kvi, KMR_KVS *kvo, struct kmr_option opt);
extern int kmr_replicate(KMR_KVS *kvi, KMR_KVS *kvo, struct kmr_option opt);

extern int kmr_reduce9(_Bool stop_when_some_added,
		       KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
		       struct kmr_option opt, kmr_redfn_t r,
		       const char *, const int, const char *);
extern int kmr_reduce_as_one(KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
			     struct kmr_option opt, kmr_redfn_t r);
extern int kmr_reduce_for_some(KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
			       struct kmr_option opt, kmr_redfn_t r);

extern int kmr_file_enumerate(KMR *mr, char **names, int n, KMR_KVS *kvo,
			      struct kmr_file_option fopt);
extern int kmr_map_file_names(KMR *mr, char **names, int n,
			      struct kmr_file_option fopt,
			      KMR_KVS *kvo, void *arg,
			      struct kmr_option opt, kmr_mapfn_t m);
extern int kmr_map_getline(KMR *mr, FILE *f, long limit, _Bool largebuffering,
			   KMR_KVS *kvo,
			   void *arg, struct kmr_option opt, kmr_mapfn_t m);
extern int kmr_map_getline_in_memory_(KMR *mr, void *b, size_t sz, long limit,
				      KMR_KVS *kvo, void *arg,
				      struct kmr_option opt, kmr_mapfn_t m);

extern int kmr_take_one(KMR_KVS *kvi, struct kmr_kv_box *kv);
extern int kmr_find_key(KMR_KVS *kvi, struct kmr_kv_box ki,
			struct kmr_kv_box *vo);
extern int kmr_find_string(KMR_KVS *kvi, const char *k, const char **vq);

extern int kmr_copy_info_to_kvs(MPI_Info src, KMR_KVS *kvo);
extern int kmr_copy_kvs_to_info(KMR_KVS *kvi, MPI_Info dst);

extern int kmr_get_element_count(KMR_KVS *kvs, long *v);
extern int kmr_local_element_count(KMR_KVS *kvs, long *v);

extern int kmr_add_identity_fn(const struct kmr_kv_box kv,
			       const KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
			       const long i);
extern int kmr_copy_to_array_fn(const struct kmr_kv_box kv,
				const KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
				const long i);
extern int kmr_save_kvs(KMR_KVS *kvi, void **dataq, size_t *szq,
			struct kmr_option opt);
extern int kmr_restore_kvs(KMR_KVS *kvo, void *data, size_t sz,
			   struct kmr_option opt);

extern int kmr_reverse(KMR_KVS *kvi, KMR_KVS *kvo, struct kmr_option opt);
extern int kmr_pairing(KMR_KVS *kvi, KMR_KVS *kvo, struct kmr_option opt);
extern int kmr_unpairing(KMR_KVS *kvi, KMR_KVS *kvo, struct kmr_option opt);

extern int kmr_sort(KMR_KVS *kvi, KMR_KVS *kvo, struct kmr_option opt);
extern int kmr_sort_small(KMR_KVS *kvi, KMR_KVS *kvo, struct kmr_option opt);
extern int kmr_sort_large(KMR_KVS *kvi, KMR_KVS *kvo, struct kmr_option opt);
extern int kmr_sort_by_one(KMR_KVS *kvi, KMR_KVS *kvo, struct kmr_option opt);

extern int kmr_match(KMR_KVS *kvi0, KMR_KVS *kvi1, KMR_KVS *kvo,
		     struct kmr_option opt);
extern int kmr_ranking(KMR_KVS *kvi, KMR_KVS *kvo, long *count,
		       struct kmr_option opt);
extern int kmr_distribute(KMR_KVS *kvi, KMR_KVS *kvo, _Bool cyclic,
			  struct kmr_option opt);
extern int kmr_choose_first_part(KMR_KVS *kvi, KMR_KVS *kvo,
				 long n, struct kmr_option opt);
extern int kmr_histogram_count_by_ranks(KMR_KVS *kvs, long *frq, double *var,
					_Bool rankzeroonly);

extern int kmr_read_files_reassemble(KMR *mr, char *file, int color,
				     off_t offset, off_t bytes,
				     void **buffer, off_t *readsize);
extern int kmr_read_file_by_segments(KMR *mr, char *file, int color,
				     void **buffer, off_t *readsize);

extern int kmr_retrieve_kvs_entries(KMR_KVS *kvs, struct kmr_kvs_entry **ev,
				    long n);
extern int kmr_retrieve_keyed_records(KMR_KVS *kvs,
				      struct kmr_keyed_record *ev,
				      long n, _Bool shuffling, _Bool ranking);
extern void kmr_dump_slot(union kmr_unit_sized e, int len,
			  enum kmr_kv_field data, char *buf, int buflen);
extern int kmr_dump_kv(struct kmr_kv_box kv, const KMR_KVS *kvs,
		       char *buf, int buflen);
extern int kmr_dump_kvs(KMR_KVS *kvs, int flag);
extern int kmr_dump_kvs_stats(KMR_KVS *, int level);
extern void kmr_dump_opaque(const char *p, int siz, char *buf, int buflen);
extern int kmr_dump_keyed_records(const struct kmr_keyed_record *ev,
				  KMR_KVS *kvi);

extern void kmr_reset_ntuple(struct kmr_ntuple *u, int n, int marker);
extern int kmr_put_ntuple(KMR *mr, struct kmr_ntuple *u, const int sz,
			  const void *v, const int vlen);
extern int kmr_put_ntuple_long(KMR *mr, struct kmr_ntuple *u, const int sz,
			       long v);
extern int kmr_put_ntuple_entry(KMR *mr, struct kmr_ntuple *u, const int sz,
				struct kmr_ntuple_entry e);
extern struct kmr_ntuple_entry kmr_nth_ntuple(struct kmr_ntuple *u, int nth);
extern int kmr_size_ntuple(struct kmr_ntuple *u);
extern int kmr_size_ntuple_by_lengths(int n, int len[]);
extern int kmr_add_ntuple(KMR_KVS *kvo, void *k, int klen,
			  struct kmr_ntuple *u);

extern int kmr_separate_ntuples(KMR *mr,
				const struct kmr_kv_box kv[], const long n,
				struct kmr_ntuple **vv[2], long cnt[2],
				int markers[2], _Bool disallow_other_entries);
extern int kmr_product_ntuples(KMR_KVS *kvo,
			       struct kmr_ntuple **vv[2], long cnt[2],
			       int newmarker,
			       int slots[][2], int nslots,
			       int keys[][2], int nkeys);

extern KMR_KVS *kmr_create_pushoff_kvs(KMR *mr, enum kmr_kv_field kf,
				       enum kmr_kv_field vf,
				       struct kmr_option opt,
				       const char*, const int, const char*);
extern void kmr_print_statistics_on_pushoff(KMR *mr, char *titlestring);
extern void kmr_init_pushoff_fast_notice_(MPI_Comm, _Bool verbose);
extern void kmr_fin_pushoff_fast_notice_(void);
extern void kmr_check_pushoff_fast_notice_(KMR *mr);

extern int kmr_assign_file(KMR_KVS *kvi, KMR_KVS *kvo, struct kmr_option opt);

/* Suppresses warnings of unused constants (for icc). */

static inline void
kmr_dummy_dummy_dummy_(void)
{
     (void)kmr_kvs_entry_header;
     (void)kmr_kvs_block_header;
     (void)kmr_noopt;
     (void)kmr_optmask;
     (void)kmr_fnoopt;
     (void)kmr_foptmask;
     (void)kmr_snoopt;
     (void)kmr_soptmask;
}

#ifdef __cplusplus
KMR_BR1
#endif

#undef KMR_BR0
#undef KMR_BR1

/*
Copyright (C) 2012-2015 RIKEN AICS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/

#endif /*_KMR_H*/
