/* kmrimpl.h (2014-02-04) */
/* Copyright (C) 2012-2015 RIKEN AICS */

#ifndef _KMRIMPL_H
#define	_KMRIMPL_H

/** \file kmrimpl.h Utilities Private Part (do not include from
    applications). */

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#ifdef __cplusplus
#error "Do not include kmrimpl.h in C++"
#endif

/* Suppress warnings/remarks of ICC. */

#ifdef __INTEL_COMPILER
/* "remark #869: parameter "xxx" was never referenced" */
#pragma warning (disable : 869)
#endif

/* Controls a code for debugging.  Define it to generate an
   expression for including debugging code. \hideinitializer */

#ifdef DEBUG
#define KMR_DEBUGX(X) (X)
#else
#define KMR_DEBUGX(X) ((void)0)
#endif

#define KMR_EMPTY

/* Adds threading directives inside mapping and reduction.  Be
   carefull that semicolons are disallowed after these directives. */

#ifdef _OPENMP
#define KMR_OMP_PARALLEL_ _Pragma("omp parallel")
#define KMR_PP_(X) _Pragma(#X)
#define KMR_OMP_PARALLEL_IF_(CC) KMR_PP_(omp parallel if (CC))
#define KMR_OMP_SINGLE_NOWAIT_ _Pragma("omp single nowait")
#define KMR_OMP_PARALLEL_FOR_ _Pragma("omp parallel for")
#define KMR_OMP_CRITICAL_ _Pragma("omp critical")
#define KMR_OMP_TASK_ _Pragma("omp task")
#define KMR_OMP_TASK_FINAL_(CC) KMR_PP_(omp task final (CC))
#define KMR_OMP_TASKWAIT_ _Pragma("omp taskwait")
#define KMR_OMP_FOR_ _Pragma("omp for")
#define KMR_OMP_GET_THREAD_NUM() omp_get_thread_num()
//#define KMR_OMP_SET_LOCK(X) omp_set_lock(X)
//#define KMR_OMP_UNSET_LOCK(X) omp_unset_lock(X)
//#define KMR_OMP_INIT_LOCK(X) omp_init_lock(X)
//#define KMR_OMP_DESTROY_LOCK(X) omp_destroy_lock(X)
#else
#define KMR_OMP_PARALLEL_ KMR_EMPTY
#define KMR_OMP_PARALLEL_IF_(CC) KMR_EMPTY
#define KMR_OMP_SINGLE_NOWAIT_ KMR_EMPTY
#define KMR_OMP_PARALLEL_FOR_ KMR_EMPTY
#define KMR_OMP_CRITICAL_ KMR_EMPTY
#define KMR_OMP_TASK_ KMR_EMPTY
#define KMR_OMP_TASK_FINAL_(CC) KMR_EMPTY
#define KMR_OMP_TASKWAIT_ KMR_EMPTY
#define KMR_OMP_FOR_ KMR_EMPTY
#define KMR_OMP_GET_THREAD_NUM() 0
//#define KMR_OMP_SET_LOCK(X) ((void)0)
//#define KMR_OMP_UNSET_LOCK(X) ((void)0)
//#define KMR_OMP_INIT_LOCK(X) ((void)0)
//#define KMR_OMP_DESTROY_LOCK(X) ((void)0)
#endif /*_OPENMP*/

/** Rounds up a given size to the alignment restriction (currently
    eight bytes). */

#define KMR_ALIGN(X) (((X)+((8)-1))&~((8)-1))

/* Maximun number of threads on a node. */

#define KMR_MAX_THREADS 200

/* MPI tags used in "kmratoa.c". */

#define KMR_TAG_ATOA 100

/* MPI tags for kmr_map_rank_by_rank(). */

#define KMR_TAG_MAP_BY_RANK 200

#define KMR_TAG_PUSHOFF 201

/* MPI tags for kmr_map_ms().  KMR_TAG_PEER(THRD) chooses a tag
   exclusively used by a thread. */

#define KMR_TAG_REQ 399
#define KMR_TAG_PEER_0 400
#define KMR_TAG_PEER_END (KMR_TAG_PEER_0 + KMR_MAX_THREADS)
#define KMR_TAG_PEER(THRD) (KMR_TAG_PEER_0 + (THRD))

/** Prefix to Trace Files. */

#define KMR_TRACE_FILE_PREFIX "kmrlog"

/* Asserts and aborts, but it cannot be disabled.  The two message
   styles are for Linux and Solaris. \hideinitializer */

#ifndef __SVR4
#define xassert(X) \
    ((X) \
     ? (void)(0) \
     : (fprintf(stderr, \
		"%s:%d: %s: Assertion '%s' failed.\n", \
		__FILE__, __LINE__, __func__, #X), \
	(void)MPI_Abort(MPI_COMM_WORLD, 1)))
#else
#define xassert(X) \
    ((X) \
     ? (void)(0) \
     : (fprintf(stderr, \
		"Assertion failed: %s, file %s, line %d, function %s\n", \
		#X, __FILE__, __LINE__, __func__), \
	(void)MPI_Abort(MPI_COMM_WORLD, 1)))
#endif

/** Positions of node by (X,Y,Z,ABC), with ABC axes collapsed. */

typedef unsigned short kmr_k_position_t[4];

extern double kmr_wtime(void);

/** Returns the clock counter value.  (See for well-written
    information on tick counters: http://www.fftw.org/cycle.h).
    "__thread" is only needed for an old compiler on K. */

static inline long
kmr_tick()
{
#if (defined(__sparc_v9__) && defined(__arch64__))
    /* SPARC V9 */
    static __thread long r;
    __asm__ __volatile__ ("rd %%tick, %0" : "=r" (r) : "0" (r));
    return r;
#elif (defined(__x86_64__) && defined(__GNUC__) && !defined(__llvm__))
    /* x86_64 (rdtsc/rdtscp) */
    unsigned int ax, dx;
    __asm__ __volatile__("rdtscp" : "=a" (ax), "=d" (dx));
    return (((long)ax) | (((long)dx) << 32));
#else
#define KMR_TICK_ONLY_FOR_AMD64_AND_SPARC 0
    /*assert(KMR_TICK_ONLY_FOR_AMD64_AND_SPARC);*/
    return 0;
#endif
}

/* Type of qsort comparator. */

typedef int (*kmr_sorter_t)(const struct kmr_kv_box *p,
			    const struct kmr_kv_box *q);
typedef int (*kmr_record_sorter_t)(const struct kmr_keyed_record *p,
				   const struct kmr_keyed_record *q);
typedef int (*kmr_qsorter_t)(const void *p, const void *q);

extern void kmr_warning(KMR *mr, unsigned int mask, char *m);
extern void kmr_error_at_site(KMR *mr, char *m, struct kmr_code_line *site);
extern void kmr_error(KMR *mr, char *m);
extern void kmr_error2(KMR *mr, char *m,
		       const char *file, const int line, const char *func);
extern void kmr_error_kvs_at_site(KMR *mr, char *m, KMR_KVS *kvs,
				  struct kmr_code_line *site);
extern void kmr_error_kvs(KMR *mr, char *m, KMR_KVS *kvs);
extern void kmr_error_kvs2(KMR *mr, char *m, KMR_KVS *kvs,
			   const char *file, const int line, const char *func);
extern void kmr_error_mpi(KMR *mr, char *m, int errorcode);
extern void kmr_string_truncation(KMR *mr, size_t sz, char *s);

/** Allocates memory, or aborts when failed. */

#define kmr_malloc(Z) kmr_safe_malloc((Z), #Z, __FILE__, __LINE__, __func__)

/** Allocates memory, or aborts when failed. */

#define kmr_realloc(P,Z) kmr_safe_realloc((P), (Z), #Z, __FILE__, __LINE__, __func__)

static inline void *
kmr_safe_malloc(size_t s, char *szexpr,
		const char *file, const int line, const char *func)
{
    void *p = malloc(s);
    if (p == 0) {
	char ee[80];
	char *m = strerror(errno);
	snprintf(ee, 80, "malloc((%s)=%zd): %s", szexpr, s, m);
	kmr_error2(0, ee, file, line, func);
    }
    return p;
}

static inline void *
kmr_safe_realloc(void *q, size_t s, char *szexpr,
		 const char *file, const int line, const char *func)
{
    void *p = realloc(q, s);
    if (p == 0) {
	char ee[80];
	char *m = strerror(errno);
	snprintf(ee, 80, "realloc((%s)=%zd): %s", szexpr, s, m);
	kmr_error2(0, ee, file, line, func);
    }
    return p;
}

static inline void
kmr_free(void *p, size_t sz)
{
    KMR_DEBUGX(memset(p, 0, sz));
    free(p);
}

/* Checks if SZ is a multiple of the alignment restriction (currently
   eight bytes). */

static inline _Bool
kmr_check_alignment(size_t sz)
{
    return ((sz & ((8)-1)) == 0);
}

/* Checks if a field (key or value) is of unit-sized. */

static inline _Bool
kmr_unit_sized_p(enum kmr_kv_field data)
{
    switch (data) {
    case KMR_KV_BAD:
	assert(data != KMR_KV_BAD);
	return 0;
    case KMR_KV_OPAQUE:
    case KMR_KV_CSTRING:
	return 0;
    case KMR_KV_INTEGER:
    case KMR_KV_FLOAT8:
    case KMR_KV_POINTER_OWNED:
    case KMR_KV_POINTER_UNMANAGED:
	return 1;
    default:
	xassert(0);
	return 0;
    }
}

/* Returns the size of the entry occupying in the storage.  It handles
   the case storing pointers.  Use kmr_kvs_entry_netsize() for the
   size of the entry stored in-line.  See
   kmr_kvs_entry_size_of_box(). */

static inline size_t
kmr_kvs_entry_size(KMR_KVS *kvs, const struct kmr_kvs_entry *e)
{
    enum kmr_kv_field keyf = kvs->c.key_data;
    enum kmr_kv_field valf = kvs->c.value_data;
    size_t ksz;
    if (kmr_unit_sized_p(keyf)) {
	ksz = sizeof(union kmr_unit_sized);
    } else {
	ksz = (size_t)KMR_ALIGN(e->klen);
    }
    size_t vsz;
    if (kmr_unit_sized_p(valf)) {
	vsz = sizeof(union kmr_unit_sized);
    } else {
	vsz = (size_t)KMR_ALIGN(e->vlen);
    }
    return (kmr_kvs_entry_header + ksz + vsz);
}

static inline size_t
kmr_kvs_entry_size_of_box(KMR_KVS *kvs, const struct kmr_kv_box kv)
{
    enum kmr_kv_field keyf = kvs->c.key_data;
    enum kmr_kv_field valf = kvs->c.value_data;
    size_t ksz;
    if (kmr_unit_sized_p(keyf)) {
	ksz = sizeof(union kmr_unit_sized);
    } else {
	ksz = (size_t)KMR_ALIGN(kv.klen);
    }
    size_t vsz;
    if (kmr_unit_sized_p(valf)) {
	vsz = sizeof(union kmr_unit_sized);
    } else {
	vsz = (size_t)KMR_ALIGN(kv.vlen);
    }
    return (kmr_kvs_entry_header + ksz + vsz);
}

/* Returns the size of the entry.  It is the size for opaque fields,
   but not for pointer fields.  Use kmr_kvs_entry_size() for the
   occupying size of the pointer fields.  See
   kmr_kvs_entry_netsize_of_box(). */

static inline size_t
kmr_kvs_entry_netsize(const struct kmr_kvs_entry *e)
{
    return (kmr_kvs_entry_header
	    + (size_t)KMR_ALIGN(e->klen) + (size_t)KMR_ALIGN(e->vlen));
}

static inline size_t
kmr_kvs_entry_netsize_of_box(const struct kmr_kv_box kv)
{
    return (kmr_kvs_entry_header
	    + (size_t)KMR_ALIGN(kv.klen) + (size_t)KMR_ALIGN(kv.vlen));
}

/* Moves a pointer to the next entry, not moving over to the next
   block. */

static inline struct kmr_kvs_entry *
kmr_kvs_next_entry(KMR_KVS *kvs, const struct kmr_kvs_entry *e)
{
    enum kmr_kv_field keyf = kvs->c.key_data;
    enum kmr_kv_field valf = kvs->c.value_data;
    size_t ksz;
    if (kmr_unit_sized_p(keyf)) {
	ksz = sizeof(union kmr_unit_sized);
    } else {
	ksz = (size_t)KMR_ALIGN(e->klen);
    }
    size_t vsz;
    if (kmr_unit_sized_p(valf)) {
	vsz = sizeof(union kmr_unit_sized);
    } else {
	vsz = (size_t)KMR_ALIGN(e->vlen);
    }
    return (void *)&(e->c[(ksz + vsz)]);
}

/* Puts an end-of-block marker.  A marker is placed in a data block
   when it becomes full or closed (stowed) by kmr_add_kv_done(). */

static inline void
kmr_kvs_mark_entry_tail(struct kmr_kvs_entry *e)
{
    assert((((intptr_t)e) & 3) == 0);
    e->klen = -1, e->vlen = -1;
}

/* Checks if the entry is an end-of-block. */

static inline _Bool
kmr_kvs_entry_tail_p(struct kmr_kvs_entry *e)
{
    assert(e != 0);
    return (e->klen == -1 && e->vlen == -1);
}

/* Returns the first block. */

static inline struct kmr_kvs_block *
kmr_kvs_first_block(KMR_KVS *kvs)
{
    xassert(kvs->c.current_block == 0);
    struct kmr_kvs_block *b = kvs->c.first_block;
    kvs->c.current_block = b;
    return b;
}

/* Returns the first entry in the block.  It returns null when the
   given block is null (for the empty case).  Note normally the
   CURRENT_BLOCK be the same as the given block B in case enumerating
   all the key-value pairs. */

static inline struct kmr_kvs_entry *
kmr_kvs_first_entry(KMR_KVS *kvs, const struct kmr_kvs_block *b)
{
    if (b == 0) {
	return 0;
    } else {
	return (void *)&(b->data[0]);
    }
}

/* Returns the upper bound of the last entry in a block.  The last
   entry never exceeds this point.  It keeps one space for an
   end-of-block marker. */

static inline struct kmr_kvs_entry *
kmr_kvs_last_entry_limit(const KMR_KVS *kvs, const struct kmr_kvs_block *b)
{
    assert(b != 0);
    size_t sz = (b->size - kmr_kvs_entry_header);
    return (void *)((char *)b + sz);
}

/* Returns a current insertion point in a block. */

static inline struct kmr_kvs_entry *
kmr_kvs_adding_point(struct kmr_kvs_block *b)
{
    struct kmr_kvs_entry *e = (void *)((char *)&b->data[0] + b->fill_size);
    return e;
}

/* Checks if a new key-value entry of a size SZ fits in a block B.  It
   keeps the space for an end-of-block marker.  SZ is the space for an
   entry (including length fields). */

static inline _Bool
kmr_kvs_entry_fits_in_block(KMR_KVS *kvs, struct kmr_kvs_block *b, size_t sz)
{
    struct kmr_kvs_entry *e = kmr_kvs_adding_point(b);
    struct kmr_kvs_entry *p = (void *)((char *)e + sz);
    struct kmr_kvs_entry *limit = kmr_kvs_last_entry_limit(kvs, b);
    assert(&b->data[0] <= e && e <= limit);
    return (p <= limit);
}

/* Returns an entry at a given BYTES offset in the block (not count
   header bytes). */

static inline struct kmr_kvs_entry *
kmr_kvs_entry_at(KMR_KVS *kvs, const struct kmr_kvs_block *b, size_t bytes)
{
    assert(b != 0);
    return (void *)((char *)&(b->data[0]) + bytes);
}

/* Moves a pointer to the next entry.  It returns 0 at the end of the
   entries.  When BOUND_IN_BLOCK is true, it does not go to the next
   block, otherwise it does. */

static inline struct kmr_kvs_entry *
kmr_kvs_next(KMR_KVS *kvs, const struct kmr_kvs_entry *e, _Bool bound_in_block)
{
    assert(kvs != 0 && e != 0 && kvs->c.current_block != 0);
    struct kmr_kvs_block *b = kvs->c.current_block;
    struct kmr_kvs_entry *p = kmr_kvs_next_entry(kvs, e);
    assert(p <= kmr_kvs_last_entry_limit(kvs, b));
    if (!kmr_kvs_entry_tail_p(p)) {
	return p;
    } else {
	if (bound_in_block) {
	    return 0;
	} else {
	    //struct kmr_kvs_block *previousblock = b;
	    b = b->next;
	    if (b != 0) {
		kvs->c.current_block = b;
		struct kmr_kvs_entry *q = kmr_kvs_first_entry(kvs, b);
		//kvs->c.adding_point = q;
		return q;
	    } else {
		//kvs->c.current_block = 0;
		//kvs->c.adding_point = p;
		return 0;
	    }
	}
    }
}

static inline void
kmr_kvs_reset_block(KMR_KVS *kvs, struct kmr_kvs_block *b,
		    size_t size, size_t netsize)
{
    KMR_DEBUGX(memset(b, 0, size));
    b->next = 0;
    b->size = size;
    b->partial_element_count = 0;
    b->fill_size = 0;
    struct kmr_kvs_entry *e = kmr_kvs_entry_at(kvs, b, netsize);
    kmr_kvs_mark_entry_tail(e);
}

/* Inserts a new storage block into a KVS. */

static inline void
kmr_kvs_insert_block(KMR_KVS *kvs, struct kmr_kvs_block *b)
{
    assert(kvs->c.magic == KMR_KVS_ONCORE);
    if (kvs->c.first_block == 0) {
	assert(kvs->c.current_block == 0 && kvs->c.block_count == 0);
	kvs->c.first_block = b;
    } else {
	assert(kvs->c.current_block != 0 && kvs->c.block_count != 0);
	kvs->c.current_block->next = b;
    }
    kvs->c.block_count++;
    kvs->c.current_block = b;
    kvs->c.adding_point = kmr_kvs_first_entry(kvs, b);
    assert(kvs->c.current_block != 0 && kvs->c.adding_point != 0);
}

/* Returns an occupying size of a key field. */

static inline size_t
kmr_kv_key_size(const KMR_KVS *kvs, const struct kmr_kv_box kv)
{
    return (size_t)KMR_ALIGN(kv.klen);
}

/* Returns an occupying size of a value field. */

static inline size_t
kmr_kv_value_size(const KMR_KVS *kvs, const struct kmr_kv_box kv)
{
    return (size_t)KMR_ALIGN(kv.vlen);
}

/* Returns a pointer pointing to a key in the entry. */

static inline union kmr_unit_sized *
kmr_point_key(const struct kmr_kvs_entry *e)
{
    return (void *)&(e->c[0]);
}

/* Returns a pointer pointing to a value in the entry. */

static inline union kmr_unit_sized *
kmr_point_value(const struct kmr_kvs_entry *e)
{
    return (void *)&(e->c[KMR_ALIGN(e->klen)]);
}

static inline struct kmr_kv_box
kmr_pick_kv2(struct kmr_kvs_entry *e,
	     enum kmr_kv_field keyf, enum kmr_kv_field valf)
{
    struct kmr_kv_box kv;
    kv.klen = e->klen;
    kv.vlen = e->vlen;
    union kmr_unit_sized *kp = kmr_point_key(e);
    union kmr_unit_sized *vp = kmr_point_value(e);
    if (kmr_unit_sized_p(keyf)) {
	kv.k = *kp;
    } else {
	kv.k.p = (void *)kp;
    }
    if (kmr_unit_sized_p(valf)) {
	kv.v = *vp;
    } else {
	kv.v.p = (void *)vp;
    }
    return kv;
}

/** Returns a handle to a key-value entry -- a reverse of
    kmr_poke_kv().  An entry does not know its data-types but they are
    taken from a key-value stream KVS. */

static inline struct kmr_kv_box
kmr_pick_kv(struct kmr_kvs_entry *e, KMR_KVS *kvs)
{
    struct kmr_kv_box kv = kmr_pick_kv2(e, kvs->c.key_data,
					kvs->c.value_data);
    return kv;
}

static inline void
kmr_poke_kv2(struct kmr_kvs_entry *e, const struct kmr_kv_box kv,
	     struct kmr_kv_box *xkv,
	     enum kmr_kv_field keyf, enum kmr_kv_field valf,
	     _Bool reserve_space_only)
{
    e->klen = kv.klen;
    e->vlen = kv.vlen;
    union kmr_unit_sized *kp = kmr_point_key(e);
    union kmr_unit_sized *vp = kmr_point_value(e);
    if (kmr_unit_sized_p(keyf)) {
	*kp = kv.k;
    } else {
	if (!reserve_space_only) {
	    memcpy(kp, kv.k.p, (size_t)kv.klen);
	}
	if (xkv != 0) {
	    xkv->k.p = (const char *)kp;
	}
    }
    if (kmr_unit_sized_p(valf)) {
	*vp = kv.v;
    } else {
	if (!reserve_space_only) {
	    memcpy(vp, kv.v.p, (size_t)kv.vlen);
	}
	if (xkv != 0) {
	    xkv->v.p = (const char *)vp;
	}
    }
    return;
}

/** Stores a key-value pair at the entry E in the store -- a reverse
    of kmr_pick_kv().  A key-value pair does not know its data-types
    but they are taken from a key-value stream.  It modifies
    kmr_kv_box XKV (when non-null) to return the pointer to the opaque
    field when a key or a value is opaque.  It does not move actual
    data when RESERVE_SPACE_ONLY=1. */

static inline void
kmr_poke_kv(struct kmr_kvs_entry *e, const struct kmr_kv_box kv,
	    struct kmr_kv_box *xkv, const KMR_KVS *kvs,
	    _Bool reserve_space_only)
{
    kmr_poke_kv2(e, kv, xkv, kvs->c.key_data, kvs->c.value_data,
		 reserve_space_only);
}

/* Checks if an input key-value stream is a proper one.  See
   kmr_assert_kvs_ok2(). */

static inline void
kmr_assert_i_kvs_ok_at_site(KMR_KVS *kvi, _Bool irequired,
			    struct kmr_code_line *site)
{
    if (irequired && kvi == 0) {
	kmr_error_at_site(0, "Null input kvs", site);
    } else if (kvi != 0 && !KMR_KVS_MAGIC_OK(kvi->c.magic)) {
	kmr_error_at_site(0, "Bad input kvs (freed or corrupted)", site);
    } else if (kvi != 0 && !kvi->c.stowed) {
	KMR *mr = kvi->c.mr;
	kmr_error_kvs_at_site(mr, "kmr_add_kv_done not called for input kvs",
			      kvi, site);
    }
}

static inline void
kmr_assert_i_kvs_ok(KMR_KVS *kvi, _Bool irequired)
{
    kmr_assert_i_kvs_ok_at_site(kvi, irequired, 0);
}

/* Checks if an output key-value stream is a proper one.  See
   kmr_assert_kvs_ok2(). */

static inline void
kmr_assert_o_kvs_ok_at_site(KMR_KVS *kvo, _Bool orequired,
			    struct kmr_code_line *site)
{
    if (orequired && kvo == 0) {
	kmr_error_at_site(0, "Null output kvs", site);
    } else if (kvo != 0 && !KMR_KVS_MAGIC_OK(kvo->c.magic)) {
	kmr_error_at_site(0, "Bad output kvs (freed or corrupted)", site);
    } else if (kvo != 0 && kvo->c.stowed) {
	KMR *mr = kvo->c.mr;
	kmr_error_kvs_at_site(mr,
			      "kmr_add_kv_done already called for output kvs",
			      kvo, site);
    }
}

static inline void
kmr_assert_o_kvs_ok(KMR_KVS *kvo, _Bool orequired)
{
    kmr_assert_o_kvs_ok_at_site(kvo, orequired, 0);
}

/* Checks if input/output key-value streams are proper ones.  An input
   stream is required as stowed, and an output stream is not.  It
   accepts null unless IREQUIRED/OREQUIRED is specified. */

static inline void
kmr_assert_kvs_ok(KMR_KVS *kvi, KMR_KVS *kvo,
		  _Bool irequired, _Bool orequired)
{
    kmr_assert_i_kvs_ok_at_site(kvi, irequired, 0);
    kmr_assert_o_kvs_ok_at_site(kvo, orequired, 0);
}

/* Checks if a key field is a pointer.  */

static inline _Bool
kmr_key_pointer_p(KMR_KVS *kvs)
{
    switch (kvs->c.key_data) {
    case KMR_KV_BAD:
	xassert(kvs->c.key_data != KMR_KV_BAD);
	return 0;
    case KMR_KV_OPAQUE:
    case KMR_KV_CSTRING:
    case KMR_KV_INTEGER:
    case KMR_KV_FLOAT8:
	return 0;
    case KMR_KV_POINTER_OWNED:
    case KMR_KV_POINTER_UNMANAGED:
	return 1;
    default:
	xassert(0);
	return 0;
    }
}

/* Checks if a value field is a pointer.  */

static inline _Bool
kmr_value_pointer_p(KMR_KVS *kvs)
{
    switch (kvs->c.value_data) {
    case KMR_KV_BAD:
	xassert(kvs->c.value_data != KMR_KV_BAD);
	return 0;
    case KMR_KV_OPAQUE:
    case KMR_KV_CSTRING:
    case KMR_KV_INTEGER:
    case KMR_KV_FLOAT8:
	return 0;
    case KMR_KV_POINTER_OWNED:
    case KMR_KV_POINTER_UNMANAGED:
	return 1;
    default:
	xassert(0);
	return 0;
    }
}

/* Checks if key or value field is a pointer.  When it is, fields need
   to be packed to make data-exchanges easy. */

static inline _Bool
kmr_fields_pointer_p(KMR_KVS *kvs)
{
    return (kmr_key_pointer_p(kvs) || kmr_value_pointer_p(kvs));
}

/* Checks a constraint on the sizes of a key and a value. */

static inline void
kmr_assert_kv_sizes(KMR_KVS *kvs, const struct kmr_kv_box kv)
{
    KMR *mr = kvs->c.mr;
    switch (kvs->c.key_data) {
    case KMR_KV_BAD:
	assert(kvs->c.key_data != KMR_KV_BAD);
	break;
    case KMR_KV_INTEGER:
	if ((size_t)kv.klen != sizeof(long)) {
	    kmr_error(mr, "Bad kv klen");
	}
	break;
    case KMR_KV_FLOAT8:
	if ((size_t)kv.klen != sizeof(double)) {
	    kmr_error(mr, "Bad kv klen");
	}
	break;
    case KMR_KV_OPAQUE:
    case KMR_KV_CSTRING:
    case KMR_KV_POINTER_OWNED:
    case KMR_KV_POINTER_UNMANAGED:
	break;
    default:
	xassert(0);
	break;
    }
    switch (kvs->c.value_data) {
    case KMR_KV_BAD:
	assert(kvs->c.value_data != KMR_KV_BAD);
	break;
    case KMR_KV_INTEGER:
	if ((size_t)kv.vlen != sizeof(long)) {
	    kmr_error(mr, "Bad kv vlen");
	}
	break;
    case KMR_KV_FLOAT8:
	if ((size_t)kv.vlen != sizeof(double)) {
	    kmr_error(mr, "Bad kv vlen");
	}
	break;
    case KMR_KV_OPAQUE:
    case KMR_KV_CSTRING:
    case KMR_KV_POINTER_OWNED:
    case KMR_KV_POINTER_UNMANAGED:
	break;
    default:
	xassert(0);
	break;
    }
}

/* Regards pointers as opaque ones.  It returns the field data for
   communication, so that pointers should be converted to opaque
   ones. */

static inline enum kmr_kv_field
kmr_unit_sized_or_opaque(enum kmr_kv_field data)
{
    switch (data) {
    case KMR_KV_BAD:
	xassert(data != KMR_KV_BAD);
	return KMR_KV_BAD;
    case KMR_KV_OPAQUE:
    case KMR_KV_CSTRING:
    case KMR_KV_INTEGER:
    case KMR_KV_FLOAT8:
	return data;
    case KMR_KV_POINTER_OWNED:
    case KMR_KV_POINTER_UNMANAGED:
	return KMR_KV_OPAQUE;
    default:
	xassert(0);
	return KMR_KV_BAD;
    }
}

/* Regards opaque data as unmanaged pointers.  It returns temporarily
   a pointer, to avoid copying opaque data . */

static inline enum kmr_kv_field
kmr_unit_sized_with_unmanaged(enum kmr_kv_field data)
{
    switch (data) {
    case KMR_KV_BAD:
	xassert(data != KMR_KV_BAD);
	return KMR_KV_BAD;
    case KMR_KV_INTEGER:
    case KMR_KV_FLOAT8:
	return data;
    case KMR_KV_OPAQUE:
    case KMR_KV_CSTRING:
    case KMR_KV_POINTER_OWNED:
    case KMR_KV_POINTER_UNMANAGED:
	return KMR_KV_POINTER_UNMANAGED;
    default:
	xassert(0);
	return KMR_KV_BAD;
    }
}

static inline _Bool
kmr_shuffle_compatible_data_p(enum kmr_kv_field d0, enum kmr_kv_field d1)
{
    return (kmr_unit_sized_or_opaque(d0) == kmr_unit_sized_or_opaque(d1));
}

/* Checks if fields are compatible for communication.  */

static inline _Bool
kmr_shuffle_compatible_p(KMR_KVS *s0, KMR_KVS *s1)
{
    return (kmr_shuffle_compatible_data_p(s0->c.key_data, s1->c.key_data)
	    && kmr_shuffle_compatible_data_p(s0->c.key_data, s1->c.key_data));
}

/* Links a key-value stream to a list on a context.  */

static inline void
kmr_link_kvs(KMR_KVS *kvs)
{
    KMR *mr = kvs->c.mr;
    assert(mr != 0);
    if (mr->kvses.tail == 0) {
	assert(mr->kvses.head == 0);
	mr->kvses.head = kvs;
	mr->kvses.tail = kvs;
	kvs->c.link.next = 0;
	kvs->c.link.prev = 0;
    } else {
	KMR_KVS *tail = mr->kvses.tail;
	assert(tail->c.link.next == 0);
	tail->c.link.next = kvs;
	mr->kvses.tail = kvs;
	kvs->c.link.next = 0;
	kvs->c.link.prev = tail;
    }
}

/* Copies options of the input and threading part. */

static inline struct kmr_option
kmr_copy_options_i_part(struct kmr_option opt)
{
    struct kmr_option xopt = {.inspect = opt.inspect,
			      .nothreading = opt.nothreading,
			      .take_ckpt = opt.take_ckpt};
    return xopt;
}

/* Copies options of the output and threading part. */

static inline struct kmr_option
kmr_copy_options_o_part(struct kmr_option opt)
{
    struct kmr_option xopt = {.keep_open = opt.keep_open,
			      .nothreading = opt.nothreading,
			      .take_ckpt = opt.take_ckpt};
    return xopt;
}

/* Copies options of the threading part. */

static inline struct kmr_option
kmr_copy_options_m_part(struct kmr_option opt)
{
    struct kmr_option xopt = {.nothreading = opt.nothreading,
			      .take_ckpt = opt.take_ckpt};
    return xopt;
}

/* Copies options for the kmr_shuffle()/kmr_replicate() part. */

static inline struct kmr_option
kmr_copy_s_option(struct kmr_option opt)
{
    struct kmr_option xopt = {.key_as_rank = opt.key_as_rank,
			      .rank_zero = opt.rank_zero,
			      .take_ckpt = opt.take_ckpt};
    return xopt;
}

/* Returns a start of data portion of an n-tuple of size N. */

static inline size_t
kmr_ntuple_data_offset(int n)
{
    size_t sz = sizeof(((struct kmr_ntuple *)0)->len[0]);
    size_t off = (size_t)KMR_ALIGN((int)sz * n);
    return (offsetof(struct kmr_ntuple, len) + off);
}

/* Returns a rounded up size of a length LEN. */

static inline int
kmr_ntuple_entry_size(int len)
{
    return KMR_ALIGN(len);
}

/* Returns an offset to the NTH position in an n-tuple. */

static inline size_t
kmr_ntuple_nth_offset(struct kmr_ntuple *u, int nth)
{
    size_t off;
    off = kmr_ntuple_data_offset(u->n);
    for (int i = 0; i < nth; i++) {
	off += (size_t)kmr_ntuple_entry_size(u->len[i]);
    }
    return off;
}

/* Returns a current insertion point. */

static inline void *
kmr_ntuple_insertion_point(struct kmr_ntuple *u)
{
    assert(u->index < u->n);
    char *p = (void *)u;
    size_t off = kmr_ntuple_nth_offset(u, u->index);
    return (void *)(p + off);
}

#define CHECK_ONE_FN_OPTION(NAME, A, B) \
    if (A < B) { \
        char ee[80]; \
        snprintf(ee, sizeof(ee), \
                 "%s() does not support '" # NAME "' option", func); \
        kmr_error(mr, ee); \
    }

/* Check options given to a kmr function */

static inline void
kmr_check_fn_options(KMR *mr, struct kmr_option provide,
                     struct kmr_option given, const char *func)
{
    CHECK_ONE_FN_OPTION(nothreading, provide.nothreading, given.nothreading);
    CHECK_ONE_FN_OPTION(inspect, provide.inspect, given.inspect);
    CHECK_ONE_FN_OPTION(keep_open, provide.keep_open, given.keep_open);
    CHECK_ONE_FN_OPTION(key_as_rank, provide.key_as_rank, given.key_as_rank);
    CHECK_ONE_FN_OPTION(rank_zero, provide.rank_zero, given.rank_zero);
    CHECK_ONE_FN_OPTION(collapse, provide.collapse, given.collapse);
    CHECK_ONE_FN_OPTION(take_ckpt, provide.take_ckpt, given.take_ckpt);
}

extern int kmr_kv_field_bad;
extern int kmr_kv_field_opaque;
extern int kmr_kv_field_cstring;
extern int kmr_kv_field_integer;
extern int kmr_kv_field_float8;
extern int kmr_kv_field_pointer_owned;
extern int kmr_kv_field_pointer_unmanaged;

extern int kmr_k_node(KMR *mr, kmr_k_position_t p);

extern char *kmr_strptr_ff(char *s);
extern char *kmr_ptrstr_ff(char *s);
extern long kmr_ptrint_ff(void *p);
extern void * kmr_intptr_ff(long p);
extern long kmr_dblint_ff(double v);
extern double kmr_intdbl_ff(long v);
extern long kmr_strint_ff(char *p);
extern int kmr_intstr_ff(long p, char *s, int n);

extern unsigned long kmr_fix_bits_endian_ff(unsigned long b);

extern int kmr_get_nprocs(const KMR *mr);
extern int kmr_get_rank(const KMR *mr);
extern int kmr_get_nprocs_ff(const KMR_KVS *kvs);
extern int kmr_get_rank_ff(const KMR_KVS *kvs);
extern int kmr_get_key_type_ff(const KMR_KVS *kvs);
extern int kmr_get_value_type_ff(const KMR_KVS *kvs);

extern int kmr_init_ff(int kf, struct kmr_option opt,
		       struct kmr_file_option fopt);
extern KMR *kmr_create_context_ff(const int fcomm, const int finfo,
				  const char *name);
extern int kmr_map_via_spawn_ff(KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
				int finfo, struct kmr_spawn_option opt,
				kmr_mapfn_t m);
extern int kmr_get_spawner_communicator_ff(KMR *mr, long i, int *comm);
extern int kmr_map_processes_null_info(_Bool nonmpi, KMR_KVS *kvi,
				       KMR_KVS *kvo, void *arg,
				       struct kmr_spawn_option opt,
				       kmr_mapfn_t mapfn);

extern void kmr_init_kvs_oncore(KMR_KVS *kvs, KMR *mr);
extern int kmr_free_kvs_pushoff(KMR_KVS *kvs, _Bool deallocate);
extern int kmr_add_kv_pushoff(KMR_KVS *kvs, const struct kmr_kv_box kv);
extern int kmr_add_kv_done_pushoff(KMR_KVS *kvs);
extern int kmr_pushoff_make_stationary(KMR_KVS *kvs);

extern int kmr_load_properties(MPI_Info conf, char *filename);
extern int kmr_copy_mpi_info(MPI_Info src, MPI_Info dst);
extern int kmr_dump_mpi_info(char *prefix, MPI_Info info);
extern int kmr_parse_int(char *s, int *r);
extern int kmr_parse_boolean(char *s, int *r);

extern int kmr_scan_argv_strings(KMR *mr, char *s, size_t len, int arglim,
				 int *argc, char **argv,
				 _Bool wssep, char *msg);

extern int kmr_allocate_block(KMR_KVS *kvs, size_t size);

extern void kmr_isort(void *a, size_t n, size_t es, int depth);
extern void *kmr_bsearch(const void *key, const void *base,
			 size_t nel, size_t size,
			 int (*compar)(const void *, const void *));

extern int kmr_reverse_fn(const struct kmr_kv_box kv,
			  const KMR_KVS *kvs, KMR_KVS *kvo, void *p,
			  const long i);
extern int kmr_pairing_fn(const struct kmr_kv_box kv,
			  const KMR_KVS *kvi, KMR_KVS *kvo, void *p,
			  const long i);
extern int kmr_unpairing_fn(const struct kmr_kv_box kv,
			    const KMR_KVS *kvs, KMR_KVS *kvo, void *p,
			    const long i);
extern int kmr_imax_one_fn(const struct kmr_kv_box kv[], const long n,
			   const KMR_KVS *kvi, KMR_KVS *kvo, void *p);
extern int kmr_isum_one_fn(const struct kmr_kv_box kv[], const long n,
			   const KMR_KVS *kvi, KMR_KVS *kvo, void *p);

extern kmr_sorter_t kmr_choose_sorter(const KMR_KVS *kvs);
extern signed long kmr_stable_key(const struct kmr_kv_box kv,
				  const KMR_KVS *kvs);
extern int kmr_pitch_rank(const struct kmr_kv_box kv, KMR_KVS *kvs);
extern int kmr_assert_sorted(KMR_KVS *kvi, _Bool locally,
			     _Bool shuffling, _Bool ranking);
extern struct kmr_kvs_entry *kmr_find_kvs_last_entry(KMR_KVS *kvs);

extern int kmr_exchange_sizes(KMR *mr, long *sbuf, long *rbuf);
extern int kmr_alltoallv(KMR *mr,
			 void *sbuf, long *scounts, long *sdsps,
			 void *rbuf, long *rcounts, long *rdsps);
extern int kmr_gather_sizes(KMR *mr, long siz, long *rbuf);
extern int kmr_allgatherv(KMR *mr, _Bool rankzeroonly,
			  void *sbuf, long scnt,
			  void *rbuf, long *rcnts, long *rdsps);

extern int kmr_iogroup_distance(int a0, int a1);
extern int kmr_iogroup_of_node(KMR *mr);
extern int kmr_iogroup_of_obd(int obdidx);

extern void *kmr_strdup(char *s);
extern FILE *kmr_fopen(const char *n, const char *m);
extern int kmr_fgetc(FILE *f);
extern int kmr_getdtablesize(KMR *mr);

extern int kmr_msleep(int msec, int interval);
extern void kmr_mfree(void *p, size_t sz);

extern int kmr_install_watch_program(KMR *mr, char *msg);

extern int kmr_check_options(KMR *mr, MPI_Info conf);
extern int kmr_load_preference(KMR *mr, MPI_Info info);
extern int kmr_set_option_by_strings(KMR *mr, char *k, char *v);
extern char *kmr_stringify_options(struct kmr_option o);
extern char *kmr_stringify_file_options(struct kmr_file_option o);
extern char *kmr_stringify_spawn_options(struct kmr_spawn_option o);
extern void kmr_print_options(struct kmr_option opt);
extern void kmr_print_file_options(struct kmr_file_option opt);
extern void kmr_print_spawn_options(struct kmr_spawn_option opt);
extern void kmr_print_string(char *msg, char *s, int len);

extern void kmr_open_log(KMR *mr);
extern void kmr_log_map(KMR *mr, KMR_KVS *kvs, struct kmr_kv_box *ev,
			long i, long cnt, kmr_mapfn_t m, double dt);
extern void kmr_log_reduce(KMR *mr, KMR_KVS *kvs, struct kmr_kv_box *ev,
			   long n, kmr_redfn_t r, double dt);

extern void kmr_ckpt_create_context(KMR *);
extern void kmr_ckpt_free_context(KMR *);
extern int kmr_ckpt_enabled(KMR *);
extern int kmr_ckpt_disable_ckpt(KMR *);
extern int kmr_ckpt_enable_ckpt(KMR *, int);
extern void kmr_ckpt_restore_ckpt(KMR_KVS *);
extern void kmr_ckpt_remove_ckpt(KMR_KVS *);
extern void kmr_ckpt_save_kvo_whole(KMR *, KMR_KVS *);
extern void kmr_ckpt_save_kvo_block_init(KMR *, KMR_KVS *);
extern void kmr_ckpt_save_kvo_block_add(KMR *, KMR_KVS *, long);
extern void kmr_ckpt_save_kvo_block_fin(KMR *, KMR_KVS *);
extern void kmr_ckpt_save_kvo_each_init(KMR *, KMR_KVS *);
extern void kmr_ckpt_save_kvo_each_add(KMR *, KMR_KVS *, long);
extern void kmr_ckpt_save_kvo_each_fin(KMR *, KMR_KVS *);
extern void kmr_ckpt_lock_start(KMR *);
extern void kmr_ckpt_lock_finish(KMR *);
extern int kmr_ckpt_progress_init(KMR_KVS *, KMR_KVS *, struct kmr_option);
extern void kmr_ckpt_progress_fin(KMR *);
extern long kmr_ckpt_first_unprocessed_kv(KMR *);

/*
Copyright (C) 2012-2015 RIKEN AICS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/

#endif /*_KMRIMPL_H*/
