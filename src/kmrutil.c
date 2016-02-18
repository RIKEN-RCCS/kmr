/* kmrutil.c (2014-02-04) */
/* Copyright (C) 2012-2016 RIKEN AICS */

/** \file kmrutil.c Utilities. */

/* _GNU_SOURCE is needed for "strnlen()" (it is POSIX Issue 7
   2006). */

#if defined(__linux__)
#define _GNU_SOURCE
#endif

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include "kmr.h"
#include "kmrimpl.h"

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))
#define NEVERHERE 0

/* Copy of constant values to make accessible from dlopen(). */

int kmr_kv_field_bad = KMR_KV_BAD;
int kmr_kv_field_opaque = KMR_KV_OPAQUE;
int kmr_kv_field_cstring = KMR_KV_CSTRING;
int kmr_kv_field_integer = KMR_KV_INTEGER;
int kmr_kv_field_float8 = KMR_KV_FLOAT8;
int kmr_kv_field_pointer_owned = KMR_KV_POINTER_OWNED;
int kmr_kv_field_pointer_unmanaged = KMR_KV_POINTER_UNMANAGED;

/* Issues warning.  MR can be null (then verbosity is 5).  MASK is 1
   to 9; 1 for printed always (unsuppressible), 5 or less for printed
   normally, 9 for printed only at highest verbosity. */

void
kmr_warning(KMR *mr, unsigned int mask, char *m)
{
    assert(1 <= mask && mask <= 9);
    int rank = 0;
    _Bool print = 1;
    if (mr != 0) {
	rank = mr->rank;
	print = (mask <= mr->verbosity);
    } else {
	int cc = MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	assert(cc == MPI_SUCCESS);
	print = (mask <= 5);
    }

    if (print) {
	fprintf(stderr, ";;KMR [%05d] warning: %s.\n", rank, m);
	fflush(0);
    }
}

void
kmr_error_at_site(KMR *mr, char *m, struct kmr_code_line *site)
{
    int rank;
    if (mr != 0) {
	rank = mr->rank;
    } else {
	rank = 0;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	/* Ignore error. */
    }

    if (site != 0) {
	fprintf(stderr, ";;KMR [%05d] error: %s: %s; at %s:%d\n",
		rank, site->func, m, site->file, site->line);
	fflush(0);
    } else {
	fprintf(stderr, ";;KMR [%05d] error: %s.\n", rank, m);
	fflush(0);
    }

    if (mr != 0 && mr->std_abort) {
	abort();
    } else {
	(void)MPI_Abort(MPI_COMM_WORLD, 1);
    }
}

/* Aborts after printing a message.  MR can be null. */

void
kmr_error(KMR *mr, char *m)
{
    kmr_error_at_site(mr, m, 0);
}

void
kmr_error2(KMR *mr, char *m,
	   const char *file, const int line, const char *func)
{
    struct kmr_code_line site = {.file = file, .line = line, .func = func};
    kmr_error_at_site(mr, m, &site);
}

void
kmr_error_kvs_at_site(KMR *mr, char *m, KMR_KVS *kvs,
		      struct kmr_code_line *site)
{
    int rank = 0;
    if (mr != 0) {
	rank = mr->rank;
    } else {
	int cc = MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	assert(cc == MPI_SUCCESS);
    }

    if (site != 0 && kvs->c.info_line0.file != 0) {
	struct kmr_code_line *info = &kvs->c.info_line0;
	fprintf(stderr, ";;KMR [%05d] error: %s: %s"
		" (kvs allocated at %s:%d: %s); at %s:%d\n",
		rank, site->func, m, info->file, info->line, info->func,
		site->file, site->line);
	fflush(0);
    } else if (kvs->c.info_line0.file != 0) {
	struct kmr_code_line *info = &kvs->c.info_line0;
	fprintf(stderr, ";;KMR [%05d] error: %s"
		" (kvs allocated at %s:%d: %s)\n",
		rank, m, info->file, info->line, info->func);
	fflush(0);
    } else if (site != 0)  {
	fprintf(stderr, ";;KMR [%05d] error: %s: %s; at %s:%d\n",
		rank, site->func, m, site->file, site->line);
	fflush(0);
    } else {
	fprintf(stderr, ";;KMR [%05d] error: %s\n",
		rank, m);
	fflush(0);
    }

    if (mr != 0 && mr->std_abort) {
	abort();
    } else {
	(void)MPI_Abort(MPI_COMM_WORLD, 1);
    }
}

void
kmr_error_kvs(KMR *mr, char *m, KMR_KVS *kvs)
{
    kmr_error_kvs_at_site(mr, m, kvs, 0);
}

void
kmr_error_kvs2(KMR *mr, char *m, KMR_KVS *kvs,
	       const char *file, const int line, const char *func)
{
    struct kmr_code_line site = {.file = file, .line = line, .func = func};
    kmr_error_kvs_at_site(mr, m, kvs, &site);
}

void
kmr_error_mpi(KMR *mr, char *m, int errorcode)
{
    int rank = 0;
    if (mr != 0) {
	rank = mr->rank;
    } else {
	int cc = MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	assert(cc == MPI_SUCCESS);
    }

    int len;
    char s[MPI_MAX_ERROR_STRING];
    int cc = MPI_Error_string(errorcode, s, &len);
    if (cc != MPI_SUCCESS) {
	snprintf(s, MPI_MAX_ERROR_STRING, "(unknown MPI error)");
    }
    fprintf(stderr, ";;KMR [%05d] %s: %s\n", rank, m, s);
    fflush(stderr);
}

/** Modifies the string end with by "..." for indicating truncation,
    used on the result of snprintf. */

void
kmr_string_truncation(KMR *mr, size_t sz, char *s)
{
    assert(sz >= 4);
    size_t m = strlen(s);
    if (m == (sz - 1)) {
	s[sz - 4] = '.';
	s[sz - 3] = '.';
	s[sz - 2] = '.';
	assert(s[sz - 1] == 0);
    }
}

/** Returns itself; this is for Fortran-binding. */

char *
kmr_strptr_ff(char *s)
{
    return s;
}

char *
kmr_ptrstr_ff(char *s)
{
    return s;
}

long
kmr_ptrint_ff(void *p)
{
    return (long)p;
}

void *
kmr_intptr_ff(long p)
{
    return (void *)p;
}

long
kmr_dblint_ff(double v)
{
    union {double d; long i;} vv = {.d = v};
    return vv.i;
}

double
kmr_intdbl_ff(long v)
{
    union {double d; long i;} vv = {.i = v};
    return vv.d;
}

long
kmr_strint_ff(char *p)
{
    /*printf("str->int(%lx,s=%s)\n", (long)p, p);*/
    return (long)(void *)p;
}

/** Fills the character array S by the contents at the pointer value
    integer P by the length N.  It returns the string length in C
    limited by N.  */

int
kmr_intstr_ff(long p, char *s, int n)
{
    /*printf("int->str(%lx,s=%s)\n", i, (char *)i);*/
    char *x = (void *)p;
    memcpy(s, x, (size_t)n);
    return (int)strnlen(x, (size_t)n);
}

static inline unsigned long
kmr_bitreverse(unsigned long bits)
{
    bits = (((bits & 0xaaaaaaaaaaaaaaaaUL) >> 1)
	    | ((bits & 0x5555555555555555UL) << 1));
    bits = (((bits & 0xccccccccccccccccUL) >> 2)
	    | ((bits & 0x3333333333333333UL) << 2));
    bits = (((bits & 0xf0f0f0f0f0f0f0f0UL) >> 4)
	    | ((bits & 0x0f0f0f0f0f0f0f0fUL) << 4));
    bits = (((bits & 0xff00ff00ff00ff00UL) >> 8)
	    | ((bits & 0x00ff00ff00ff00ffUL) << 8));
    bits = (((bits & 0xffff0000ffff0000UL) >> 16)
	    | ((bits & 0x0000ffff0000ffffUL) << 16));
    return ((bits >> 32) | (bits << 32));
}

/** Fixes little-endian bits used in Fortran to host-endian. */

unsigned long
kmr_fix_bits_endian_ff(unsigned long b)
{
    static union {struct {_Bool b : 1;} s; unsigned long i;}
    kmr_bitpos0 = {.s={.b=1}};
    assert(kmr_bitpos0.i == 1 || kmr_bitpos0.i == 0x8000000000000000UL);
    unsigned long optionbits;
    if (kmr_bitpos0.i == 1) {
	/* BIT-LE */
	optionbits = b;
    } else {
	/* BIT-BE */
	optionbits = kmr_bitreverse(b);
    }
    unsigned long optionmask = (kmr_optmask.bits
				| kmr_foptmask.bits
				| kmr_soptmask.bits);
    assert((optionbits & ~optionmask) == 0);
    return optionbits;
}

int
kmr_get_nprocs(const KMR *mr)
{
    assert(mr != 0);
    return mr->nprocs;
}

int
kmr_get_rank(const KMR *mr)
{
    assert(mr != 0);
    return mr->rank;
}

int
kmr_get_nprocs_ff(const KMR_KVS *kvs)
{
    assert(kvs != 0);
    return kvs->c.mr->nprocs;
}

int
kmr_get_rank_ff(const KMR_KVS *kvs)
{
    assert(kvs != 0);
    return kvs->c.mr->rank;
}

int
kmr_get_key_type_ff(const KMR_KVS *kvs)
{
    assert(kvs != 0);
    return kvs->c.key_data;
}

int
kmr_get_value_type_ff(const KMR_KVS *kvs)
{
    assert(kvs != 0);
    return kvs->c.value_data;
}

/** Gets the number of key-value pairs locally on each rank. */

int
kmr_local_element_count(KMR_KVS *kvs, long *v)
{
    kmr_assert_kvs_ok(kvs, 0, 1, 0);
    assert(v != 0);
    *v = kvs->c.element_count;
    return MPI_SUCCESS;
}

/** Returns a print string of a single option, to check the bits are
    properly encoded in foreign language interfaces. */

char *
kmr_stringify_options(struct kmr_option o)
{
    if (o.nothreading) {
	return "nothreading";
    } else if (o.inspect) {
	return "inspect";
    } else if (o.keep_open) {
	return "keep_open";
    } else if (o.key_as_rank) {
	return "key_as_rank";
    } else if (o.rank_zero) {
	return "rank_zero";
    } else if (o.collapse) {
	return "collapse";
    } else if (o.take_ckpt) {
	return "take_ckpt";
    } else {
	return 0;
    }
}

/** Returns a print string of a single option, to check the bits are
    properly encoded in foreign language interfaces. */

char *
kmr_stringify_file_options(struct kmr_file_option o)
{
    if (o.each_rank) {
	return "each_rank";
    } else if (o.subdirectories) {
	return "subdirectories";
    } else if (o.list_file) {
	return "list_file";
    } else if (o.shuffle_names) {
	return "shuffle_names";
    } else {
	return 0;
    }
}

/** Returns a print string of a single option, to check the bits are
    properly encoded in foreign language interfaces. */

char *
kmr_stringify_spawn_options(struct kmr_spawn_option o)
{
    if (o.separator_space) {
	return "separator_space";
    } else if (o.reply_each) {
	return "reply_each";
    } else if (o.reply_root) {
	return "reply_root";
    } else if (o.one_by_one) {
	return "one_by_one";
    } else if (o.take_ckpt) {
	return "take_ckpt";
    } else {
	return 0;
    }
}

/* ================================================================ */

static int
kmr_k_node_by_rank(KMR *mr, kmr_k_position_t p)
{
    int rank;
    int cc = MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    assert(cc == MPI_SUCCESS);
    p[0] = 0;
    p[1] = (unsigned short)rank;
    p[2] = 0;
    p[3] = 0;
    return MPI_SUCCESS;
}

/** Gets TOFU position (physical coordinates) of the node.  Errors are
    fatal (aborts). */

int
kmr_k_node(KMR *mr, kmr_k_position_t p)
{
#ifndef __K
    int cc = kmr_k_node_by_rank(mr, p);
    return cc;
#elif 0
    if (!mr->onk) {
	int cc = kmr_k_node_by_rank(mr, p);
	return cc;
    } else {
	/* This code is for GM-1.2.0-12 and later (it is already
	   available, but is not default yet -- 2013-04). */
	int cc;
	int rank;
	int x, y, z, a, b, c;
	cc = MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	assert(cc == MPI_SUCCESS);
	cc = FJMPI_Topology_sys_rank2xyzabc(rank, &x, &y, &z, &a, &b, &c);
	assert(cc == MPI_SUCCESS);
	p[0] = (unsigned short)x;
	p[1] = (unsigned short)y;
	p[2] = (unsigned short)z;
	p[3] = (unsigned short)((a << 4) | (b << 2) | c);
	printf("[%05d] Coord x=%d, y=%d, z=%d, a=%d, b=%d, c=%d\n",
	       rank, x, y, z, a, b, c);
	return MPI_SUCCESS;
    }
#else
    if (!mr->onk) {
	int cc = kmr_k_node_by_rank(mr, p);
	return cc;
    } else {
	char *tofupos = "/proc/tofu/position";
	char buf[128];
	int fd;
	do {
	    fd = open(tofupos, O_RDONLY, 0);
	} while (fd == -1 && errno == EINTR);
	if (fd == -1) {
	    char ee[80];
	    char *m = strerror(errno);
	    snprintf(ee, 80, "open(%s): %s", tofupos, m);
	    kmr_error(0, ee);
	}
	int cc = 0;
	off_t rc = 0;
	while (rc < sizeof(buf)) {
	    ssize_t cx;
	    do {
		cx = read(fd, &buf[rc], (sizeof(buf) - rc));
	    } while (cx == -1 && errno == EINTR);
	    if (cx == 0) {
		break;
	    }
	    if (cx == -1) {
		char ee[80];
		char *m = strerror(errno);
		snprintf(ee, 80, "read(%s): %s", tofupos, m);
		kmr_error(0, ee);
	    }
	    rc += cx;
	}
	do {
	    cc = close(fd);
	} while (cc == -1 && errno == EINTR);
	assert(rc > 18 && rc < sizeof(buf));
	buf[rc] = 0;
	unsigned int x, y, z, a, b, c;
	char nl, gomi;
	cc = sscanf(buf, "TOFU NODE ADDRESS:%d,%d,%d,%d,%d,%d%c%c",
		    &x, &y, &z, &a, &b, &c, &nl, &gomi);
	assert(cc == 7 && nl == '\n');
	assert(a <= 1 && b <= 2 && c <= 1);
	p[0] = x;
	p[1] = y;
	p[2] = z;
	p[3] = ((a << 4) | (b << 2) | c);
	return MPI_SUCCESS;
    }
#endif
}

/* ================================================================ */

/* Returns wall-time in sec.  NEVER USE CLOCK_GETTIME().  It needs
   "rt"-library and avoided.  (It prefers POSIX clock_gettime() to
   MPI_Wtime(), although Open-MPI reads CPU clock counter which may be
   more precise.  On K, the returned resolution is nsec, but it
   actually changes at usec rate). */

double
kmr_wtime()
{
#if 1
    return MPI_Wtime();
#else
#ifdef CLOCK_REALTIME
    static double t0 = 0.0;
    struct timespec ts;
    int cc;
    if (t0 == 0.0) {
	cc = clock_getres(CLOCK_REALTIME, &ts);
	assert(cc == 0);
	double timerres = (double)ts.tv_sec + ((double)ts.tv_nsec * 1e-9);
	//printf("hr-timer resolution %e sec\n", timerres);
	assert(timerres <= 1e-4);
	cc = clock_gettime(CLOCK_REALTIME, &ts);
	assert(cc == 0);
	t0 = (double)ts.tv_sec + ((double)ts.tv_nsec * 1e-9);
	assert(t0 != 0.0);
    }
    cc = clock_gettime(CLOCK_REALTIME, &ts);
    assert(cc == 0);
    double t1 = (double)ts.tv_sec + ((double)ts.tv_nsec * 1e-9);
    return (t1 - t0);
#endif
#endif
}

/** Searches a key entry like bsearch(3C), but returns a next greater
    entry instead of null on no match.  Thus, it may return a pointer
    ((char *)base+(nel*size)) when a key is larger than all. */

void *
kmr_bsearch(const void *key, const void *base, size_t nel, size_t size,
	    int (*compar)(const void *, const void *))
{
    assert(key != 0 && base != 0 && compar != 0);
    const char *lb = base;
    size_t w = nel;
    while (w != 0) {
	const char *p = lb + (w >> 1) * size;
	int r = (*compar)(key, p);
	if (r == 0) {
	    return (void *)p;
	} else if (r > 0) {
	    lb = p + size;
	    w--;
	    w >>= 1;
	} else {
	    w >>= 1;
	}
    }
    return (void *)lb;
}

/* MISCELLANEOUS */

/** STRDUP, but aborts on failure. */

void *
kmr_strdup(char *s)
{
    void *p = strdup(s);
    if (p == 0) {
	char ee[80];
	char *m = strerror(errno);
	snprintf(ee, 80, "strdup(%s): %s", s, m);
	kmr_error(0, ee);
    }
    return p;
}

//extern FILE *kmr_fopen(const char *n, const char *m);
//extern int kmr_fgetc(FILE *f);

/** Does fopen, avoiding EINTR. */

FILE *
kmr_fopen(const char *n, const char *m)
{
    FILE *f = 0;
    do {
	f = fopen(n, m);
    } while (f == 0 && errno == EINTR);
    return f;
}

/** Does fgetc, avoiding EINTR. */

int
kmr_fgetc(FILE *f)
{
    errno = 0;
    int c;
    do {
	c = fgetc(f);
    } while (c == EOF && errno == EINTR);
    return c;
}

/** Does getdtablesize(); it is defined, because it is not Posix. */

int
kmr_getdtablesize(KMR *mr)
{
    int cc;
    struct rlimit r;
    cc = getrlimit(RLIMIT_NOFILE, &r);
    int n;
    if (cc == -1) {
	char ee[80];
	char *m = strerror(errno);
	snprintf(ee, sizeof(ee), "getrlimit(RLIMIT_NOFILE) failed: %s", m);
	kmr_warning(mr, 5, ee);
	n = 20;
    } else {
	n = (int)r.rlim_cur;
    }
    return n;
}

int
kmr_parse_int(char *s, int *r)
{
    int v;
    char gomi[sizeof(int)];
    int cc = sscanf(s, "%d%c", &v, gomi);
    if (cc == 1 && r != 0) {
	*r = v;
    }
    return (cc == 1);
}

int
kmr_parse_boolean(char *s, int *r)
{
    int v = -1;
    int vv;
    char gomi[sizeof(int)];
    if (strcasecmp(s, "true") == 0) {
	v = 1;
    } else if (strcasecmp(s, "false") == 0) {
	v = 0;
    } else if (sscanf(s, "%d%c", &vv, gomi) == 1) {
	if (vv == 0 || vv == 1) {
	    v = vv;
	}
    }
    if (v != -1 && r != 0) {
	*r = v;
    }
    return (v != -1);
}

static int
kmr_parse_size_t(char *s, size_t *r)
{
    long v;
    char gomi[sizeof(int)];
    int cc = sscanf(s, "%ld%c", &v, gomi);
    if (cc == 1 && r != 0) {
	*r = (size_t)v;
    }
    return (cc == 1);
}

/** Checks a key-value stream is sorted.  When not LOCALLY, it
    collects all the entries to rank-zero for checking. */

int
kmr_assert_sorted(KMR_KVS *kvi, _Bool locally, _Bool shuffling, _Bool ranking)
{
    int cc;
    KMR *mr = kvi->c.mr;
    int kcdc = kmr_ckpt_disable_ckpt(mr);
    int rank = mr->rank;
    kmr_sorter_t cmp = kmr_choose_sorter(kvi);
    KMR_KVS *kvs1;
    if (locally) {
	kvs1 = kvi;
    } else {
	kvs1 = kmr_create_kvs(mr, kvi->c.key_data, kvi->c.value_data);
	struct kmr_option rankzero = {.rank_zero = 1};
	cc = kmr_replicate(kvi, kvs1, rankzero);
	assert(cc == MPI_SUCCESS);
    }
    if (locally || rank == 0) {
	long cnt = kvs1->c.element_count;
	size_t evsz = (sizeof(struct kmr_kvs_entry *) * (size_t)cnt);
	struct kmr_kvs_entry **ev = kmr_malloc(evsz);
	cc = kmr_retrieve_kvs_entries(kvs1, ev, cnt);
	assert(cc == MPI_SUCCESS);
	for (long i = 1; i < cnt; i++) {
	    if (!shuffling) {
		struct kmr_kv_box b0 = kmr_pick_kv(ev[i - 1], kvs1);
		struct kmr_kv_box b1 = kmr_pick_kv(ev[i], kvs1);
		assert(cmp(&b0, &b1) <= 0);
	    } else {
		struct kmr_kv_box b0 = kmr_pick_kv(ev[i - 1], kvs1);
		struct kmr_kv_box b1 = kmr_pick_kv(ev[i], kvs1);
		int r0 = (ranking ? (int)b0.k.i : kmr_pitch_rank(b0, kvs1));
		int r1 = (ranking ? (int)b1.k.i : kmr_pitch_rank(b1, kvs1));
		assert(r0 <= r1);
	    }
	}
	kmr_free(ev, evsz);
    }
    if (!locally) {
	assert(kvs1 != kvi);
	cc = kmr_free_kvs(kvs1);
	assert(cc == MPI_SUCCESS);
    }
    kmr_ckpt_enable_ckpt(mr, kcdc);
    return MPI_SUCCESS;
}

/* Scans a given string to find strings separated by nulls or
   whitespaces, and returns the count and the strings.  A string is
   given by S and a length LEN (including null).  The string will be
   modified to change whitespaces to nulls.  MAXARGC is the size of a
   vector ARGV, limiting the maximum number of the arguments to
   (MAXARGC-1), with one spare for a terminating null.  ARGC is set to
   the count and ARGV is filled with the arguments on return.  ARGV
   must have at least the size MAXARGC.  When ARGV is null (and
   MAXARGC is zero), it returns only the count in ARGC (without
   counting a terminating null).  The option WS means the separator
   character is whatespaces instead of nulls.  MSG is a message prefix
   printed on errors. */

int
kmr_scan_argv_strings(KMR *mr, char *s, size_t len, int maxargc,
		      int *argc, char **argv, _Bool ws, char *msg)
{
    assert(s != 0 && len > 0);
    assert(argc != 0 || argv != 0);
    assert((maxargc != 0) == (argv != 0));
    assert(!isblank('\0'));
    if (s[len - 1] != 0) {
	char ee[80];
	snprintf(ee, sizeof(ee), ("%s: argument strings"
				  " not terminated with a null"), msg);
	/*kmr_warning(mr, 5, ee);*/
	kmr_error(mr, ee);
    }
    _Bool counting = (argv == 0);
    char * const lim = &s[len - 1];
    char *p = s;
    int index = 0;
    for (;;) {
	while (p < lim && (ws && isblank(*p))) {
	    p++;
	}
	if (p == lim) {
	    break;
	}
	if (!counting && index < (maxargc - 1)) {
	    argv[index] = p;
	}
	index++;
	while (p < lim && !(*p == 0 || (ws && isblank(*p)))) {
	    p++;
	}
	assert(p <= lim);
	if (!counting && *p != 0) {
	    assert(ws && isblank(*p));
	    *p = 0;
	}
	if (p < lim) {
	    p++;
	}
    }
    assert(p == lim);
    if (!counting && index > (maxargc - 1)) {
	char ee[80];
	snprintf(ee, sizeof(ee),
		 ("%s: argument count exceeds the limit (%d)"), msg, maxargc);
	kmr_error(mr, ee);
    }
    if (!counting && index < maxargc) {
	argv[index] = 0;
    }
    if (argc != 0) {
	*argc = index;
    }
    return MPI_SUCCESS;
}

/* Sleeps for MSEC, but calls MPI_Testany() periodically.  (It avoids
   using MPI_STATUS_IGNORE in MPI_Testany() for a bug in some versions
   of Open MPI (around 1.6.3)). */

int
kmr_msleep(int msec, int interval)
{
    assert(msec >= 1 && interval >= 1);
    int gap = MIN(msec, interval);
    double t0 = MPI_Wtime();
    double t1 = (t0 + 1e-3 * msec);
    for (;;) {
	int index;
	int ok;
	MPI_Status st;
	int cc = MPI_Testany(0, 0, &index, &ok, &st);
	assert(cc == MPI_SUCCESS);
	double t2 = MPI_Wtime();
	if (t2 > t1) {
	    break;
	}
	usleep((useconds_t)(gap * 1000));
    }
    return MPI_SUCCESS;
}

/* Frees memory by free() (3C).  It is for calling free() safely from
   users of the .so library, even if free() is substituted by
   anything. */

void
kmr_mfree(void *p, size_t sz)
{
    kmr_free(p, sz);
}

/* (mpi routines for python-ctypes) Returns a sizeof a MPI type given
   by a string. */

size_t
kmr_mpi_type_size(char *s)
{
    if (strcasecmp(s, "MPI_Group") == 0) {
	return sizeof(MPI_Group);
    } else if (strcasecmp(s, "MPI_Comm") == 0) {
	return sizeof(MPI_Comm);
    } else if (strcasecmp(s, "MPI_Datatype") == 0) {
	return sizeof(MPI_Datatype);
    } else if (strcasecmp(s, "MPI_Request") == 0) {
	return sizeof(MPI_Request);
    } else if (strcasecmp(s, "MPI_Op") == 0) {
	return sizeof(MPI_Op);
    } else if (strcasecmp(s, "MPI_Errhandler") == 0) {
	return sizeof(MPI_Errhandler);
    } else if (strcasecmp(s, "MPI_Info") == 0) {
	return sizeof(MPI_Info);
    } else {
	char ee[80];
	snprintf(ee, sizeof(ee),
		 "kmr_mpi_type_size() unknown name (%s)", s);
	kmr_warning(0, 5, ee);
	return 0;
    }
}

/* (mpi routines for python-ctypes) Returns a value of some MPI named
   constants given by a string. */

uint64_t
kmr_mpi_constant_value(char *s)
{
    assert(sizeof(MPI_Group) <= sizeof(uint64_t)
	   && sizeof(MPI_Comm) <= sizeof(uint64_t)
	   && sizeof(MPI_Datatype) <= sizeof(uint64_t)
	   && sizeof(MPI_Request) <= sizeof(uint64_t)
	   && sizeof(MPI_Op) <= sizeof(uint64_t)
	   && sizeof(MPI_Errhandler) <= sizeof(uint64_t)
	   && sizeof(MPI_Info) <= sizeof(uint64_t));

    if (strcasecmp(s, "MPI_COMM_WORLD") == 0) {
	return (uint64_t)MPI_COMM_WORLD;
    } else if (strcasecmp(s, "MPI_COMM_SELF") == 0) {
	return (uint64_t)MPI_COMM_SELF;
    } else if (strcasecmp(s, "MPI_COMM_NULL") == 0) {
	return (uint64_t)MPI_COMM_NULL;
    } else if (strcasecmp(s, "MPI_GROUP_NULL") == 0) {
	return (uint64_t)MPI_GROUP_NULL;
    } else if (strcasecmp(s, "MPI_DATATYPE_NULL") == 0) {
	return (uint64_t)MPI_DATATYPE_NULL;
    } else if (strcasecmp(s, "MPI_REQUEST_NULL") == 0) {
	return (uint64_t)MPI_REQUEST_NULL;
    } else if (strcasecmp(s, "MPI_OP_NULL") == 0) {
	return (uint64_t)MPI_OP_NULL;
    } else if (strcasecmp(s, "MPI_ERRHANDLER_NULL") == 0) {
	return (uint64_t)MPI_ERRHANDLER_NULL;
    } else if (strcasecmp(s, "MPI_GROUP_EMPTY") == 0) {
	return (uint64_t)MPI_GROUP_EMPTY;
    } else if (strcasecmp(s, "MPI_INFO_NULL") == 0) {
	return (uint64_t)MPI_INFO_NULL;
    } else {
	char ee[80];
	snprintf(ee, sizeof(ee),
		 "kmr_mpi_constant_value() unknown name (%s)", s);
	kmr_warning(0, 5, ee);
	return 0;
    }
}

/* ================================================================ */

/** Copies the entry in the array.  It should be used with the INSPECT
    option for map, because the array entries may point into the input
    key-value stream.  It is a map-function. */

int
kmr_copy_to_array_fn(const struct kmr_kv_box kv,
		     const KMR_KVS *kvi, KMR_KVS *kvo, void *arg, const long i)
{
    struct kmr_kv_box *v = arg;
    v[i] = kv;
    return MPI_SUCCESS;
}

/* Reduces the argument integers to the maximum, only for a single
   reduction (the all keys are the same). */

int
kmr_imax_one_fn(const struct kmr_kv_box kv[], const long n,
		const KMR_KVS *kvi, KMR_KVS *kvo, void *p)
{
    assert(n > 0);
    long *zz = p;
    long m = 0;
    for (long i = 0; i < n; i++) {
	long v = kv[i].v.i;
	m = MAX(v, m);
    }
    *zz = m;
    return MPI_SUCCESS;
}

int
kmr_isum_one_fn(const struct kmr_kv_box kv[], const long n,
		const KMR_KVS *kvi, KMR_KVS *kvo, void *p)
{
    assert(n > 0);
    long *zz = p;
    long m = 0;
    for (long i = 0; i < n; i++) {
	long v = kv[i].v.i;
	m = v + m;
    }
    *zz = m;
    return MPI_SUCCESS;
}

/* ================================================================ */

/* PREFERENCE/OPTIONS */

/** Copies mpi-info entires into kvs. */

int
kmr_copy_info_to_kvs(MPI_Info src, KMR_KVS *kvo)
{
    kmr_assert_kvs_ok(0, kvo, 0, 1);
    assert(src != MPI_INFO_NULL);
    int cc;
    int nkeys;
    char key[MPI_MAX_INFO_KEY + 1];
    char value[MPI_MAX_INFO_VAL + 1];
    cc = MPI_Info_get_nkeys(src, &nkeys);
    assert(cc == MPI_SUCCESS);
    for (int i = 0; i < nkeys; i++) {
	int vlen;
	int flag;
	cc = MPI_Info_get_nthkey(src, i, key);
	assert(cc == MPI_SUCCESS);
	cc = MPI_Info_get_valuelen(src, key, &vlen, &flag);
	assert(cc == MPI_SUCCESS && flag != 0);
	assert(vlen <= MPI_MAX_INFO_VAL);
	cc = MPI_Info_get(src, key, MPI_MAX_INFO_VAL, value, &flag);
	assert(cc == MPI_SUCCESS && flag != 0);
	cc = kmr_add_string(kvo, key, value);
	assert(cc == MPI_SUCCESS);
    }
    cc = kmr_add_kv_done(kvo);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

static int
kmr_set_info_fn(const struct kmr_kv_box kv,
		const KMR_KVS *kvi, KMR_KVS *kvo, void *p, const long i)
{
    MPI_Info *dstp = p;
    MPI_Info dst = *dstp;
    char *k = (char *)kv.k.p;
    char *v = (char *)kv.v.p;
    if (k[0] == 0) {
	kmr_warning(0, 5, "empty key string for MPI_Info_set(), ignored");
    } else if (v[0] == 0) {
	/* OPEN MPI (1.6.4) DOES NOT ALLOW EMPTY VALUE. */
	kmr_warning(0, 5, "empty value string for MPI_Info_set(), ignored");
    } else {
	int cc = MPI_Info_set(dst, (char *)kv.k.p, (char *)kv.v.p);
	assert(cc == MPI_SUCCESS);
    }
    return MPI_SUCCESS;
}

/** Copies kvs entires into mpi-info.  It assumes keys/values are
    strings (no checks).  It consumes KVI. */

int
kmr_copy_kvs_to_info(KMR_KVS *kvi, MPI_Info dst)
{
    kmr_assert_kvs_ok(kvi, 0, 1, 0);
    int cc;
    struct kmr_option nothreading = {.nothreading = 1};
    cc = kmr_map(kvi, 0, &dst, nothreading, kmr_set_info_fn);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/* Loads configuration options from preferences into INFO.
   Preferences are taken from a file with a name specified by an
   environment variable "KMROPTION" on rank0. */

int
kmr_load_preference(KMR *mr, MPI_Info info)
{
    int cc;
    MPI_Info inforank0;
    cc = MPI_Info_create(&inforank0);
    assert(cc == MPI_SUCCESS);
    do {
	if (mr->rank == 0) {
	    char *name = getenv("KMROPTION");
	    if (name == 0) {
		break;
	    }
	    cc = kmr_load_properties(inforank0, name);
	    if (cc != MPI_SUCCESS) {
		break;
	    }
	}
    } while (0);
    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_copy_info_to_kvs(inforank0, kvs0);
    assert(cc == MPI_SUCCESS);
    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_replicate(kvs0, kvs1, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    cc = kmr_copy_kvs_to_info(kvs1, info);
    assert(cc == MPI_SUCCESS);
    MPI_Info_free(&inforank0);
    if (0) {
	char ee[80];
	snprintf(ee, sizeof(ee), "[%05d]", mr->rank);
	printf("%s dumpinfo info...\n", ee);
	kmr_dump_mpi_info(ee, info);
    }
    return MPI_SUCCESS;
}

/* Checks configuration options.  It takes merges of two mpi-infos,
   one from preferences and one given.  The given one overrides
   preferences. */

int
kmr_check_options(KMR *mr, MPI_Info info)
{
    int cc;
    /* Check options. */
    int n;
    if (info == MPI_INFO_NULL) {
	n = 0;
    } else {
	cc = MPI_Info_get_nkeys(info, &n);
	assert(cc == MPI_SUCCESS);
    }

    for (int i = 0; i < n; i++) {
	char k[MPI_MAX_INFO_KEY + 1];
	char v[MPI_MAX_INFO_VAL + 1];
	int flag;
	cc = MPI_Info_get_nthkey(info, i, k);
	assert(cc == MPI_SUCCESS);
	cc = MPI_Info_get(info, k, MPI_MAX_INFO_VAL, v, &flag);
	assert(cc == MPI_SUCCESS && flag != 0);
	if (flag == 1) {
	    kmr_set_option_by_strings(mr, k, v);
	} else {
	    char ee[80];
	    snprintf(ee, 80, "option \"%s\" ignored", k);
	    kmr_warning(mr, 1, ee);
	}
    }

    if (mr->verbosity == 9) {
	int r = mr->rank;
	printf("[%05d] Dumping KMR options:\n", r);
	printf("[%05d] verbosity=%d\n", r, mr->verbosity);
	printf("[%05d] sort_threads_depth=%d\n", r, mr->sort_threads_depth);
	printf("[%05d] onk=%d\n", r, mr->onk);
	printf("[%05d] atoa_threshold=%ld\n", r, mr->atoa_threshold);
	printf("[%05d] single_thread=%d\n", r, mr->single_thread);
	printf("[%05d] step_sync=%d\n", r, mr->step_sync);
	printf("[%05d] trace_file_io=%d\n", r, mr->trace_file_io);
	printf("[%05d] trace_map_ms=%d\n", r, mr->trace_map_ms);
	printf("[%05d] trace_map_spawn=%d\n", r, mr->trace_map_spawn);
	printf("[%05d] trace_map_mp=%d\n", r, mr->trace_map_mp);
	printf("[%05d] trace_alltoall=%d\n", r, mr->trace_alltoall);
	printf("[%05d] trace_kmrdp=%d\n", r, mr->trace_kmrdp);
	printf("[%05d] std_abort=%d\n", r, mr->std_abort);
	printf("[%05d] log_traces=%d\n", r, (mr->log_traces != 0));
	printf("[%05d] ckpt_enable=%d\n", r, mr->ckpt_enable);
	printf("[%05d] ckpt_selective=%d\n", r, mr->ckpt_selective);
	printf("[%05d] ckpt_no_fsync=%d\n", r, mr->ckpt_no_fsync);
	printf("[%05d] pushoff_block_size=%zd\n", r, mr->pushoff_block_size);
	printf("[%05d] pushoff_poll_rate=%d\n", r, mr->pushoff_poll_rate);
	printf("[%05d] pushoff_fast_notice=%d\n", r, mr->pushoff_fast_notice);
	printf("[%05d] kmrviz_trace=%d\n", r, mr->kmrviz_trace);
    }
    return MPI_SUCCESS;
}

/* Set an option in KMR context as given by a key and a value. */

int
kmr_set_option_by_strings(KMR *mr, char *k, char *v)
{
    int x;
    if (strcasecmp("log_traces", k) == 0) {
	if (kmr_parse_boolean(v, &x)) {
	    if (mr->log_traces == 0) {
		kmr_open_log(mr);
	    }
	} else {
	    kmr_warning(mr, 1, "option log_traces be boolean");
	}
    } else if (strcasecmp("sort_threads_depth", k) == 0) {
	if (kmr_parse_int(v, &x) && x >= 0) {
	    mr->sort_threads_depth = x;
	} else {
	    kmr_warning(mr, 1, ("option sort_threads_depth be"
				" non-negative integer"));
	}
    } else if (strcasecmp("verbosity", k) == 0) {
	if (kmr_parse_int(v, &x) && (1 <= x && x <= 9)) {
	    mr->verbosity = (uint8_t)x;
	} else {
	    kmr_warning(mr, 1, "option verbosity be 1-9");
	}
    } else if (strcasecmp("k", k) == 0) {
	if (kmr_parse_boolean(v, &x)) {
	    mr->onk = (_Bool)x;
	} else {
	    kmr_warning(mr, 1, "option k be boolean");
	}
    } else if (strcasecmp("single_thread", k) == 0) {
	if (kmr_parse_boolean(v, &x)) {
	    mr->single_thread = (_Bool)x;
	} else {
	    kmr_warning(mr, 1, "option single_thread be boolean");
	}
    } else if (strcasecmp("step_sync", k) == 0) {
	if (kmr_parse_boolean(v, &x)) {
	    mr->step_sync = (_Bool)x;
	} else {
	    kmr_warning(mr, 1, "option step_sync be boolean");
	}
    } else if (strcasecmp("trace_file_io", k) == 0) {
	if (kmr_parse_boolean(v, &x)) {
	    mr->trace_file_io = (_Bool)x;
	} else {
	    kmr_warning(mr, 1, "option trace_file_io be boolean");
	}
    } else if (strcasecmp("trace_map_ms", k) == 0) {
	if (kmr_parse_boolean(v, &x)) {
	    mr->trace_map_ms = (_Bool)x;
	} else {
	    kmr_warning(mr, 1, "option trace_map_ms be boolean");
	}
    } else if (strcasecmp("trace_map_spawn", k) == 0) {
	if (kmr_parse_boolean(v, &x)) {
	    mr->trace_map_spawn = (_Bool)x;
	} else {
	    kmr_warning(mr, 1, "option trace_map_spawn be boolean");
	}
    } else if (strcasecmp("trace_map_mp", k) == 0) {
	if (kmr_parse_boolean(v, &x)) {
	    mr->trace_map_mp = (_Bool)x;
	} else {
	    kmr_warning(mr, 1, "option trace_map_mp be boolean");
	}
    } else if (strcasecmp("std_abort", k) == 0) {
	if (kmr_parse_boolean(v, &x)) {
	    mr->std_abort = (_Bool)x;
	} else {
	    kmr_warning(mr, 1, "option std_abort be boolean");
	}
    } else if (strcasecmp("trace_alltoall", k) == 0) {
	if (kmr_parse_boolean(v, &x)) {
	    mr->trace_alltoall = (_Bool)x;
	} else {
	    kmr_warning(mr, 1, "option trace_alltoall be boolean");
	}
    } else if (strcasecmp("atoa_threshold", k) == 0) {
	if (kmr_parse_int(v, &x) && x >= 0) {
	    mr->atoa_threshold = x;
	} else {
	    kmr_warning(mr, 1, ("option atoa_threshold be"
				" non-negative integer"));
	}
    } else if (strcasecmp("spawn_max_processes", k) == 0) {
	if (kmr_parse_int(v, &x) && x >= 0) {
	    mr->spawn_max_processes = x;
	} else {
	    kmr_warning(mr, 1, ("option spawn_max_processes be"
				" non-negative integer"));
	}
    } else if (strcasecmp("ckpt_enable", k) == 0) {
	if (kmr_parse_boolean(v, &x)) {
	    mr->ckpt_enable = (_Bool)x;
	} else {
	    kmr_warning(mr, 1, "option ckpt_enable be boolean");
	}
    } else if (strcasecmp("ckpt_selective", k) == 0) {
	if (kmr_parse_boolean(v, &x)) {
	    mr->ckpt_selective = (_Bool)x;
	} else {
	    kmr_warning(mr, 1, "option ckpt_selective be boolean");
	}
    } else if (strcasecmp("ckpt_no_fsync", k) == 0) {
	if (kmr_parse_boolean(v, &x)) {
	    mr->ckpt_no_fsync = (_Bool)x;
	} else {
	    kmr_warning(mr, 1, "option ckpt_no_fsync be boolean");
	}
    } else if (strcasecmp("pushoff_block_size", k) == 0) {
	size_t z;
	if (kmr_parse_size_t(v, &z)) {
	    mr->pushoff_block_size = z;
	}
    } else if (strcasecmp("pushoff_poll_rate", k) == 0) {
	if (kmr_parse_int(v, &x)) {
	    mr->pushoff_poll_rate = x;
	}
    } else if (strcasecmp("pushoff_fast_notice", k) == 0) {
	if (kmr_parse_boolean(v, &x)) {
	    mr->pushoff_fast_notice = (_Bool)x;
	} else {
	    kmr_warning(mr, 1, "option pushoff_fast_notice be boolean");
	}
    } else if (strcasecmp("kmrviz_trace", k) == 0) {
	if (kmr_parse_boolean(v, &x)) {
	    mr->kmrviz_trace = (_Bool)x;
	} else {
	    kmr_warning(mr, 1, "option kmrviz_trace be boolean");
	}
    } else {
	char ee[80];
	snprintf(ee, 80, "option \"%s\" ignored", k);
	kmr_warning(mr, 1, ee);
    }
    return MPI_SUCCESS;
}

/* ================================================================ */

/* Checks if a COMMAND is found.  If SEARCH=true, it checks in the
   PATH directories.  Or, it checks just existence of a file.  MSG is
   a string prefixing to the trace messages. */

static _Bool
kmr_check_command_existence(KMR *mr, char *command, _Bool search, char *msg)
{
    int cc;
    char ss[256];
    _Bool tracing7 = (mr->trace_map_spawn && (7 <= mr->verbosity));
    if (!search) {
	if (tracing7) {
	    fprintf(stderr, (";;KMR [%05d] %s:"
			     " checking a watch-program: %s\n"),
		    mr->rank, msg, command);
	    fflush(0);
	}
	do {
	    cc = access(command, X_OK);
	} while (cc != 0 && errno == EINTR);
	if (cc != 0 && !(errno == ENOENT || errno == EACCES)) {
	    char ee[80];
	    char *m = strerror(errno);
	    snprintf(ee, sizeof(ee), "access() returned: %s", m);
	    kmr_warning(mr, 1, ee);
	}
	return (cc == 0);
    } else {
	_Bool fixed = 0;
	for (char *p = command; *p != 0; p++) {
	    if (*p == '/') {
		fixed = 1;
		break;
	    }
	}
	char *path = getenv("PATH");
	if (fixed || path == 0) {
	    _Bool ok = kmr_check_command_existence(mr, command, 0, msg);
	    return ok;
	}
	size_t s = strlen(path);
	char *buf = kmr_malloc(s + (size_t)1);
	memcpy(buf, path, (s + 1));
	_Bool ok = 0;
	char *prefix = buf;
	char *p = buf;
	while (p < &buf[s]) {
	    while (p < &buf[s] && *p != ':') {
		p++;
	    }
	    if (*p == ':') {
		*p = 0;
		p++;
	    } else {
		assert(*p == 0);
	    }
	    cc = snprintf(ss, sizeof(ss), "%s/%s", prefix, command);
	    assert(cc < (int)sizeof(ss));
	    ok = kmr_check_command_existence(mr, ss, 0, msg);
	    if (ok) {
		break;
	    }
	    prefix = p;
	}
	kmr_free(buf, (s + 1));
	return ok;
    }
}

/* Assures a watch-program is available as a command, and returns a
   command string to it.  It installs a new watch-program file in the
   home directory when it is not available.  Home is taken from "HOME"
   or "PJM_JOBDIR".  It works on the rank0 only.  MSG is a string
   prefixing to the trace messages.  */

static char *
kmr_install_watch_program_on_rank0(KMR *mr, char *msg)
{
    char *name = "kmrwatch0";
    assert(mr->rank == 0);
    int cc;
    static char command[256];
    _Bool ok = 0;
    cc = snprintf(command, sizeof(command), "%s", name);
    assert(cc < (int)sizeof(command));
    ok = kmr_check_command_existence(mr, command, 1, msg);
    if (ok) {
	return command;
    }
    if (mr->kmr_installation_path != 0) {
	char *prefix = mr->kmr_installation_path;
	cc = snprintf(command, sizeof(command), "%s/bin/%s", prefix, name);
	assert(cc < (int)sizeof(command));
	ok = kmr_check_command_existence(mr, command, 0, msg);
	if (ok) {
	    return command;
	}
	cc = snprintf(command, sizeof(command), "%s/lib/%s", prefix, name);
	assert(cc < (int)sizeof(command));
	ok = kmr_check_command_existence(mr, command, 0, msg);
	if (ok) {
	    return command;
	}
    }
    if (mr->spawn_watch_prefix != 0) {
	char *prefix = mr->spawn_watch_prefix;
	cc = snprintf(command, sizeof(command), "%s/%s", prefix, name);
	assert(cc < (int)sizeof(command));
	ok = kmr_check_command_existence(mr, command, 0, msg);
	if (ok) {
	    return command;
	}
    } else {
	char *prefix = 0;
	prefix = getenv("HOME");
	if (prefix == 0) {
	    /* On K, HOME is not set but PJM_JOBDIR is. */
	    prefix = getenv("PJM_JOBDIR");
	}
	if (prefix == 0) {
	    kmr_error(mr, ("installing a watch-program:"
			   " environment variable HOME not set."
			   " Try setting spawn_watch_prefix"));
	}
	cc = snprintf(command, sizeof(command), "%s/%s", prefix, name);
	assert(cc < (int)sizeof(command));
	ok = kmr_check_command_existence(mr, command, 0, msg);
	if (ok) {
	    return command;
	}
    }
#if !defined(KMRBINEMBED)
    {
	cc = snprintf(command, sizeof(command), "%s", name);
	assert(cc < (int)sizeof(command));
	return command;
    }
#else /*KMRBINEMBEDH*/
    {
	extern unsigned char kmr_binary_kmrwatch0_start[];
	extern unsigned char *kmr_binary_kmrwatch0_end;
	extern unsigned long kmr_binary_kmrwatch0_size;
	char *p0 = (void *)kmr_binary_kmrwatch0_start;
	char *p1 = (void *)kmr_binary_kmrwatch0_end;
	size_t sz = kmr_binary_kmrwatch0_size;
	assert((p1 - p0) == (long)sz);
	int fd;
	do {
	    mode_t mode = (S_IRWXU|S_IRWXG|S_IRWXO);
	    fd = open(command, (O_WRONLY|O_CREAT|O_TRUNC), mode);
	} while (fd == -1 && errno == EINTR);
	if (fd == -1) {
	    char ee[160];
	    char *m = strerror(errno);
	    snprintf(ee, sizeof(ee), "open(%s): %s", command, m);
	    kmr_error(mr, ee);
	}
	size_t ss = 0;
	while (ss < sz) {
	    ssize_t xx = write(fd, &p0[ss], (sz - ss));
	    if (xx == -1) {
		char ee[160];
		char *m = strerror(errno);
		snprintf(ee, sizeof(ee), "write(%s): %s", command, m);
		kmr_error(mr, ee);
	    }
	    if (xx == 0) {
		char ee[160];
		snprintf(ee, sizeof(ee), "write(%s): write by zero size",
			 command);
		kmr_error(mr, ee);
	    }
	    ss += (size_t)xx;
	}
	cc = close(fd);
	assert(cc == 0);
	char ee[80];
	snprintf(ee, sizeof(ee),
		 "a watch-program for spawning has been installed (%s)",
		 command);
	kmr_warning(mr, 5, ee);
	return command;
    }
#endif /*KMRBINEMBED*/
}

/* Assures a watch-program is available as a command, and stores its
   file name in the context as SPAWN_WATCH_PROGRAM. */

int
kmr_install_watch_program(KMR *mr, char *msg)
{
    int cc;
    if (mr->spawn_watch_program == 0) {
	KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	if (mr->rank == 0) {
	    char *command = kmr_install_watch_program_on_rank0(mr, msg);
	    assert(command != 0 && command[0] != 0);
	    cc = kmr_add_string(kvs0, "", command);
	    assert(cc == MPI_SUCCESS);
	}
	cc = kmr_add_kv_done(kvs0);
	assert(cc == MPI_SUCCESS);
	KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	cc = kmr_replicate(kvs0, kvs1, kmr_noopt);
	assert(cc == MPI_SUCCESS);
	struct kmr_kv_box kv;
	cc = kmr_take_one(kvs1, &kv);
	assert(cc == MPI_SUCCESS);
	char *b = kmr_malloc((size_t)kv.vlen);
	memcpy(b, kv.v.p, (size_t)kv.vlen);
	mr->spawn_watch_program = b;
	cc = kmr_free_kvs(kvs1);
	assert(cc == MPI_SUCCESS);
    }
    return MPI_SUCCESS;
}

/* ================================================================ */

/* DUMPERS */

/** Puts the string of the key or value field into a buffer BUF as
    printable string.  Ellipses appear if string does not fit in the
    buffer. */

void
kmr_dump_opaque(const char *p, int sz, char *buf, int buflen)
{
    int printable = 1; /*printable*/
    int seezero = 0;
    for (int i = 0; i < sz; i++) {
	if (p[i] == 0) {
	    seezero = 1;
	} else if (!isprint(p[i])) {
	    printable = 0; /*unprintable*/
	    break;
	} else {
	    if (seezero && printable == 1) {
		printable = 2; /*null-in-the-middle*/
	    }
	}
    }
    if (printable == 1) {
	int z = (int)strnlen(p, (size_t)sz);
	int n = MIN(z, ((int)buflen - 5 - 1));
	if (z == n) {
	    snprintf(buf, (size_t)(n + 3), "\"%s\"", p);
	} else {
	    snprintf(buf, (size_t)(n + 6), "\"%s...\"", p);
	}
    } else if (printable == 2) {
	int z = (int)strnlen(p, (size_t)sz);
	int n = MIN(z, (buflen - 5 - 1));
	if (z == n) {
	    snprintf(buf, (size_t)(n + 6), "\"%s???\"", p);
	} else {
	    snprintf(buf, (size_t)(n + 6), "\"%s...\"", p);
	}
    } else {
	int n = MIN(sz, ((buflen - 3 - 1) / 3));
	char *q = buf;
	for (int i = 0; i < n; i++) {
	    snprintf(q, 4, "%02x ", (p[i] & 0xff));
	    q += 3;
	}
	if (n != sz) {
	    snprintf(q, 4, "...");
	}
    }
}

void
kmr_dump_slot(union kmr_unit_sized e, int len, enum kmr_kv_field data,
	      char *buf, int buflen)
{
    switch (data) {
    case KMR_KV_BAD:
	assert(data != KMR_KV_BAD);
	break;
    case KMR_KV_INTEGER:
	snprintf(buf, (size_t)buflen, "%ld", e.i);
	break;
    case KMR_KV_FLOAT8:
	snprintf(buf, (size_t)buflen, "%e", e.d);
	break;
    case KMR_KV_OPAQUE:
    case KMR_KV_CSTRING:
    case KMR_KV_POINTER_OWNED:
    case KMR_KV_POINTER_UNMANAGED:
	kmr_dump_opaque(e.p, len, buf, buflen);
	break;
    default:
	assert(NEVERHERE);
	break;
    }
}

/** Dumps contents of a key-value. */

int
kmr_dump_kv(const struct kmr_kv_box kv, const KMR_KVS *kvs,
	    char *buf, int buflen)
{
    char kbuf[48], vbuf[48];
    kmr_dump_slot(kv.k, kv.klen, kvs->c.key_data, kbuf, sizeof(kbuf));
    kmr_dump_slot(kv.v, kv.vlen, kvs->c.value_data, vbuf, sizeof(vbuf));
    snprintf(buf, (size_t)buflen, "k[%d]=%s;v[%d]=%s", kv.klen, kbuf, kv.vlen, vbuf);
    return MPI_SUCCESS;
}

static int
kmr_dump_kvs_fn(const struct kmr_kv_box kv,
		const KMR_KVS *kvs, KMR_KVS *kvso, void *p, const long i)
{
    char b[80];
    kmr_dump_kv(kv, kvs, b, sizeof(b));
    printf("[%05d][%ld] %s\n", kvs->c.mr->rank, i, b);
    return MPI_SUCCESS;
}

/** Dumps contents of a key-value stream to stdout.  Argument FLAG is
    nothing, ignored. */

int
kmr_dump_kvs(KMR_KVS *kvs, int flag)
{
    assert(kvs->c.magic != KMR_KVS_BAD);
    int rank = kvs->c.mr->rank;
    printf("[%05d] element_count=%ld\n", rank, kvs->c.element_count);
    struct kmr_option opt = {.inspect = 1, .nothreading = 1};
    int kcdc = kmr_ckpt_disable_ckpt(kvs->c.mr);
    int cc = kmr_map_rank_by_rank(kvs, 0, 0, opt, kmr_dump_kvs_fn);
    assert(cc == MPI_SUCCESS);
    kmr_ckpt_enable_ckpt(kvs->c.mr, kcdc);
    return MPI_SUCCESS;
}

#if 0
static int
kmr_dump_kvs2_fn(const struct kmr_kv_box kv,
		 const KMR_KVS *kvs, KMR_KVS *kvso, void *p, const long i)
{
    char kbuf[48], vbuf[48];
    assert(kvs->c.magic != KMR_KVS_BAD);
    int rank = kvs->c.mr->rank;
    kmr_dump_slot(kv.k, kv.klen, kvs->c.key_data, kbuf, sizeof(kbuf));
    //kmr_dump_slot(kv.v, kv.vlen, , vbuf, sizeof(vbuf));
    printf("[%05d] k[%d]=%s;v[%d]=%s\n", rank, kv.klen, kbuf, kv.vlen, vbuf);
    return MPI_SUCCESS;
}
#endif

/** Dumps contents of a key-value stream, with values are pairs. */

#if 0
static int
kmr_dump_kvs_pair_value(KMR_KVS *kvs, int flag)
{
    assert(kvs->c.magic != KMR_KVS_BAD);
    assert(kvs->c.value_data == KMR_KV_OPAQUE
	   || kvs->c.value_data == KMR_KV_CSTRING);
    int rank = kvs->c.mr->rank;
    printf("[%05d] element_count=%ld\n", rank, kvs->c.element_count);
    struct kmr_option opt = {.inspect = 1};
    int cc = kmr_map_rank_by_rank(kvs, 0, 0, opt, kmr_dump_kvs2_fn);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}
#endif

/** Prints the total number of key-value pairs.  It prints on the
    rank0 only. */

int
kmr_dump_kvs_stats(KMR_KVS *kvs, int level)
{
    long v;
    kmr_get_element_count(kvs, &v);
    if (kvs->c.mr->rank == 0) {
	printf("element_count=%ld\n", v);
    }
    return MPI_SUCCESS;
}

int
kmr_dump_keyed_records(const struct kmr_keyed_record *ev, KMR_KVS *kvi)
{
    long cnt = kvi->c.element_count;
    for (long i = 0; i < cnt; i++) {
	int rank = kvi->c.mr->rank;
	char kbuf[48], vbuf[48];
	struct kmr_kv_box kv = kmr_pick_kv(ev[i].e, kvi);
	kmr_dump_slot(kv.k, kv.klen, kvi->c.key_data, kbuf, sizeof(kbuf));
	kmr_dump_slot(kv.v, kv.vlen, kvi->c.value_data, vbuf, sizeof(vbuf));
	printf("[%05d] h=%ld;k[%d]=%s;v[%d]=%s\n", rank,
	       ev[i].v, kv.klen, kbuf, kv.vlen, vbuf);
    }
    return MPI_SUCCESS;
}

void
kmr_print_options(struct kmr_option opt)
{
    printf(".nothreading=%d,"
	   " .inspect=%d,"
	   " .keep_open=%d,"
	   " .key_as_rank=%d,"
	   " .rank_zero=%d,"
	   " .take_ckpt=%d,"
	   " .collapse=%d\n",
	   opt.nothreading,
	   opt.inspect,
	   opt.keep_open,
	   opt.key_as_rank,
	   opt.rank_zero,
	   opt.collapse,
	   opt.take_ckpt);
}

void
kmr_print_file_options(struct kmr_file_option opt)
{
    printf(".each_rank=%d,"
	   " .subdirectories=%d,"
	   " .list_file=%d,"
	   " .shuffle_names=%d\n",
	   opt.each_rank,
	   opt.subdirectories,
	   opt.list_file,
	   opt.shuffle_names);
}

void
kmr_print_spawn_options(struct kmr_spawn_option opt)
{
    printf((".separator_space=%d,"
	    " .reply_each=%d,"
	    " .reply_root=%d,"
	    " .one_by_one=%d,"
	    " .take_ckpt=%d\n"),
	   opt.separator_space,
	   opt.reply_each,
	   opt.reply_root,
	   opt.one_by_one,
	   opt.take_ckpt);
}

void
kmr_print_string(char *msg, char *s, int len)
{
    /* LEN includes terminating zero. */
    assert(len >= 1);
    printf("%s(len=%d)=", msg, len);
    for (int i = 0; i < len; i++) {
	if (s[i] == 0) {
	    printf("$");
	} else if (isblank(s[i])) {
	    printf("_");
	} else if (isprint(s[i])) {
	    printf("%c", s[i]);
	} else {
	    printf("?");
	}
    }
    printf("\n");
}

/* Opens a file for trace logging. */

void
kmr_open_log(KMR *mr)
{
    assert(mr->log_traces == 0);
    int cc;
    char file[256];
    cc = snprintf(file, sizeof(file), "./%s_%05d",
		  KMR_TRACE_FILE_PREFIX, mr->rank);
    assert(cc < (int)sizeof(file));
    mr->log_traces = fopen(file, "w");
    if (mr->log_traces == 0) {
	char ee[80];
	char *m = strerror(errno);
	snprintf(ee, sizeof(ee),
		 "Opening log file (%s) failed"
		 " (disable tracing): %s",
		 file, m);
	kmr_warning(mr, 1, ee);
	mr->log_traces = 0;
    }
    if (mr->log_traces != 0) {
	time_t ct = time(0);
	char *cs = ctime(&ct);
	fprintf(mr->log_traces, "kmr trace (%s)\n", cs);
    }
}

/* Trace Logging. */

void
kmr_log_map(KMR *mr, KMR_KVS *kvs, struct kmr_kv_box *ev,
	    long i, long cnt, kmr_mapfn_t m, double dt)
{
    assert(mr->log_traces != 0);
    if (mr->atwork != 0) {
	struct kmr_code_line *info = mr->atwork;
	char s[32];
	kmr_dump_slot(ev->k, ev->klen, kvs->c.key_data, s, sizeof(s));
	fprintf(mr->log_traces,
		"file:%s, line:%d, kmr_func:%s,"
		" user_func:%p, key:[%ld/%ld]%s, time:%.lf\n",
		info->file, info->line, info->func,
		(void *)(intptr_t)m, (i + 1), cnt, s, (dt * 1000.0));
    }
}

/* Trace Logging. */

void
kmr_log_reduce(KMR *mr, KMR_KVS *kvs, struct kmr_kv_box *ev,
	       long n, kmr_redfn_t r, double dt)
{
    assert(mr->log_traces != 0);
    if (mr->atwork != 0) {
	struct kmr_code_line *info = mr->atwork;
	char s[32];
	kmr_dump_slot(ev->k, ev->klen, kvs->c.key_data, s, sizeof(s));
	fprintf(mr->log_traces,
		"file:%s, line:%d, kmr_func:%s,"
		" user_func:%p, key:[%ld]%s, time:%.lf\n",
		info->file, info->line, info->func,
		(void *)(intptr_t)r, n, s, (dt * 1000.0));
    }
}

/* ================================================================ */

/* CONFIGURATION */

/* Puts property into MPI_Info.  B points to the key, and &B[VALPOS]
   points to the value.  END points to one past the 0-terminator. */

static int
kmr_put_property(MPI_Info info, char *b, int valpos, int end)
{
    char *k = b;
    char *v = &b[valpos];
    /*printf("setting \"%s\"=\"%s\"\n", b, &b[valpos]);*/
    if (k[0] == 0) {
	kmr_warning(0, 5, "empty key string for MPI_Info_set(), ignored");
    } else if (v[0] == 0) {
	/* OPEN MPI (1.6.4) DOES NOT ALLOW EMPTY VALUE. */
	kmr_warning(0, 5, "empty value string for MPI_Info_set(), ignored");
    } else {
	int cc = MPI_Info_set(info, b, &b[valpos]);
	assert(cc == MPI_SUCCESS);
    }
    return MPI_SUCCESS;
}

/** Loads properties into MPI_Info (in Latin1 characters).  It runs
    only on the main-thread.  It returns MPI_SUCCESS normally.  It
    stores only Latin1 strings because MPI_Info does.  Refer to the
    JDK document "java.util.Properties.load()" for the ".properties"
    format. */

int
kmr_load_properties(MPI_Info info, char *filename)
{
#define CONTNL 0x010000
#define kmr_load_properties_check_getc_error(C) \
    if ((C) == EOF && errno != 0) { \
	char *e = strerror(errno); \
	snprintf(ee, sizeof(ee), "loading properties (%s), fgetc(): %s", \
		 filename, e); \
	kmr_warning(0, 1, ee); \
	fclose(f); \
	free(b); \
	return MPI_ERR_ARG; \
    }
#define kmr_load_properties_reset() \
    {pos = -1; valpos = -1; scan = ForKey;}
#define kmr_load_properties_grow() \
    if (pos >= (blen - 1)) { \
	blen *= 2; b = realloc(b, (size_t)blen); assert(b != 0); }
#define kmr_load_properties_putc(C) { \
	assert(pos != -1); \
	b[pos++] = (char)(C); kmr_load_properties_grow(); }
#define kmr_load_properties_hex(C) \
    (('0' <= (C) && (C) <= '9') \
     ? ((C) - '0') \
     : (('a' <= (C) && (C) <= 'f') \
	? ((C) - 'a') : ((C) - 'A')))
#define kmr_load_properties_replace_cr() \
    if (c == '\r') { \
	int c1 = kmr_fgetc(f); \
	kmr_load_properties_check_getc_error(c1); \
	if (c1 != '\n') { \
	    ungetc(c1, f); \
	} \
	c = '\n'; \
    }

    char ee[160];
    int blen = 4096;
    char *b = kmr_malloc((size_t)blen);

    FILE *f = kmr_fopen(filename, "r");
    if (f == 0) {
	char *e = strerror(errno);
	char *cwd = getcwd(b, 64);
	snprintf(ee, sizeof(ee), "loading properties, fopen(%s): %s (cwd=%s)",
		 filename, e, (cwd == 0 ? "?" : cwd));
	kmr_warning(0, 1, ee);
	return MPI_ERR_ARG;
    }

    errno = 0;
    enum {ForKey, Com, Key, KeySkp, ForSep, ForVal, Val, ValSkp} scan = ForKey;
    int pos = -1;
    int valpos = -1;
    kmr_load_properties_reset();
    int lines = 0;

    for (;;) {
	_Bool escaped = 0;
	int c = kmr_fgetc(f);
	kmr_load_properties_check_getc_error(c);
	if (c == EOF) {
	    switch (scan) {
	    case ForKey:
		assert(pos == -1 && valpos == -1);
		break;
	    case Com:
		assert(pos == -1 && valpos == -1);
		break;
	    case Key: case KeySkp:
		assert(pos != -1 && valpos == -1);
		kmr_put_property(info, b, valpos, pos);
		break;
	    case ForSep: case ForVal: case Val: case ValSkp:
		assert(pos != -1 && valpos != -1);
		kmr_put_property(info, b, valpos, pos);
		break;
	    }
	    break;
	}

	/* Replace '\r\n' as a single '\n'. */
	kmr_load_properties_replace_cr();

	switch (c) {
	case '\\':
	{
	    /* Look at a backslash. */
	    escaped = 1;
	    c = kmr_fgetc(f);
	    kmr_load_properties_check_getc_error(c);
	    if (c == EOF) {
		snprintf(ee, sizeof(ee),
			 ("loading properties (%s),"
			  " file ends with a backslash"), filename);
		kmr_warning(0, 1, ee);
		fclose(f);
		free(b);
		return MPI_ERR_ARG;
	    }
	    switch (c) {
	    case '\n':
		lines++;
		c = CONTNL;
		break;

	    case 'n': case 'r': case 't': case 'f':
		switch (c) {
		case 'n': c = '\n'; break;
		case 'r': c = '\r'; break;
		case 't': c = '\t'; break;
		case 'f': c = '\f'; break;
		}
		break;

	    case 'u':
	    {
		int c0 = kmr_fgetc(f);
		int c1 = kmr_fgetc(f);
		int c2 = kmr_fgetc(f);
		int c3 = kmr_fgetc(f);
		if (c1 == EOF || c2 == EOF || c3 == EOF) {
		    if (errno != 0) {
			char *e = strerror(errno);
			snprintf(ee, sizeof(ee),
				 ("loading properties (%s),"
				  " fgetc(): %s"),
				 filename, e);
			kmr_warning(0, 1, ee);
			fclose(f);
			free(b);
			return MPI_ERR_ARG;
		    } else {
			snprintf(ee, sizeof(ee),
				 ("loading properties (%s),"
				  " file ends amid unicode (at line %d)"),
				 filename, (lines + 1));
			kmr_warning(0, 1, ee);
			fclose(f);
			free(b);
			return MPI_ERR_ARG;
		    }
		}
		if (!(isxdigit(c0) && isxdigit(c1)
		      && isxdigit(c2) && isxdigit(c3))) {
		    snprintf(ee, sizeof(ee),
			     ("loading properties (%s),"
			      " file includes bad character"
			      " in unicode (at line %d)"),
			     filename, (lines + 1));
		    kmr_warning(0, 1, ee);
		    fclose(f);
		    free(b);
		    return MPI_ERR_ARG;
		}
		c = (kmr_load_properties_hex(c0) << 12);
		c |= (kmr_load_properties_hex(c1) << 8);
		c |= (kmr_load_properties_hex(c2) << 4);
		c |= kmr_load_properties_hex(c3);
		assert(c >= 0);
		if (c >= 256) {
		    snprintf(ee, sizeof(ee),
			     ("loading properties (%s),"
			      " file includes non-latin character"
			      " in unicode (at line %d)"),
			     filename, (lines + 1));
		    kmr_warning(0, 1, ee);
		    fclose(f);
		    free(b);
		    return MPI_ERR_ARG;
		}
	    }
	    break;

	    default:
		break;
	    }

	    if (c == CONTNL) {
		switch (scan) {
		case ForKey:
		    assert(pos == -1 && valpos == -1);
		    break;
		case Com:
		    assert(pos == -1 && valpos == -1);
		    scan = ForKey;
		    break;
		case Key: case KeySkp:
		    assert(pos != -1 && valpos == -1);
		    scan = KeySkp;
		    break;
		case ForSep: case ForVal:
		    assert(pos != -1 && valpos != -1);
		    break;
		case Val: case ValSkp:
		    assert(pos != -1 && valpos != -1);
		    scan = ValSkp;
		    break;
		}
		break;
	    }
	}
	/* Fall thru with reading c as an ordinary character. */

	default:
	    /* Look at an ordinary character. */
	    /* (c is not in {CONTNL, " \n\t\f#!=:"}) */
	    if (iscntrl(c) && !escaped) {
		snprintf(ee, sizeof(ee),
			 ("loading properties (%s),"
			  " file includes bad control code (at line %d)"),
			 filename, (lines + 1));
		kmr_warning(0, 1, ee);
		fclose(f);
		free(b);
		return MPI_ERR_ARG;
	    }
	    switch (scan) {
	    case ForKey:
		assert(pos == -1 && valpos == -1);
		pos = 0;
		kmr_load_properties_putc(c);
		scan = Key;
		break;
	    case Com:
		assert(pos == -1 && valpos == -1);
		/*skip*/
		break;
	    case Key: case KeySkp:
		assert(pos != -1 && valpos == -1);
		kmr_load_properties_putc(c);
		scan = Key;
		break;
	    case ForSep: case ForVal: case Val: case ValSkp:
		assert(pos != -1 && valpos != -1);
		kmr_load_properties_putc(c);
		scan = Val;
		break;
	    }
	    break;

	case '\n':
	    lines++;
	    switch (scan) {
	    case ForKey:
		assert(pos == -1 && valpos == -1);
		/*skip*/
		break;
	    case Com:
		assert(pos == -1 && valpos == -1);
		scan = ForKey;
		break;
	    case Key: case KeySkp:
		assert(pos != -1 && valpos == -1);
		kmr_load_properties_putc('\0');
		valpos = pos;
		kmr_load_properties_putc('\0');
		kmr_put_property(info, b, valpos, pos);
		kmr_load_properties_reset();
		break;
	    case ForSep: case ForVal: case Val: case ValSkp:
		assert(pos != -1 && valpos != -1);
		kmr_load_properties_putc('\0');
		kmr_put_property(info, b, valpos, pos);
		kmr_load_properties_reset();
		break;
	    }
	    break;

	case ' ': case '\t': case '\f':
	    switch (scan) {
	    case ForKey:
		assert(pos == -1 && valpos == -1);
		break;
	    case Com:
		assert(pos == -1 && valpos == -1);
		break;
	    case Key:
		assert(pos != -1 && valpos == -1);
		kmr_load_properties_putc('\0');
		valpos = pos;
		scan = ForSep;
		break;
	    case KeySkp:
		assert(pos != -1 && valpos == -1);
		break;
	    case ForSep: case ForVal:
		assert(pos != -1 && valpos != -1);
		break;
	    case Val:
		assert(pos != -1 && valpos != -1);
		kmr_load_properties_putc(c);
		break;
	    case ValSkp:
		assert(pos != -1 && valpos != -1);
		break;
	    }
	    break;

	case '#': case '!':
	    switch (scan) {
	    case ForKey:
		assert(pos == -1 && valpos == -1);
		scan = Com;
		break;
	    case Com:
		assert(pos == -1 && valpos == -1);
		break;
	    case Key: case KeySkp:
		assert(pos != -1 && valpos == -1);
		kmr_load_properties_putc(c);
		scan = Key;
		break;
	    case ForSep: case ForVal: case Val: case ValSkp:
		assert(pos != -1 && valpos != -1);
		kmr_load_properties_putc(c);
		scan = Val;
		break;
	    }
	    break;

	case '=': case ':':
	    switch (scan) {
	    case ForKey:
		assert(pos == -1 && valpos == -1);
		pos = 0;
		kmr_load_properties_putc('\0');
		valpos = pos;
		scan = ForVal;
		break;
	    case Com:
		assert(pos == -1 && valpos == -1);
		break;
	    case Key: case KeySkp:
		assert(pos != -1 && valpos == -1);
		kmr_load_properties_putc('\0');
		valpos = pos;
		scan = ForVal;
		break;
	    case ForSep:
		assert(pos != -1 && valpos != -1);
		scan = ForVal;
		break;
	    case ForVal: case Val: case ValSkp:
		assert(pos != -1 && valpos != -1);
		kmr_load_properties_putc(c);
		scan = Val;
		break;
	    }
	    break;
	}
    }

    fclose(f);
    free(b);
    return MPI_SUCCESS;
}

/** Dumps simply contents in MPI_Info. */

int
kmr_dump_mpi_info(char *prefix, MPI_Info info)
{
    int cc;
    int nkeys;
    cc = MPI_Info_get_nkeys(info, &nkeys);
    assert(cc == MPI_SUCCESS);
    for (int i = 0; i < nkeys; i++) {
	char key[MPI_MAX_INFO_KEY + 1];
	char value[MPI_MAX_INFO_VAL + 1];
	int vlen;
	int flag;
	cc = MPI_Info_get_nthkey(info, i, key);
	assert(cc == MPI_SUCCESS);
	cc = MPI_Info_get_valuelen(info, key, &vlen, &flag);
	assert(cc == MPI_SUCCESS && flag != 0);
	assert(vlen <= MPI_MAX_INFO_VAL);
	cc = MPI_Info_get(info, key, MPI_MAX_INFO_VAL, value, &flag);
	assert(cc == MPI_SUCCESS && flag != 0);
	printf("%s \"%s\"=\"%s\"\n", prefix, key, value);
    }
    return MPI_SUCCESS;
}

/** Copies contents of MPI_Info.  The destination info is modified. */

int
kmr_copy_mpi_info(MPI_Info src, MPI_Info dst)
{
    int cc;
    int nkeys;
    cc = MPI_Info_get_nkeys(src, &nkeys);
    assert(cc == MPI_SUCCESS);
    for (int i = 0; i < nkeys; i++) {
	char key[MPI_MAX_INFO_KEY + 1];
	char value[MPI_MAX_INFO_VAL + 1];
	int vlen;
	int flag;
	cc = MPI_Info_get_nthkey(src, i, key);
	assert(cc == MPI_SUCCESS);
	cc = MPI_Info_get_valuelen(src, key, &vlen, &flag);
	assert(cc == MPI_SUCCESS && flag != 0);
	assert(vlen <= MPI_MAX_INFO_VAL);
	cc = MPI_Info_get(src, key, MPI_MAX_INFO_VAL, value, &flag);
	assert(cc == MPI_SUCCESS && flag != 0);
	/*printf("\"%s\"=\"%s\"\n", key, value);*/
	cc = MPI_Info_set(dst, key, value);
	assert(cc == MPI_SUCCESS);
    }
    return MPI_SUCCESS;
}

/*
Copyright (C) 2012-2016 RIKEN AICS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/
