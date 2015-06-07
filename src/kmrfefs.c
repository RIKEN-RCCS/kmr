/* kmrfefs.c (2014-02-04) */
/* Copyright (C) 2012-2015 RIKEN AICS */

/** \file kmrfefs.c Lustre File-System (or Fujitsu FEFS) Support.  The
    definitions of IOCTL are changed to match the Fujitsu Extended
    File-System (FEFS) version of the Lustre.  THERE IS NO WARRANTY
    THE VALUES ARE OK.  This only works with Linux. */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
//#include <stropts.h>
#include <stdint.h>
#include <limits.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#if defined(__SVR4)
#include <sys/ioccom.h>
#endif
#include "kmrfefs.h"

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

/* Magic Numbers and IOCTL (structures).  LOV_USER_MAGIC_OST_INDEX is
   Fujitsu FEFS extension. */

#define KMR_LOV_MAGIC_V1 0x0BD10BD0
#define KMR_LOV_MAGIC_V3 0x0BD30BD0
#define KMR_LOV_USER_MAGIC_OST_INDEX 0x0BD70BD0
#define KMR_LOV_PATTERN_RAID0 0x001

#define KMR_OST_MAX_PRECREATE 20000

struct kmr_lov_md {
    uint32_t magic;
    uint32_t pattern;
    uint64_t id;
    uint64_t gr;
    uint32_t stripe_size;
    uint16_t stripe_count;
    uint16_t stripe_offset;
};

struct kmr_lov_ost {
    uint64_t id;
    uint64_t gr;
    uint32_t gen;
    uint32_t idx;
};

struct kmr_lov_md_ost {
    struct kmr_lov_md md;
    struct kmr_lov_ost o[KMR_OST_MAX_PRECREATE];
};

struct kmr_lov_md_ostidx {
    struct stat st;
    struct kmr_lov_md md;
    uint16_t o[KMR_OST_MAX_PRECREATE];
};

#define KMR_IOC_MDC_GETFILESTRIPE _IOWR('i', 21, struct kmr_lov_md_ost *)
#define KMR_IOC_MDC_GETINFO _IOWR('i', 23, struct kmr_lov_md_ostidx *)

/** Gets the OBDIDX information on the file or directory.  FILE be
    null for directory.  It fills STRIPE, which contains stripe size,
    count, offset, and an obdidx array.  It returns non-zero on error:
    1 for unsupported OS, 2 malloc failure, 3 open failure, 4 ioctl
    failure (not Lustre FS), 5 bad magic, 6 bad version (V3), and 7
    bad pattern.  Note the structure and the _IOWR definitions are
    changed to match the arguments of the Fujitsu FEFS.  See the
    source "cb_getstripe()", near "ioctl(_,LL_IOC_LOV_GETSTRIPE,...)"
    in "liblustreapi.c". */

int
kmr_fefs_get_stripe(const char *dir, const char *file,
		    struct kmr_fefs_stripe *stripe,
		    int *err, _Bool debug_and_dump)
{
#if defined(__linux__)
    int cc;
    size_t sz = MAX(sizeof(struct kmr_lov_md_ost),
		    sizeof(struct kmr_lov_md_ostidx));
    void *b = malloc(sz);
    if (b == 0) {
	*err = errno;
	if (debug_and_dump) {
	    perror("malloc(lov_user_md)");
	}
	/* malloc fails. */
	return 2;
    }
    struct kmr_lov_md_ost *lov1 = b;
    struct kmr_lov_md_ostidx *lov7 = b;
    int fd = -1;
    do {
	fd = open(dir, O_RDONLY);
    } while (fd == -1 && errno == EINTR);
    if (fd == -1) {
	*err = errno;
	if (debug_and_dump) {
	    char ee[80];
	    snprintf(ee, sizeof(ee), "open(%s)", dir);
	    perror(ee);
	}
	/* open fails. */
	return 3;
    }
    do {
	if (file != 0) {
	    strcpy((void *)lov1, file);
	    cc = ioctl(fd, KMR_IOC_MDC_GETFILESTRIPE, (void *)lov1);
	} else {
	    cc = ioctl(fd, KMR_IOC_MDC_GETINFO, (void *)lov7);
	}
    } while (cc == -1 && errno == EINTR);
    if (cc == -1) {
	*err = errno;
	if (debug_and_dump) {
	    char ee[80];
	    snprintf(ee, sizeof(ee), "ioctl(%s|%s)",
		     dir, (file == 0 ? "." : file));
	    perror(ee);
	}
	/* ioctl fails (Not Lustre FS). */
	return 4;
    }
    cc = close(fd);
    /*ignore*/
    struct kmr_lov_md *md;
    if (file != 0) {
	md = &lov1->md;
    } else {
	md = &lov7->md;
    }
    if (!(md->magic == KMR_LOV_MAGIC_V1
	  || md->magic == KMR_LOV_MAGIC_V3
	  || md->magic == KMR_LOV_USER_MAGIC_OST_INDEX)) {
	/* Not Lustre FS. */
	if (debug_and_dump) {
	    char ee[80];
	    snprintf(ee, sizeof(ee),
		     "Not Lustre FS (%s/%s), bad magic (%x)",
		     dir, (file == 0 ? "." : file), md->magic);
	    fprintf(stderr, "%s\n", ee);
	}
	/* Bad magic. */
	*err = 0;
	return 5;
    }
    if (md->magic == KMR_LOV_MAGIC_V3) {
	if (debug_and_dump) {
	    char ee[80];
	    snprintf(ee, sizeof(ee),
		     "Lustre FS (%s/%s), bad version (%x)",
		     dir, (file == 0 ? "." : file), md->magic);
	    fprintf(stderr, "%s\n", ee);
	}
	/* Bad version. */
	*err = 0;
	return 6;
    }
    assert(md->magic == KMR_LOV_MAGIC_V1
	   || md->magic == KMR_LOV_USER_MAGIC_OST_INDEX);
    if (md->pattern != KMR_LOV_PATTERN_RAID0) {
	if (debug_and_dump) {
	    char ee[80];
	    snprintf(ee, sizeof(ee),
		     "Lustre FS (%s/%s), bad pattern (%x)",
		     dir, (file == 0 ? "." : file), md->pattern);
	    fprintf(stderr, "%s\n", ee);
	}
	/* Bad pattern. */
	*err = 0;
	return 7;
    }
    struct kmr_lov_ost *o1 = &lov1->o[0];
    uint16_t *o7 = &lov7->o[0];
    if (debug_and_dump) {
	fprintf(stderr, "lov_user_md(%s)=\n", (file != 0 ? "file" : "dir"));
	fprintf(stderr, "magic=%x\n", md->magic);
	fprintf(stderr, "pattern=%x\n", md->pattern);
	fprintf(stderr, "id=%lx\n", md->id);
	fprintf(stderr, "gr=%lx\n", md->gr);
	fprintf(stderr, "stripe_size=%ud\n", md->stripe_size);
	fprintf(stderr, "stripe_count=%d\n", md->stripe_count);
	fprintf(stderr, "stripe_offset=%d\n", md->stripe_offset);
	if (md->magic == KMR_LOV_MAGIC_V1) {
	    for (int i = 0; i < md->stripe_count; i++) {
		fprintf(stderr, "[%d] id=%lud gr=%lud gen=%ud idx=%ud\n",
			i, o1[i].id, o1[i].gr, o1[i].gen, o1[i].idx);
	    }
	} else if (md->magic == KMR_LOV_USER_MAGIC_OST_INDEX) {
	    for (int i = 0; i < md->stripe_count; i++) {
		fprintf(stderr, "[%d] idx=%d\n", i, o7[i]);
	    }
	} else {
	    assert(0);
	}
    }
    assert(md->stripe_count < 20000);
    stripe->s.size = md->stripe_size;
    stripe->s.count = md->stripe_count;
    stripe->s.offset = md->stripe_offset;
    if (md->magic == KMR_LOV_MAGIC_V1) {
	for (int i = 0; i < md->stripe_count; i++) {
	    assert(o1[i].idx < USHRT_MAX);
	    stripe->obdidx[i] = (uint16_t)o1[i].idx;
	}
    } else if (md->magic == KMR_LOV_USER_MAGIC_OST_INDEX) {
	for (int i = 0; i < md->stripe_count; i++) {
	    stripe->obdidx[i] = o7[i];
	}
    } else {
	assert(0);
    }
    free(b);
    return 0;
#elif defined(__APPLE__) && defined(__MACH__)
    /* OS unsupported */
    return 1;
#elif defined(__SVR4)
    /* OS unsupported */
    return 1;
#else
    /* OS unsupported */
    return 1;
#endif
}

/* TEST CODE. */

#ifdef KMRLUSTEST

static int
kmr_readin(char *f, char *b, int sz)
{
    int cc;
    int fd = open(f, O_RDONLY, 0);
    assert(fd != -1);
    int ii = 0;
    while (ii < sz) {
	cc = read(fd, &b[ii], (sz - ii));
	assert(cc != -1);
	if (cc == 0) {
	    break;
	}
	ii += cc;
    }
    close(fd);
    if (ii < sz) {
	b[ii] = 0;
    } else {
	b[sz - 1] = 0;
    }
    return 0;
}

int
main(int argc, char **argv)
{
    static char b[8 * 1024];
    int cc;
    MPI_Init(&argc, &argv);
    int nprocs, rank;
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Barrier(MPI_COMM_WORLD);

    struct kmr_fefs_stripe stripe;

    if (rank == 0) {
	printf("print ost of shared directory on each rank.\n");
	fflush(0);
    }
    usleep(200 * 1000);
    MPI_Barrier(MPI_COMM_WORLD);

    for (int i = 0; i < nprocs; i++) {
	if (rank == i) {
	    kmr_readin("/proc/tofu/position", b, sizeof(b));
	    printf("rank=%d position=%s\n", rank, b);

	    cc = kmr_fefs_get_stripe(".", "a.out", &stripe, 0);
	    assert(cc == 0);
	    printf("[%04d] rankdirfile=./a.out stripe_count=%d\n",
		   rank, stripe.count);
	    for (int j = 0; j < stripe.count; j++) {
		printf("[%04d] idx=%d\n", rank, stripe.obdidx[j]);
	    }
	    printf("\n");

	    cc = kmr_fefs_get_stripe("..", 0, &stripe, 0);
	    assert(cc == 0);
	    printf("[%04d] sharedir=.. stripe_count=%d\n",
		   rank, stripe.count);
	    for (int j = 0; j < stripe.count; j++) {
		printf("[%04d] idx=%d\n", rank, stripe.obdidx[j]);
	    }
	    printf("\n");
	}
	fflush(0);
	usleep(200 * 1000);
	MPI_Barrier(MPI_COMM_WORLD);
    }

    if (rank == 0) {
	int fd = open("../aho", O_WRONLY, O_CREAT, 0);
	assert(fd != -1);
	close(fd);

	cc = kmr_fefs_get_stripe("..", "aho", &stripe, 0);
	assert(cc == 0);
	printf("[%04d] sharedir=../aho stripe_count=%d\n",
	       rank, stripe.count);
	for (int j = 0; j < stripe.count; j++) {
	    printf("[%04d] idx=%d\n", rank, stripe.obdidx[j]);
	}
	printf("\n");
    }

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
    return 0;
}

#endif /*KMRLUSTEST*/

/*
NOTICE-NOTICE-NOTICE
*/
