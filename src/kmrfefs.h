/* kmrfefs.h (2014-02-04) */
/* Copyright (C) 2012-2015 RIKEN AICS */

/** \file kmrfefs.h Lustre File System (or Fujitsu FEFS) Support. */

#include <stddef.h>

/** Lustre Striping Information.  A single stripe consists of a unit
    of SIZE block repeated by COUNT times.  OFFSET is the first OBDIDX
    allocated for a file. */

struct kmr_fefs_stripe_info {
    uint32_t size;
    uint16_t count;
    uint16_t offset;
};

/** Lustre Striping Information with OBDIDX. */

struct kmr_fefs_stripe {
    struct kmr_fefs_stripe_info s;
    uint16_t obdidx[20000];
};

/** Offset to Striping OBDIDX Information.  */

static const size_t
kmr_fefs_stripe_info_size = offsetof(struct kmr_fefs_stripe, obdidx);

extern int kmr_fefs_get_stripe(const char *dir, const char *file,
			       struct kmr_fefs_stripe *stripe,
			       int *err, _Bool debug_and_dump);

/*
Copyright (C) 2012-2015 RIKEN AICS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/
