/* kmrmapmp.c (2015-06-04) */
/* Copyright (C) 2012-2015 RIKEN AICS */

/** \file kmrmapmp.c MPI Parappel Mapping on Key-Value Pairs. */


#include <mpi.h>
#include "kmr.h"
#include "kmrimpl.h"

/*
  Each Key-Value pair stored in rank 0 is processed in paralell using
  multiple MPI processes.

  Key-Value pairs stored in other ranks are ignored.
  To process all Key-Value pairs, call kmr_replicate() before calling this
  function.

  This is a collective operation.

  'max_nprocs' is the maximum number of processes in a sub-communicator.

  Effective-options:
  INSPECT, TAKE_CKPT. See struct kmr_option

  TODO support ckpt(whole)
*/
int
kmr_map_multiprocess(KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
                     struct kmr_option opt, int max_nprocs, kmr_mapfn_t m)
{
    kmr_assert_kvs_ok(kvi, kvo, 1, 0);
    KMR *mr = kvi->c.mr;
    int cc;

    /* split communicator */
    int rem = mr->nprocs % max_nprocs;
    if (rem != 0) {
        if (mr->rank == 0) {
            kmr_warning(mr, 1, "Not all the number of processes in "
                        "sub-communicators are same");
        }
    }
    MPI_Comm task_comm;
    int task_color = mr->rank / max_nprocs;
    MPI_Comm_split(mr->comm, task_color, mr->rank, &task_comm);

    MPI_Barrier(mr->comm);
    MPI_Comm_free(&task_comm);

    return MPI_SUCCESS;
}

#if 0
int
kmr_map_multiprocess_by_key(KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
			    struct kmr_option opt, int rank_key, kmr_mapfn_t m)
{
    return MPI_SUCCESS;
}
#endif
