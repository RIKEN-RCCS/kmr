/* kmrckpt.c (2014-04-01) */
/* Copyright (C) 2012-2018 RIKEN R-CCS */

/** \file kmrckpt.c Checkpoint/Restart Support. */

#include <mpi.h>
#include <assert.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <limits.h>

#ifdef _OPENMP
#include <omp.h>
#endif
#include "../config.h"
#include "kmr.h"
#include "kmrimpl.h"
#include "kmrckpt.h"

/* Functions for initialization */
static void kmr_ckpt_init_environment(KMR *);
static int kmr_ckpt_check_restart(KMR *, int **, int *, int *);
static void kmr_ckpt_restore_prev_progress(KMR *, int *, int);
static void kmr_ckpt_restore_prev_state(KMR *, const char *, int*, int, int);
static void kmr_ckpt_restore_prev_state_each_rank(KMR *,
						  struct kmr_ckpt_prev_state *,
						  struct kmr_ckpt_merge_ctx *);
static int kmr_ckpt_merge_check_ignorable(struct kmr_ckpt_kvs_chains *, long);
static void kmr_ckpt_merge_ignore_ckpt_data(long,
					    struct kmr_ckpt_prev_state *,
					    struct kmr_ckpt_merge_ctx *);
static void kmr_ckpt_merge_store_ckpt_data(long, int, long,
					   struct kmr_ckpt_prev_state *,
					   struct kmr_ckpt_merge_ctx *);
static void kmr_ckpt_merge_update_ckpt_data(long, int, long, long,
					    struct kmr_ckpt_list *,
					    struct kmr_ckpt_prev_state *,
					    struct kmr_ckpt_merge_ctx *);
static void kmr_ckpt_merge_sort_data(KMR *, const char *, long,
				     struct kmr_ckpt_merge_source *);
static void kmr_ckpt_merge_write_file(KMR *, const char *,
				      struct kmr_ckpt_merge *);
/* Functions for logging */
static void kmr_ckpt_init_log(KMR *, const char *);
static void kmr_ckpt_fin_log(KMR *);
static FILE *kmr_ckpt_open_log(KMR *, const char *, struct kmr_ckpt_log *,
			       unsigned long *);
static void kmr_ckpt_log_whole_start(KMR *);
static void kmr_ckpt_log_whole_finish(KMR *);
static void kmr_ckpt_log_block_start(KMR *, KMR_KVS *);
static void kmr_ckpt_log_block_add(KMR *, long, long);
static void kmr_ckpt_log_block_finish(KMR *);
static void kmr_ckpt_log_index_start(KMR *, KMR_KVS *);
static void kmr_ckpt_log_index_add(KMR *, long, long);
static void kmr_ckpt_log_index_finish(KMR *);
static void kmr_ckpt_log_delete_start(KMR *, long);
static void kmr_ckpt_log_delete_finish(KMR *, long);
static void kmr_ckpt_log_deletable(KMR *, long );
static void kmr_ckpt_log_progress(KMR *);
static void kmr_ckpt_log_skipped(KMR *);
/* Functions for checkpoint data management */
static void kmr_ckpt_delete_ckpt_data(KMR *, long);
static void kmr_ckpt_delete_ckpt_files(KMR *, const char *, int);
static void kmr_ckpt_save_ckpt(KMR_KVS *);
static void kmr_ckpt_kv_record_init(KMR *, KMR_KVS *);
static long kmr_ckpt_kv_record_add(KMR_KVS *);
static void kmr_ckpt_kv_record_fin(KMR *);
static FILE *kmr_ckpt_open(KMR_KVS *, const char *);
static FILE *kmr_ckpt_open_path(KMR *, const char *, const char *);
static void kmr_ckpt_save_nprocs(KMR *, const char *);
static void kmr_ckpt_make_fname(const char *, const char *,
				enum kmr_ckpt_type, int, long, char *, size_t);
static void kmr_ckpt_get_data_flist(KMR *, const char *,
				    struct kmr_ckpt_data_file **, int *,
				    _Bool);
static void kmr_ckpt_flush(KMR *, FILE *);
/* Utility functions */
static void kmr_ckpt_list_init(struct kmr_ckpt_list *, kmr_ckpt_list_alocfn_t,
			       kmr_ckpt_list_freefn_t, kmr_ckpt_list_compfn_t);
static void kmr_ckpt_list_free(struct kmr_ckpt_list *);
static void kmr_ckpt_list_add(struct kmr_ckpt_list *, void *);
static void *kmr_ckpt_list_del(struct kmr_ckpt_list *, void *);
static void *kmr_ckpt_list_search(struct kmr_ckpt_list *, void *);
static void *kmr_ckpt_list_rsearch(struct kmr_ckpt_list *, void *);
static void kmr_ckpt_int_list_init(struct kmr_ckpt_list *);
static void kmr_ckpt_int_list_free(struct kmr_ckpt_list *);
static void kmr_ckpt_int_list_add(struct kmr_ckpt_list *, long);
static long kmr_ckpt_int_list_del(struct kmr_ckpt_list *, long);
static long kmr_ckpt_int_list_search(struct kmr_ckpt_list *, long);
static long kmr_ckpt_int_list_rsearch(struct kmr_ckpt_list *, long);
static void kmr_ckpt_opr_list_init(struct kmr_ckpt_list *);
static void kmr_ckpt_opr_list_free(struct kmr_ckpt_list *);
static void kmr_ckpt_opr_list_add(struct kmr_ckpt_list *,
				  struct kmr_ckpt_operation);
static void kmr_ckpt_kvs_chains_init(struct kmr_ckpt_kvs_chains *);
static void kmr_ckpt_kvs_chains_free(struct kmr_ckpt_kvs_chains *);
static void kmr_ckpt_kvs_chains_new_chain(struct kmr_ckpt_kvs_chains *,
					  struct kmr_ckpt_operation);
static void kmr_ckpt_kvs_chains_connect(struct kmr_ckpt_kvs_chains *,
					struct kmr_ckpt_operation);
static struct kmr_ckpt_list *
kmr_ckpt_kvs_chains_find(struct kmr_ckpt_kvs_chains *, long);


/** Initialize checkpoint context.  This function should be called only once
    when MapReduce data type is initialized.

    \param[in]  mr    MapReduce data type
*/
void
kmr_ckpt_create_context(KMR *mr)
{
    struct kmr_ckpt_ctx *
	ckptctx = kmr_malloc(sizeof(struct kmr_ckpt_ctx));
    mr->ckpt_ctx = ckptctx;
    snprintf(ckptctx->ckpt_dname, KMR_CKPT_DIRLEN, "./%s%05d",
	     KMR_CKPT_DIRNAME, mr->rank);
    ckptctx->prev_mode = KMR_CKPT_ALL;
    ckptctx->ckpt_log_fp  = NULL;
    ckptctx->progress_counter = 0;
    ckptctx->prev_progress = 0;
    ckptctx->prev_global_progress = 0;
    ckptctx->cur_kvi_id = KMR_CKPT_DUMMY_ID;
    ckptctx->cur_kvo_id = KMR_CKPT_DUMMY_ID;
    ckptctx->ckpt_data_fp = NULL;
    ckptctx->saved_element_count = 0;
    ckptctx->saved_adding_point = NULL;
    ckptctx->saved_current_block = NULL;
    ckptctx->kv_positions = NULL;
    ckptctx->kv_positions_count = 0;
    ckptctx->lock_id = 0;
    ckptctx->lock_counter = 0;
    ckptctx->initialized = 0;
    ckptctx->slct_cur_take_ckpt = 0;
    if (mr->ckpt_selective) {
	ckptctx->slct_skip_ops = (struct kmr_ckpt_list *)
	    kmr_malloc(sizeof(struct kmr_ckpt_list));
	kmr_ckpt_int_list_init(ckptctx->slct_skip_ops);
    } else {
	ckptctx->slct_skip_ops = NULL;
    }

    if (mr->ckpt_enable) {
	kmr_ckpt_init_environment(mr);
    }
}

/** Free checkpoint context.  This function should be called only once
    when MapReduce data type is freed.

    \param[in]  mr  MapReduce data type
*/
void
kmr_ckpt_free_context(KMR *mr)
{
    struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
    if (kmr_ckpt_enabled(mr)) {
	MPI_Barrier(mr->comm);
	kmr_ckpt_fin_log(mr);
	kmr_ckpt_delete_ckpt_files(mr, ckptctx->ckpt_dname, mr->rank);
	kmr_free(ckptctx->kv_positions,
		 sizeof(struct kv_position) * (size_t)ckptctx->kv_positions_count);
    }
    if (mr->ckpt_selective) {
	kmr_ckpt_int_list_free(ckptctx->slct_skip_ops);
	kmr_free(ckptctx->slct_skip_ops, sizeof(struct kmr_ckpt_list));
    }
    kmr_free(ckptctx, sizeof(struct kmr_ckpt_ctx));
    mr->ckpt_ctx = 0;
}

/***************************************************************/
/*  Functions for initilizing checkpoint/restart environment   */
/***************************************************************/

/* Initialize checkpoint environment */
static void
kmr_ckpt_init_environment(KMR *mr)
{
    struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
    if (ckptctx->initialized) {
	return;
    }

    /* Check if restarted or not */
    int *prev_ranks = NULL;
    int prev_rank_count = 0;
    int prev_nprocs = 0;
    int restarted = kmr_ckpt_check_restart(mr, &prev_ranks, &prev_rank_count,
					   &prev_nprocs);
    {
	int all_restarted;
	int cc = MPI_Allreduce(&restarted, &all_restarted, 1, MPI_INT,
			       MPI_LAND, mr->comm);
	assert(cc == MPI_SUCCESS);
	assert(restarted == all_restarted);
    }

    /* Create a temporal directory for checkpoint files */
    char tmp_dname[KMR_CKPT_DIRLEN];
    snprintf(tmp_dname, KMR_CKPT_DIRLEN, "./tmp_%s%05d",
	     KMR_CKPT_DIRNAME, mr->rank);
    kmr_ckpt_delete_ckpt_files(mr, tmp_dname, mr->rank);
    int cc = mkdir(tmp_dname, S_IRWXU);
    if (cc != 0) {
	char msg[KMR_CKPT_MSGLEN];
	snprintf(msg, sizeof(msg),
		 "Failed to create a directory for checkpoint %s", tmp_dname);
	kmr_error(mr, msg);
    }

    /* Load checkpoint files to restart */
    if (restarted) {
	kmr_ckpt_restore_prev_progress(mr, prev_ranks, prev_rank_count);
	kmr_ckpt_restore_prev_state(mr, tmp_dname, prev_ranks, prev_rank_count,
				    prev_nprocs);
    }

    /* Initialize a log file */
    char log_fpath[KMR_CKPT_PATHLEN];
    kmr_ckpt_make_fname(tmp_dname, KMR_CKPT_FNAME_PREFIX, KMR_CKPT_LOG,
			mr->rank, 0, log_fpath, sizeof(log_fpath));
    kmr_ckpt_init_log(mr, log_fpath);

    /* save nprocs to file */
    if (mr->rank == 0) {
	kmr_ckpt_save_nprocs(mr, tmp_dname);
    }

    /* Rename directories */
    for (int i = 0; i < prev_rank_count; i++) {
	char old_dname[KMR_CKPT_DIRLEN];
	snprintf(old_dname, KMR_CKPT_DIRLEN, "./%s%05d.old",
		 KMR_CKPT_DIRNAME, prev_ranks[i]);
	kmr_ckpt_delete_ckpt_files(mr, old_dname, prev_ranks[i]);
	char cur_dname[KMR_CKPT_DIRLEN];
	snprintf(cur_dname, KMR_CKPT_DIRLEN, "./%s%05d",
		 KMR_CKPT_DIRNAME, prev_ranks[i]);
	struct stat sb;
	cc = stat(cur_dname, &sb);
	if (cc == 0) {
	    cc = rename(cur_dname, old_dname);
	    assert(cc == 0);
	}
    }
    MPI_Barrier(mr->comm);
    cc = rename(tmp_dname, ckptctx->ckpt_dname);
    assert(cc == 0);

    if (restarted) {
	kmr_free(prev_ranks, sizeof(int) * (size_t)prev_rank_count);
    }
    ckptctx->initialized = 1;
}

/* Check if this run is restarted or not.
   It also finds target checkpoint files of the previous run if restarted. */
static int
kmr_ckpt_check_restart(KMR *mr, int **target_ranks, int *target_rank_count,
		       int *target_nprocs)
{
    int restarted = 0;
    _Bool force_start_from_scratch = 0;
    struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
    struct stat sb;
    int cc = stat(ckptctx->ckpt_dname, &sb);
    if (cc == 0) {
	if (!S_ISDIR(sb.st_mode)) {
	    char msg[KMR_CKPT_MSGLEN];
	    snprintf(msg, sizeof(msg),
		     "Non-directory file for checkpoint directory %s "
		     "already exist",
		     ckptctx->ckpt_dname);
	    kmr_error(mr, msg);
	}
	/* Read this rank's log and find target ranks in the previous run */
	char fpath[KMR_CKPT_PATHLEN];
	kmr_ckpt_make_fname(ckptctx->ckpt_dname, KMR_CKPT_FNAME_PREFIX,
			    KMR_CKPT_LOG, mr->rank, 0, fpath, sizeof(fpath));
	cc = access(fpath, R_OK);
	if (cc == 0) {
	    struct kmr_ckpt_log log_hdr;
	    unsigned long log_size = 0;
	    FILE *fp = kmr_ckpt_open_log(mr, fpath, &log_hdr, &log_size);
	    fclose(fp);
	    assert(mr->rank == log_hdr.rank);
	    assert(log_hdr.nprocs > 0);
	    if (log_size == 0) {
		force_start_from_scratch = 1;
		char msg[KMR_CKPT_MSGLEN];
		snprintf(msg, sizeof(msg),
			 "Log file exists, but no log is recorded in %s. "
			 "All logs are discarded and start from scratch",
			 fpath);
		kmr_warning(mr, 1, msg);
	    }
	    int quotient = log_hdr.nprocs / mr->nprocs;
	    int rest     = log_hdr.nprocs % mr->nprocs;
	    int cnt = quotient + ((mr->rank < rest) ? 1 : 0);
	    if (cnt != 0) {
		*target_ranks = (int*)kmr_malloc(sizeof(int) * (size_t)cnt);
		int offset = mr->rank * quotient +
		    ((mr->rank < rest) ? mr->rank : rest);
		for (int i = 0; i < cnt; i++) {
		    (*target_ranks)[i] = offset + i;
		}
	    }
	    *target_rank_count = cnt;
	    *target_nprocs = log_hdr.nprocs;
	    if (mr->nprocs > log_hdr.nprocs) {
		// TODO support future
		char msg[KMR_CKPT_MSGLEN];
		snprintf(msg, sizeof(msg),
			 "Currently restart with bigger number of processes "
			 "is not supported");
		kmr_error(mr, msg);
	    }
	    if (mr->ckpt_selective && mr->nprocs != log_hdr.nprocs) {
		char msg[KMR_CKPT_MSGLEN];
		snprintf(msg, sizeof(msg),
			 "Restart with different number of processes "
			 "is not supported in selective mode");
		kmr_error(mr, msg);
	    }
	    ckptctx->prev_mode = log_hdr.mode;
	} else {
	    char msg[KMR_CKPT_MSGLEN];
	    snprintf(msg, sizeof(msg),
		     "Structure of a checkpoint directory may be wrong %s. "
		     "Delete all checkpoint directories",
		     ckptctx->ckpt_dname);
	    kmr_error(mr, msg);
	}
    } else {
	if (errno != ENOENT) {
	    char msg[KMR_CKPT_MSGLEN];
	    snprintf(msg, sizeof(msg),
		     "Unknown error on checkpoint directory %s",
		     ckptctx->ckpt_dname);
	    kmr_error(mr, msg);
	}
	assert(*target_rank_count == 0);
    }

    /* Check consistency of target checkpoint log files to decide restart */
    if (*target_rank_count > 0) {
	for (int i = 1; i < *target_rank_count; i++) {
	    _Bool success = 1;
	    int t_rank = (*target_ranks)[i];
	    char dpath[KMR_CKPT_DIRLEN];
	    snprintf(dpath, KMR_CKPT_DIRLEN, "./%s%05d", KMR_CKPT_DIRNAME,
		     t_rank);
	    char fpath[KMR_CKPT_PATHLEN];
	    kmr_ckpt_make_fname(dpath, KMR_CKPT_FNAME_PREFIX, KMR_CKPT_LOG,
				t_rank, 0, fpath, sizeof(fpath));
	    cc = access(fpath, R_OK);
	    if (cc == 0) {
		struct kmr_ckpt_log log_hdr;
		unsigned long log_size = 0;
		FILE *fp = kmr_ckpt_open_log(mr, fpath, &log_hdr, &log_size);
		fclose(fp);
		if (log_hdr.nprocs < 0) {
		    success = 0;
		}
		if (log_size == 0) {
		    force_start_from_scratch = 1;
		    char msg[KMR_CKPT_MSGLEN];
		    snprintf(msg, sizeof(msg),
			     "Log file exists, but no log is recorded in %s. "
			     "All logs are discarded and start from scratch",
			     fpath);
		    kmr_warning(mr, 1, msg);
		}
	    } else {
		success = 0;
	    }
	    if (!success) {
		kmr_free(*target_ranks, (size_t)*target_rank_count);
		char msg[KMR_CKPT_MSGLEN];
		snprintf(msg, sizeof(msg),
			 "Wrong structure of checkpoint directory %s. ",
			 dpath);
		kmr_error(mr, msg);
	    }
	}
	if (!force_start_from_scratch) {
	    restarted = 1;
	}
    }
    return restarted;
}

/* Restore MapReduce progress of the previous run for all mode. */
static void
kmr_ckpt_restore_prev_progress_all(KMR *mr,
				   int *target_ranks, int target_rank_count)
{
    struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
    long min_progress = -1;

    /* read all ckpt logs files to find the minimum progress */
    for (int i = 0; i < target_rank_count; i++) {
	int rank = target_ranks[i];
	char dpath[KMR_CKPT_DIRLEN];
	snprintf(dpath, KMR_CKPT_DIRLEN, "./%s%05d", KMR_CKPT_DIRNAME, rank);
	char fpath[KMR_CKPT_PATHLEN];
	kmr_ckpt_make_fname(dpath, KMR_CKPT_FNAME_PREFIX, KMR_CKPT_LOG,
			    rank, 0, fpath, sizeof(fpath));
	struct kmr_ckpt_log log_hdr;
	unsigned long total, size = 0;
	FILE *fp = kmr_ckpt_open_log(mr, fpath, &log_hdr, &total);
	long max_done_op = 0, cur_op = 0;
	_Bool num_procs_locked = 0;
	while (size < total) {
	    struct kmr_ckpt_log_entry e;
	    size_t rc = fread((void *)&e, sizeof(e), 1, fp);
	    if (rc != 1) {
		char msg[KMR_CKPT_MSGLEN];
		snprintf(msg, sizeof(msg),
			 "Failed to read a checkpoint log entry");
		kmr_error(mr, msg);
	    }
	    switch (e.state) {
	    case KMR_CKPT_LOG_WHOLE_START:
	    case KMR_CKPT_LOG_BLOCK_START:
	    case KMR_CKPT_LOG_INDEX_START:
		cur_op = e.op_seqno;
		break;
	    case KMR_CKPT_LOG_WHOLE_FINISH:
	    case KMR_CKPT_LOG_BLOCK_FINISH:
	    case KMR_CKPT_LOG_INDEX_FINISH:
		max_done_op = cur_op;
		cur_op = 0;
		break;
	    case KMR_CKPT_LOG_SKIPPED:
		max_done_op = e.op_seqno;
		break;
	    case KMR_CKPT_LOG_LOCK_START:
		assert(num_procs_locked == 0);
		num_procs_locked = 1;
		break;
	    case KMR_CKPT_LOG_LOCK_FINISH:
		assert(num_procs_locked == 1);
		num_procs_locked = 0;
		break;
	    }
	    size += sizeof(e);
	}
	fclose(fp);
	if (num_procs_locked && target_rank_count > 1) {
	    /* Can not restart with the different number of processes */
	    char msg[KMR_CKPT_MSGLEN];
	    snprintf(msg, sizeof(msg),
		     "Fault occurred in a critical region and can not restart "
		     "with the different number of processes. "
		     "Restart with the same number of processes with "
		     "the previous run.");
	    kmr_error(mr, msg);
	}
	if (min_progress < 0) {
	    min_progress = max_done_op;
	} else {
	    if (max_done_op < min_progress) {
		min_progress = max_done_op;
	    }
	}
    }
    assert(min_progress >= 0);

    /* Find global minimal progress */
    long global_min_progress;
    int cc = MPI_Allreduce(&min_progress, &global_min_progress, 1, MPI_LONG,
			   MPI_MIN, mr->comm);
    assert(cc == MPI_SUCCESS);

    ckptctx->prev_progress = min_progress;
    ckptctx->prev_global_progress = global_min_progress;
}

/* Restore MapReduce progress of the previous run for selective mode. */
static void
kmr_ckpt_restore_prev_progress_selective(KMR *mr, int *target_ranks,
					 int target_rank_count)
{
    long min_progress = -1, max_progress = -1;
    /* read all ckpt logs files to find the minimum progress */
    for (int i = 0; i < target_rank_count; i++) {
	int rank = target_ranks[i];
	char dpath[KMR_CKPT_DIRLEN];
	snprintf(dpath, KMR_CKPT_DIRLEN, "./%s%05d", KMR_CKPT_DIRNAME, rank);
	char fpath[KMR_CKPT_PATHLEN];
	kmr_ckpt_make_fname(dpath, KMR_CKPT_FNAME_PREFIX, KMR_CKPT_LOG,
			    rank, 0, fpath, sizeof(fpath));
	struct kmr_ckpt_log log_hdr;
	unsigned long total, size = 0;
	FILE *fp = kmr_ckpt_open_log(mr, fpath, &log_hdr, &total);
	long target_kvs_id = KMR_CKPT_DUMMY_ID;
	/* stores kvs transition chains */
	struct kmr_ckpt_kvs_chains chains;
	kmr_ckpt_kvs_chains_init(&chains);
	/* stores id of kvses that has been checkpointed */
	struct kmr_ckpt_list kvses;
	kmr_ckpt_int_list_init(&kvses);
	_Bool num_procs_locked = 0;
	while (size < total) {
	    struct kmr_ckpt_log_entry e;
	    size_t rc = fread((void *)&e, sizeof(e), 1, fp);
	    if (rc != 1) {
		char msg[KMR_CKPT_MSGLEN];
		snprintf(msg, sizeof(msg),
			 "Failed to read a checkpoint log entry");
		kmr_error(mr, msg);
	    }
	    struct kmr_ckpt_operation op = { .op_seqno = e.op_seqno,
					     .kvi_id = e.kvi_id,
					     .kvo_id = e.kvo_id };
	    long v;
	    switch (e.state) {
	    case KMR_CKPT_LOG_WHOLE_START:
		target_kvs_id = e.kvo_id;
		break;
	    case KMR_CKPT_LOG_WHOLE_FINISH:
		kmr_ckpt_int_list_add(&kvses, target_kvs_id);
		target_kvs_id = KMR_CKPT_DUMMY_ID;
		break;
	    case KMR_CKPT_LOG_DELETABLE:
		v = kmr_ckpt_int_list_del(&kvses, e.kvo_id);
		assert(v == e.kvo_id);
		break;
	    case KMR_CKPT_LOG_PROGRESS:
	    case KMR_CKPT_LOG_SKIPPED:
		if (op.kvi_id == KMR_CKPT_DUMMY_ID) {
		    kmr_ckpt_kvs_chains_new_chain(&chains, op);
		} else {
		    kmr_ckpt_kvs_chains_connect(&chains, op);
		}
		break;
	    case KMR_CKPT_LOG_LOCK_START:
		assert(num_procs_locked == 0);
		num_procs_locked = 1;
		break;
	    case KMR_CKPT_LOG_LOCK_FINISH:
		assert(num_procs_locked == 1);
		num_procs_locked = 0;
		break;
	    }
	    size += sizeof(e);
	}
	fclose(fp);
	if (num_procs_locked) {
	    /* nothing to do as currently selective mode does not support
	       restart with different number of procs */
	}

	/* calculate progress */
	long open_min_progress = LONG_MAX;
	long open_max_progress = 0;
	long last_op_id = 0;
	for (int j = 0; j < chains.chainlst_size; j++) {
	    struct kmr_ckpt_list *list = &(chains.chainlst[j]);
	    struct kmr_ckpt_operation *last_op =
		(struct kmr_ckpt_operation *)list->tail->val;
	    if (last_op->op_seqno > last_op_id) {
		last_op_id = last_op->op_seqno;
	    }
	    if (last_op->kvo_id != KMR_CKPT_DUMMY_ID) {
		/* chain is open */
		struct kmr_ckpt_list_item *item;
		for (item = list->tail; item != 0; item = item->prev) {
		    struct kmr_ckpt_operation *op =
			(struct kmr_ckpt_operation *)item->val;
		    long v = kmr_ckpt_int_list_search(&kvses, op->kvo_id);
		    if (v == op->kvo_id) {
			if (op->op_seqno < open_min_progress) {
			    open_min_progress = op->op_seqno;
			}
			if (op->op_seqno > open_max_progress) {
			    open_max_progress = op->op_seqno;
			}
			break;
		    }
		}
	    }
	}
	if (open_min_progress == LONG_MAX && open_max_progress == 0) {
	    open_min_progress = last_op_id;
	    open_max_progress = last_op_id;
	}

	/* initialize the skip operation list */
	struct kmr_ckpt_list *skip_ops = mr->ckpt_ctx->slct_skip_ops;
	for (int j = 0; j < chains.chainlst_size; j++) {
	    struct kmr_ckpt_list *list = &(chains.chainlst[j]);
	    struct kmr_ckpt_operation *last_op =
		(struct kmr_ckpt_operation *)list->tail->val;
	    if (last_op->op_seqno <= open_min_progress) {
		continue;
	    }
	    struct kmr_ckpt_operation *head_op =
		(struct kmr_ckpt_operation *)list->head->val;
	    if (head_op->op_seqno > open_max_progress) {
		continue;
	    }
	    if (last_op->kvo_id == KMR_CKPT_DUMMY_ID) {
		/* chain is closed.
		   add all operations larger than 'open_min_progress' */
		struct kmr_ckpt_list_item *item;
		for (item = list->head; item != 0; item = item->next) {
		    struct kmr_ckpt_operation *op =
			(struct kmr_ckpt_operation *)item->val;
		    if (op->op_seqno > open_min_progress) {
			kmr_ckpt_int_list_add(skip_ops, op->op_seqno);
		    }
		}
	    } else {
		/* chain is open.
		   add all operations larger than 'open_min_progress' and
		   smaller than last-ckpt-saved-operation */
		_Bool f_add = 0;
		struct kmr_ckpt_list_item *item;
		for (item = list->tail; item != 0; item = item->prev) {
		    struct kmr_ckpt_operation *op =
			(struct kmr_ckpt_operation *)item->val;
		    long v = kmr_ckpt_int_list_search(&kvses, op->kvo_id);
		    if (v == op->kvo_id) {
			f_add = 1;
		    }
		    if (f_add) {
			if (op->op_seqno > open_min_progress) {
			    kmr_ckpt_int_list_add(skip_ops, op->op_seqno);
			}
		    }
		}
	    }
	}
	kmr_ckpt_kvs_chains_free(&chains);
	kmr_ckpt_int_list_free(&kvses);
	min_progress = open_min_progress;
	max_progress = open_max_progress;
    }

    assert(max_progress >= 0 && min_progress >= 0);
    mr->ckpt_ctx->prev_progress = max_progress;
    mr->ckpt_ctx->prev_global_progress = min_progress;
}

/* Restore MapReduce progress of the previous run. */
static void
kmr_ckpt_restore_prev_progress(KMR *mr,
			       int *target_ranks, int target_rank_count)
{
    if (!mr->ckpt_selective) {
	kmr_ckpt_restore_prev_progress_all(mr, target_ranks,
					   target_rank_count);
    } else {
	kmr_ckpt_restore_prev_progress_selective(mr, target_ranks,
						 target_rank_count);
    }
}

/* Read checkpoints of previous run and create restart files */
static void
kmr_ckpt_restore_prev_state(KMR *mr, const char *wdpath,
			    int *target_ranks, int target_rank_count,
			    int prev_nprocs)
{
    /* Load checkpoint data files of each rank */
    char **rdpaths =
	(char **)kmr_malloc(sizeof(char *) * (size_t)target_rank_count);
    struct kmr_ckpt_data_file **dataflsts = (struct kmr_ckpt_data_file **)
	kmr_malloc(sizeof(struct kmr_ckpt_data_file *) * (size_t)target_rank_count);
    int *nfiles = (int *)kmr_malloc(sizeof(int) * (size_t)target_rank_count);
    int max_merge_count = 0;
    for (int i = 0; i < target_rank_count; i++) {
	rdpaths[i] = (char*)kmr_malloc(sizeof(char) * KMR_CKPT_DIRLEN);
	snprintf(rdpaths[i], KMR_CKPT_DIRLEN, "./%s%05d",
		 KMR_CKPT_DIRNAME, target_ranks[i]);
	kmr_ckpt_get_data_flist(mr, rdpaths[i], &dataflsts[i], &nfiles[i], 1);
	max_merge_count += nfiles[i];
    }

    /* Collect information for merging checkpoint data */
    struct kmr_ckpt_merge_ctx merge_ctx;
    merge_ctx.max_each_merge = target_rank_count;
    merge_ctx.merges_count = 0;
    merge_ctx.merges = (struct kmr_ckpt_merge *)
	kmr_malloc(sizeof(struct kmr_ckpt_merge) * (size_t)max_merge_count);

    for (int i = 0; i < target_rank_count; i++) {
	struct kmr_ckpt_prev_state prev_state;
	prev_state.prev_rank = target_ranks[i];
	prev_state.prev_nprocs = prev_nprocs;
	prev_state.ckpt_dir = rdpaths[i];
	prev_state.dataflst = dataflsts[i];
	prev_state.dataflst_size = nfiles[i];
	kmr_ckpt_restore_prev_state_each_rank(mr, &prev_state, &merge_ctx);
    }

#if 0
    /* debug print */
    if (mr->rank == 0) {
	for (int i = 0; i < target_rank_count; i++) {
	    fprintf(stderr, "index: %d\n", i);
	    fprintf(stderr, "  rdpath: %s\n", rdpaths[i]);
	    fprintf(stderr, "  nfiles: %d\n", nfiles[i]);
	    for (int j = 0; j < nfiles[i]; j++) {
		struct kmr_ckpt_data_file *file = &dataflsts[i][j];
		fprintf(stderr, "  ckptflst: %ld, %s/%s\n",
			file->kvs_id, file->dname, file->fname);
	    }
	}
	fprintf(stderr, "max_merge_count: %d\n", max_merge_count);

	fprintf(stderr, "\n\n");

	fprintf(stderr, "merge_count: %d\n", merge_ctx.merges_count);
	for (int i = 0; i < merge_ctx.merges_count; i++) {
	    fprintf(stderr, "merge\n");
	    fprintf(stderr, "  rank: %d\n", merge_ctx.merges[i].rank);
	    fprintf(stderr, "  kvs_id: %ld\n", merge_ctx.merges[i].kvs_id);
	    fprintf(stderr, "  src_lst: %d\n",
		    merge_ctx.merges[i].src_lst_count);
	    for (int j = 0; j < merge_ctx.merges[i].src_lst_count; j++) {
		struct kmr_ckpt_merge_source *source =
		    &(merge_ctx.merges[i].src_lst[j]);
		fprintf(stderr, "    rank: %d, n_kvi: %ld, n_kvo: %ld\n",
			source->rank, source->n_kvi, source->n_kvo);
		fprintf(stderr, "              file: %s/%s\n",
			source->file->dname, source->file->fname);
		if (merge_ctx.merges[i].src_lst[j].done_ikv_lst_size > 0) {
		    fprintf(stderr, "              done ikvs index: ");
		    for (int k = 0; k < source->done_ikv_lst_size; k++) {
			fprintf(stderr, "%ld,", source->done_ikv_lst[k]);
		    }
		    fprintf(stderr, "\n");
		}
	    }
	}
    }
#endif

    /* Create sorted checkpoint file */
    for (int i = 0; i < merge_ctx.merges_count; i++) {
	struct kmr_ckpt_merge *merge = &merge_ctx.merges[i];
	for (int j = 0; j < merge->src_lst_count; j++) {
	    if (merge->src_lst[j].n_kvi > 0 &&
		merge->src_lst[j].done_ikv_lst_size > 0) {
		/* checkpoint data should be sorted */
		kmr_ckpt_merge_sort_data(mr, wdpath, merge->kvs_id,
					 &merge->src_lst[j]);
	    }
	}
    }

    /* Write merged checkpoint data to files */
    for (int i = 0; i < merge_ctx.merges_count; i++) {
	kmr_ckpt_merge_write_file(mr, wdpath, &merge_ctx.merges[i]);
    }

    /* Setup map/reduce operation start point */
    struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
    for (int i = 0; i < merge_ctx.merges_count; i++) {
	struct kmr_ckpt_merge *merge = &merge_ctx.merges[i];
	for (int j = 0; j < merge->src_lst_count; j++) {
	    struct kmr_ckpt_merge_source* mg_src = &merge->src_lst[j];
	    if (mg_src->kvi_op_seqno > 0) {
		ckptctx->kv_positions_count++;
		break;
	    }
	}
    }
    ckptctx->kv_positions = (struct kv_position *)
	kmr_malloc(sizeof(struct kv_position) * (size_t)ckptctx->kv_positions_count);
    int idx = 0;
    for (int i = 0; i < merge_ctx.merges_count; i++) {
	struct kmr_ckpt_merge *merge = &merge_ctx.merges[i];
	_Bool found = 0;
	for (int j = 0; j < merge->src_lst_count; j++) {
	    struct kmr_ckpt_merge_source *mg_src = &merge->src_lst[j];
	    if (mg_src->n_kvi > 0) {
		struct kv_position *kvpos = &ckptctx->kv_positions[idx];
		if (!found){
		    found = 1;
		    kvpos->op_seqno = mg_src->kvi_op_seqno;
		    kvpos->start_from = mg_src->n_kvi;
		} else {
		    assert(mg_src->kvi_op_seqno == kvpos->op_seqno);
		    kvpos->start_from += mg_src->n_kvi;
		}
	    }
	}
	if (found) {
	    idx++;
	}
    }

#if 0
    /* debug print */
    for (int i = 0; i < ckptctx->kv_positions_count; i++) {
	fprintf(stderr, "op_seqno: %ld, start_from: %ld\n",
		ckptctx->kv_positions[i].op_seqno,
		ckptctx->kv_positions[i].start_from);
    }
#endif

    /* post process */
    for (int i = 0; i < merge_ctx.merges_count; i++) {
	for (int j = 0; j < merge_ctx.merges[i].src_lst_count; j++) {
	    if (merge_ctx.merges[i].src_lst[j].done_ikv_lst_size > 0) {
		struct kmr_ckpt_merge_source *mg_src = &(merge_ctx.merges[i].src_lst[j]);
		kmr_free(mg_src->done_ikv_lst,
			 sizeof(long) * (size_t)mg_src->done_ikv_lst_size);
		char fpath[KMR_CKPT_PATHLEN];
		snprintf(fpath, KMR_CKPT_PATHLEN, "%s/%s",
			 mg_src->file->dname, mg_src->file->fname);
		unlink(fpath);
		kmr_free(mg_src->file, sizeof(struct kmr_ckpt_data_file));
	    }
	}
    }
    kmr_free(merge_ctx.merges,
	     sizeof(struct kmr_ckpt_merge) * (size_t)max_merge_count);
    for (int i = 0; i < target_rank_count; i++) {
	kmr_free(dataflsts[i],
		 sizeof(struct kmr_ckpt_data_file) * (size_t)nfiles[i]);
	kmr_free(rdpaths[i], sizeof(char) * KMR_CKPT_DIRLEN);
    }
    kmr_free(dataflsts,
	     sizeof(struct kmr_ckpt_data_file *) * (size_t)target_rank_count);
    kmr_free(nfiles, sizeof(int) * (size_t)target_rank_count);
    kmr_free(rdpaths, sizeof(char *) * (size_t)target_rank_count);
}

/* Read previous checkpoint data of a rank and record merge information
   for all mode. */
static void
kmr_ckpt_restore_prev_state_each_rank_all
(KMR *mr, struct kmr_ckpt_prev_state *prev_state,
 struct kmr_ckpt_merge_ctx *merge_ctx)
{
    struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
    char logfile[KMR_CKPT_PATHLEN];
    kmr_ckpt_make_fname(prev_state->ckpt_dir, KMR_CKPT_FNAME_PREFIX,
			KMR_CKPT_LOG, prev_state->prev_rank, 0,
			logfile, sizeof(logfile));
    struct kmr_ckpt_log log_hdr;
    unsigned long total, size = 0;
    FILE *fp = kmr_ckpt_open_log(mr, logfile, &log_hdr, &total);

    long cur_op = 0;
    /* list of ignored inconsistent KVSes */
    struct kmr_ckpt_kvs_chains kvs_chains;
    kmr_ckpt_kvs_chains_init(&kvs_chains);
    /* list of completed spawn key-values */
    struct kmr_ckpt_list spawn_dones;
    /* used for map/reduce & spawn map */
    long nkvi = 0, nkvo = 0;
    /* used for find undeleted kvs */
    long undel_kvs_id = 0;
    struct kmr_ckpt_log_entry last_log = { 0, 0, 0, 0, 0, 0 };
    while (size < total) {
	struct kmr_ckpt_log_entry e;
	size_t rc = fread((void *)&e, sizeof(e), 1, fp);
	if (rc != 1) {
	    char msg[KMR_CKPT_MSGLEN];
	    snprintf(msg, sizeof(msg),
		     "Failed to read a checkpoint log entry");
	    kmr_error(mr, msg);
	}
	struct kmr_ckpt_operation op = { .op_seqno = e.op_seqno,
					 .kvi_id = e.kvi_id,
					 .kvo_id = e.kvo_id };
	switch (e.state) {
	case KMR_CKPT_LOG_WHOLE_START:
	    cur_op = e.op_seqno;
	    if (cur_op <= ckptctx->prev_global_progress) {
		/* Operation can be skipped
		   Procesed in KMR_CKPT_LOG_WHOLE_FINISH */
	    } else { /* cur_op > ckptctx->prev_global_progress */
		if (e.kvi_id == KMR_CKPT_DUMMY_ID) {
		    /* Ignore a kvs generated by kmr_map_once called
		       in this region */
		    kmr_ckpt_merge_ignore_ckpt_data(e.kvo_id, prev_state,
						    merge_ctx);
		    kmr_ckpt_kvs_chains_new_chain(&kvs_chains, op);
		} else {
		    /* If the kvs is generated from a KVS generated by
		       kmr_map_once in this regin, ignore it */
		    int cc = kmr_ckpt_merge_check_ignorable(&kvs_chains,
							    e.kvi_id);
		    if (cc == 0) {
			kmr_ckpt_merge_ignore_ckpt_data(e.kvo_id, prev_state,
							merge_ctx);
			kmr_ckpt_kvs_chains_connect(&kvs_chains, op);
		    }
		}
	    }
	    if (cur_op > ckptctx->prev_progress) {
		last_log = e;
	    }
	    break;
	case KMR_CKPT_LOG_WHOLE_FINISH:
	    assert(e.op_seqno == cur_op);
	    kmr_ckpt_merge_store_ckpt_data(e.kvo_id, mr->rank, -1,
					   prev_state, merge_ctx);
	    cur_op = 0;
	    break;
	case KMR_CKPT_LOG_BLOCK_START:
	    cur_op = e.op_seqno;
	    assert(e.kvi_id != KMR_CKPT_DUMMY_ID);
	    if (cur_op <= ckptctx->prev_global_progress) {
		/* Operation can be skipped
		   Procesed in KMR_CKPT_LOG_BLOCK_FINISH */
	    } else { /* cur_op > ckptctx->prev_global_progress */
		/* If the kvs is generated from a KVS generated by
		   kmr_map_once in this regin, ignore it */
		int cc = kmr_ckpt_merge_check_ignorable(&kvs_chains, e.kvi_id);
		if (cc == 0) {
		    kmr_ckpt_merge_ignore_ckpt_data(e.kvo_id, prev_state,
						    merge_ctx);
		    kmr_ckpt_kvs_chains_connect(&kvs_chains, op);
		}
	    }
	    if (cur_op > ckptctx->prev_progress) {
		last_log = e;
		nkvi = e.n_kvi;
		nkvo = e.n_kvo;
	    }
	    break;
	case KMR_CKPT_LOG_BLOCK_ADD:
	    assert(e.op_seqno == cur_op);
	    if (cur_op > ckptctx->prev_progress) {
		last_log = e;
		nkvi += e.n_kvi;
		nkvo += e.n_kvo;
	    }
	    break;
	case KMR_CKPT_LOG_BLOCK_FINISH:
	    assert(e.op_seqno == cur_op);
	    kmr_ckpt_merge_store_ckpt_data(e.kvo_id, mr->rank, -1,
					   prev_state, merge_ctx);
	    cur_op = 0;
	    if (cur_op > ckptctx->prev_progress) {
		nkvi = 0;
		nkvo = 0;
	    }
	    break;
	case KMR_CKPT_LOG_INDEX_START:
	    cur_op = e.op_seqno;
	    assert(e.kvi_id != KMR_CKPT_DUMMY_ID);
	    if (cur_op <= ckptctx->prev_global_progress) {
		/* Operation can be skipped
		   Procesed in KMR_CKPT_LOG_BLOCK_FINISH */
	    } else { /* cur_op > ckptctx->prev_global_progress */
		/* If the kvs is generated from a KVS generated by
		   kmr_map_once in this regin, ignore it */
		int cc = kmr_ckpt_merge_check_ignorable(&kvs_chains, e.kvi_id);
		if (cc == 0) {
		    kmr_ckpt_merge_ignore_ckpt_data(e.kvo_id, prev_state,
						    merge_ctx);
		    kmr_ckpt_kvs_chains_connect(&kvs_chains, op);
		}
	    }
	    if (cur_op > ckptctx->prev_progress) {
		last_log = e;
		kmr_ckpt_int_list_init(&spawn_dones);
		nkvi = e.n_kvi;
		nkvo = e.n_kvo;
	    }
	    break;
	case KMR_CKPT_LOG_INDEX_ADD:
	    assert(e.op_seqno == cur_op);
	    if (cur_op > ckptctx->prev_progress) {
		last_log = e;
		kmr_ckpt_int_list_add(&spawn_dones, e.n_kvi);
		nkvi += 1;
		nkvo += e.n_kvo;
	    }
	    break;
	case KMR_CKPT_LOG_INDEX_FINISH:
	    assert(e.op_seqno == cur_op);
	    kmr_ckpt_merge_store_ckpt_data(e.kvo_id, mr->rank, -1,
					   prev_state, merge_ctx);
	    cur_op = 0;
	    if (cur_op > ckptctx->prev_progress) {
		kmr_ckpt_int_list_free(&spawn_dones);
		nkvi = 0;
		nkvo = 0;
	    }
	    break;
	case KMR_CKPT_LOG_DELETE_START:
	    undel_kvs_id = e.kvi_id;
	    break;
	case KMR_CKPT_LOG_DELETE_FINISH:
	    assert(e.kvi_id = undel_kvs_id);
	    undel_kvs_id = 0;
	    break;
	case KMR_CKPT_LOG_SKIPPED:
	    kmr_ckpt_merge_store_ckpt_data(e.kvo_id, mr->rank, -1,
					   prev_state, merge_ctx);
	    break;
	}
	size += sizeof(e);
    }
    if (cur_op != 0) {
	/* Process the last log */
	switch (last_log.state) {
	case KMR_CKPT_LOG_WHOLE_START:
	    kmr_ckpt_merge_ignore_ckpt_data(last_log.kvo_id, prev_state,
					    merge_ctx);
	    break;
	case KMR_CKPT_LOG_BLOCK_START:
	    kmr_ckpt_merge_ignore_ckpt_data(last_log.kvo_id, prev_state,
					    merge_ctx);
	    break;
	case KMR_CKPT_LOG_BLOCK_ADD:
	    kmr_ckpt_merge_store_ckpt_data(last_log.kvo_id, mr->rank, nkvo,
					   prev_state, merge_ctx);
	    kmr_ckpt_merge_update_ckpt_data(last_log.kvi_id, mr->rank,
					    last_log.op_seqno, nkvi, NULL,
					    prev_state, merge_ctx);
	    break;
	case KMR_CKPT_LOG_INDEX_START:
	    kmr_ckpt_merge_ignore_ckpt_data(last_log.kvo_id, prev_state,
					    merge_ctx);
	    break;
	case KMR_CKPT_LOG_INDEX_ADD:
	    kmr_ckpt_merge_store_ckpt_data(last_log.kvo_id, mr->rank, nkvo,
					   prev_state, merge_ctx);
	    assert(nkvi >= spawn_dones.size);
	    kmr_ckpt_merge_update_ckpt_data(last_log.kvi_id, mr->rank,
					    last_log.op_seqno, nkvi,
					    &spawn_dones, prev_state,
					    merge_ctx);
	    kmr_ckpt_int_list_free(&spawn_dones);
	    break;
	}
    }
    if (undel_kvs_id != 0) {
	/* Ignore the ckpt data */
	kmr_ckpt_merge_ignore_ckpt_data(undel_kvs_id, prev_state, merge_ctx);
    }
    kmr_ckpt_kvs_chains_free(&kvs_chains);
    fclose(fp);

    for (int i = 0; i < prev_state->dataflst_size; i++) {
	if (prev_state->dataflst[i].checked != 1) {
	    char msg[KMR_CKPT_MSGLEN];
	    snprintf(msg, sizeof(msg),
		     "Checkpoint state is wrong. "
		     "Delete all checkpoint and restart again");
	    kmr_error(mr, msg);
	}
    }
}

/* Read previous checkpoint data of a rank and record merge information
   for selective mode. */
static void
kmr_ckpt_restore_prev_state_each_rank_selective
(KMR *mr, struct kmr_ckpt_prev_state *prev_state,
 struct kmr_ckpt_merge_ctx *merge_ctx)
{
    char logfile[KMR_CKPT_PATHLEN];
    kmr_ckpt_make_fname(prev_state->ckpt_dir, KMR_CKPT_FNAME_PREFIX,
			KMR_CKPT_LOG, prev_state->prev_rank, 0,
			logfile, sizeof(logfile));
    struct kmr_ckpt_log log_hdr;
    unsigned long total, size = 0;
    FILE *fp = kmr_ckpt_open_log(mr, logfile, &log_hdr, &total);

    /* stores id of kvses whose ckpt data files should be deleted */
    struct kmr_ckpt_list kvses;
    kmr_ckpt_int_list_init(&kvses);
    while (size < total) {
	struct kmr_ckpt_log_entry e;
	size_t rc = fread((void *)&e, sizeof(e), 1, fp);
	if (rc != 1) {
	    char msg[KMR_CKPT_MSGLEN];
	    snprintf(msg, sizeof(msg),
		     "Failed to read a checkpoint log entry");
	    kmr_error(mr, msg);
	}
	switch (e.state) {
	case KMR_CKPT_LOG_DELETABLE:
	    kmr_ckpt_int_list_add(&kvses, e.kvo_id);
	    break;
	}
	size += sizeof(e);
    }
    struct kmr_ckpt_data_file *dataflst = prev_state->dataflst;
    for (int i = 0; i < prev_state->dataflst_size; i++) {
	struct kmr_ckpt_data_file *file = &dataflst[i];
	long v = kmr_ckpt_int_list_rsearch(&kvses, file->kvs_id);
	if (v == file->kvs_id) {
	    /* ignore should-be-deleted checkpoint data */
	    kmr_ckpt_merge_ignore_ckpt_data(file->kvs_id, prev_state,
					    merge_ctx);
	} else {
	    kmr_ckpt_merge_store_ckpt_data(file->kvs_id, mr->rank, -1,
					   prev_state, merge_ctx);
	}
    }
    kmr_ckpt_int_list_free(&kvses);
    fclose(fp);

    for (int i = 0; i < prev_state->dataflst_size; i++) {
	if (prev_state->dataflst[i].checked != 1) {
	    char msg[KMR_CKPT_MSGLEN];
	    snprintf(msg, sizeof(msg),
		     "Checkpoint state is wrong. "
		     "Delete all checkpoint and restart again");
	    kmr_error(mr, msg);
	}
    }
}

/* Read previous checkpoint data of a rank and record merge information. */
static void
kmr_ckpt_restore_prev_state_each_rank(KMR *mr,
				      struct kmr_ckpt_prev_state *prev_state,
				      struct kmr_ckpt_merge_ctx *merge_ctx)
{
    if (!mr->ckpt_selective) {
	kmr_ckpt_restore_prev_state_each_rank_all(mr, prev_state, merge_ctx);
    } else {
	kmr_ckpt_restore_prev_state_each_rank_selective(mr, prev_state,
							merge_ctx);
    }
}

/* It checks if KVI_ID is contained in kvs_chains.
   If KVI_ID is contained it returns 0, otherwise it returns -1. */
static int
kmr_ckpt_merge_check_ignorable(struct kmr_ckpt_kvs_chains *chains, long kvi_id)
{
    struct kmr_ckpt_list *c = kmr_ckpt_kvs_chains_find(chains, kvi_id);
    if (c != 0) {
	return 0;
    } else {
	return -1;
    }
}

static struct kmr_ckpt_data_file *
kmr_ckpt_find_data_file(long kvs_id,
			struct kmr_ckpt_data_file *dataflst, int nfiles)
{
    struct kmr_ckpt_data_file *file = 0;
    for (int i = 0; i < nfiles; i++) {
	if (dataflst[i].kvs_id == kvs_id) {
	    file = &dataflst[i];
	    break;
	}
    }
    return file;
}

/* Marks the ckpt data as not-to-be-merged. */
static void
kmr_ckpt_merge_ignore_ckpt_data(long kvo_id,
				struct kmr_ckpt_prev_state *prev_state,
				struct kmr_ckpt_merge_ctx *merge_ctx)
{
    struct kmr_ckpt_data_file *file =
	kmr_ckpt_find_data_file(kvo_id, prev_state->dataflst,
				prev_state->dataflst_size);
    if (file == 0) {
	return;
    }
    file->checked = 1;

    /* If the ckpt data is once marked as 'to-be-merged',
       remove this ckpt data from merge source list. */
    if (file->merged == 1) {
	struct kmr_ckpt_merge *merge = 0;
	for (int i = 0; i < merge_ctx->merges_count; i++) {
	    if (merge_ctx->merges[i].kvs_id == kvo_id) {
		merge = &merge_ctx->merges[i];
		break;
	    }
	}
	assert(merge != 0);
	int idx = -1;
	struct kmr_ckpt_merge_source *mg_src = 0;
	for (int i = 0; i < merge->src_lst_count; i++) {
	    if (merge->src_lst[i].rank == prev_state->prev_rank) {
		idx = i;
		mg_src = &merge->src_lst[i];
		break;
	    }
	}
	assert(idx != -1 && mg_src != 0);
	/* Delete the found mg_src */
	if (mg_src->done_ikv_lst_size != 0) {
	    kmr_free(mg_src->done_ikv_lst,
		     sizeof(long) * (size_t)mg_src->done_ikv_lst_size);
	}
	if (merge->src_lst_count == 1) {
	    memset(mg_src, 0, sizeof(struct kmr_ckpt_merge_source));
	} else {
	    for (int i = idx; i < merge->src_lst_count - 1; i++) {
		struct kmr_ckpt_merge_source *target = &merge->src_lst[i];
		struct kmr_ckpt_merge_source *source = &merge->src_lst[i + 1];
		memcpy(target, source, sizeof(struct kmr_ckpt_merge_source));
	    }
	    memset(&merge->src_lst[merge->src_lst_count - 1], 0,
		   sizeof(struct kmr_ckpt_merge_source));
	}
	merge->src_lst_count -= 1;
    }
    file->merged = 0;
}

/* Marks the ckpt data as to-be-merged. */
static void
kmr_ckpt_merge_store_ckpt_data(long kvo_id, int rank, long nkvo,
			       struct kmr_ckpt_prev_state *prev_state,
			       struct kmr_ckpt_merge_ctx *merge_ctx)
{
    struct kmr_ckpt_data_file *file =
	kmr_ckpt_find_data_file(kvo_id, prev_state->dataflst,
				prev_state->dataflst_size);
    if (file == 0 || file->merged == 1) {
	return;
    }
    file->checked = 1;
    file->merged = 1;

    struct kmr_ckpt_merge *merge = 0;
    int cnt = merge_ctx->merges_count;
    for (int i = 0; i < cnt; i++) {
	if (merge_ctx->merges[i].kvs_id == kvo_id) {
	    merge = &merge_ctx->merges[i];
	    break;
	}
    }
    if (merge == 0) {
	/* Initialize a new merge */
	merge = &merge_ctx->merges[cnt++];
	merge->rank = rank;
	merge->kvs_id = kvo_id;
	merge->src_lst = (struct kmr_ckpt_merge_source *)
	    kmr_malloc(sizeof(struct kmr_ckpt_merge_source) * (size_t)merge_ctx->max_each_merge);
	merge->src_lst_count = 0;
	merge_ctx->merges_count = cnt;
    }
    struct kmr_ckpt_merge_source *mg_src = &merge->src_lst[merge->src_lst_count];
    mg_src->rank = prev_state->prev_rank;
    mg_src->n_kvi = -1;
    mg_src->n_kvo = nkvo;
    mg_src->done_ikv_lst = 0;
    mg_src->done_ikv_lst_size = 0;
    mg_src->kvi_op_seqno = -1;
    mg_src->file = file;
    merge->src_lst_count += 1;
}

static int
kmr_ckpt_cmp_long(const void *v1, const void *v2)
{
    long _v1 = *((long *)v1);
    long _v2 = *((long *)v2);
    if ( _v1 > _v2 ) {
	return 1;
    } else if ( _v1 < _v2 ) {
	return -1;
    } else {
	return 0;
    }
}

/* Update information of ckpt data merge. */
static void
kmr_ckpt_merge_update_ckpt_data(long kvi_id, int rank,
				long kvi_op_seqno, long nkvi,
				struct kmr_ckpt_list *done_ikv_lst,
				struct kmr_ckpt_prev_state *prev_state,
				struct kmr_ckpt_merge_ctx *merge_ctx)
{
    struct kmr_ckpt_data_file *file =
	kmr_ckpt_find_data_file(kvi_id, prev_state->dataflst,
				prev_state->dataflst_size);
    assert(file != 0);
    assert(file->checked == 1 && file->merged == 1);

    struct kmr_ckpt_merge *merge = 0;
    for (int i = 0; i < merge_ctx->merges_count; i++) {
	if (merge_ctx->merges[i].kvs_id == kvi_id) {
	    merge = &merge_ctx->merges[i];
	    break;
	}
    }
    assert(merge != 0);
    struct kmr_ckpt_merge_source *mg_src = 0;
    for (int i = 0; i < merge->src_lst_count; i++) {
	if (merge->src_lst[i].rank == prev_state->prev_rank) {
	    mg_src = &merge->src_lst[i];
	    break;
	}
    }
    assert(mg_src != 0);
    assert(mg_src->n_kvo == -1);
    mg_src->n_kvi = nkvi;
    mg_src->kvi_op_seqno = kvi_op_seqno;
    if (done_ikv_lst->size != 0) {
	mg_src->done_ikv_lst =
	    (long *)kmr_malloc(sizeof(long) * (size_t)done_ikv_lst->size);
	struct kmr_ckpt_list_item *item;
	int idx = 0;
	for (item = done_ikv_lst->head; item != 0; item = item->next) {
	    mg_src->done_ikv_lst[idx] = *(long *)item->val;
	    idx += 1;
	}
	qsort(mg_src->done_ikv_lst, (size_t)done_ikv_lst->size, sizeof(long),
	      kmr_ckpt_cmp_long);
	mg_src->done_ikv_lst_size = done_ikv_lst->size;
    }
}

/* Sort a previous checkpoint data of a kvs so that processed key-values
   are moved to the front and unprocessed ones are moved to the back.
   It creates a new checkpoint data and replaces pointer to data. */
static void
kmr_ckpt_merge_sort_data(KMR *mr, const char *wdpath, long kvs_id,
			 struct kmr_ckpt_merge_source *mrg_src)
{
    assert(mrg_src->file->merged == 1);
    struct kmr_ckpt_data_file *ndata = (struct kmr_ckpt_data_file *)
	kmr_malloc(sizeof(struct kmr_ckpt_data_file));
    ndata->kvs_id = kvs_id;
    ndata->checked = 1;
    ndata->merged = 1;
    snprintf(ndata->fname, sizeof(ndata->fname), "%s.sorted",
	     mrg_src->file->fname);
    strncpy(ndata->dname, wdpath, sizeof(ndata->dname) - 1);

    char dst_fpath[KMR_CKPT_PATHLEN];
    snprintf(dst_fpath, KMR_CKPT_PATHLEN, "%s/%s", ndata->dname, ndata->fname);
    int cc = access(dst_fpath, F_OK);
    assert(cc != 0);
    FILE *wfp = kmr_ckpt_open_path(mr, dst_fpath, "w");

    char tmp_fpath[KMR_CKPT_PATHLEN];
    snprintf(tmp_fpath, KMR_CKPT_PATHLEN, "%s/%s.rest",
	     ndata->dname, ndata->fname);
    cc = access(tmp_fpath, F_OK);
    assert(cc != 0);

    char src_fpath[KMR_CKPT_PATHLEN];
    snprintf(src_fpath, KMR_CKPT_PATHLEN, "%s/%s",
	     mrg_src->file->dname, mrg_src->file->fname);
    struct stat sb;
    cc = stat(src_fpath, &sb);
    if (cc != 0) {
	char msg[KMR_CKPT_MSGLEN];
	snprintf(msg, sizeof(msg),
		 "Failed to access a checkpoint data file %s", src_fpath);
	kmr_error(mr, msg);
    }
    FILE *rfp = kmr_ckpt_open_path(mr, src_fpath, "r");

    /* Write header */
    size_t hdrsiz = offsetof(struct kmr_ckpt_data, data);
    struct kmr_ckpt_data hdr;
    size_t rc = fread((void *)&hdr, hdrsiz, 1, rfp);
    if (rc != 1) {
	char msg[KMR_CKPT_MSGLEN];
	snprintf(msg, sizeof(msg),
		 "Failed to read a checkpoint data file %s", src_fpath);
	kmr_error(mr, msg);
    }
    rc = fwrite((void *)&hdr, hdrsiz, 1, wfp);
    if (rc != 1) {
	char msg[KMR_CKPT_MSGLEN];
	snprintf(msg, sizeof(msg),
		 "Failed to write a checkpoint data file %s", dst_fpath);
	kmr_error(mr, msg);
    }

    /* Write kv */
    size_t total_size = (size_t)sb.st_size - hdrsiz;
    size_t cur_size = 0;
    /* Write processed kv */
    {
	size_t read_size = 0;
	long idx = 0, start_idx = 0;
	size_t bufsiz = 8;
	void *buf = kmr_malloc(bufsiz);
	FILE *wfp2 = kmr_ckpt_open_path(mr, tmp_fpath, "w");
	while (read_size < total_size) {
	    struct kmr_kvs_entry e;
	    /* Read */
	    size_t kv_hdrsiz = offsetof(struct kmr_kvs_entry, c);
	    rc = fread((void *)&e, kv_hdrsiz, 1, rfp);
	    if (rc != 1) {
		char msg[KMR_CKPT_MSGLEN];
		snprintf(msg, sizeof(msg),
			 "Failed to read a checkpoint data file %s", src_fpath);
		kmr_error(mr, msg);
	    }
	    size_t kv_bdysiz =
		(size_t)KMR_ALIGN(e.klen) + (size_t)KMR_ALIGN(e.vlen);
	    if (bufsiz < kv_bdysiz) {
		bufsiz = kv_bdysiz;
		buf = kmr_realloc(buf, bufsiz);
	    }
	    rc = fread(buf, kv_bdysiz, 1, rfp);
	    if (rc != 1) {
		char msg[KMR_CKPT_MSGLEN];
		snprintf(msg, sizeof(msg),
			 "Failed to read a checkpoint data file %s", src_fpath);
		kmr_error(mr, msg);
	    }
	    /* Write */
	    FILE *twfp;
	    _Bool incp = 0;
	    for (long i = start_idx; i < mrg_src->done_ikv_lst_size; i++) {
		if (idx == mrg_src->done_ikv_lst[i]) {
		    incp = 1;
		    break;
		}
	    }
	    if (incp) {
		twfp = wfp;
		cur_size += kv_hdrsiz + kv_bdysiz;
		start_idx++;
	    } else {
		twfp = wfp2;
	    }
	    rc = fwrite((void *)&e, kv_hdrsiz, 1, twfp);
	    if (rc != 1) {
		char msg[KMR_CKPT_MSGLEN];
		snprintf(msg, sizeof(msg),
			 "Failed to write a checkpoint data file %s",
			 dst_fpath);
		kmr_error(mr, msg);
	    }
	    rc = fwrite(buf, kv_bdysiz, 1, twfp);
	    if (rc != 1) {
		char msg[KMR_CKPT_MSGLEN];
		snprintf(msg, sizeof(msg),
			 "Failed to write a checkpoint data file %s",
			 dst_fpath);
		kmr_error(mr, msg);
	    }
	    read_size += kv_hdrsiz + kv_bdysiz;
	    idx++;
	}
	kmr_free(buf, bufsiz);
	kmr_ckpt_flush(mr, wfp2);
	fclose(wfp2);
    }
    fclose(rfp);

    /* Write unprocessed kv */
    cc = stat(tmp_fpath, &sb);
    if (cc != 0) {
	char msg[KMR_CKPT_MSGLEN];
	snprintf(msg, sizeof(msg),
		 "Failed to access a checkpoint data file %s", tmp_fpath);
	kmr_error(mr, msg);
    }
    rfp = kmr_ckpt_open_path(mr, tmp_fpath, "r");
    void *buf = kmr_malloc((size_t)sb.st_size);
    rc = fread(buf, (size_t)sb.st_size, 1, rfp);
    if (rc != 1) {
	char msg[KMR_CKPT_MSGLEN];
	snprintf(msg, sizeof(msg),
		 "Failed to read a checkpoint data file %s", tmp_fpath);
	kmr_error(mr, msg);
    }
    rc = fwrite(buf, (size_t)sb.st_size, 1, wfp);
    if (rc != 1) {
	char msg[KMR_CKPT_MSGLEN];
	snprintf(msg, sizeof(msg),
		 "Failed to write a checkpoint data file %s", dst_fpath);
	kmr_error(mr, msg);
    }
    kmr_free(buf, (size_t)sb.st_size);
    assert((cur_size + (size_t)sb.st_size) == total_size);
    fclose(rfp);
    unlink(tmp_fpath);
    kmr_ckpt_flush(mr, wfp);
    fclose(wfp);

    mrg_src->file = ndata;
}

/* Merge checkpoint data files */
static void
kmr_ckpt_merge_write_file(KMR *mr, const char *wdpath,
			  struct kmr_ckpt_merge *merge)
{
    char dst_fpath[KMR_CKPT_PATHLEN];
    kmr_ckpt_make_fname(wdpath, KMR_CKPT_FNAME_PREFIX,
			KMR_CKPT_DATA, merge->rank, merge->kvs_id,
			dst_fpath, sizeof(dst_fpath));
    int cc = access(dst_fpath, F_OK);
    assert(cc != 0);
    FILE *wfp = kmr_ckpt_open_path(mr, dst_fpath, "w");

    /* Write header */
    char hdr_src_fpath[KMR_CKPT_PATHLEN];
    snprintf(hdr_src_fpath, KMR_CKPT_PATHLEN, "%s/%s",
	     merge->src_lst[0].file->dname, merge->src_lst[0].file->fname);
    FILE *rfp = kmr_ckpt_open_path(mr, hdr_src_fpath, "r");
    size_t hdrsiz = offsetof(struct kmr_ckpt_data, data);
    /* Update nprocs and rank in header */
    struct kmr_ckpt_data hdr;
    size_t rc = fread((void *)&hdr, hdrsiz, 1, rfp);
    if (rc != 1) {
	char msg[KMR_CKPT_MSGLEN];
	snprintf(msg, sizeof(msg),
		 "Failed to read a checkpoint data file %s", hdr_src_fpath);
	kmr_error(mr, msg);
    }
    hdr.nprocs = mr->nprocs;
    hdr.rank = mr->rank;
    rc = fwrite((void *)&hdr, hdrsiz, 1, wfp);
    if (rc != 1) {
	char msg[KMR_CKPT_MSGLEN];
	snprintf(msg, sizeof(msg),
		 "Failed to write a checkpoint data file %s", dst_fpath);
	kmr_error(mr, msg);
    }
    fclose(rfp);

    /* Open to-be-merged files */
    struct merge_file {
	FILE *fp;
	size_t size;      /* total key-value size in byte */
	size_t cur_size;  /* read key-value size in byte */
    };
    struct merge_file *mfs = (struct merge_file *)
	kmr_malloc(sizeof(struct merge_file) * (size_t)merge->src_lst_count);
    for (int i = 0; i < merge->src_lst_count; i++) {
	struct kmr_ckpt_data_file *file = merge->src_lst[i].file;
	assert(file->merged == 1);
	char fpath[KMR_CKPT_PATHLEN];
	snprintf(fpath, KMR_CKPT_PATHLEN, "%s/%s", file->dname, file->fname);
	struct stat sb;
	cc = stat(fpath, &sb);
	if (cc != 0) {
	    char msg[KMR_CKPT_MSGLEN];
	    snprintf(msg, sizeof(msg),
		     "Failed to access a checkpoint data file %s", fpath);
	    kmr_error(mr, msg);
	}
	hdrsiz = offsetof(struct kmr_ckpt_data, data);
	mfs[i].fp = kmr_ckpt_open_path(mr, fpath, "r");
	mfs[i].size = (size_t)sb.st_size - hdrsiz;
	mfs[i].cur_size = 0;
	fseek(mfs[i].fp, (long)hdrsiz, SEEK_SET);
    }

    /* Write processed kv when used as kvi */
    for (int i = 0; i < merge->src_lst_count; i++) {
	struct kmr_ckpt_merge_source *mg_src = &merge->src_lst[i];
	if (mg_src->n_kvi > 0) {
	    assert(mg_src->n_kvo == -1);
	    long kvicnt = 0;
	    size_t bufsiz = 8;
	    unsigned char *buf = (unsigned char *)kmr_malloc(bufsiz);
	    while (mfs[i].cur_size < mfs[i].size) {
		if (kvicnt >= mg_src->n_kvi) {
		    break;
		}
		struct kmr_kvs_entry e;
		/* Read */
		size_t kv_hdrsiz = offsetof(struct kmr_kvs_entry, c);
		rc = fread((void *)&e, kv_hdrsiz, 1, mfs[i].fp);
		if (rc != 1) {
		    char msg[KMR_CKPT_MSGLEN];
		    snprintf(msg, sizeof(msg),
			     "Failed to read a checkpoint data file");
		    kmr_error(mr, msg);
		}
		size_t kv_bdysiz =
		    (size_t)KMR_ALIGN(e.klen) + (size_t)KMR_ALIGN(e.vlen);
		if (bufsiz < kv_bdysiz) {
		    bufsiz = kv_bdysiz;
		    buf = (unsigned char *)kmr_realloc(buf, bufsiz);
		}
		rc = fread((void *)buf, kv_bdysiz, 1, mfs[i].fp);
		if (rc != 1) {
		    char msg[KMR_CKPT_MSGLEN];
		    snprintf(msg, sizeof(msg),
			     "Failed to read a checkpoint data file");
		    kmr_error(mr, msg);
		}
		/* Write */
		rc = fwrite((void *)&e, kv_hdrsiz, 1, wfp);
		if (rc != 1) {
		    char msg[KMR_CKPT_MSGLEN];
		    snprintf(msg, sizeof(msg),
			     "Failed to write a checkpoint data file %s",
			     dst_fpath);
		    kmr_error(mr, msg);
		}
		rc = fwrite((void *)buf, kv_bdysiz, 1, wfp);
		if (rc != 1) {
		    char msg[KMR_CKPT_MSGLEN];
		    snprintf(msg, sizeof(msg),
			     "Failed to write a checkpoint data file %s",
			     dst_fpath);
		    kmr_error(mr, msg);
		}
		kvicnt += 1;
		mfs[i].cur_size += kv_hdrsiz + kv_bdysiz;
	    }
	    kmr_free(buf, bufsiz);
	}
    }

    /* Write generated kv when used as kvo */
    for (int i = 0; i < merge->src_lst_count; i++) {
	struct kmr_ckpt_merge_source *mg_src = &merge->src_lst[i];
	long kvocnt = 0;
	size_t bufsiz = 8;
	unsigned char *buf = (unsigned char *)kmr_malloc(bufsiz);
	while (mfs[i].cur_size < mfs[i].size) {
	    if ((mg_src->n_kvo >= 0) && (kvocnt >= mg_src->n_kvo)) {
		break;
	    }
	    struct kmr_kvs_entry e;
	    /* Read */
	    size_t kv_hdrsiz = offsetof(struct kmr_kvs_entry, c);
	    rc = fread((void *)&e, kv_hdrsiz, 1, mfs[i].fp);
	    if (rc != 1) {
		char msg[KMR_CKPT_MSGLEN];
		snprintf(msg, sizeof(msg),
			 "Failed to read a checkpoint data file");
		kmr_error(mr, msg);
	    }
	    size_t kv_bdysiz =
		(size_t)KMR_ALIGN(e.klen) + (size_t)KMR_ALIGN(e.vlen);
	    if (bufsiz < kv_bdysiz) {
		bufsiz = kv_bdysiz;
		buf = (unsigned char *)kmr_realloc(buf, bufsiz);
	    }
	    rc = fread((void *)buf, kv_bdysiz, 1, mfs[i].fp);
	    if (rc != 1) {
		char msg[KMR_CKPT_MSGLEN];
		snprintf(msg, sizeof(msg),
			 "Failed to read a checkpoint data file");
		kmr_error(mr, msg);
	    }
	    /* Write */
	    rc = fwrite((void *)&e, kv_hdrsiz, 1, wfp);
	    if (rc != 1) {
		char msg[KMR_CKPT_MSGLEN];
		snprintf(msg, sizeof(msg),
			 "Failed to write a checkpoint data file %s",
			 dst_fpath);
		kmr_error(mr, msg);
	    }
	    rc = fwrite((void *)buf, kv_bdysiz, 1, wfp);
	    if (rc != 1) {
		char msg[KMR_CKPT_MSGLEN];
		snprintf(msg, sizeof(msg),
			 "Failed to write a checkpoint data file %s",
			 dst_fpath);
		kmr_error(mr, msg);
	    }
	    kvocnt += 1;
	    mfs[i].cur_size += kv_hdrsiz + kv_bdysiz;
	}
	kmr_free(buf, bufsiz);
    }

    for (int i = 0; i < merge->src_lst_count; i++) {
	fclose(mfs[i].fp);
    }
    kmr_free(mfs, sizeof(struct merge_file) * (size_t)merge->src_lst_count);
    kmr_ckpt_flush(mr, wfp);
    fclose(wfp);
}

/***************************************************************/
/*  Functions for logging                                      */
/***************************************************************/

/* Initialize checkpoint log file */
static void
kmr_ckpt_init_log(KMR *mr, const char *log_fpath)
{
    struct kmr_ckpt_log ckptld;
    memset((void *)&ckptld, 0, sizeof(ckptld));
    if (mr->ckpt_selective) {
	ckptld.mode = KMR_CKPT_SELECTIVE;
    } else {
	ckptld.mode = KMR_CKPT_ALL;
    }
    ckptld.nprocs = mr->nprocs;
    ckptld.rank = mr->rank;
    FILE *fp = kmr_ckpt_open_path(mr, log_fpath, "w");
    size_t size = offsetof(struct kmr_ckpt_log, data);
    size_t ret = fwrite((void *)&ckptld, size, 1, fp);
    if (ret != 1) {
	char msg[KMR_CKPT_MSGLEN];
	snprintf(msg, sizeof(msg),
		 "Failed to write header of checkpoint log %s", log_fpath);
	kmr_error(mr, msg);
    }
    kmr_ckpt_flush(mr, fp);
    mr->ckpt_ctx->ckpt_log_fp = fp;
}

static void
kmr_ckpt_fin_log(KMR *mr)
{
    fclose(mr->ckpt_ctx->ckpt_log_fp);
}

static inline void
kmr_ckpt_save_log_raw(KMR *mr, struct kmr_ckpt_log_entry *ckptle)
{
    struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
    size_t ret = fwrite((void *)ckptle, sizeof(struct kmr_ckpt_log_entry), 1,
			ckptctx->ckpt_log_fp);
    if (ret != 1) {
	char msg[KMR_CKPT_MSGLEN];
	snprintf(msg, sizeof(msg), "Failed to add checkpoint log");
	kmr_error(mr, msg);
    }
    kmr_ckpt_flush(mr, ckptctx->ckpt_log_fp);
}

static inline void
kmr_ckpt_save_log2(KMR *mr, int state)
{
    struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
    struct kmr_ckpt_log_entry ckptle;
    ckptle.op_seqno = ckptctx->progress_counter;
    ckptle.kvi_id   = ckptctx->cur_kvi_id;
    ckptle.kvo_id   = ckptctx->cur_kvo_id;
    ckptle.state    = state;
    ckptle.n_kvi    = -1;
    ckptle.n_kvo    = -1;
    kmr_ckpt_save_log_raw(mr, &ckptle);
}

static inline void
kmr_ckpt_save_log4(KMR *mr, int state, long nkvi, long nkvo)
{
    struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
    struct kmr_ckpt_log_entry ckptle;
    ckptle.op_seqno = ckptctx->progress_counter;
    ckptle.kvi_id   = ckptctx->cur_kvi_id;
    ckptle.kvo_id   = ckptctx->cur_kvo_id;
    ckptle.state    = state;
    ckptle.n_kvi    = nkvi;
    ckptle.n_kvo    = nkvo;
    kmr_ckpt_save_log_raw(mr, &ckptle);
}

static inline void
kmr_ckpt_save_log_del(KMR *mr, int state, long kvs_id)
{
    struct kmr_ckpt_log_entry ckptle;
    ckptle.op_seqno = -1;
    ckptle.kvi_id   = kvs_id;
    ckptle.kvo_id   = kvs_id;
    ckptle.state    = state;
    ckptle.n_kvi    = -1;
    ckptle.n_kvo    = -1;
    kmr_ckpt_save_log_raw(mr, &ckptle);
}

static inline void
kmr_ckpt_save_log_lock(KMR *mr, int state)
{
    struct kmr_ckpt_log_entry ckptle;
    ckptle.op_seqno = -1;
    ckptle.kvi_id   = KMR_CKPT_DUMMY_ID;
    ckptle.kvo_id   = KMR_CKPT_DUMMY_ID;
    ckptle.state    = state;
    ckptle.n_kvi    = -1;
    ckptle.n_kvo    = -1;
    kmr_ckpt_save_log_raw(mr, &ckptle);
}

/* Log the start of kvs operation.
   \param[in]  mr  MapReduce data type
*/
static void
kmr_ckpt_log_whole_start(KMR *mr)
{
    kmr_ckpt_save_log2(mr, KMR_CKPT_LOG_WHOLE_START);
}

/* Log the end of kvs operation.
   \param[in]  mr  MapReduce data type
*/
static void
kmr_ckpt_log_whole_finish(KMR *mr)
{
    kmr_ckpt_save_log2(mr, KMR_CKPT_LOG_WHOLE_FINISH);
}

/* Log the start of kvs operation.
   \param[in]  mr  MapReduce data type
   \param[in]  kvo output KVS
*/
static void
kmr_ckpt_log_block_start(KMR *mr, KMR_KVS *kvo)
{
    long nkvi = kmr_ckpt_first_unprocessed_kv(mr);
    long nkvo = (kvo == 0) ? 0 : kvo->c.element_count;
    kmr_ckpt_save_log4(mr, KMR_CKPT_LOG_BLOCK_START, nkvi, nkvo);
}

/* Log the progress of kvs operation.
   \param[in]  mr   MapReduce data type
   \param[in]  nkvi number of processed kv in kvi
   \param[in]  nkvo number of generated kv in kvo
*/
static void
kmr_ckpt_log_block_add(KMR *mr, long nkvi, long nkvo)
{
    kmr_ckpt_save_log4(mr, KMR_CKPT_LOG_BLOCK_ADD, nkvi, nkvo);
}

/* Log the end of kvs operation.
   \param[in]  mr  MapReduce data type
   \param[in]  kvo output KVS
*/
static void
kmr_ckpt_log_block_finish(KMR *mr)
{
    kmr_ckpt_save_log2(mr, KMR_CKPT_LOG_BLOCK_FINISH);
}

/* Log the start of kvs operation.
   \param[in]  mr  MapReduce data type
   \param[in]  kvo output KVS
*/
static void
kmr_ckpt_log_index_start(KMR *mr, KMR_KVS *kvo)
{
    long nkvi = kmr_ckpt_first_unprocessed_kv(mr);
    long nkvo = (kvo == 0) ? 0 : kvo->c.element_count;
    kmr_ckpt_save_log4(mr, KMR_CKPT_LOG_INDEX_START, nkvi, nkvo);
}

/* Log the progress of kvs operation.
   \param[in]  mr        MapReduce data type
   \param[in]  ikv_index index of processed kv in kvi
   \param[in]  nkvo      number of generated kv in kvo
*/
static void
kmr_ckpt_log_index_add(KMR *mr, long ikv_index, long nkvo)
{
    kmr_ckpt_save_log4(mr, KMR_CKPT_LOG_INDEX_ADD, ikv_index, nkvo);
}

/* Log the end of kvs operation.
   \param[in]  mr  MapReduce data type
*/
static void
kmr_ckpt_log_index_finish(KMR *mr)
{
    kmr_ckpt_save_log2(mr, KMR_CKPT_LOG_INDEX_FINISH);
}

/* Log the start of kvs delete.
   \param[in]  mr      MapReduce data type
   \param[in]  kvs_id  ID of target KVS
*/
static void
kmr_ckpt_log_delete_start(KMR *mr, long kvs_id)
{
    kmr_ckpt_save_log_del(mr, KMR_CKPT_LOG_DELETE_START, kvs_id);
}

/* Log the end of kvs delete.
   \param[in]  mr      MapReduce data type
   \param[in]  kvs_id  ID of target KVS
*/
static void
kmr_ckpt_log_delete_finish(KMR *mr, long kvs_id)
{
    kmr_ckpt_save_log_del(mr, KMR_CKPT_LOG_DELETE_FINISH, kvs_id);
}

/* Log that the spcified kvs can be deleted.
   (used in selective mode)
   \param[in]  mr      MapReduce data type
   \param[in]  kvs_id  ID of target KVS
*/
static void
kmr_ckpt_log_deletable(KMR *mr, long kvs_id)
{
    kmr_ckpt_save_log_del(mr, KMR_CKPT_LOG_DELETABLE, kvs_id);
}

/* Log the progress of kvs operation.
   \param[in]  mr      MapReduce data type
*/
static void
kmr_ckpt_log_progress(KMR *mr)
{
    struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
    if (!(ckptctx->cur_kvi_id == KMR_CKPT_DUMMY_ID &&
	  ckptctx->cur_kvo_id == KMR_CKPT_DUMMY_ID) ) {
	kmr_ckpt_save_log2(mr, KMR_CKPT_LOG_PROGRESS);
    }
}

/* Log the skip of kvs operation.
   \param[in]  mr  MapReduce data type
*/
static void
kmr_ckpt_log_skipped(KMR *mr)
{
    kmr_ckpt_save_log2(mr, KMR_CKPT_LOG_SKIPPED);
}

/** Define the start position of code region that is referred when restart.
    If an execution is stopped due to an error in this region, restart with
    the different number of processes is not allowed.

    \param[in]  mr  MapReduce data type
*/
void kmr_ckpt_lock_start(KMR *mr)
{
    kmr_ckpt_save_log_lock(mr, KMR_CKPT_LOG_LOCK_START);
}

/** Define the end position of code region that is referred when restart.
    If an execution is stopped due to an error in this region, restart with
    the different number of processes is not allowed.

    \param[in]  mr  MapReduce data type
*/
void kmr_ckpt_lock_finish(KMR *mr)
{
    kmr_ckpt_save_log_lock(mr, KMR_CKPT_LOG_LOCK_FINISH);
}

static FILE *
kmr_ckpt_open_log(KMR *mr, const char *path, struct kmr_ckpt_log *log_hdr,
		  unsigned long *size)
{
    struct stat sb;
    int cc = stat(path, &sb);
    if (cc != 0) {
	char msg[KMR_CKPT_MSGLEN];
	snprintf(msg, sizeof(msg),
		 "Failed to access a checkpoint log %s", path);
	kmr_error(mr, msg);
    }
    FILE *fp = kmr_ckpt_open_path(mr, path, "r");
    size_t hdrsz = offsetof(struct kmr_ckpt_log, data);
    size_t rc = fread((void *)log_hdr, hdrsz, 1, fp);
    if (rc != 1) {
	char msg[KMR_CKPT_MSGLEN];
	snprintf(msg, sizeof(msg),
		 "Failed to read a checkpoint log %s", path);
	kmr_error(mr, msg);
    }
    assert(sb.st_size >= 0);
    assert((size_t)sb.st_size >= hdrsz);
    *size = (size_t)sb.st_size - hdrsz;
    return fp;
}

/***************************************************************/
/*  Functions for checkpoint data management                   */
/***************************************************************/

/* It returns 1 if a checkpoint data file should be written to disk.
   Otherwise it returns 0. */
static _Bool
kmr_ckpt_write_file_p(KMR *mr)
{
    assert(mr->ckpt_enable);
    if (mr->ckpt_selective && !mr->ckpt_ctx->slct_cur_take_ckpt) {
	return 0;
    } else {
	return 1;
    }
}

/* This deletes a checkpoint data file of the specified kvs */
static void
kmr_ckpt_delete_ckpt_data(KMR *mr, long kvs_id)
{
    char fpath[KMR_CKPT_PATHLEN];
    struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
    kmr_ckpt_make_fname(ckptctx->ckpt_dname, KMR_CKPT_FNAME_PREFIX,
			KMR_CKPT_DATA, mr->rank, kvs_id, fpath, sizeof(fpath));
    int cc = access(fpath, F_OK);
    if (cc == 0) {
	kmr_ckpt_log_delete_start(mr, kvs_id);
	cc = unlink(fpath);
	assert(cc == 0);
	kmr_ckpt_log_delete_finish(mr, kvs_id);
    } else {
	/* checkpoint file does not exist. do nothing. */
    }
}

/* This deletes checkpoint files in specified directory.
   It also deletes the directory if it becomes empty.
   If the directory does not exist, it does nothing. */
static void
kmr_ckpt_delete_ckpt_files(KMR *mr, const char *target_dir, int rank)
{
    struct stat sb;
    int cc = stat(target_dir, &sb);
    if (cc == 0) {
	if (!S_ISDIR(sb.st_mode)) {
	    char msg[KMR_CKPT_MSGLEN];
	    snprintf(msg, sizeof(msg),
		     "File %s should not exist or "
		     "if exists, shoud be a directory.", target_dir);
	    kmr_error(mr, msg);
	}
    } else {
	return; /* directory does not exist. */
    }

    /* Remove all checkpoint data file */
    struct kmr_ckpt_data_file *dataflst = NULL;
    int nfiles;
    kmr_ckpt_get_data_flist(mr, target_dir, &dataflst, &nfiles, 0);
    for (int i = 0; i < nfiles; i++) {
	char fpath[KMR_CKPT_PATHLEN];
	snprintf(fpath, sizeof(fpath), "%s/%s", target_dir, dataflst[i].fname);
	cc = access(fpath, F_OK);
	if (cc == 0) {
	    unlink(fpath);
	} else {
	    fprintf(stderr,
		    "Failed to delete checkpoint file %s on rank[%05d]\n",
		    fpath, rank);
	}
    }
    if (dataflst != NULL) {
	kmr_free(dataflst, sizeof(struct kmr_ckpt_data_file) * (size_t)nfiles);
    }
    /* Remove checkpoint log file */
    {
	char fpath[KMR_CKPT_PATHLEN];
	kmr_ckpt_make_fname(target_dir, KMR_CKPT_FNAME_PREFIX, KMR_CKPT_LOG,
			    rank, 0, fpath, sizeof(fpath));
	cc = access(fpath, F_OK);
	if (cc == 0) {
	    unlink(fpath);
	}
    }
    /* Delete nprocs file on rank 0 */
    if (mr->rank == 0) {
	char fpath[KMR_CKPT_PATHLEN];
	memset(fpath, 0, sizeof(fpath));
	snprintf(fpath, sizeof(fpath), "%s/nprocs", target_dir);
	cc = access(fpath, F_OK);
	if (cc == 0) {
	    unlink(fpath);
	}
    }
    /* Delete checkpoint directory */
    cc = rmdir(target_dir);
    assert(cc == 0);
}

/* It finds a checkpoint data file named FNAME from DNAME directory and
   stores the file info. to FILE.  If SETALL is 1, it reads the file to
   fill in all fields of FILE. */
static void
kmr_ckpt_init_data_file(KMR *mr, const char *dname , const char *fname,
			_Bool setall, struct kmr_ckpt_data_file *file)
{
    char fpath[KMR_CKPT_PATHLEN];
    snprintf(fpath, KMR_CKPT_PATHLEN, "%s/%s", dname, fname);
    int cc = access(fpath, F_OK);
    if (cc != 0) {
	char msg[KMR_CKPT_MSGLEN];
	snprintf(msg, sizeof(msg),
		 "Failed to access checkpoint file %s", fpath);
	kmr_error(mr, msg);
    }
    if (setall) {
	struct kmr_ckpt_data hdr;
	size_t hdrsz = offsetof(struct kmr_ckpt_data, data);
	FILE *fp = kmr_ckpt_open_path(mr, fpath, "r");
	size_t rc = fread((void *)&hdr, hdrsz, 1, fp);
	if (rc == 1) {
	    file->kvs_id = hdr.kvs_id;
	} else {
	    char msg[KMR_CKPT_MSGLEN];
	    snprintf(msg, sizeof(msg),
		     "Failed to read checkpoint file %s. Ignore this file",
		     fpath);
	    kmr_warning(mr, 1, msg);
	    file->kvs_id = KMR_CKPT_DUMMY_ID;
	    file->checked = 1;
	}
	fclose(fp);
    }
    strncpy(file->fname, fname, sizeof(file->fname) - 1);
    strncpy(file->dname, dname, sizeof(file->dname) - 1);
}

/* Save all kv in kvs to a checkpoint data file.
   \param[in]  kvs  target KVS
*/
static inline void
kmr_ckpt_save_ckpt(KMR_KVS *kvs) {
    struct kmr_ckpt_ctx *ckptctx = kvs->c.mr->ckpt_ctx;
    size_t tsize = kvs->c.storage_netsize + offsetof(struct kmr_ckpt_data, data);
    void *buf = kmr_malloc(tsize);
    memset(buf, 0, tsize);

    struct kmr_ckpt_data *ckpt = (struct kmr_ckpt_data *)buf;
    ckpt->nprocs = kvs->c.mr->nprocs;
    ckpt->rank = kvs->c.mr->rank;
    ckpt->kvs_id = kvs->c.ckpt_kvs_id;
    ckpt->key_data = kvs->c.key_data;
    ckpt->value_data = kvs->c.value_data;

    enum kmr_kv_field keyf = kmr_unit_sized_or_opaque(kvs->c.key_data);
    enum kmr_kv_field valf = kmr_unit_sized_or_opaque(kvs->c.value_data);

    unsigned char *p = (unsigned char *)&ckpt->data[0];
    ckptctx->saved_current_block = kvs->c.current_block;  // save current_block
    kvs->c.current_block = kvs->c.first_block;
    long cnt = 0;
    while (cnt < kvs->c.element_count) {
	assert(kvs->c.current_block != 0);
	struct kmr_kv_box ev;
	struct kmr_kvs_block *b = kvs->c.current_block;
	struct kmr_kvs_entry *e = kmr_kvs_first_entry(kvs, b);
	for (long i = 0; i < b->partial_element_count; i++) {
	    assert(e != 0);
	    ev = kmr_pick_kv(e, kvs);
	    kmr_poke_kv2((struct kmr_kvs_entry *)p, ev, 0, keyf, valf, 0);
	    p += kmr_kvs_entry_netsize((struct kmr_kvs_entry *)p);
	    e = kmr_kvs_next(kvs, e, 1);
	    cnt++;
	}
	kvs->c.current_block = b->next;
    }
    kvs->c.current_block = ckptctx->saved_current_block;  // restore current_block

    FILE *fp = kmr_ckpt_open(kvs, "w");
    size_t ret = fwrite(buf, tsize, 1, fp);
    if (ret != 1) {
	char msg[KMR_CKPT_MSGLEN];
	snprintf(msg, sizeof(msg), "Checkpoint: save checkpoint error write failed");
	kmr_error(kvs->c.mr, msg);
    }
    kmr_ckpt_flush(kvs->c.mr, fp);
    fclose(fp);
    if (buf != NULL) {
	free(buf);
    }
}

/* Create a checkpoint file only with header and open it */
static inline void
kmr_ckpt_kv_record_init_data(KMR *mr, KMR_KVS *kvs)
{
    if (kvs == 0) {
	mr->ckpt_ctx->ckpt_data_fp = NULL;
	return;
    }

    FILE *fp;
    char fpath[KMR_CKPT_PATHLEN];
    struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
    kmr_ckpt_make_fname(ckptctx->ckpt_dname, KMR_CKPT_FNAME_PREFIX,
			KMR_CKPT_DATA, mr->rank, kvs->c.ckpt_kvs_id,
			fpath, sizeof(fpath));
    int cc = access(fpath, W_OK);
    if (cc == 0) {
	/* if checkpoint already exist, open it. */
	fp = kmr_ckpt_open_path(mr, fpath, "a+");
    } else if (cc < 0 && errno == ENOENT) {
	/* if checkpoint does not exist, create it. */
	struct kmr_ckpt_data ckpt;
	memset((void *)&ckpt, 0, sizeof(ckpt));
	ckpt.nprocs = mr->nprocs;
	ckpt.rank = mr->rank;
	ckpt.kvs_id = kvs->c.ckpt_kvs_id;
	ckpt.key_data = kvs->c.key_data;
	ckpt.value_data = kvs->c.value_data;
	fp = kmr_ckpt_open_path(mr, fpath, "w+");
	size_t size = offsetof(struct kmr_ckpt_data, data);
	size_t ret = fwrite((void *)&ckpt, size, 1, fp);
	if (ret != 1) {
	    char msg[KMR_CKPT_MSGLEN];
	    snprintf(msg, sizeof(msg),
		     "Failed to write header of checkpoint file %s", fpath);
	    kmr_error(mr, msg);
	}
	kmr_ckpt_flush(mr, fp);
    } else {
	assert(0);
    }
    ckptctx->ckpt_data_fp = fp;
}

/* Initialization for KV recording while executing Map/Reduce.
   \param[in]  kvi  input KVS
   \param[in]  kvo  output KVS
*/
static void
kmr_ckpt_kv_record_init(KMR *mr, KMR_KVS *kvo)
{
    /* initialize kvo checkpoint file */
    kmr_ckpt_kv_record_init_data(mr, kvo);
    /* set element_count, adding_point, currnet_block to ckpt_ctx */
    if (kvo != 0) {
	struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
	ckptctx->saved_current_block = kvo->c.current_block;
	ckptctx->saved_adding_point  = kvo->c.adding_point;
	ckptctx->saved_element_count = kvo->c.element_count;
    }
}

/* Add new kv in kvs to checkpoint data.
   As a result of Map/Reduce operation, newly generated kv(s)
   are added to checkpoint data file.
   \param[in]  kvo  target KVS
   \return     number of kv added to a checkpoint data
*/
static long
kmr_ckpt_kv_record_add(KMR_KVS *kvo)
{
    if (kvo == 0) {
	return 0;
    }

    struct kmr_ckpt_ctx *ckptctx = kvo->c.mr->ckpt_ctx;
    assert(ckptctx->ckpt_data_fp != NULL);
    long cnt = kvo->c.element_count - ckptctx->saved_element_count;
    assert(cnt >= 0);
    struct kmr_kvs_block *b = ckptctx->saved_current_block;
    if (b == 0) {
	b = kvo->c.first_block;
    }
    struct kmr_kvs_entry *e = ckptctx->saved_adding_point;
    if (e == 0) {
	e = kmr_kvs_first_entry(kvo, b);
    }

    for (long i = 0; i < cnt; i++) {
	if (kmr_kvs_entry_tail_p(e)) {
	    b = b->next;
	    assert(b != 0);
	    e = kmr_kvs_first_entry(kvo, b);
	}
	/* save data in e as checkpoint data. */
	size_t size = kmr_kvs_entry_netsize(e);
	size_t ret = fwrite((void *)e, size, 1, ckptctx->ckpt_data_fp);
	if (ret != 1) {
	    char msg[KMR_CKPT_MSGLEN];
	    snprintf(msg, sizeof(msg),
		     "Failed to add kv to a checkpoint file");
	    kmr_error(kvo->c.mr, msg);
	}
	e = kmr_kvs_next_entry(kvo, e);
    }
    kmr_ckpt_flush(kvo->c.mr, ckptctx->ckpt_data_fp);
    ckptctx->saved_current_block = b;
    ckptctx->saved_adding_point = e;
    ckptctx->saved_element_count = kvo->c.element_count;
    return cnt;
}

/* Finish KV recording
   \param[in]  kvi  input KVS
   \param[in]  kvo  output KVS
*/
static void
kmr_ckpt_kv_record_fin(KMR *mr)
{
    struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
    /* close opened checkpoint data file. */
    if (ckptctx->ckpt_data_fp != NULL) {
	kmr_ckpt_flush(mr, ckptctx->ckpt_data_fp);
	fclose(ckptctx->ckpt_data_fp);
    }
    /* cleanup context. */
    ckptctx->ckpt_data_fp = NULL;
    ckptctx->saved_element_count = 0;
    ckptctx->saved_adding_point = NULL;
    ckptctx->saved_current_block = NULL;
}

/* List up ckeckpoint data files in the specified directory DNAME.
   It sets files to FLIST and its count to NFILES.
   If SETALL is 1, it reads data files to initialize all fields
   of structure. */
static void
kmr_ckpt_get_data_flist(KMR *mr, const char *dname,
			struct kmr_ckpt_data_file **flist, int *nfiles,
			_Bool setall)
{
    struct stat sb;
    int cc = stat(dname, &sb);
    if (cc < 0) {
	*nfiles = 0;
	return;
    }
    if (!S_ISDIR(sb.st_mode)) {
	fprintf(stderr, "File %s is not a directory.\n", dname);
	*nfiles = 0;
	return;
    }

    size_t direntsz;
    long nmax = pathconf(dname, _PC_NAME_MAX);
    if (nmax == -1) {
	direntsz = (64 * 1024);
    } else {
	direntsz = (offsetof(struct dirent, d_name) + (size_t)nmax + 1);
    }
    DIR *d;
    struct dirent *dent;
    char b[direntsz];

    d = opendir(dname);
    if (d == NULL) {
	fprintf(stderr, "Failed to open directory %s.\n", dname);
	*nfiles = 0;
	return;
    }

    char prefix[KMR_CKPT_PATHLEN];
    snprintf(prefix, KMR_CKPT_PATHLEN, KMR_CKPT_FNAME_PREFIX"_data_");
    int cnt = 0;
    while (readdir_r(d, (void *)b, &dent) == 0) {
	if (dent == NULL) {
	    break;
	}
	cc = strncmp(dent->d_name, prefix, strlen(prefix));
	if (cc == 0) {
	    cnt++;
	}
    }

    size_t siz = sizeof(struct kmr_ckpt_data_file) * (size_t)cnt;
    struct kmr_ckpt_data_file *dataflst = kmr_malloc(siz);
    memset(dataflst, 0, siz);

    rewinddir(d);
    cnt = 0;
    while (readdir_r(d, (void *)b, &dent) == 0) {
	if (dent == NULL) {
	    break;
	}
	cc = strncmp(dent->d_name, prefix, strlen(prefix));
	if (cc == 0) {
	    kmr_ckpt_init_data_file(mr, dname, dent->d_name, setall,
				    &dataflst[cnt]);
	    cnt++;
	}
    }
    (void)closedir(d);

    *flist = dataflst;
    *nfiles = cnt;
}

/* save mr->nprocs to file. */
static void
kmr_ckpt_save_nprocs(KMR *mr, const char *dname)
{
    struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
    const char *target = (dname != 0) ? dname : ckptctx->ckpt_dname;
    char fpath[KMR_CKPT_PATHLEN], wstring[128], msg[KMR_CKPT_MSGLEN];
    memset(fpath, 0, sizeof(fpath));
    snprintf(fpath, sizeof(fpath), "%s/nprocs", target);
    int cc = access(fpath, R_OK);
    if (cc == 0) {
	return;
    } else {
	FILE *fp = kmr_ckpt_open_path(mr, fpath, "w");
	if (fp == NULL) {
	    snprintf(msg, sizeof(msg),
		     "Failed to open nprocs file %s", fpath);
	    kmr_error(mr, msg);
	}
	memset(wstring, 0, sizeof(wstring));
	snprintf(wstring, sizeof(wstring), "nprocs=%d\n", mr->nprocs);
	size_t ret = fwrite(wstring, strlen(wstring), 1, fp);
	if (ret != 1) {
	    snprintf(msg, sizeof(msg), "Failed to save nprocs to file %s",
		     fpath);
	    kmr_error(mr, msg);
	}
	kmr_ckpt_flush(mr, fp);
	fclose(fp);
    }
}

/* Open KVS checkpoint data file. */
static FILE *
kmr_ckpt_open(KMR_KVS *kvs, const char *mode)
{
    char fpath[KMR_CKPT_PATHLEN];
    KMR *mr = kvs->c.mr;
    struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
    kmr_ckpt_make_fname(ckptctx->ckpt_dname, KMR_CKPT_FNAME_PREFIX,
			KMR_CKPT_DATA, mr->rank, kvs->c.ckpt_kvs_id,
			fpath, sizeof(fpath));
    FILE *fp = kmr_ckpt_open_path(mr, fpath, mode);
    return fp;
}

/* Open file using file path. */
static FILE *
kmr_ckpt_open_path(KMR *mr, const char *fpath, const char *mode)
{
    FILE *fp = fopen(fpath, mode);
    if (fp == NULL) {
	char msg[KMR_CKPT_MSGLEN];
	snprintf(msg, sizeof(msg),
		 "Failed to open a checkpoint file %s", fpath);
	kmr_error(mr, msg);
    }
    int cc = fcntl(fileno(fp), F_SETFD, FD_CLOEXEC);
    assert(cc == 0);
    return fp;
}

/* Compose checkpoint file name. */
static void
kmr_ckpt_make_fname(const char *dirname, const char *fprefix,
		    enum kmr_ckpt_type type,
		    int rank, long kvs_id, char *fpath, size_t len)
{
    memset(fpath, 0, len);
    assert(type == KMR_CKPT_DATA || type == KMR_CKPT_LOG);
    if (type == KMR_CKPT_DATA) {
	snprintf(fpath, len-1, "%s/%s_data_%05d_%03ld",
		 dirname, fprefix, rank, kvs_id);
    } else if (type == KMR_CKPT_LOG) {
	snprintf(fpath, len-1, "%s/%s_log_%05d",
		 dirname, fprefix, rank);
    }
}

/* Flush write to the specified file */
static void
kmr_ckpt_flush(KMR *mr, FILE *fp)
{
    fflush(fp);
    if (!mr->ckpt_no_fsync) {
	int cc = fsync(fileno(fp));
	assert(cc == 0);
    }
}

/***************************************************************/
/*  Public functions                                           */
/***************************************************************/

/** Check if checkpoint/restart is enabled.

    \param[in]  mr  MapReduce data type
    \return     It returns 1 if checkpoint/restart is enabled.
                Otherwise it returns 0.
*/
int
kmr_ckpt_enabled(KMR *mr)
{
    struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
    if (mr->ckpt_enable == 1 && ckptctx->initialized) {
	return 1;
    } else {
	return 0;
    }
}

/** It temporally disables checkpoint/restart.

    \param[in]  mr  MapReduce data type
    \return     If it succeeds disabling, it returns a lock id.
                Otherwise it returns 0.
*/
int kmr_ckpt_disable_ckpt(KMR *mr)
{
    struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
    if (mr->ckpt_enable == 1 && ckptctx->initialized &&
	ckptctx->lock_id == 0) {
	mr->ckpt_enable = 0;
	ckptctx->lock_id = ++ckptctx->lock_counter;
	return ckptctx->lock_id;
    } else {
	return 0;
    }
}

/** It temporally enables checkpoint/restart which has been
    disabled by calling kmr_ckpt_disable_ckpt().

    \param[in]  mr        MapReduce data type
    \param[in]  lock_id   ID of lock returned by kmr_ckpt_disable_ckpt()
    \return     If it succeeds enabling, it returns 1.
                Otherwise it returns 0.
*/
int kmr_ckpt_enable_ckpt(KMR *mr, int lock_id)
{
    struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
    if (mr->ckpt_enable == 0 && ckptctx->initialized &&
	ckptctx->lock_id == lock_id) {
	mr->ckpt_enable = 1;
	ckptctx->lock_id = 0;
	return 1;
    } else {
	return 0;
    }
}

/** It returns the index of the first unprocessed key-value in the input KVS.

    \param[in]  mr   MapReduce data type
    \return     It returns the index of the first unprocessed key-value
                in the input KVS.
*/
long
kmr_ckpt_first_unprocessed_kv(KMR *mr)
{
    if (mr->ckpt_selective) {
	return 0;
    }
    struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
    long op_seqno = ckptctx->progress_counter;
    long start_from = 0;
    for (int i = 0; i < ckptctx->kv_positions_count; i++) {
	if (ckptctx->kv_positions[i].op_seqno == op_seqno) {
	    start_from = ckptctx->kv_positions[i].start_from;
	    break;
	}
    }
    return start_from;
}

/** It restores checkpoint data to kvs.

    \param[out]  kvs  an KVS where the checkpoint data will be restored
*/
void
kmr_ckpt_restore_ckpt(KMR_KVS *kvs)
{
    KMR *mr = kvs->c.mr;
    char fpath[KMR_CKPT_PATHLEN];
    kmr_ckpt_make_fname(mr->ckpt_ctx->ckpt_dname, KMR_CKPT_FNAME_PREFIX,
			KMR_CKPT_DATA, mr->rank, kvs->c.ckpt_kvs_id,
			fpath, sizeof(fpath));
    int cc = access(fpath, R_OK);
    if (cc != 0) {
	/* checkpoint file does not exist. */
	return;
    }
    struct stat sb;
    cc = stat(fpath, &sb);
    if (cc != 0) {
	char msg[KMR_CKPT_MSGLEN];
	snprintf(msg, sizeof(msg),
		 "Failed to access a checkpoint file %s", fpath);
	kmr_error(kvs->c.mr, msg);
    }
    size_t siz = (size_t)sb.st_size;
    void *buf = kmr_malloc(siz);
    FILE *fp = kmr_ckpt_open_path(kvs->c.mr, fpath, "r");
    size_t ret = fread(buf, siz, 1, fp);
    if (ret != 1) {
	char msg[KMR_CKPT_MSGLEN];
	snprintf(msg, sizeof(msg),
		 "Failed to load a checkpoint file %s", fpath);
	kmr_error(kvs->c.mr, msg);
    }
    fclose(fp);

    struct kmr_ckpt_data *ckpt = (struct kmr_ckpt_data *)buf;
    size_t cur_siz = offsetof(struct kmr_ckpt_data, data);
    struct kmr_kvs_entry *e = (struct kmr_kvs_entry *)&ckpt->data[0];
    while (cur_siz < siz) {
	struct kmr_kv_box kv;
//	if ( kmr_kvs_entry_tail_p(e) ) {
//	    break;
//	}
	assert(e != 0);
	kv = kmr_pick_kv(e, kvs); // This is ok.
	kmr_add_kv(kvs, kv);
	cur_siz += kmr_kvs_entry_netsize(e);
	e = kmr_kvs_next_entry(kvs, e);
    }
    kmr_free(buf, siz);
    assert(cur_siz == siz);
}

/** It removes checkpoint data file.

    \param[in]  kvs  KVS whose checkpoint data is removed
*/
void
kmr_ckpt_remove_ckpt(KMR_KVS *kvs)
{
    KMR *mr = kvs->c.mr;
    if (!mr->ckpt_selective) {
	/* delete the checkpoint data file */
	kmr_ckpt_delete_ckpt_data(mr, kvs->c.ckpt_kvs_id);
    } else {
	/* just mark as deletable */
	char fpath[KMR_CKPT_PATHLEN];
	struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
	kmr_ckpt_make_fname(ckptctx->ckpt_dname, KMR_CKPT_FNAME_PREFIX,
			    KMR_CKPT_DATA, mr->rank, kvs->c.ckpt_kvs_id,
			    fpath, sizeof(fpath));
	int cc = access(fpath, F_OK);
	if (cc == 0) {
	    kmr_ckpt_log_deletable(mr, kvs->c.ckpt_kvs_id);
	}
    }
}

/** It saves all key-value pairs in the output KVS to a checkpoint data file.

    \param[in]  kvi  input KVS
    \param[in]  kvo  output KVS
*/
void
kmr_ckpt_save_kvo_whole(KMR *mr, KMR_KVS *kvo)
{
    if (kmr_ckpt_write_file_p(mr)) {
	kmr_ckpt_log_whole_start(mr);
	kmr_ckpt_save_ckpt(kvo);
	kmr_ckpt_log_whole_finish(mr);
    }
}

/** It initializes saving blocks of key-value pairs of the output KVS to
    a checkpoint data file.

    \param[in]  mr   MapReduce data type
    \param[in]  kvo  output KVS
*/
void
kmr_ckpt_save_kvo_block_init(KMR *mr, KMR_KVS *kvo)
{
    if (!mr->ckpt_selective) {
	kmr_ckpt_log_block_start(mr, kvo);
	kmr_ckpt_kv_record_init(mr, kvo);
    }
}

/** It adds a new block of key-value pairs of the output KVS to the
    checkpoint data file.

    \param[in]  mr   MapReduce data type
    \param[in]  kvo  output KVS
    \param[in]  nkvi number of processed kv in the input KVS
*/
void
kmr_ckpt_save_kvo_block_add(KMR *mr, KMR_KVS *kvo, long nkvi)
{
    if (!mr->ckpt_selective) {
	long nkvo = kmr_ckpt_kv_record_add(kvo);
	kmr_ckpt_log_block_add(mr, nkvi, nkvo);
    }
}

/** It finalizes saving block of key-value pairs of the output KVS to
    the checkpoint data file.

    \param[in]  mr   MapReduce data type
    \param[in]  kvo  output KVS
*/
void
kmr_ckpt_save_kvo_block_fin(KMR *mr, KMR_KVS *kvo)
{
    if (!mr->ckpt_selective) {
	kmr_ckpt_kv_record_fin(mr);
	kmr_ckpt_log_block_finish(mr);
    } else {
	/* incase of selective mode, save all key-values here */
	kmr_ckpt_save_kvo_whole(mr, kvo);
    }
}

/** It initializes saving indexed key-value pairs of the output KVS
    to a checkpoint data file.

    \param[in]  mr   MapReduce data type
    \param[in]  kvo  output KVS
*/
void
kmr_ckpt_save_kvo_each_init(KMR *mr, KMR_KVS *kvo)
{
    if (!mr->ckpt_selective) {
	kmr_ckpt_log_index_start(mr, kvo);
	kmr_ckpt_kv_record_init(mr, kvo);
    }
}

/** It adds new key-value pairs of the output KVS to the checkpoint data file.

    \param[in]  mr        MapReduce data type
    \param[in]  kvo       output KVS
    \param[in]  ikv_index index of processed kv in the input KVS
*/
void
kmr_ckpt_save_kvo_each_add(KMR *mr, KMR_KVS *kvo, long ikv_index)
{
    if (!mr->ckpt_selective) {
	long nkvo = kmr_ckpt_kv_record_add(kvo);
	kmr_ckpt_log_index_add(mr, ikv_index, nkvo);
    }
}

/** It finalizes saving indexed key-value pairs of the output KVS
    to the checkpoint data file.

    \param[in]  mr   MapReduce data type
    \param[in]  kvo  output KVS
*/
void
kmr_ckpt_save_kvo_each_fin(KMR *mr, KMR_KVS *kvo)
{
    if (!mr->ckpt_selective) {
	kmr_ckpt_kv_record_fin(mr);
	kmr_ckpt_log_index_finish(mr);
    } else {
	/* incase of selective mode, save all key-values here */
	kmr_ckpt_save_kvo_whole(mr, kvo);
    }
}

/** It initializes a progress of MapReduce checkpointing.

    \param[in]  kvi  input KVS to a MapReduce operation
    \param[in]  kvo  output KVS to the MapReduce operation
    \param[in]  opt  struct kmr_option
    \return     It returns 1 if operation can be skipped.
                Otherwise it returns 0.
*/
int
kmr_ckpt_progress_init(KMR_KVS *kvi, KMR_KVS *kvo, struct kmr_option opt)
{
    KMR *mr = (kvo != 0) ? kvo->c.mr : kvi->c.mr;
    if (opt.keep_open) {
	char msg[KMR_CKPT_MSGLEN];
	snprintf(msg, sizeof(msg),
		 "'keep_open' option can't be used when checkpoint/restart"
		 " is enabled");
	kmr_error(mr, msg);
    }

    /* initialize progress */
    struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
    ckptctx->progress_counter += 1;
    if (kvi != 0) {
	kvi->c.ckpt_consumed_op = ckptctx->progress_counter;
    }
    if (kvo != 0) {
	kvo->c.ckpt_generated_op = ckptctx->progress_counter;
    }
    assert(ckptctx->cur_kvi_id == KMR_CKPT_DUMMY_ID);
    assert(ckptctx->cur_kvo_id == KMR_CKPT_DUMMY_ID);
    if (kvi != 0) {
	ckptctx->cur_kvi_id = kvi->c.ckpt_kvs_id;
    } else {
	ckptctx->cur_kvi_id = KMR_CKPT_DUMMY_ID;
    }
    if (kvo != 0) {
	ckptctx->cur_kvo_id = kvo->c.ckpt_kvs_id;
    } else {
	ckptctx->cur_kvo_id = KMR_CKPT_DUMMY_ID;
    }

    /* check if the operation can be skipped or not */
    int do_skip;
    long progress = ckptctx->progress_counter;
    if (!mr->ckpt_selective) {
	/* ckpt all */
	if (progress <= ckptctx->prev_global_progress) {
	    do_skip = 1;
	} else if (progress >  ckptctx->prev_global_progress &&
		   progress <= ckptctx->prev_progress          ) {
	    if (kvi == 0) { /* In case of kmr_map_once */
		do_skip = 0;
	    } else {
		if (kvi->c.element_count == 0) {
		    do_skip = 1;
		} else {
		    do_skip = 0;
		}
	    }
	} else { /* progress > ckptctx->prev_progress */
	    do_skip = 0;
	}
    } else {
	/* ckpt selective */
	if (progress <= ckptctx->prev_global_progress) {
	    do_skip = 1;
	} else if (progress >  ckptctx->prev_global_progress &&
		   progress <= ckptctx->prev_progress          ) {
	    long v = kmr_ckpt_int_list_del(ckptctx->slct_skip_ops, progress);
	    if (v == progress) {
		do_skip = 1;
	    } else {
		do_skip = 0;
	    }
	} else { /* progress > ckptctx->prev_progress */
	    do_skip = 0;
	}
    }
    if (do_skip == 1) {
	kmr_ckpt_log_skipped(mr);
	ckptctx->cur_kvi_id = KMR_CKPT_DUMMY_ID;
	ckptctx->cur_kvo_id = KMR_CKPT_DUMMY_ID;
	return 1;
    }

    /* initialize a checkpoint */
    if (mr->ckpt_selective) {
	if (opt.take_ckpt) {
	    ckptctx->slct_cur_take_ckpt = 1;
	}
    }

    return 0;
}

/** It finalizes the progress of MapReduce checkpointing.

    \param[in]  mr    MapReduce data type
*/
void
kmr_ckpt_progress_fin(KMR *mr)
{
    kmr_ckpt_log_progress(mr);
    struct kmr_ckpt_ctx *ckptctx = mr->ckpt_ctx;
    if (mr->ckpt_selective) {
	ckptctx->slct_cur_take_ckpt = 0;
    }
    ckptctx->cur_kvi_id = KMR_CKPT_DUMMY_ID;
    ckptctx->cur_kvo_id = KMR_CKPT_DUMMY_ID;
}

/***************************************************************/
/*  Utility functions                                          */
/***************************************************************/

/* Initialize list */
static void
kmr_ckpt_list_init(struct kmr_ckpt_list *list,
		   kmr_ckpt_list_alocfn_t alocfn,
		   kmr_ckpt_list_freefn_t freefn,
		   kmr_ckpt_list_compfn_t compfn)
{
    list->head = 0;
    list->tail = 0;
    list->size = 0;
    list->alocfn = alocfn;
    list->freefn = freefn;
    list->compfn = compfn;
}

/* Clear all data in the list */
static void
kmr_ckpt_list_free(struct kmr_ckpt_list *list)
{
    struct kmr_ckpt_list_item *item, *del;
    for (item = list->head; item != 0; ) {
	del = item;
	item = item->next;
	(*(list->freefn))(del);
    }
    kmr_ckpt_list_init(list, list->alocfn, list->freefn, list->compfn);
}

/* Add an item to the tail of the list.
   If size of list is over max count, it deletes the oldest item. */
static void
kmr_ckpt_list_add(struct kmr_ckpt_list *list, void *val)
{
    struct kmr_ckpt_list_item *item;
    size_t isize = sizeof(struct kmr_ckpt_list_item);
    if (list->size == KMR_CKPT_LIST_MAX) {
	item = list->head;
	list->head = list->head->next;
	list->head->prev = 0;
	(*(list->freefn))(item->val);
	kmr_free(item, isize);
	list->size -= 1;
    }
    item = (struct kmr_ckpt_list_item *)kmr_malloc(isize);
    item->val = (*(list->alocfn))(val);
    item->next = 0;
    item->prev = 0;
    if (list->head == 0) {
	list->head = item;
	list->tail = item;
    } else {
	list->tail->next = item;
	item->prev = list->tail;
	list->tail = item;
    }
    list->size += 1;
}

/* Delete the specified value from the list.
   If it successes to delete it returns the value, otherwise it returns NULL.
   It searches from the head. */
static void *
kmr_ckpt_list_del(struct kmr_ckpt_list *list, void *val)
{
    _Bool found = 0;
    struct kmr_ckpt_list_item *item;
    for (item = list->head; item != 0; item = item->next) {
	if ((*(list->compfn))(item->val, val) == 0) {
	    found = 1;
	    break;
	}
    }
    if (found) {
	void *ret = item->val;
	if (!(item == list->head || item == list->tail)) {
	    item->prev->next = item->next;
	    item->next->prev = item->prev;
	}
	if (item == list->head) {
	    list->head = item->next;
	    if (list->head != 0) {
		list->head->prev = 0;
	    }
	}
	if (item == list->tail) {
	    list->tail = item->prev;
	    if (list->tail != 0) {
		list->tail->next = 0;
	    }
	}
	kmr_free(item, sizeof(struct kmr_ckpt_list_item));
	list->size -= 1;
	return ret;
    } else {
	return 0;
    }
}

/* It searchs the specified value from the head of the list.
   If the value is found it returns the value, otherwise it returns NULL. */
static void *
kmr_ckpt_list_search(struct kmr_ckpt_list *list, void *val)
{
    struct kmr_ckpt_list_item *item;
    for (item = list->head; item != 0; item = item->next) {
	if ((*(list->compfn))(item->val, val) == 0) {
	    return item->val;
	}
    }
    return 0;
}

/* It searchs the specified value from the tail of the list.
   If the value is found it returns the value, otherwise it returns NULL. */
static void *
kmr_ckpt_list_rsearch(struct kmr_ckpt_list *list, void *val)
{
    struct kmr_ckpt_list_item *item;
    for (item = list->tail; item != 0; item = item->prev) {
	if ((*(list->compfn))(item->val, val) == 0) {
	    return item->val;
	}
    }
    return 0;
}

/* allocator for int item */
static void *
kmr_ckpt_int_list_alocfn(void *val)
{
    long *v = kmr_malloc(sizeof(long));
    *v = *(long *)val;
    return v;
}

/* deallocator for int item */
static void
kmr_ckpt_int_list_freefn(void *val)
{
    kmr_free(val, sizeof(long));
}

/* comparetor for int item */
static int
kmr_ckpt_int_list_compfn(void *v1, void *v2)
{
    long _v1 = *(long *)v1;
    long _v2 = *(long *)v2;
    if ( _v1 > _v2 ) {
	return 1;
    } else if ( _v1 < _v2 ) {
	return -1;
    } else {
	return 0;
    }
}

/* Initialize integer list */
static void
kmr_ckpt_int_list_init(struct kmr_ckpt_list *list)
{
    kmr_ckpt_list_init(list, kmr_ckpt_int_list_alocfn,
		       kmr_ckpt_int_list_freefn, kmr_ckpt_int_list_compfn);
}

/* Clear all data in the list */
static void
kmr_ckpt_int_list_free(struct kmr_ckpt_list *list)
{
    kmr_ckpt_list_free(list);
}

/* Add an integer to the tail of the list.
   If size of list is over max count, it deletes the oldest value. */
static void
kmr_ckpt_int_list_add(struct kmr_ckpt_list *list, long val)
{
    kmr_ckpt_list_add(list, &val);
}

/* Delete the specified integer from the list.
   If it successes to delete it returns the value, otherwise it returns -1.
   It searches from the head. */
static long
kmr_ckpt_int_list_del(struct kmr_ckpt_list *list, long val)
{
    long *v = (long *)kmr_ckpt_list_del(list, &val);
    if (v != 0) {
	return *v;
    } else {
	return -1;
    }
}

/* It searchs the specified integer from the head of the list.
   If the value is found it returns the value, otherwise it returns -1. */
static long
kmr_ckpt_int_list_search(struct kmr_ckpt_list *list, long val)
{
    long *v = (long *)kmr_ckpt_list_search(list, &val);
    if (v != 0) {
	return *v;
    } else {
	return -1;
    }
}

/* It searchs the specified integer from the tail of the list.
   If the value is found it returns the value, otherwise it returns -1. */
static long
kmr_ckpt_int_list_rsearch(struct kmr_ckpt_list *list, long val)
{
    long *v = (long *)kmr_ckpt_list_rsearch(list, &val);
    if (v != 0) {
	return *v;
    } else {
	return -1;
    }
}

#if 0
/* Test integer list.
   Before call this function, set KMR_CKPT_LIST_MAX as 2. */
static void test_kmr_ckpt_int_list()
{
    struct kmr_ckpt_list list;
    kmr_ckpt_int_list_init(&list);
    long v = kmr_ckpt_int_list_del(&list, 1);
    assert(v == -1);
    assert(list.size == 0);
    v = kmr_ckpt_int_list_search(&list, 1);
    assert(v == -1);
    assert(list.size == 0);
    v = kmr_ckpt_int_list_rsearch(&list, 1);
    assert(v == -1);
    assert(list.size == 0);
    kmr_ckpt_int_list_add(&list, 10);
    assert(list.size == 1);
    kmr_ckpt_int_list_add(&list, 20);
    assert(list.size == 2);
    kmr_ckpt_int_list_add(&list, 30);
    assert(list.size == 2);
    v = kmr_ckpt_int_list_search(&list, 10);
    assert(v == -1);
    v = kmr_ckpt_int_list_rsearch(&list, 10);
    assert(v == -1);
    v = kmr_ckpt_int_list_search(&list, 20);
    assert(v == 20);
    v = kmr_ckpt_int_list_rsearch(&list, 20);
    assert(v == 20);
    v = kmr_ckpt_int_list_search(&list, 30);
    assert(v == 30);
    v = kmr_ckpt_int_list_rsearch(&list, 30);
    assert(v == 30);
    v = kmr_ckpt_int_list_del(&list, 1);
    assert(v == -1);
    assert(list.size == 2);
    v = kmr_ckpt_int_list_del(&list, 20);
    assert(v == 20);
    assert(list.size == 1);
    v = kmr_ckpt_int_list_del(&list, 30);
    assert(v == 30);
    assert(list.head == 0);
    assert(list.tail == 0);
    kmr_ckpt_int_list_free(&list);
    fprintf(stderr, "interger list test done.\n");
}
#endif

/* allocator for operation item */
static void *
kmr_ckpt_opr_list_alocfn(void *val)
{
    struct kmr_ckpt_operation *v = kmr_malloc(sizeof(struct kmr_ckpt_operation));
    *v = *(struct kmr_ckpt_operation *)val;
    return v;
}

/* deallocator for operation item */
static void
kmr_ckpt_opr_list_freefn(void *val)
{
    kmr_free(val, sizeof(struct kmr_ckpt_operation));
}

/* comparetor for operation item */
static int
kmr_ckpt_opr_list_compfn(void *v1, void *v2)
{
    struct kmr_ckpt_operation _v1 = *(struct kmr_ckpt_operation *)v1;
    struct kmr_ckpt_operation _v2 = *(struct kmr_ckpt_operation *)v2;
    if ( _v1.op_seqno > _v2.op_seqno ) {
	return 1;
    } else if ( _v1.op_seqno < _v2.op_seqno ) {
	return -1;
    } else {
	return 0;
    }
}

/* Initialize operation list */
static void
kmr_ckpt_opr_list_init(struct kmr_ckpt_list *list)
{
    kmr_ckpt_list_init(list, kmr_ckpt_opr_list_alocfn,
		       kmr_ckpt_opr_list_freefn, kmr_ckpt_opr_list_compfn);
}

/* Clear all data in the list */
static void
kmr_ckpt_opr_list_free(struct kmr_ckpt_list *list)
{
    kmr_ckpt_list_free(list);
}

/* Add an operation to the tail of the list.
   If size of list is over max count, it deletes the oldest value. */
static void
kmr_ckpt_opr_list_add(struct kmr_ckpt_list *list, struct kmr_ckpt_operation op)
{
    kmr_ckpt_list_add(list, &op);
}

/* Initialize kvs chains. */
static void
kmr_ckpt_kvs_chains_init(struct kmr_ckpt_kvs_chains *chains)
{
    chains->chainlst = 0;
    chains->chainlst_size = 0;
}

/* Free kvs chains */
static void
kmr_ckpt_kvs_chains_free(struct kmr_ckpt_kvs_chains *chains)
{
    for (int i = 0; i < chains->chainlst_size; i++) {
	struct kmr_ckpt_list *list = &chains->chainlst[i];
	kmr_ckpt_opr_list_free(list);
    }
    kmr_ckpt_kvs_chains_init(chains);
}

/* Create a new chain. */
static void
kmr_ckpt_kvs_chains_new_chain(struct kmr_ckpt_kvs_chains *chains,
			      struct kmr_ckpt_operation op)
{
    int idx = chains->chainlst_size;
    chains->chainlst_size += 1;
    chains->chainlst = (struct kmr_ckpt_list *)
	kmr_realloc(chains->chainlst,
		    sizeof(struct kmr_ckpt_list) * (size_t)chains->chainlst_size);
    struct kmr_ckpt_list *list = &chains->chainlst[idx];
    kmr_ckpt_opr_list_init(list);
    kmr_ckpt_opr_list_add(list, op);
}

/* Connect an operation to an existing chain. */
static void
kmr_ckpt_kvs_chains_connect(struct kmr_ckpt_kvs_chains *chains,
			    struct kmr_ckpt_operation op)
{
    struct kmr_ckpt_list *list = kmr_ckpt_kvs_chains_find(chains, op.kvi_id);
    if (list != 0) {
	kmr_ckpt_opr_list_add(list, op);
    } else {
	kmr_ckpt_kvs_chains_new_chain(chains, op);
    }
}

/* Find a chain that contains KVS_ID.
   If all chains are closed (tha last operation's kvo is not KMR_CKPT_DUMMY_ID),
   it returns null. */
static struct kmr_ckpt_list *
kmr_ckpt_kvs_chains_find(struct kmr_ckpt_kvs_chains *chains, long kvo_id)
{
    for (int i = 0; i < chains->chainlst_size; i++) {
	struct kmr_ckpt_list *list = &chains->chainlst[i];
	struct kmr_ckpt_operation *last_op =
	    (struct kmr_ckpt_operation *)list->tail->val;
	if (last_op->kvo_id == KMR_CKPT_DUMMY_ID) {
	    /* chain is closed */
	    continue;
	}
	struct kmr_ckpt_list_item *item;
	for (item = list->tail; item != 0; item = item->prev) {
	    struct kmr_ckpt_operation *op =
		(struct kmr_ckpt_operation *)item->val;
	    if (op->kvo_id == kvo_id) {
		return list;
	    }
	}
    }
    return 0;
}

#if 0
/* Test KVS chains. */
static void test_kmr_ckpt_kvs_chains()
{
    struct kmr_ckpt_kvs_chains chains;
    kmr_ckpt_kvs_chains_init(&chains);
    struct kmr_ckpt_operation op1 = { .op_seqno = 1,
				      .kvi_id = KMR_CKPT_DUMMY_ID,
				      .kvo_id = 1 };
    kmr_ckpt_kvs_chains_new_chain(&chains, op1);
    assert(chains.chainlst_size == 1);
    struct kmr_ckpt_operation op2 = { .op_seqno = 2,
				      .kvi_id = 1,
				      .kvo_id = 2 };
    kmr_ckpt_kvs_chains_connect(&chains, op2);
    assert(chains.chainlst_size == 1);
    assert(chains.chainlst[0].size == 2);
    struct kmr_ckpt_operation op3 = { .op_seqno = 3,
				      .kvi_id = KMR_CKPT_DUMMY_ID,
				      .kvo_id = 3 };
    kmr_ckpt_kvs_chains_new_chain(&chains, op3);
    assert(chains.chainlst_size == 2);
    kmr_ckpt_kvs_chains_free(&chains);
    fprintf(stderr, "kvs chains test done.\n");
}
#endif


/*
Copyright (C) 2012-2018 RIKEN R-CCS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/
