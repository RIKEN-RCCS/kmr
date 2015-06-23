/* kmriolb.c (2014-08-08) */
/* Copyright (C) 2012-2015 RIKEN AICS */

/** \file kmriolb.c load balanced MapReduce based on data locality */

#include <mpi.h>
#include <assert.h>
#include <math.h>
#ifdef _OPENMP
#include <omp.h>
#endif

#include "../config.h"
#include "kmr.h"
#include "kmrimpl.h"
#include "kmrfefs.h"

#ifdef __K
#include <mpi-ext.h>


#define K_MAX_X 24
#define K_MAX_Y 18
#define MAX_IO_GRPS 432  /* = 82944/192, 24x18 */

struct io_grp {
    int gid_x; /* x coordinate */
    int gid_y; /* y coordinate */
    int count; /* count of file chunks in this group */
};

/* Calculates (x,y) coordinates of the specified rank.
 */
static int
kmr_iolb_calc_xy_of_rank(int rank)
{
    int x, y, z, a, b, c;
    int cc = FJMPI_Topology_sys_rank2xyzabc(rank, &x, &y, &z, &a, &b, &c);
    assert(cc == MPI_SUCCESS);
    return (x << 16) + y;
}

/* Loads file stripe information of the specified file.
   It reads the file chunks' (x,y) coordinates and counts them.
*/
static void
kmr_iolb_load_stripe(const char *filename,
                     struct io_grp *grps, int *grps_cnt)
{
    size_t len = strlen(filename);
    char *path = (char *)malloc((len + 1) * sizeof(char));
    memcpy(path, filename, (len + 1));
    char *d = path;
    char *f = NULL;
    for (char *p = &path[len - 1]; p >= path; p--) {
        if (*p == '/') {
            f = (p + 1);
            *p = 0;
            break;
        }
    }
    if (f == NULL) {
        /* No directory part in file name. */
        d = ".";
        f = path;
    }
    struct kmr_fefs_stripe stripe;
    int errori = 0;
    int cc = kmr_fefs_get_stripe(d, f, &stripe, &errori, 0);
    assert(cc == 0);
    free(path);

    for (int i = 0; i < stripe.s.count; i++) {
        int x = stripe.obdidx[i] / 2048;
        int y = (stripe.obdidx[i] % 2048) / 64;
        _Bool added = 0;
        for (int j = 0; j < *grps_cnt; j++) {
            if (x == grps[j].gid_x && y == grps[j].gid_y) {
                grps[j].count += 1;
                added = 1;
                break;
            }
        }
        if (!added) {
            grps[*grps_cnt].gid_x = x;
            grps[*grps_cnt].gid_y = y;
            grps[*grps_cnt].count = 1;
            *grps_cnt += 1;
        }
    }
}

/* Shifts (x,y) coordinates of IO groups so that the coordinates will be
   contiguous.
*/
static void
kmr_iolb_shift_grps(struct io_grp *grps, int grps_cnt,
                    int *shift_x, int *shift_y)
{
    int xs[K_MAX_X] = { 0 };
    int ys[K_MAX_Y] = { 0 };
    for (int i = 0; i < grps_cnt; i++) {
        xs[grps[i].gid_x] = 1;
        ys[grps[i].gid_y] = 1;
    }
    if (xs[0] == 1 && xs[K_MAX_X - 1] == 1) {
        for (int i = K_MAX_X - 1; i >= 0; i--) {
            if (xs[i] == 1) {
                *shift_x += 1;
            }
            if (xs[i] == 0) {
                break;
            }
        }
        for (int i = 0; i < grps_cnt; i++) {
            grps[i].gid_x = (grps[i].gid_x + *shift_x) % K_MAX_X;
        }
    }
    if (ys[0] == 1 && ys[K_MAX_Y - 1] == 1) {
        for (int i = K_MAX_Y - 1; i >= 0; i--) {
            if (ys[i] == 1) {
                *shift_y += 1;
            }
            if (ys[i] == 0) {
                break;
            }
        }
        for (int i = 0; i < grps_cnt; i++) {
            grps[i].gid_y = (grps[i].gid_y + *shift_y) % K_MAX_Y;
        }
    }
}

/* Unshifts (x,y) coordinates of IO groups shifted by kmr_iolb_shift_grps()
   to restore the original coordinates.
*/
static void
kmr_iolb_unshift_xy(int *x, int *y, int shift_x, int shift_y)
{
    if (shift_x > 0) {
        int sub_x = *x - shift_x;
        *x = (sub_x < 0)? sub_x + K_MAX_X : sub_x;
    }
    if (shift_y > 0) {
        int sub_y = *y - shift_y;
        *y = (sub_y < 0)? sub_y + K_MAX_Y : sub_y;
    }
}

/* Finds location of files specified by a value of a key-value pair.
   It generates a key-value pair whose key is (x,y) coordinates and value is
   filename(s).
*/
static int
kmr_iolb_find_file_location_k(const struct kmr_kv_box kv, const KMR_KVS *kvi,
                              KMR_KVS *kvo, void *p, long i_)
{
    struct io_grp grps[MAX_IO_GRPS];
    int grps_cnt = 0;
    char *p1 = (char *)kv.v.p;
    for (char *p2 = p1; p2 < kv.v.p + kv.vlen; p2++) {
        if (*p2 == '\0') {
            kmr_iolb_load_stripe(p1, grps, &grps_cnt);
            p1 = p2 + 1;
        }
    }
    int shift_x = 0, shift_y = 0;
    kmr_iolb_shift_grps(grps, grps_cnt, &shift_x, &shift_y);
    int x_sum = 0, y_sum = 0, cnt_sum = 0;
    for (int i = 0; i < grps_cnt; i++) {
        x_sum += grps[i].gid_x * grps[i].count;
        y_sum += grps[i].gid_y * grps[i].count;
        cnt_sum += grps[i].count;
    }
    int x = (int)ceil((double)x_sum / cnt_sum);
    int y = (int)ceil((double)y_sum / cnt_sum);
    kmr_iolb_unshift_xy(&x, &y, shift_x, shift_y);

    int file_xy = (x << 16) + y;
    struct kmr_kv_box nkv = { .klen = sizeof(long),
                              .vlen = kv.vlen,
                              .k.i  = file_xy,
                              .v.p  = kv.v.p };
    int cc = kmr_add_kv(kvo, nkv);
    assert(cc == MPI_SUCCESS);

    _Bool tracing5 = (kvi->c.mr->trace_iolb && (5 <= kvi->c.mr->verbosity));
    if (tracing5) {
        char *filename = (char *)malloc((size_t)kv.vlen * sizeof(char));
        memcpy(filename, kv.v.p, (size_t)kv.vlen);
        for (int i = 0; i < kv.vlen; i++) {
            if (filename[i] == '\0') {
                filename[i] = ' ';
            }
        }
        filename[kv.vlen - 1] = '\0';
        fprintf(stderr,
                ";;KMR IOLB [%05d]: Group[%d] - File[%s]\n",
                kvi->c.mr->rank, file_xy, filename);
        fflush(stderr);
        free(filename);
    }

    return MPI_SUCCESS;
}

/* Prints assigned rank and files pairs.
 */
static int
kmr_iolb_print_assigned_files(const struct kmr_kv_box kv, const KMR_KVS *kvi,
                              KMR_KVS *kvo, void *p, long i_)
{
    int rank = (int)kv.k.i;
    char *filename = (char *)malloc((size_t)kv.vlen * sizeof(char));
    memcpy(filename, kv.v.p, (size_t)kv.vlen);
    for (int i = 0; i < kv.vlen; i++) {
        if (filename[i] == '\0') {
            filename[i] = ' ';
        }
    }
    filename[kv.vlen - 1] = '\0';
    fprintf(stderr, ";;KMR IOLB [%05d]: Rank[%05d] - File[%s]\n",
            rank, rank, filename);
    fflush(stderr);
    free(filename);
    return MPI_SUCCESS;
}

/* Checks if values of the specified two key-value pair is same.
 */
static _Bool
kmr_iolb_value_is_equal(const struct kmr_kv_box kv1,
                        const struct kmr_kv_box kv2)
{
    if (kv1.vlen != kv2.vlen) {
        return 0;
    }
    for (int i = 0; i < kv1.vlen; i++) {
        if (kv1.v.p[i] != kv2.v.p[i]) {
            return 0;
        }
    }
    return 1;
}
#endif /*__K*/

/** Assigns files to ranks based on data locality.  It assumes that values
    of key-value pairs in the input KVS are file paths and it shuffles the
    key-value pairs and writes results to the output KVS so that the files
    are assigned to near ranks.  If the value of a key-value pair is file
    paths separated by '\0', it will find a rank near from all the files
    specified in the value.  Currently, it only works on the K computer.
    On the other systems, it just performs kmr_shuffle().
    Effective-options: INSPECT, TAKE_CKPT.  See struct kmr_option. */

int
kmr_assign_file(KMR_KVS *kvi, KMR_KVS *kvo, struct kmr_option opt)
{
#ifdef __K
    KMR *mr = kvi->c.mr;
    struct kmr_option kmr_supported = {.inspect = 1, .take_ckpt = 1};
    kmr_check_fn_options(mr, kmr_supported, opt, __func__);
    if (kmr_ckpt_enabled(mr)) {
        if (kmr_ckpt_progress_init(kvi, kvo, opt)) {
            if (!opt.keep_open) {
                kmr_add_kv_done(kvo);
            }
            if (!opt.inspect) {
                kmr_free_kvs(kvi);
            }
            return MPI_SUCCESS;
        }
    }
    int kcdc = kmr_ckpt_disable_ckpt(mr);
    _Bool tracing5 = (mr->trace_iolb && (5 <= mr->verbosity));
    /*----------------------------------------------------------*/

    /* Create <iog_id,rank> kvs and shuffle it */
    int rank_xy = kmr_iolb_calc_xy_of_rank(mr->rank);
    KMR_KVS *kvs_myrank = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    struct kmr_kv_box kv_rnk = { .klen = sizeof(long),
                                 .vlen = sizeof(long),
                                 .k.i  = rank_xy,
                                 .v.i  = mr->rank };
    int cc = kmr_add_kv(kvs_myrank, kv_rnk);
    assert(cc == MPI_SUCCESS);
    cc = kmr_add_kv_done(kvs_myrank);
    assert(cc == MPI_SUCCESS);
    KMR_KVS *kvs_rank = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    cc = kmr_shuffle(kvs_myrank, kvs_rank, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    if (tracing5) {
        fprintf(stderr, ";;KMR IOLB [%05d]: Group[%d] - Rank[%05d]\n",
                mr->rank, rank_xy, mr->rank);
        fflush(stderr);
    }

    /* Create <iog_id,file_name> kvs and shuffle it */
    enum kmr_kv_field keyf = kvi->c.key_data;
    enum kmr_kv_field valf = kvi->c.value_data;
    assert(valf == KMR_KV_OPAQUE || valf == KMR_KV_CSTRING);
    KMR_KVS *kvs_each_file = kmr_create_kvs(mr, keyf, valf);
    struct kmr_option inspect = {.inspect = 1};
    cc = kmr_shuffle(kvi, kvs_each_file, inspect);
    assert(cc == MPI_SUCCESS);
    KMR_KVS *kvs_fileloc_each = kmr_create_kvs(mr, KMR_KV_INTEGER,
                                               KMR_KV_OPAQUE);
    cc = kmr_map(kvs_each_file, kvs_fileloc_each, NULL, kmr_noopt,
                 kmr_iolb_find_file_location_k);
    assert(cc == MPI_SUCCESS);
    KMR_KVS *kvs_fileloc = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    cc = kmr_shuffle(kvs_fileloc_each, kvs_fileloc, kmr_noopt);
    assert(cc == MPI_SUCCESS);

    /* Merge <iog_id,rank> kvs and <iog_id,file_name> kvs to
       create <rank, file_name> kvs */
    long nranks, nfilelocs;
    cc = kmr_local_element_count(kvs_rank, &nranks);
    assert(cc == MPI_SUCCESS);
    cc = kmr_local_element_count(kvs_fileloc, &nfilelocs);
    assert(cc == MPI_SUCCESS);

    KMR_KVS *kvs_map = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    if (nranks > 0 && nfilelocs > 0) {
        struct kmr_kv_box *kvs_ary1 =
            (struct kmr_kv_box *)malloc((size_t)nranks * sizeof(struct kmr_kv_box));
        cc = kmr_map(kvs_rank, NULL, kvs_ary1, inspect, kmr_copy_to_array_fn);
        assert(cc == MPI_SUCCESS);
        struct kmr_kv_box *kvs_ary2 =
            (struct kmr_kv_box *)malloc((size_t)nfilelocs * sizeof(struct kmr_kv_box));
        cc = kmr_map(kvs_fileloc, NULL, kvs_ary2, inspect, kmr_copy_to_array_fn);
        assert(cc == MPI_SUCCESS);

        int n = (int)(nfilelocs / nranks);
        int r = (int)(nfilelocs % nranks);
        int asgn_cnt = (n == 0)? r : (int)nranks;
        int assigned = 0;
        for (int i = 0; i < asgn_cnt; i++) {
            long t_rank = kvs_ary1[i].v.i;
            int cnt = n + ((i < r)? 1 : 0);
            for (int j = 0; j < cnt; j++) {
                char *t_file = (char *)kvs_ary2[assigned + j].v.p;
                int t_file_siz = kvs_ary2[assigned + j].vlen;
                struct kmr_kv_box nkv = { .klen = sizeof(long),
                                          .vlen = t_file_siz,
                                          .k.i  = t_rank,
                                          .v.p  = (char *)t_file };
		kmr_add_kv(kvs_map, nkv);
            }
            assigned += cnt;
        }
        free(kvs_ary1);
        free(kvs_ary2);
    } else {
        /* TODO
           nranks > 0 && nfilelocs == 0 :
	         no need to do, or read files from other groups
           nranks == 0 && nfilelocs > 0 :
             read files from other groups
           nranks == 0 && nfilelocs == 0 :
             no need to do
	     */
        assert(nfilelocs <= 0);
    }
    kmr_add_kv_done(kvs_map);
    kmr_free_kvs(kvs_rank);
    kmr_free_kvs(kvs_fileloc);

    KMR_KVS *kvs_myfile = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    cc = kmr_shuffle(kvs_map, kvs_myfile, kmr_noopt);
    assert(cc == MPI_SUCCESS);
    if (tracing5) {
        cc = kmr_map(kvs_myfile, NULL, NULL, inspect,
                     kmr_iolb_print_assigned_files);
    }

    /* Create <key_in_kvi,assigned_file_name> kvs and save it as kvo> */
    KMR_KVS *kvi_all = kmr_create_kvs(mr, keyf, valf);
    cc = kmr_replicate(kvi, kvi_all, inspect);
    assert(cc == MPI_SUCCESS);
    long nkvi_all, nkvo;
    cc = kmr_local_element_count(kvi_all, &nkvi_all);
    assert(cc == MPI_SUCCESS);
    struct kmr_kv_box *kvs_ary1 =
        (struct kmr_kv_box *)malloc((size_t)nkvi_all * sizeof(struct kmr_kv_box));
    cc = kmr_map(kvi_all, NULL, kvs_ary1, inspect, kmr_copy_to_array_fn);
    assert(cc == MPI_SUCCESS);
    cc = kmr_local_element_count(kvs_myfile, &nkvo);
    assert(cc == MPI_SUCCESS);
    struct kmr_kv_box *kvs_ary2 =
        (struct kmr_kv_box *)malloc((size_t)nkvo * sizeof(struct kmr_kv_box));
    cc = kmr_map(kvs_myfile, NULL, kvs_ary2, inspect, kmr_copy_to_array_fn);
    assert(cc == MPI_SUCCESS);
    KMR_OMP_PARALLEL_FOR_
	for (int i = 0; i < nkvo; i++) {
	    _Bool kv_added = 0;
	    for (int j = 0; j < nkvi_all; j++) {
		if (kmr_iolb_value_is_equal(kvs_ary1[j], kvs_ary2[i])) {
		    kmr_add_kv(kvo, kvs_ary1[j]);
		    kv_added = 1;
		    break;
		}
	    }
	    assert(kv_added);
	}
    free(kvs_ary1);
    free(kvs_ary2);
    assert(kvs_myfile->c.element_count == kvo->c.element_count);
    kmr_free_kvs(kvs_myfile);
    kmr_free_kvs(kvi_all);

    /*----------------------------------------------------------*/
    kmr_ckpt_enable_ckpt(mr, kcdc);
    if (!opt.keep_open) {
        kmr_add_kv_done(kvo);
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
    return MPI_SUCCESS;
#else
    return kmr_shuffle(kvi, kvo, opt);
#endif /*__K*/
}

/*
Copyright (C) 2012-2015 RIKEN AICS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/
