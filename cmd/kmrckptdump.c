/* kmrckptdump.c */
/* Copyright (C) 2012-2015 RIKEN AICS */

#include <mpi.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>

#include "kmr.h"
#include "kmrimpl.h"
#include "kmrckpt.h"

#define MAX_ELEMENT_SIZE  8192

/* It returns a string that represents a key-value type. */
static char *
kv_field_str(enum kmr_kv_field v)
{
    switch (v) {
    case KMR_KV_BAD:
	return "BAD";
    case KMR_KV_OPAQUE:
	return "OPAQUE";
    case KMR_KV_INTEGER:
	return "INTEGER";
    case KMR_KV_FLOAT8:
	return "FLOAT8";
    case KMR_KV_POINTER_OWNED:
	return "POINTER_OWNED";
    case KMR_KV_POINTER_UNMANAGED:
	return "KMR_KV_POINTER_UNMANAGED";
    default:
	return "UNKNOWN";
    }
}

/* It dumps a value to string. */
static void
dump_value(union kmr_unit_sized e, int len, enum kmr_kv_field data,
	   char *buf, int buflen)
{
    if ( (data == KMR_KV_OPAQUE) && (strncmp("maxprocs", e.p, 8) == 0) ) {
	int clen = (len < buflen) ? len : buflen;
	memcpy(buf, e.p, (size_t)clen);
	buf[clen - 1] = '\0';
	for (int i = 0; i < len - 1; i++) {
	    if (buf[i] == '\0') {
		buf[i] = ' ';
	    }
	}
    } else {
	kmr_dump_slot(e, len, data, buf, buflen);
    }
}

int
main(int argc, char *argv[])
{
    if (argc != 2) {
	printf("%s ckpt-filename\n", argv[0]);
	exit(1);
    }

    // read checkpoint header info.
    FILE *fp = fopen(argv[1], "r");
    if (fp == NULL) {
	perror("fopen");
	exit(1);
    }

    struct stat sb;
    int ret = stat(argv[1], &sb);
    if (ret != 0) {
	perror("stat");
	exit(1);
    }

    if (strstr(argv[1], "_data_") != NULL) {
	struct kmr_ckpt_data data_hdr;
	size_t hdrsz = offsetof(struct kmr_ckpt_data, data);
	size_t rc = fread((void *)&data_hdr, hdrsz, 1, fp);
	if (rc != 1 && feof(fp) == 1) {
	    fprintf(stderr, "fread failed\n");
	    return 1;
	}

	printf("================Header================\n");
	printf("kvs_id        = %ld\n", data_hdr.kvs_id);
	printf("nprocs        = %d\n",  data_hdr.nprocs);
	printf("rank          = %d\n",  data_hdr.rank);
	printf("key_data      = %s\n",  kv_field_str(data_hdr.key_data));
	printf("value_data    = %s\n",  kv_field_str(data_hdr.value_data));
	printf("======================================\n");

	int cnt = 0;
	unsigned int total = (unsigned int)hdrsz;
	while(total < sb.st_size){
	    char buf[MAX_ELEMENT_SIZE];
	    // read kvs_entry header
	    size_t evhdrsz = offsetof(struct kmr_kvs_entry, c);
	    rc = fread(buf, evhdrsz, 1, fp);
	    if (rc != 1 && feof(fp) == 1) {
		fprintf(stderr, "fread failed\n");
		return 1;
	    }
	    struct kmr_kvs_entry *e = (struct kmr_kvs_entry *)buf;
	    // read ckpt kv data
	    size_t datasz = (size_t)(KMR_ALIGN(e->klen) + KMR_ALIGN(e->vlen));
	    if (datasz + evhdrsz > sizeof(buf)) {
		printf("Key-value data too long for dump.\n");
		break;
	    }
	    rc = fread(buf+evhdrsz, datasz, 1, fp);
	    if (rc != 1 && feof(fp) == 1) {
		fprintf(stderr, "fread failed\n");
		return 1;
	    }
	    struct kmr_kv_box ev;
	    ev = kmr_pick_kv2(e, data_hdr.key_data, data_hdr.value_data);
	    int kvlen = MAX_ELEMENT_SIZE / 2;
	    char keystr[kvlen], valstr[kvlen];
	    kmr_dump_slot(ev.k, e->klen, data_hdr.key_data, keystr,
			  (int)sizeof(keystr));
	    dump_value(ev.v, e->vlen, data_hdr.value_data, valstr,
		       (int)sizeof(valstr));
	    printf("[%d] key[%d] = %s | value[%d] = %s\n", cnt, ev.klen,
		   keystr, ev.vlen, valstr);
	    cnt++;
	    total += (unsigned int)(evhdrsz + datasz);
	}
    } else if (strstr(argv[1], "_log_") != NULL) {
	struct kmr_ckpt_log  log_hdr;
	size_t hdrsz = offsetof(struct kmr_ckpt_log, data);
	size_t rc = fread((void *)&log_hdr, hdrsz, 1, fp);
	if (rc != 1 && feof(fp) == 1) {
	    fprintf(stderr, "fread failed\n");
	    return 1;
	}

	printf("================Header================\n");
	switch (log_hdr.mode) {
	case KMR_CKPT_ALL:
	    printf("mode          = SAVE_ALL\n");
	    break;
	case KMR_CKPT_SELECTIVE:
	    printf("mode          = SAVE_SELECTIVE\n");
	    break;
	}
	printf("nprocs        = %d\n", log_hdr.nprocs);
	printf("rank          = %d\n", log_hdr.rank);
	printf("======================================\n");

	int cnt = 0;
	unsigned int total = (unsigned int)hdrsz;
	while(total < sb.st_size){
	    struct kmr_ckpt_log_entry e;
	    // read ckpt log data
	    rc = fread((void *)&e, sizeof(e), 1, fp);
	    if (rc != 1 && feof(fp) == 1) {
		fprintf(stderr, "fread failed\n");
		return 1;
	    }
	    switch (e.state) {
	    case KMR_CKPT_LOG_WHOLE_START:
		printf("[%05d] state    = SAVE_WHOLE_START\n"
		       "        op_seqno = %ld\n"
		       "        kvi_id   = %ld\n"
		       "        kvo_id   = %ld\n",
		       cnt, e.op_seqno, e.kvi_id, e.kvo_id);
		break;
	    case KMR_CKPT_LOG_WHOLE_FINISH:
		printf("[%05d] state    = SAVE_WHOLE_FINISH\n"
		       "        op_seqno = %ld\n"
		       "        kvi_id   = %ld\n"
		       "        kvo_id   = %ld\n",
		       cnt, e.op_seqno, e.kvi_id, e.kvo_id);
		break;
	    case KMR_CKPT_LOG_BLOCK_START:
		printf("[%05d] state    = SAVE_BLOCK_START\n"
		       "        op_seqno = %ld\n"
		       "        kvi_id   = %ld\n"
		       "        kvo_id   = %ld\n"
		       "        kvi_cnt  = %ld\n"
		       "        kvo_cnt  = %ld\n",
		       cnt, e.op_seqno, e.kvi_id, e.kvo_id, e.n_kvi, e.n_kvo);
		break;
	    case KMR_CKPT_LOG_BLOCK_ADD:
		printf("[%05d] state    = SAVE_BLOCK_ADD\n"
		       "        op_seqno = %ld\n"
		       "        kvi_id   = %ld\n"
		       "        kvo_id   = %ld\n"
		       "        kvi_cnt  = %ld\n"
		       "        kvo_cnt  = %ld\n",
		       cnt, e.op_seqno, e.kvi_id, e.kvo_id, e.n_kvi, e.n_kvo);
		break;
	    case KMR_CKPT_LOG_BLOCK_FINISH:
		printf("[%05d] state    = SAVE_BLOCK_FINISH\n", cnt);
		break;
	    case KMR_CKPT_LOG_INDEX_START:
		printf("[%05d] state    = SAVE_INDEX_START\n"
		       "        op_seqno = %ld\n"
		       "        kvi_id   = %ld\n"
		       "        kvo_id   = %ld\n"
		       "        kvi_cnt  = %ld\n"
		       "        kvo_cnt  = %ld\n",
		       cnt, e.op_seqno, e.kvi_id, e.kvo_id, e.n_kvi, e.n_kvo);
		break;
	    case KMR_CKPT_LOG_INDEX_ADD:
		printf("[%05d] state    = SAVE_INDEX_ADD\n"
		       "        op_seqno = %ld\n"
		       "        kvi_id   = %ld\n"
		       "        kvo_id   = %ld\n"
		       "        ikv_idx  = %ld\n"
		       "        kvo_cnt  = %ld\n",
		       cnt, e.op_seqno, e.kvi_id, e.kvo_id, e.n_kvi, e.n_kvo);
		break;
	    case KMR_CKPT_LOG_INDEX_FINISH:
		printf("[%05d] state    = SAVE_INDEX_FINISH\n", cnt);
		break;
	    case KMR_CKPT_LOG_DELETE_START:
		printf("[%05d] state    = DATA_DELETE_START\n"
		       "        kvs_id   = %ld\n", cnt, e.kvi_id);
		break;
	    case KMR_CKPT_LOG_DELETE_FINISH:
		printf("[%05d] state    = DATA_DELETE_FINISH\n"
		       "        kvs_id   = %ld\n", cnt, e.kvi_id);
		break;
	    case KMR_CKPT_LOG_DELETABLE:
		printf("[%05d] state    = DATA_DELETABLE\n"
		       "        kvs_id   = %ld\n", cnt, e.kvi_id);
		break;
	    case KMR_CKPT_LOG_PROGRESS:
		printf("[%05d] state    = PROGRESS\n"
		       "        op_seqno = %ld\n"
		       "        kvi_id   = %ld\n"
		       "        kvo_id   = %ld\n",
		       cnt, e.op_seqno, e.kvi_id, e.kvo_id);
		break;
	    case KMR_CKPT_LOG_SKIPPED:
		printf("[%05d] state    = SKIPPED\n"
		       "        op_seqno = %ld\n"
		       "        kvi_id   = %ld\n"
		       "        kvo_id   = %ld\n",
		       cnt, e.op_seqno, e.kvi_id, e.kvo_id);
		break;
	    case KMR_CKPT_LOG_LOCK_START:
		printf("[%05d] state    = LOCK_START\n", cnt);
		break;
	    case KMR_CKPT_LOG_LOCK_FINISH:
		printf("[%05d] state    = LOCK_FINISH\n", cnt);
		break;
	    }
	    cnt++;
	    total += (unsigned int)sizeof(e);
	}
    } else {
	fprintf(stderr, "File is not a checkpoint file\n");
	return 1;
    }

    fclose(fp);
    return 0;
}

/*
Copyright (C) 2012-2015 RIKEN AICS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/
