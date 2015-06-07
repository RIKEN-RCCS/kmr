/* kmrshuller.c */
/* Copyright (C) 2012-2015 RIKEN AICS */

/** \file kmrshuffler.c KMR-Shell Shuffler.  It is a processor for
    map-reduce by shell command pipelining ("streaming").  It works on
    stdin and stdout, with one line for one key-value pair.  It reads
    lines of key-value pairs from stdin, shuffles the pairs, and
    writes lines of key-value pairs to stdout.  The fields of a
    key-value pair are separated by a whitespace.  Lines with the same
    keys constitutes consecutive lines in the output for reduction.
    It is a simple application.  Lines are limited to 32K bytes and
    have no escaping of whitespaces in keys. */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "kmr.h"

/** Maximum length of a line of data. */
#define LINELEN 32767

/** Reads-in key-value lines from stdin into KVS.  It reads a line
    from mapper output, and sets it as a key-value pair in KVS. */

static int
streaminputfn(const struct kmr_kv_box kv0,
	      const KMR_KVS *kvs0, KMR_KVS *kvo, void *p, const long i_)
{
    char line[LINELEN];
    long missingnl = 0;
    long badlines = 0;
    assert(kvs0 == 0 && kv0.klen == 0 && kv0.vlen == 0 && kvo != 0);
    while (fgets(line, sizeof(line), stdin) != NULL) {
	int cc;
	char *cp0 = strchr(line, '\n');
	if (cp0 != NULL) {
	    /* Chomp. */
	    assert(*cp0 == '\n');
	    *cp0 = '\0';
	} else {
	    missingnl++;
	}
	char *cp1 = strchr(line, ' ');
	if (cp1 == NULL) {
	    /* No value field. */
	    badlines++;
	    continue;
	}
	*cp1 = '\0';
	char *key = line;
	char *value = (cp1 + 1);
	struct kmr_kv_box kv;
	kv.klen = (int)strlen(key) + 1;
	kv.vlen = (int)strlen(value) + 1;
	kv.k.p = key;
	kv.v.p = value;
	cc = kmr_add_kv(kvo, kv);
	assert(cc == MPI_SUCCESS);
    }
    if (missingnl) {
	fprintf(stderr, ("kmrshuller: warning: "
			 "Line too long or missing last newline.\n"));
    }
    if (badlines) {
	fprintf(stderr, ("kmrshuller: warning: "
			 "Some lines have no pairs (ignored).\n"));
    }
    return MPI_SUCCESS;
}

/** Writes-out key-value pairs in KVS to stdout.  */

static int
streamoutputfn(const struct kmr_kv_box kv[], const long n,
	       const KMR_KVS *kvs, KMR_KVS *kvo, void *p)
{
    long *badlines = p;
    char *cp1 = strchr(kv[0].k.p, ' ');
    if (cp1 != NULL) {
	(*badlines)++;
    }
    for (long i = 0; i < n; i++) {
	printf("%s %s\n", kv[i].k.p, kv[i].v.p);
    }
    return MPI_SUCCESS;
}

/** Runs KMR shuffler for streaming map-reduce.  It reads maped data
    from stdin, shuffles, and prints shuffled data to stdout.  */

int
main(int argc, char *argv[])
{
    int nprocs, rank, thlv;
    int cc;

    struct kmr_option opt_nothreading;
    opt_nothreading.nothreading = 1;

    /*MPI_Init(&argc, &argv);*/
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &thlv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    kmr_init();

    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);

    /* Read mapper output. */

    cc = kmr_map_once(kvs0, 0, opt_nothreading, 0, streaminputfn);
    assert(cc == MPI_SUCCESS);

    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    cc = kmr_shuffle(kvs0, kvs1, kmr_noopt);
    assert(cc == MPI_SUCCESS);

    /* Output pairs to reducer. */

    long badlines = 0;
    cc = kmr_reduce(kvs1, 0, &badlines, opt_nothreading, streamoutputfn);
    assert(cc == MPI_SUCCESS);
    if (badlines > 0) {
	fprintf(stderr, ("kmrshuller: warning: "
			 "Some keys have whitespaces (ignored).\n"));
    }

    kmr_free_context(mr);
    kmr_fin();
    MPI_Finalize();
    return 0;
}

/*
NOTICE-NOTICE-NOTICE
*/
