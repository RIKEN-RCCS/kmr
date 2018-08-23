/* kmrspawn.c (2016-07-09) -*-Coding: us-ascii;-*- */
/* Copyright (C) 2012-2016 RIKEN AICS */

/** \file kmrspawn.c Static-Spawning Interface.  This is a part of the
    spawning library (https://github.com/pf-aics-riken/mpispawner).
    See also the source code of the KMR map-reduce library for the use
    of spawning (https://github.com/pf-aics-riken/kmr).  It is the
    worker-side of the master-worker protocol, and the master-side is
    in "kmrwfmap.c" in KMR.  Note that this code is also included in
    KMR to implement a dummy spawning for testing. */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <dlfcn.h>
#include <assert.h>
#include "kmrspawn.h"

/* KMR_MAIN_LIBRARY switches compiling for the KMR main library or
   compiling for the static-spawning library.  The purpose of
   compiling for the main library is for testing the workflow
   scheduler.  The interface functions are renamed by suffixing with
   "_standin" for the main library.  */

#define KMR_NO_STATIC_SPAWNING

#ifdef KMR_NO_STATIC_SPAWNING
#define KMR_MAIN_LIBRARY (1)
#else
#define KMR_MAIN_LIBRARY (0)
#endif

#if KMR_MAIN_LIBRARY
#include "kmr.h"
#include "kmrimpl.h"
#else
#include "kmrld.h"
#endif

#if KMR_MAIN_LIBRARY
/*NOTHING*/
#else
#define kmr_free(P,S) free((P))
#define kmr_error(MR,S) ((*kmr_ld_err)(DIE,(S)))
#define kmr_malloc(S) malloc((S))
#endif

#if KMR_MAIN_LIBRARY
#define kmr_mpi_byte MPI_BYTE
#define kmr_mpi_comm_null MPI_COMM_NULL
#else
#define kmr_mpi_byte (hooks->h.mpi_byte)
#define kmr_mpi_comm_null (hooks->h.mpi_comm_null)
#endif

#if KMR_MAIN_LIBRARY
#define kmr_spawn_true_exit exit
#define kmr_spawn_true_mpi_finalize MPI_Finalize
#define kmr_spawn_mpi_comm_size MPI_Comm_size
#define kmr_spawn_mpi_comm_rank MPI_Comm_rank
#define kmr_spawn_mpi_send MPI_Send
#define kmr_spawn_mpi_recv MPI_Recv
#define kmr_spawn_mpi_get_count MPI_Get_count
#define kmr_spawn_mpi_intercomm_create MPI_Intercomm_create
#define kmr_spawn_mpi_comm_dup MPI_Comm_dup
#define kmr_spawn_mpi_comm_free MPI_Comm_free
#define kmr_spawn_mpi_comm_set_name MPI_Comm_set_name
#else
/*NOTHING*/
#endif

#if KMR_MAIN_LIBRARY
#define KMR_LIBAPI(FN) FN ## _standin
#else
#define KMR_LIBAPI(FN) FN
#endif

/* Save area of hooks.  It is set by kmr_spawn_hookup().  This is used
   as a fake of libkmrspawn.so. */

#if KMR_MAIN_LIBRARY
static struct kmr_spawn_hooks *kmr_fake_spawn_hooks = 0;
#else
/*NOTHING*/
#endif

#if KMR_MAIN_LIBRARY
int
kmr_spawn_hookup_standin(struct kmr_spawn_hooks *hooks)
{
    if (kmr_fake_spawn_hooks != 0 && kmr_fake_spawn_hooks != hooks) {
	kmr_free(kmr_fake_spawn_hooks, sizeof(struct kmr_spawn_hooks));
    }
    kmr_fake_spawn_hooks = hooks;
    return MPI_SUCCESS;
}
#else
/*NOTHING*/
#endif

static int kmr_spawn_exec_command(struct kmr_spawn_hooks *hooks,
				 int argc, char **argv);

/* Records the parameters to spawning.  It registers an exec-function,
   subworld communicators, and their colors.  EXECFN is called at
   starting a work-item.  A communicator in SUBWORLDS will be selected
   as a world for spawning.  A color in COLORS is passed to check the
   correspondence of the subworld at spawning.  ARGSSIZE is the limit
   of the size of the argv strings in RPC messages.  It needs to set
   the MR field in the HOOKS before calling this.  */

int
KMR_LIBAPI(kmr_spawn_setup) (struct kmr_spawn_hooks *hooks,
			     MPI_Comm basecomm, int masterrank,
			     int (*execfn)(struct kmr_spawn_hooks *hooks,
					   int argc, char **argv),
			     int nsubworlds, MPI_Comm subworlds[],
			     unsigned long colors[], size_t argssize)
{
    int cc;

    int nprocs;
    int rank;
    cc = kmr_spawn_mpi_comm_size(basecomm, &nprocs);
    assert(cc == MPI_SUCCESS);
    cc = kmr_spawn_mpi_comm_rank(basecomm, &rank);
    assert(cc == MPI_SUCCESS);

    if (!(0 <= masterrank && masterrank < nprocs)) {
	char ee[80];
	snprintf(ee, sizeof(ee),
		 ("Bad master rank to kmr_spawn_setup() (master=%d).\n"),
		 masterrank);
	kmr_error(hooks->s.mr, ee);
	abort();
    }
    if (rank == masterrank) {
	char ee[80];
	snprintf(ee, sizeof(ee),
		 ("Bad master rank to kmr_spawn_setup();"
		  " master in workers.\n"));
	kmr_error(hooks->s.mr, ee);
	abort();
    }
    if (nsubworlds > KMR_SPAWN_SUBWORLDS) {
	char ee[80];
	snprintf(ee, sizeof(ee),
		 ("Too many subworlds to kmr_spawn_setup()"
		  " (n=%d, limit=%d).\n"),
		 nsubworlds, KMR_SPAWN_SUBWORLDS);
	kmr_error(hooks->s.mr, ee);
	abort();
    }

    hooks->s.master_rank = masterrank;
    hooks->s.base_rank = rank;
    hooks->s.base_comm = basecomm;
    for (int i = 0; i < KMR_SPAWN_SUBWORLDS; i++) {
	if (i < nsubworlds) {
	    hooks->s.subworlds[i].comm = subworlds[i];
	    hooks->s.subworlds[i].color = colors[i];
	} else {
	    hooks->s.subworlds[i].comm = kmr_mpi_comm_null;
	    hooks->s.subworlds[i].color = 0;
	}
    }

    if (execfn != 0) {
	hooks->s.exec_fn = execfn;
    } else {
	hooks->s.exec_fn = kmr_spawn_exec_command;
    }

    if (hooks->s.rpc_buffer != 0) {
	kmr_free(hooks->s.rpc_buffer, hooks->s.rpc_size);
	hooks->s.rpc_buffer = 0;
    }
    size_t msz = (offsetof(struct kmr_spawn_work, args) + argssize);
    hooks->s.rpc_buffer = kmr_malloc(msz);
    hooks->s.rpc_size = msz;

    hooks->s.spawn_world = kmr_mpi_comm_null;
    hooks->s.spawn_parent = kmr_mpi_comm_null;
    hooks->s.running_work = 0;
    hooks->s.mpi_initialized = 0;
    hooks->s.abort_when_mpi_abort = 0;

    return MPI_SUCCESS;
}

void
KMR_LIBAPI(kmr_spawn_set_verbosity) (struct kmr_spawn_hooks *hooks, int level)
{
    hooks->s.print_trace = (level >= 2);
#if KMR_MAIN_LIBRARY
/*NOTHING*/
#else
    kmr_ld_set_error_printer(level, 0);
#endif
}

static int kmr_spawn_join_to_master(struct kmr_spawn_hooks *hooks,
				    struct kmr_spawn_work *w, size_t msglen);
static int kmr_spawn_clean_process(struct kmr_spawn_hooks *hooks);
static int kmr_spawn_start_work(struct kmr_spawn_hooks *hooks,
				struct kmr_spawn_work *w, size_t msglen);

/* Waits for a work request from the master and handles it.  It idles
   forever by receiving a message.  Furthermore, it never returns,
   when it starts a new executable.  Thus, it is normally entered to
   resume the service instead of looping inside.  It should be called
   with (hooks->s.service_count=0) at the very first time, which
   causes to send a joining message to the master. */

void
KMR_LIBAPI(kmr_spawn_service) (struct kmr_spawn_hooks *hooks, int status)
{
    _Bool tracing5 = (hooks->s.print_trace);
    MPI_Comm basecomm = hooks->s.base_comm;
    const int master = hooks->s.master_rank;

    int cc;

    assert(hooks->s.base_rank != hooks->s.master_rank);
    assert(hooks->s.rpc_buffer != 0 && hooks->s.rpc_size > 0);

    /* Reset the previous state. */

#if 0
    cc = kmr_spawn_clean_process(hooks);
    assert(cc == MPI_SUCCESS);
#endif

    int exitstatus;
    exitstatus = status;

    for (;;) {
	if (tracing5) {
	    fprintf(stderr, ";;KMR [%05d] Entering service loop.\n",
		    hooks->s.base_rank);
	    fflush(0);
	}

	{
	    struct kmr_spawn_next mm2;
	    struct kmr_spawn_next *mbuf = &mm2;
	    mbuf->req = KMR_SPAWN_NEXT;
	    mbuf->protocol_version = KMR_SPAWN_MAGIC;
	    mbuf->initial_message = (hooks->s.service_count == 0);
	    mbuf->status = exitstatus;
	    int msz = (int)sizeof(struct kmr_spawn_next);
	    cc = kmr_spawn_mpi_send(mbuf, msz, kmr_mpi_byte, master,
				    KMR_SPAWN_RPC_TAG, basecomm);
	    assert(cc == MPI_SUCCESS);
	}

	union kmr_spawn_rpc *mbuf = hooks->s.rpc_buffer;
	int msz = (int)hooks->s.rpc_size;
	mbuf->req = KMR_SPAWN_NONE;
	MPI_Status st;
	int len;
	cc = kmr_spawn_mpi_recv(mbuf, msz, kmr_mpi_byte, master,
				KMR_SPAWN_RPC_TAG, basecomm, &st);
	assert(cc == MPI_SUCCESS);
	cc = kmr_spawn_mpi_get_count(&st, kmr_mpi_byte, &len);
	assert(cc == MPI_SUCCESS);
	size_t msglen = (size_t)len;
	int rank = st.MPI_SOURCE;

	hooks->s.service_count++;
	assert(hooks->s.service_count != 0);

	switch (mbuf->req) {
	case KMR_SPAWN_NONE: {
	    assert(msglen == sizeof(struct kmr_spawn_none));
	    struct kmr_spawn_none *w = &(mbuf->m2);
	    assert(w->protocol_version == KMR_SPAWN_MAGIC);
	    /* Exit for-loop. */
	    break;
	}

	case KMR_SPAWN_WORK: {
	    struct kmr_spawn_work *w0 = &(mbuf->m1);
	    if (msglen != (size_t)w0->message_size) {
		kmr_error(hooks->s.mr,
			  "Bad RPC message size");
		abort();
	    }
	    assert(w0->protocol_version == KMR_SPAWN_MAGIC);
	    size_t asz = (msglen - offsetof(struct kmr_spawn_work, args));

	    if (0) {
		if (asz == 0) {
		    fprintf(stderr,
			    ";;KMR [%05d] Receive an activate message.\n",
			    hooks->s.base_rank);
		    fflush(0);
		}
	    }

	    if (asz == 0) {
		/* Skip an activation message. */
		continue;
	    } else {
		cc = kmr_spawn_clean_process(hooks);
		assert(cc == MPI_SUCCESS);

		struct kmr_spawn_work *w = kmr_malloc(msglen);
		assert(w != 0);
		memcpy(w, w0, msglen);
		cc = kmr_spawn_join_to_master(hooks, w, msglen);
		assert(cc == MPI_SUCCESS);
		exitstatus = kmr_spawn_start_work(hooks, w, msglen);

		/* (NEVER RETURNS WHEN EXECED). */

		exit(exitstatus);

		cc = kmr_spawn_clean_process(hooks);
		assert(cc == MPI_SUCCESS);
		continue;
	    }
	}

	default: {
	    char ee[80];
	    snprintf(ee, sizeof(ee),
		     "Bad RPC message request=0x%x length=%zd from rank=%d.\n",
		     mbuf->req, msglen, rank);
	    kmr_error(hooks->s.mr, ee);
	    abort();
	    break;
	}
	}
	/* Exit for-loop. */
	break;
    }

    if (hooks->s.rpc_buffer != 0) {
	kmr_free(hooks->s.rpc_buffer, hooks->s.rpc_size);
	hooks->s.rpc_buffer = 0;
	hooks->s.rpc_size = 0;
    }

    kmr_spawn_true_mpi_finalize();
    kmr_spawn_true_exit(0);
    abort();
}

static int kmr_spawn_make_argv_printable(char *s, size_t sz, char **argv);

/* Runs the command. */

static int
kmr_spawn_start_work(struct kmr_spawn_hooks *hooks,
		     struct kmr_spawn_work *w, size_t msglen)
{
    _Bool tracing5 = (hooks->s.print_trace || w->print_trace != 0);
    size_t asz = (msglen - offsetof(struct kmr_spawn_work, args));

    if (!(0 <= w->subworld && w->subworld < KMR_SPAWN_SUBWORLDS)
	|| hooks->s.subworlds[w->subworld].comm == kmr_mpi_comm_null) {
	char ee[80];
	snprintf(ee, sizeof(ee),
		 ("Bad subworld index for spawning (index=%d).\n"),
		 w->subworld);
	kmr_error(hooks->s.mr, ee);
	abort();
    }

    int argc;
    argc = 0;

    {
	char *e = &(w->args[asz]);
	char *p;
	p = w->args;
	while (p[0] != 0 && p < (e - 1)) {
	    argc++;
	    while (p[0] != 0 && p < (e - 1)) {
		p++;
	    }
	    if (p < (e - 1)) {
		assert(p[0] == 0);
		p++;
	    }
	}
	assert(p == (e - 1) || p[0] == 0);
    }

    char *argv[argc + 1];

    {
	int i;
	i = 0;
	char *e = &(w->args[asz]);
	char *p;
	p = w->args;
	while (p[0] != 0 && p < (e - 1)) {
	    assert(i < argc);
	    argv[i] = p;
	    i++;
	    while (p[0] != 0 && p < (e - 1)) {
		p++;
	    }
	    if (p < (e - 1)) {
		assert(p[0] == 0);
		p++;
	    }
	}
	assert(p == (e - 1) || p[0] == 0);
	assert(i == argc);
	argv[argc] = 0;
    }

    hooks->s.running_work = w;
    hooks->s.mpi_initialized = 0;

    if (tracing5) {
	char aa[80];
	kmr_spawn_make_argv_printable(aa, 45, argv);
	printf(";;KMR [%05d] EXEC: %s\n",
	       hooks->s.base_rank, aa);
	fflush(0);
    }

    assert(hooks->s.exec_fn != 0);
    int exitstatus = (*hooks->s.exec_fn)(hooks, argc, argv);
    /* (NEVER RETURNS WHEN EXECED). */

    hooks->s.running_work = 0;
    hooks->s.mpi_initialized = 0;

    return exitstatus;
}

static int
kmr_spawn_exec_command(struct kmr_spawn_hooks *hooks, int argc, char **argv)
{
#if KMR_MAIN_LIBRARY
    return MPI_SUCCESS;
#else
    kmr_ld_usoexec(argv, 0, hooks->d.initial_argv, hooks->d.options_flags,
		   hooks->d.options_heap_bottom);
    return MPI_SUCCESS;
#endif
}

static int
kmr_spawn_make_argv_printable(char *s, size_t sz, char **argv)
{
    assert(sz > 4);
    int cc;
    *s = 0;
    size_t cnt;
    cnt = 0;
    for (int i = 0; argv[i] != 0; i++) {
	cc = snprintf(&s[cnt], (sz - cnt), "%s%s",
		      (i == 0 ? "" : ","), argv[i]);
	cnt += (size_t)cc;
	if (cnt >= sz) {
	    snprintf(&s[sz - 4], 4, "...");
	    return 0;
	}
    }
    return 0;
}

/* Connects to the master by MPI_Intercomm_create(). */

static int
kmr_spawn_join_to_master(struct kmr_spawn_hooks *hooks,
			 struct kmr_spawn_work *w, size_t msglen)
{
    assert(w->subworld < KMR_SPAWN_SUBWORLDS);
    assert(hooks->s.subworlds[w->subworld].comm != kmr_mpi_comm_null);
    MPI_Comm basecomm = hooks->s.base_comm;
    //const int master = hooks->s.master_rank;
    MPI_Comm subworld = hooks->s.subworlds[w->subworld].comm;
    unsigned long color = hooks->s.subworlds[w->subworld].color;
    _Bool tracing5 = hooks->s.print_trace;

    int cc;

    int nprocs;
    cc = kmr_spawn_mpi_comm_size(subworld, &nprocs);
    assert(cc == MPI_SUCCESS);
    if (w->nprocs > nprocs) {
	char ee[80];
	snprintf(ee, sizeof(ee),
		 ("Bad spawn call; number of procs mismatch"
		  " (requested=%d size=%d)"),
		 w->nprocs, nprocs);
	kmr_error(hooks->s.mr, ee);
	abort();
    }
    if (w->color != color) {
	char ee[80];
	snprintf(ee, sizeof(ee),
		 ("Bad spawn call; color mismatch (%lu,%lu).\n"),
		 w->color, color);
	kmr_error(hooks->s.mr, ee);
	abort();
    }

    assert(hooks->s.spawn_world == kmr_mpi_comm_null);
    assert(hooks->s.spawn_parent == kmr_mpi_comm_null);

    cc = kmr_spawn_mpi_comm_dup(subworld, &hooks->s.spawn_world);
    assert(cc == MPI_SUCCESS);
    cc = kmr_spawn_mpi_comm_set_name(hooks->s.spawn_world,
				     hooks->h.world_name);
    assert(cc == MPI_SUCCESS);

    if (tracing5) {
	fprintf(stderr, ";;KMR [%05d] Connect to master.\n",
		hooks->s.base_rank);
	fflush(0);
    }

    cc = kmr_spawn_mpi_intercomm_create(hooks->s.spawn_world, 0,
					basecomm,
					hooks->s.master_rank,
					KMR_SPAWN_ICOMM_TAG,
					&hooks->s.spawn_parent);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/* Cleans the resources of the worker state.  It frees the current
   work-item.  */

static int
kmr_spawn_clean_process(struct kmr_spawn_hooks *hooks)
{
    int cc;

    if (hooks->s.running_work != 0) {
	kmr_free(hooks->s.running_work,
		 (size_t)hooks->s.running_work->message_size);
	hooks->s.running_work = 0;
    }

    hooks->s.mpi_initialized = 0;

    if (hooks->s.spawn_world != kmr_mpi_comm_null) {
	cc = kmr_spawn_mpi_comm_free(&hooks->s.spawn_world);
	assert(cc == MPI_SUCCESS);
    }
    if (hooks->s.spawn_parent != kmr_mpi_comm_null) {
	cc = kmr_spawn_mpi_comm_free(&hooks->s.spawn_parent);
	assert(cc == MPI_SUCCESS);
    }

    /* MORE NEEDED */

    return MPI_SUCCESS;
}

#if 0
__attribute__ ((noreturn)) void
kmr_spawn_idle(struct kmr_spawn_hooks *hooks)
{
    while (1) {sleep(3600);}
}
#endif

/*
Copyright (C) 2012-2016 RIKEN AICS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/
