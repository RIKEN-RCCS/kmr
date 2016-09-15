/* kmrwfmap.c (2016-07-14) -*-Coding: us-ascii;-*- */
/* Copyright (C) 2012-2016 RIKEN AICS */

/** \file kmrwfmap.c Simple Workflow by Static-Spawning.  It needs a
    library for static-spawning "libkmrspawn.so" (it is only available
    on K).  It runs MPI executables under the control of a simple
    master/worker scheduler.  It groups ranks as lanes, where the
    lanes are hierarchically split into maximum four levels of
    sublanes.  Each lane is associated with a subworld communicator.
    In the following diagram, lane (0) is split into two lanes (0.0)
    and (0.1), and they are split further.  A work-item (or job/task)
    is enqueued in a lane specified by a list of lane-numbers like
    (0.1) or (0.1.1).  A work-item entered in a lane runs using all
    sublanes below it.  Work-items in each lane are scheduled in the
    FIFO order.  The single and dedicated master rank keeps track of
    running/idling lanes.

    ~~~~
    Split of Lanes (three levels):
    (0).-.(0.0).-.(0.0.0).-.a-number-of-ranks
    ....-.......-.(0.0.1).-.a-number-of-ranks
    ....-.......-.(0.0.2).-.a-number-of-ranks
    ....-.(0.1).-.(0.1.0).-.a-number-of-ranks
    ....-.......-.(0.1.1).-.a-number-of-ranks
    ....-.......-.(0.1.2).-.a-number-of-ranks
    (1).-.(1.0).-.(1.0.0).-.a-number-of-ranks
    ....-.......-.(1.0.1).-.a-number-of-ranks
    ....-.......-.(1.0.2).-.a-number-of-ranks
    ~~~~

    The design of this workflow specifies explicitly the scheduling,
    and thus the data-flow (dependency) is implicit.  The scheduler at
    each rank is almost stateless, where the state is stored in the
    spawning library.  The include file "kmrspawn.h" is an interface
    to the spawning library.  IMPLEMENTATION NOTE: The file
    "kmrspawn.c" is copied from the spawning library to use it as a
    dummy worker.  IMPLEMENTATION NOTE: Creation of
    inter-communicators for subworlds is serialized, and a use of a
    single tag is sufficient. */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <unistd.h>
#include <limits.h>
#include <string.h>
#include <dlfcn.h>
#include <errno.h>
#include <assert.h>
#ifdef _OPENMP
#include <omp.h>
#endif
#include "kmr.h"
#include "kmrimpl.h"
#include "kmrspawn.h"

#define MAX(a,b) (((a)>(b))?(a):(b))

/** Maximum Levels of Lanes.  */

#define KMR_LANE_LEVELS (4)

static short const KMR_NO_LANE = -1;
static short const KMR_ANY_LANE = -2;

/** Lane Number (at-all-ranks).  Lanes are hierarchical partitioning
    of communicators, and their depth is limited by KMR_LANE_LEVELS.
    Currently, the lane-number (structure kmr_lane_no) is stored as
    unsigned-long and the levels are limited by four.  Unspecifed
    levels must be set to KMR_NO_LANE.  If KMR_ANY_LANE appears at a
    level, it should so at the subordinate levels. */

struct kmr_lane_no {short v[KMR_LANE_LEVELS];};

/*static const int KMR_ENQ_TAG = 610;*/

/** Work Description (at-the-master).  The tail of this structure is
    truncated by the size of the argument string.  It has only a
    proper format lane-number. */

struct kmr_work_item {
    enum kmr_spawn_req req;
    struct kmr_lane_no requesting_lane;
    struct kmr_lane_no assigned_lane;
    int level;
    int sequence_no;
    struct kmr_spawn_work work;
};

/** Work-Item Queue Entry (at-the-master).  Lists are linked on
    QUEUE_HEAD and QUEUE_INSERTION_TAIL of a lane structure.  A
    work-item is placed in a lane and also its sublanes to mark for
    yielding sublanes.  A work-item to any-lane is placed in multiple
    lanes.  The entries in multiple lanes are removed at the first
    dequeuing. */

struct kmr_work_list {
    struct kmr_work_list *next;
    struct kmr_work_item *item;
};

/** Work-Item Queue of a Lane (at-the-master).  TOTAL_SUBLANES is the
    count of all descendent sublanes (includes itself), and thus, the
    bottom lanes have one for it.  SUBLANES holds the sublanes, and it
    is null for the bottom lanes.  WORKERS holds the ranks of workers
    of the bottom lanes.  Either SUBLANES or WORKERS is null.
    CURRENT_WORK holds the running work-item.  N_JOINED_RANKS and
    RUNNING_SUBLANES is used only for assertions.  N_JOINED_RANKS is
    valid only on the top and the bottom lanes. */

struct kmr_lane_state {
    struct kmr_lane_no lane_id;
    int level;
    int leader_rank;
    int total_sublanes;
    int total_ranks;
    struct kmr_lane_state *superlane;
    struct kmr_lane_vector *sublanes;
    struct kmr_rank_vector *workers;

    /* Transient states below. */

    MPI_Comm icomm;
    struct kmr_work_list queue_head;
    struct kmr_work_list *queue_insertion_tail;
    struct kmr_work_item *current_work;
    struct kmr_work_item *yielding_to_superlane;
    int n_joined_ranks;
    int n_running_sublanes;
    _Bool *running_sublanes;

    /* (Temporary used in initialization). */

    struct kmr_lane_state *link;
};

/** Vector of Lanes (at-the-master). */

struct kmr_lane_vector {
    int n;
    struct kmr_lane_state *lanes[1] /*[n]*/;
};

/** Vector of Ranks (at-the-master). */

struct kmr_rank_vector {
    int n;
    int ranks[1] /*[n]*/;
};

struct kmr_pair {int size; int rank;};

/** Workflow State (at-all-ranks).  It is saved in KMR context as
    (mr->simple_workflow).  The state on workers is only available
    during a setup.  BASE_COMM, NPROCS, and RANK are of the copies of
    ones of the KMR context MR.  BASE_COMM is a communicator of all
    ranks.  LANE_COMMS holds the communicators associated with the
    lanes, which are passed by kmr_init_swf().  TOP_LANE is a
    superlane of all lanes.  LIST_OF_ALL_LANES holds all lanes used
    for initialization and finalization. */

struct kmr_swf {
    KMR *mr;
    MPI_Comm base_comm;
    int nprocs;
    int rank;

    int master_rank;
    struct kmr_lane_no lane_id_on_proc;
    MPI_Comm lane_comms[KMR_LANE_LEVELS];

    size_t args_size;

    /* Slots on the master. */

    struct {
	int idle_ranks;
	struct kmr_lane_state *top_lane;
	struct kmr_lane_state **lane_of_workers /*[nprocs]*/;
	struct kmr_lane_state *list_of_all_lanes;
	union kmr_spawn_rpc *rpc_buffer;
	size_t rpc_size;
	struct kmr_work_list history_head;
	struct kmr_work_list *history_insertion_tail;
	_Bool record_history;
	//_Bool force_free_communicators;
    } master;

    /* Static-Spawning API for Wokers. */

    struct kmr_spawn_hooks *hooks;
    int (*kmr_spawn_hookup)(struct kmr_spawn_hooks *hooks);
    int (*kmr_spawn_setup)(struct kmr_spawn_hooks *hooks,
			   MPI_Comm basecomm, int masterrank,
			   int (*execfn)(struct kmr_spawn_hooks *,
					 int, char **),
			   int nsubworlds,
			   MPI_Comm subworlds[], unsigned long colors[],
			   size_t argssize);
    void (*kmr_spawn_service)(struct kmr_spawn_hooks *hooks, int status);
    void (*kmr_spawn_set_verbosity)(struct kmr_spawn_hooks *hooks, int level);
};

/* Save area of hooks.  It is set by kmr_spawn_hookup().  This is used
   as a fake of libkmrspawn.so. */

#if 0 /*AHO*/
static struct kmr_spawn_hooks *kmr_fake_spawn_hooks = 0;
#endif

/* ================================================================ */

/* Initialization of Lanes of Workflow. */

/** Compares lane-numbers up to the LEVEL.  Note that comparison
    includes the LEVEL.  It returns true when (LEVEL=-1). */

static inline _Bool
kmr_lane_eq(struct kmr_lane_no n0, struct kmr_lane_no n1, int level)
{
    assert(-1 <= level && level < KMR_LANE_LEVELS);
    for (int i = 0; i <= level; i++) {
	if (n0.v[i] != n1.v[i]) {
	    return 0;
	}
    }
    return 1;
}

/** Clears the lane-number to a null-lane. */

static inline void
kmr_clear_lane_id(struct kmr_lane_no *id)
{
    for (int i = 0; i < KMR_LANE_LEVELS; i++) {
	id->v[i] = KMR_NO_LANE;
    }
}

/** Parses a string as a lane-number.  It signals an error for
    improper format strings, but, it does not check the range of each
    index.  Examples are: "", "3", "3.3.1", "*", "3.3.*", "3.*.*" (""
    is parsed but unusable).  Illegal examples are: ".", "3.",
    "3.*.3". */

static struct kmr_lane_no
kmr_name_lane(KMR *mr, const char *s)
{
    struct kmr_lane_no n;
    for (int i = 0; i < KMR_LANE_LEVELS; i++) {
	n.v[i] = KMR_NO_LANE;
    }
    const char *e = (s + strlen(s));
    const char *p;
    p = s;
    for (int i = 0; i < KMR_LANE_LEVELS && p < e; i++) {
	if (*p == '*') {
	    if ((p + 1) == e) {
		/* Tail, OK. */
		n.v[i] = KMR_ANY_LANE;
		p++;
		break;
	    } else if (*(p + 1) == '.' && (p + 2) == e) {
		char ee[80];
		snprintf(ee, sizeof(ee),
			 ("Bad lane string; dot at the tail (%s)"), s);
		kmr_error(mr, ee);
		abort();
		break;
	    } else if (*(p + 1) == '.') {
		/* "*.", OK. */
		n.v[i] = KMR_ANY_LANE;
		p += 2;
	    } else {
		char ee[80];
		snprintf(ee, sizeof(ee),
			 ("Bad lane string; * followed by something (%s)"), s);
		kmr_error(mr, ee);
		abort();
		break;
	    }
	} else {
	    int d;
	    char dot[8];
	    int cc = sscanf(p, "%d%c", &d, dot);
	    if (cc == 1 || (cc == 2 && dot[0] == '.')) {
		while (p < e && *p != '.') {
		    assert('0' <= *p && *p <= '9');
		    p++;
		}
		if (p < e && *p == '.') {
		    p++;
		}
		_Bool sawany = (i > 0 && n.v[i - 1] == KMR_ANY_LANE);
		if (sawany) {
		    char ee[80];
		    snprintf(ee, sizeof(ee),
			     ("Bad lane string; * at non-tail (%s)"), s);
		    kmr_error(mr, ee);
		    abort();
		    break;
		} else if (cc == 2 && p == e) {
		    char ee[80];
		    snprintf(ee, sizeof(ee),
			     ("Bad lane string; dot at the tail (%s)"), s);
		    kmr_error(mr, ee);
		    abort();
		    break;
		} else {
		    n.v[i] = (short)d;
		}
	    } else if (cc == 0) {
		char ee[80];
		snprintf(ee, sizeof(ee),
			 ("Bad lane string; non-digit appears (%s)"), s);
		kmr_error(mr, ee);
		abort();
		break;
	    } else {
		char ee[80];
		snprintf(ee, sizeof(ee),
			 ("Bad lane string; non-digit or bad dot (%s)"), s);
		kmr_error(mr, ee);
		abort();
		break;
	    }
	}
    }
    if (p != e) {
	char ee[80];
	snprintf(ee, sizeof(ee),
		 ("Bad lane string; garbage at the tail (%s)"), s);
	kmr_error(mr, ee);
	abort();
    }
    return n;
}

/** (NO-THREAD-SAFE) Returns a string representation of a lane-number.
    It returns "-" for a null-lane. */

static char *
kmr_lane_string(struct kmr_lane_no n, _Bool print_all_levels)
{
    static char buf[20];
    snprintf(buf, sizeof(buf), "-");
    char *e = (buf + sizeof(buf));
    char *p;
    p = buf;
    for (int i = 0; i < KMR_LANE_LEVELS && p < e; i++) {
	int q = n.v[i];
	if (q == KMR_NO_LANE) {
	    if (print_all_levels) {
		char *dot = (i == 0 ? "" : ".");
		int cc = snprintf(p, (size_t)(e - p), "%s-", dot);
		p += cc;
	    }
	    if (!print_all_levels) {
		break;
	    }
	} else if (q == KMR_ANY_LANE) {
	    char *dot = (i == 0 ? "" : ".");
	    int cc = snprintf(p, (size_t)(e - p), "%s*", dot);
	    p += cc;
	    if (!print_all_levels) {
		break;
	    }
	} else {
	    char *dot = (i == 0 ? "" : ".");
	    int cc = snprintf(p, (size_t)(e - p), "%s%d", dot, q);
	    p += cc;
	}
    }
    buf[sizeof(buf) - 1] = 0;
    return buf;
}

/** Returns the maximum level of a given lane-number (zero to
    KMR_LANE_LEVELS-1), or returns -1 for a null-lane. */

static int
kmr_level_of_lane(struct kmr_lane_no n, _Bool admit_any)
{
    int level;
    level = (KMR_LANE_LEVELS - 1);
    for (int i = 0; i < KMR_LANE_LEVELS; i++) {
	assert(admit_any || n.v[i] != KMR_ANY_LANE);
	assert(!(n.v[i] == KMR_NO_LANE && (i + 1) < KMR_LANE_LEVELS)
	       || n.v[i + 1] == KMR_NO_LANE);
	assert(!(n.v[i] == KMR_ANY_LANE && (i + 1) < KMR_LANE_LEVELS)
	       || n.v[i + 1] == KMR_NO_LANE || n.v[i + 1] == KMR_ANY_LANE);
	if (level == (KMR_LANE_LEVELS - 1) && n.v[i] == KMR_NO_LANE) {
	    level = (i - 1);
	}
    }
    return level;
}

/** Returns a lane-number as a single integer color.  A color is used
    to check the identity of a lane in assertions.  A color is used in
    place of a lane in the spawn-library, because it does not know the
    lanes. */

static inline unsigned long
kmr_color_of_lane(struct kmr_lane_no id)
{
    union {struct kmr_lane_no id; unsigned long color;} u = {.id = id};
    return u.color;
}

/** Finds a worker index of a lane for a rank. */

static inline int
kmr_find_worker_index(struct kmr_swf *wf, struct kmr_lane_state *lane,
		      int rank)
{
    assert(lane->workers != 0);
    struct kmr_rank_vector *u = lane->workers;
    int s;
    s = -1;
    for (int i = 0; i < u->n; i++) {
	if (u->ranks[i] == rank) {
	    s = i;
	    break;
	}
    }
    assert(s != -1);
    return s;
}

/** Finds a sublane index of a superlane. */

static inline int
kmr_find_sublane_index(struct kmr_swf *wf, struct kmr_lane_state *lane)
{
    assert(lane->superlane != 0);
    struct kmr_lane_state *sup = lane->superlane;
    assert(sup->sublanes != 0);
    struct kmr_lane_vector *v = sup->sublanes;
    int s;
    s = -1;
    for (int i = 0; i < v->n; i++) {
	if (v->lanes[i] == lane) {
	    s = i;
	    break;
	}
    }
    assert(s != -1);
    return s;
}

/** Packs lanes in a vector.  It returns an unfilled vector when a
    null LANES is passed. */

static struct kmr_lane_vector *
kmr_make_lane_vector(int n, struct kmr_lane_state *lanes[])
{
    size_t vsz = (offsetof(struct kmr_lane_vector, lanes)
		  + sizeof(struct kmr_lane_state *) * (size_t)n);
    struct kmr_lane_vector *v = kmr_malloc(vsz);
    assert(v != 0);
    memset(v, 0, vsz);
    v->n = n;
    if (lanes != 0) {
	for (int q = 0; q < n; q++) {
	    v->lanes[q] = lanes[q];
	}
    }
    return v;
}

/** Allocates a rank vector, filling all entries with -1. */

static struct kmr_rank_vector *
kmr_make_rank_vector(int n)
{
    size_t vsz = (offsetof(struct kmr_rank_vector, ranks)
		  + sizeof(int) * (size_t)n);
    struct kmr_rank_vector *v = kmr_malloc(vsz);
    assert(v != 0);
    v->n = n;
    for (int i = 0; i < n; i++) {
	v->ranks[i] = -1;
    }
    return v;
}

static void
kmr_err_when_swf_is_not_initialized(KMR *mr)
{
    struct kmr_swf *wf = mr->simple_workflow;
    if (wf == 0) {
	kmr_error(mr, "Workflow-mapper is not initialized");
	abort();
    }
}

/** Sets the verbosity of the spawn-library.  LEVEL is 1 to 3, where 3
    is the most verbose.  It should be called after kmr_init_swf() and
    before detaching by kmr_detach_swf_workers() to affect worker
    ranks. */

void
kmr_set_swf_verbosity(KMR *mr, int level)
{
    kmr_err_when_swf_is_not_initialized(mr);
    struct kmr_swf *wf = mr->simple_workflow;
    if (wf->kmr_spawn_set_verbosity != 0) {
	(*wf->kmr_spawn_set_verbosity)(wf->hooks, level);
    }
}

static int kmr_load_spawn_library(struct kmr_swf *wf,
				  _Bool test_with_fake_spawn);
static void kmr_resolve_lanes(struct kmr_swf *wf);
static void kmr_make_lanes(struct kmr_swf *wf);
static void kmr_free_lanes(struct kmr_swf *wf, struct kmr_lane_state *lane);

/** Initializes the lanes of simple workflow.  Lanes of workflow are
    created corresponding to communicators in LANECOMMS[], one lane
    for each communicator.  Work-items in a lane at a level are
    executed in the communicator LANECOMMS[level].  MASTER specifies
    the master rank, which should not be included in any communicator,
    because the master needs to be distinct from the workers.  */

int
kmr_init_swf(KMR *mr, MPI_Comm lanecomms[KMR_LANE_LEVELS], int master)
{
    _Bool tracing5 = (mr->trace_map_spawn && (5 <= mr->verbosity));
    _Bool test_with_fake_spawn = mr->swf_debug_master;

    int cc;

    /* Check if a lane-number can be stored in kmr_spawn_work.color,
       which is used to check the consistent use of split
       communicators. */

    assert(sizeof(struct kmr_lane_no) <= sizeof(unsigned long));

    if (mr->simple_workflow != 0) {
	kmr_error(mr, "Workflow-mapper is already initialized");
	abort();
	return MPI_ERR_SPAWN;
    }

    if (!(master < mr->nprocs)) {
	char ee[80];
	snprintf(ee, sizeof(ee),
		 "Bad master rank specified (rank=%d)", master);
	kmr_error(mr, ee);
	abort();
	return MPI_ERR_SPAWN;
    }

    struct kmr_swf *wf = kmr_malloc(sizeof(struct kmr_swf));
    assert(wf != 0);
    memset(wf, 0, sizeof(struct kmr_swf));
    mr->simple_workflow = wf;
    wf->mr = mr;
    wf->base_comm = mr->comm;
    wf->nprocs = mr->nprocs;
    wf->rank = mr->rank;

    wf->master_rank = master;
    for (int i = 0; i < KMR_LANE_LEVELS; i++) {
	wf->lane_comms[i] = lanecomms[i];
    }

    wf->args_size = ((mr->swf_args_size != 0)
		     ? mr->swf_args_size : KMR_SPAWN_ARGS_SIZE);

    wf->master.idle_ranks = 0;
    wf->master.top_lane = 0;
    wf->master.lane_of_workers = 0;
    wf->master.list_of_all_lanes = 0;
    wf->master.rpc_buffer = 0;
    wf->master.rpc_size = 0;
    wf->master.history_head.next = 0;
    wf->master.history_head.item = 0;
    wf->master.history_insertion_tail = 0;
    wf->master.record_history = 0;
    //wf->master.force_free_communicators = 0;

    /* Load the spawn-library. */

    cc = kmr_load_spawn_library(wf, test_with_fake_spawn);
    /* IGNORE FAILURES IN LOADING A LIBRARY. */

    struct kmr_spawn_hooks *hooks;
    if (wf->hooks == 0) {
	hooks = kmr_malloc(sizeof(struct kmr_spawn_hooks));
	assert(hooks != 0);
	memset(hooks, 0, sizeof(struct kmr_spawn_hooks));
	hooks->s.mr = wf->mr;
	hooks->s.print_trace = tracing5;
	wf->hooks = hooks;
    } else {
	hooks = wf->hooks;
    }
    assert(wf->kmr_spawn_hookup != 0 && wf->hooks != 0);
    cc = (*wf->kmr_spawn_hookup)(wf->hooks);
    assert(cc == MPI_SUCCESS);

    /* Make lanes on the master. */

    kmr_resolve_lanes(wf);
    kmr_make_lanes(wf);

    /* Inform communicators to the spawn-library. */

    {
	unsigned long colors[KMR_LANE_LEVELS];
	for (int level = 0; level < KMR_LANE_LEVELS; level++) {
	    struct kmr_lane_no id;
	    id = wf->lane_id_on_proc;
	    for (int i = (level + 1); i < KMR_LANE_LEVELS; i++) {
		id.v[i] = KMR_NO_LANE;
	    }
	    unsigned long u = kmr_color_of_lane(id);
	    colors[level] = u;
	}

	int level = kmr_level_of_lane(wf->lane_id_on_proc, 0);
	assert(wf->rank != master || level == -1);
	if (wf->rank != master) {
	    assert(wf->kmr_spawn_setup != 0);
	    cc = (*wf->kmr_spawn_setup)(hooks, wf->base_comm,
					master, /*fn*/ 0,
					KMR_LANE_LEVELS, wf->lane_comms,
					colors, wf->args_size);
	    assert(cc == MPI_SUCCESS);
	}
    }

    if (wf->rank == wf->master_rank) {
	size_t msz = (offsetof(struct kmr_spawn_work, args) + wf->args_size);
	wf->master.rpc_buffer = kmr_malloc(msz);
	assert(wf->master.rpc_buffer != 0);
	wf->master.rpc_size = msz;
    }

    return MPI_SUCCESS;
}

static int kmr_handle_worker_request(struct kmr_swf *wf, _Bool joining);
static int kmr_activate_workers(struct kmr_swf *wf, _Bool shutdown);

/** Disengages the workers from main processing and puts them in the
    service loop for spawning.  Only the master rank returns from this
    call and continues processing, but the worker ranks never return
    as if they call exit().  It replaces the communicator in the KMR
    context with a self-communicator after saving the old communicator
    for workflow.  Replacing the communicator makes the context
    independent from the other ranks and safe to free it.  It
    finalizes the context of workers.*/

int
kmr_detach_swf_workers(KMR *mr)
{
    kmr_err_when_swf_is_not_initialized(mr);
    struct kmr_swf *wf = mr->simple_workflow;
    _Bool tracing5 = (wf->mr->trace_map_spawn && (5 <= wf->mr->verbosity));

    int cc;

    if (tracing5 && (wf->rank == wf->master_rank)) {
	fprintf(stderr, (";;KMR [%05d] Detach worker ranks"
			 " and wait for join messages.\n"), wf->rank);
	fflush(0);
    }

    mr->comm = MPI_COMM_NULL;
    cc = MPI_Comm_dup(MPI_COMM_SELF, &mr->comm);
    assert(cc == MPI_SUCCESS);

    int level = kmr_level_of_lane(wf->lane_id_on_proc, 0);
    if (wf->rank == wf->master_rank) {
	for (;;) {
	    cc = kmr_handle_worker_request(wf, 1);
	    if (cc == MPI_SUCCESS) {
		break;
	    }
	}
	return MPI_SUCCESS;
    } else {
	/*AHO*/ //assert(wf->mr->swf_spawner_so == 0);

	if (level == -1) {
	    if (tracing5) {
		fprintf(stderr, (";;KMR [%05d]"
				 " Rank %d is not involved in workflow.\n"),
			wf->rank, wf->rank);
		fflush(0);
	    }
	}

	/* Keep the so and hooks from freeing in kmr_free_context(). */

	void *spawnerso = wf->mr->swf_spawner_so;
	struct kmr_spawn_hooks *hooks = wf->hooks;
	void (*service)(struct kmr_spawn_hooks *, int) = wf->kmr_spawn_service;
	wf->mr->swf_spawner_so = 0;
	wf->hooks = 0;

	cc = kmr_free_context(mr);
	assert(cc == MPI_SUCCESS);
	cc = kmr_fin();
	assert(cc == MPI_SUCCESS);

	hooks->s.service_count = 0;

	if (spawnerso != 0) {
	    /*exit(551);*/
	    assert(service != 0 && hooks != 0);
	    (*service)(hooks, 0);
	} else {
	    assert(service != 0 && hooks != 0);
	    (*service)(hooks, 0);
	}
	abort();
	return MPI_SUCCESS;
    }
}

/** Finishes the workers of workflow.  It stops the service loop of
    the workers and lets them exit.  It should be called (immediately)
    before MPI_Finalize() at the master rank. */

int
kmr_stop_swf_workers(KMR *mr)
{
    kmr_err_when_swf_is_not_initialized(mr);
    struct kmr_swf *wf = mr->simple_workflow;

    int cc;

    cc = kmr_activate_workers(wf, 1);
    assert(cc == MPI_SUCCESS);

    return MPI_SUCCESS;
}

static void kmr_free_work_list(struct kmr_swf *wf, struct kmr_work_list *h,
			       struct kmr_lane_state *lane, _Bool warn);

/** Clears the lanes of simple workflow. */

int
kmr_finish_swf(KMR *mr)
{
    kmr_err_when_swf_is_not_initialized(mr);
    struct kmr_swf *wf = mr->simple_workflow;

    int cc;

    if (wf->rank == wf->master_rank) {
	if (wf->master.history_head.next != 0) {
	    assert(wf->master.record_history);
	    assert(wf->master.history_insertion_tail != 0);
	    kmr_free_work_list(wf, wf->master.history_head.next, 0, 0);
	    wf->master.history_head.next = 0;
	    wf->master.history_insertion_tail = 0;
	}

	assert(wf->master.top_lane != 0);
	kmr_free_lanes(wf, wf->master.top_lane);
	wf->master.top_lane = 0;

	assert(wf->master.lane_of_workers != 0);
	size_t qsz = (sizeof(struct kmr_lane_state *) * (size_t)wf->nprocs);
	kmr_free(wf->master.lane_of_workers, qsz);
	wf->master.lane_of_workers = 0;

	assert(wf->master.rpc_buffer != 0);
	kmr_free(wf->master.rpc_buffer, wf->master.rpc_size);
	wf->master.rpc_buffer = 0;
	wf->master.rpc_size = 0;
    } else {
	/*(worker)*/
    }

    if (wf->hooks != 0) {
	kmr_free(wf->hooks, sizeof(struct kmr_spawn_hooks));
	wf->hooks = 0;
    }

    if (wf->mr->swf_spawner_so != 0) {
	char *soname = (wf->mr->swf_spawner_library != 0
			? wf->mr->swf_spawner_library : "libkmrspawn.so");

	cc = dlclose(wf->mr->swf_spawner_so);
	if (cc != 0) {
	    char ee[80];
	    snprintf(ee, sizeof(ee), "dlclose(%s): %s\n",
		     soname, dlerror());
	    kmr_warning(wf->mr, 5, ee);
	}
	wf->mr->swf_spawner_so = 0;

	wf->kmr_spawn_hookup = 0;
	wf->kmr_spawn_setup = 0;
	wf->kmr_spawn_service = 0;
	wf->kmr_spawn_set_verbosity = 0;
    }

    kmr_free(wf, sizeof(struct kmr_swf));
    mr->simple_workflow = 0;

    return MPI_SUCCESS;
}

static int kmr_start_worker(struct kmr_spawn_work *w, size_t msglen,
			    int rank, MPI_Comm basecomm);

static int kmr_join_to_workers(struct kmr_swf *wf,
			       struct kmr_lane_state *lane);

#if 0
static int kmr_spawn_hookup_fake(struct kmr_spawn_hooks *hooks);
#endif

/** Loads the spawn-library "libkmrspawn.so".  It implements
    static-spawning, which is only available on K, FX10, and FX100. */

static int
kmr_load_spawn_library(struct kmr_swf *wf, _Bool test_with_fake_spawn)
{
    MPI_Comm basecomm = wf->base_comm;
    int nprocs = wf->nprocs;
    int rank = wf->rank;

    int cc;

    char *soname = (wf->mr->swf_spawner_library != 0
		    ? wf->mr->swf_spawner_library : "libkmrspawn.so");

    void *m = dlopen(soname, (RTLD_NOW|RTLD_GLOBAL));

    /* Check all ranks succeeded with dlopen(). */

    int ng = ((m != 0) ? nprocs : rank);
    int ngmin;
    cc = MPI_Allreduce(&ng, &ngmin, 1, MPI_INT, MPI_MIN, basecomm);
    assert(cc == MPI_SUCCESS);
    _Bool allok = (ngmin == nprocs);

    if (!allok) {
	if (!test_with_fake_spawn) {
	    char ee[160];
	    if (m == 0) {
		snprintf(ee, sizeof(ee), "dlopen(%s) failed: %s",
			 soname, dlerror());
		kmr_error(wf->mr, ee);
		abort();
	    } else {
		kmr_error(wf->mr, "Some ranks failed to dleopn()");
		abort();
	    }
	} else {
	    if (rank == ngmin) {
		assert(m == 0);
		char ee[160];
		snprintf(ee, sizeof(ee), "%s", dlerror());
		kmr_warning(wf->mr, 1,
			    ("WORKFLOW-MAPPER IS UNUSABLE;"
			     " spawn-library unavailable"));
		kmr_warning(wf->mr, 1, ee);
	    }

	    if (m != 0) {
		cc = dlclose(m);
		if (cc != 0) {
		    char ee[160];
		    snprintf(ee, sizeof(ee), "%s", dlerror());
		    kmr_warning(wf->mr, 5, ee);
		}
	    }

	    wf->kmr_spawn_hookup = kmr_spawn_hookup_standin;
	    wf->kmr_spawn_setup = kmr_spawn_setup_standin;
	    wf->kmr_spawn_service = kmr_spawn_service_standin;
	    wf->kmr_spawn_set_verbosity = kmr_spawn_set_verbosity_standin;
	}
	assert(wf->mr->swf_spawner_so == 0);
	return MPI_ERR_SPAWN;
    } else {
	char *fn[10] = {"kmr_spawn_hookup",
			"kmr_spawn_setup",
			"kmr_spawn_service",
			"kmr_spawn_set_verbosity", 0};
	intptr_t fp[10];

	wf->mr->swf_spawner_so = m;

	for (int i = 0; (i < 10 && fn[i] != 0); i++) {
	    fp[i] = (intptr_t)dlsym(m, fn[i]);
	    if (fp[i] == 0) {
		char ee[80];
		snprintf(ee, sizeof(ee), "dlsym(%s): %s\n", fn[i], dlerror());
		kmr_warning(wf->mr, 5, ee);
	    }
	}
	wf->kmr_spawn_hookup = (int (*)())fp[0];
	wf->kmr_spawn_setup = (int (*)())fp[1];
	wf->kmr_spawn_service = (void (*)())fp[2];
	wf->kmr_spawn_set_verbosity = (void (*)())fp[3];
	return MPI_SUCCESS;
    }
}

static void kmr_dump_split_lanes(KMR *mr, struct kmr_lane_no id);

/** Splits a communicator in a KMR context to ones to be used for
    kmr_init_swf().  This is a utility.  It is restricted to make
    lanes having the same depth of levels.  DESCRIPTION is an array of
    a list of positive integers terminated by zeros in the form
    D[0]={L0,L1,...,Lk-1,0}, D[1]={M0,M1,...,Mj-1,0},
    D[2]={N0,N1,...,Ni-1,0}.  Here, it is the depth=3 case.  N0 to
    Ni-1 specifies how to split ranks (the bottom level) to i groups.
    They must satisfy (N0+...+Ni-1)<=(nprocs-1).  It needs at least
    one spare rank for the master.  M0 to Mj-1 specifies the 2nd
    bottom level, and it must satisfy (M0+...+Mj-1)=i.  L0 to Lk-1
    specifies the top level, and it must satisfy (L0+...+Lk-1)=j.
    DESCRIPTION and NLAYERS need to be valid only on rank0 (they are
    broadcasted inside this routine).  The ranks not a member of a
    lane have a null communicator at each level. */

int
kmr_split_swf_lanes_a(KMR *mr, MPI_Comm splitcomms[KMR_LANE_LEVELS],
		      int root, int *description[], _Bool dump)
{
    const MPI_Comm basecomm = mr->comm;
    const int nprocs = mr->nprocs;
    const int rank = mr->rank;
    /*const int root = 0;*/

    int cc;

    struct kmr_lane_no illegallane;
    kmr_clear_lane_id(&illegallane);

    for (int d = 0; d < KMR_LANE_LEVELS; d++) {
	splitcomms[d] = MPI_COMM_NULL;
    }

    /* Broadcast description arrays from rank0. */

    int depth;
    int len[KMR_LANE_LEVELS];
    int *desc[KMR_LANE_LEVELS];

    {
	if (mr->rank == root) {
	    depth = (KMR_LANE_LEVELS + 1);
	    for (int i = 0; i < (KMR_LANE_LEVELS + 1); i++) {
		if (description[i] == 0) {
		    depth = i;
		    break;
		}
	    }
	    if (depth > KMR_LANE_LEVELS) {
		char ee[80];
		snprintf(ee, sizeof(ee),
			 ("Bad lane description,"
			  " no terminating null"));
		kmr_error(mr, ee);
		abort();
	    }
	}
	cc = MPI_Bcast(&depth, 1, MPI_INT, root, basecomm);
	assert(cc == MPI_SUCCESS);

	if (mr->rank == root) {
	    for (int d = 0; d < depth; d++) {
		int *v = description[d];
		int i;
		for (i = 0; v[i] != 0; i++);
		len[d] = i;
	    }
	}
	cc = MPI_Bcast(len, depth, MPI_INT, root, basecomm);
	assert(cc == MPI_SUCCESS);

	for (int d = 0; d < depth; d++) {
	    if (mr->rank == root) {
		desc[d] = description[d];
	    } else {
		desc[d] = kmr_malloc(sizeof(int) * (size_t)(len[d] + 1));
		assert(desc[d] != 0);
	    }
	    cc = MPI_Bcast(desc[d], (len[d] + 1), MPI_INT, root, basecomm);
	    assert(cc == MPI_SUCCESS);
	}
    }

    /* Color by descriptions from the bottom. */

    struct kmr_lane_no colors = illegallane;

    for (int d = (depth - 1); d >= 0; d--) {
	assert(KMR_NO_LANE == -1);

	int sublanecolor = ((d == (depth - 1)) ? mr->rank : (colors.v[d + 1]));
	int *v = desc[d];

	int color;
	color = -1;
	int sum;
	sum = 0;
	for (int i = 0; i < len[d]; i++) {
	    sum += v[i];
	    if (sublanecolor != -1 && sublanecolor < sum && color == -1) {
		color = i;
	    }
	}

	_Bool ok = ((d == (depth - 1)) ? (sum < nprocs) : (sum == len[d + 1]));
	if (!ok) {
	    char ee[80];
	    snprintf(ee, sizeof(ee),
		     ("Bad lane description,"
		      " sum of ranks/lanes are too large"
		      " (lanes=%d level=%d)"),
		     sum, d);
	    kmr_error(mr, ee);
	    abort();
	}

	assert(d == (depth - 1) || color != -1 || sublanecolor == -1);
	colors.v[d] = (short)color;
    }

    /* Split the base communicator from the bottom. */

    for (int d = (depth - 1); d >= 0; d--) {
	int color = ((colors.v[d] != -1) ? colors.v[d] : MPI_UNDEFINED);
	cc = MPI_Comm_split(basecomm, color, rank, &splitcomms[d]);
	assert(cc == MPI_SUCCESS);
	assert(color != MPI_UNDEFINED || splitcomms[d] == MPI_COMM_NULL);
    }

    if (dump) {
	kmr_dump_split_lanes(mr, colors);
    }

    for (int d = 0; d < depth; d++) {
	if (mr->rank != root) {
	    kmr_free(desc[d], (sizeof(int) * (size_t)(len[d] + 1)));
	    desc[d] = 0;
	}
    }

    return MPI_SUCCESS;
}

/** Splits a communicator in a KMR context to ones to be used for
    kmr_init_swf().  This is a utility.  DESCRIPTION is a vector of
    strings terminated by a null-string.  A line consists of a
    lane-number, a separator colon, and a number of ranks.  Thus, each
    line looks like "3.3.3:4".  It does not accept any whitespaces.
    Note that the descriptions are to distinguish lanes, and the
    lane-numbers can change, because they are once translated to
    communicators. */

int
kmr_split_swf_lanes(KMR *mr, MPI_Comm splitcomms[KMR_LANE_LEVELS],
		    int root, char *description[], _Bool dump)
{
    const MPI_Comm basecomm = mr->comm;
    const int nprocs = mr->nprocs;
    const int rank = mr->rank;
    /*const int root = 0;*/
    struct desc {struct kmr_lane_no colors; int ranks;};

    struct kmr_lane_no illegalcolor;
    kmr_clear_lane_id(&illegalcolor);

    int cc;

    for (int d = 0; d < KMR_LANE_LEVELS; d++) {
	splitcomms[d] = MPI_COMM_NULL;
    }

    /* Count description lines. */

    int nlines;
    if (rank == root) {
	nlines = -1;
	for (int i = 0; i < nprocs; i++) {
	    if (description[i] == 0) {
		nlines = i;
		break;
	    }
	}
	if (nlines == -1) {
	    char ee[80];
	    snprintf(ee, sizeof(ee),
		     ("Bad lane description,"
		      " no terminating null"));
	    kmr_error(mr, ee);
	    abort();
	}
    }

    struct desc *lines;
    if (rank == root) {
	lines = kmr_malloc(sizeof(struct desc) * (size_t)nlines);
	assert(lines != 0);
    } else {
	lines = 0;
    }

    /* Scan description lines. */

    if (rank == root) {
	for (int i = 0; i < nlines; i++) {
	    char *s = description[i];
	    char buf[80];
	    size_t len = (strlen(s) + 1);
	    if (len > sizeof(buf)) {
		char ee[80];
		snprintf(ee, sizeof(ee),
			 ("Bad lane description,"
			  " string too long (%s)"), s);
		kmr_error(mr, ee);
		abort();
	    }
	    memcpy(buf, s, len);
	    char *p;
	    p = buf;
	    while (p[0] != 0 && p[0] != ':') {p++;}
	    if (p[0] != ':') {
		char ee[80];
		snprintf(ee, sizeof(ee),
			 ("Bad lane description,"
			  " no separator colon (%s)"), s);
		kmr_error(mr, ee);
		abort();
	    }
	    p[0] = 0;
	    struct kmr_lane_no id = kmr_name_lane(mr, buf);

	    int count;
	    char garbage;
	    cc  = sscanf((p + 1), "%d%c", &count, &garbage);
	    if (cc != 1) {
		char ee[80];
		snprintf(ee, sizeof(ee),
			 ("Bad lane description,"
			  " bad number of ranks (%s)"), s);
		kmr_error(mr, ee);
		abort();
	    }

	    /* Check duplicate lane-numbers. */

	    for (int j = 0; j < i; j++) {
		if (kmr_lane_eq(id, lines[i].colors, (KMR_LANE_LEVELS - 1))) {
		    char ee[80];
		    snprintf(ee, sizeof(ee),
			     ("Bad lane description,"
			      " duplicate lane-numbers (%s)"),
			     kmr_lane_string(id, 0));
		    kmr_error(mr, ee);
		    abort();
		}
	    }

	    lines[i].colors = id;
	    lines[i].ranks = count;
	}
    }

    /* Count the depth of description lines. */

    int depth;
    depth = 0;
    if (rank == root) {
	for (int i = 0; i < nlines; i++) {
	    struct kmr_lane_no co = lines[i].colors;
	    while (depth < KMR_LANE_LEVELS && co.v[depth] != KMR_NO_LANE) {
		depth++;
	    }
	}
    }

    if (rank == root) {

	/* Check the total count of ranks. */

	{
	    int rankcount;
	    rankcount = 0;
	    for (int i = 0; i < nlines; i++) {
		rankcount += lines[i].ranks;
	    }
	    if (rankcount > (nprocs - 1)) {
		char ee[80];
		snprintf(ee, sizeof(ee),
			 ("Bad lane description,"
			  " total rank count too large (%d)"),
			 rankcount);
		kmr_error(mr, ee);
		abort();
	    }
	}

	/* Check lanes have no large entry. */

	{
	    for (int d = 0; d < depth; d++) {
		for (int i = 0; i < nlines; i++) {
		    int q = lines[i].colors.v[d];
		    if (q > (nprocs - 1)) {
			char ee[80];
			snprintf(ee, sizeof(ee),
				 ("Bad lane description,"
				  " lane number too large (%s)"),
				 kmr_lane_string(lines[i].colors, 0));
			kmr_error(mr, ee);
			abort();
		    }
		}
	    }
	}
    }

    struct kmr_lane_no *allcolors;
    if (rank == root) {
	allcolors = kmr_malloc(sizeof(struct kmr_lane_no) * (size_t)nprocs);
	assert(allcolors != 0);
    } else {
	allcolors = 0;
    }

    if (rank == root) {
	/* Translate lane-numbers to distinct colors. */

	for (int d = 1; d < depth; d++) {
	    int extent;
	    extent = 0;
	    for (int i = 0; i < nlines; i++) {
		int q = lines[i].colors.v[d];
		extent = MAX(extent, (q + 1));
	    }
	    for (int i = 0; i < nlines; i++) {
		int o = lines[i].colors.v[d - 1] * extent;
		lines[i].colors.v[d] = (short)(lines[i].colors.v[d] + o);
	    }
	}

	/* Fill array of colors for ranks. */

	int rankcount;
	rankcount = 0;
	for (int i = 0; i < nlines; i++) {
	    assert((rankcount + lines[i].ranks) < nprocs);
	    for (int j = 0; j < lines[i].ranks; j++) {
		allcolors[rankcount + j] = lines[i].colors;
	    }
	    rankcount += lines[i].ranks;
	}
	assert(rankcount < nprocs);
	for (int j = rankcount; j < nprocs; j++) {
	    allcolors[j] = illegalcolor;
	}
    }
    cc = MPI_Bcast(&depth, 1, MPI_INT, root, basecomm);
    assert(cc == MPI_SUCCESS);

    struct kmr_lane_no colors;
    int sz = (int)sizeof(struct kmr_lane_no);
    cc = MPI_Scatter(allcolors, sz, MPI_BYTE, &colors, sz, MPI_BYTE,
		     root, basecomm);
    assert(cc == MPI_SUCCESS);

    /* Split the base communicator from the bottom. */

    for (int d = (depth - 1); d >= 0; d--) {
	int color = ((colors.v[d] != -1) ? colors.v[d] : MPI_UNDEFINED);
	cc = MPI_Comm_split(basecomm, color, rank, &splitcomms[d]);
	assert(cc == MPI_SUCCESS);
	assert(color != MPI_UNDEFINED || splitcomms[d] == MPI_COMM_NULL);
    }

    if (dump) {
	kmr_dump_split_lanes(mr, colors);
    }

    if (rank == root) {
	kmr_free(lines, (sizeof(struct desc) * (size_t)nlines));
	kmr_free(allcolors, (sizeof(struct kmr_lane_no) * (size_t)nprocs));
    } else {
	assert(lines == 0);
	assert(allcolors == 0);
    }

    return MPI_SUCCESS;
}

static void
kmr_dump_split_lanes(KMR *mr, struct kmr_lane_no colors)
{
    const MPI_Comm basecomm = mr->comm;
    const int nprocs = mr->nprocs;
    const int rank = mr->rank;
    const int root = 0;
    const int depth = KMR_LANE_LEVELS;

    int cc;

    struct kmr_lane_no *allcolors;
    if (rank == root) {
	allcolors = kmr_malloc(sizeof(struct kmr_lane_no) * (size_t)nprocs);
	assert(allcolors != 0);
    } else {
	allcolors = 0;
    }
    int sz = (int)sizeof(struct kmr_lane_no);
    cc = MPI_Gather(&colors, sz, MPI_BYTE, allcolors, sz, MPI_BYTE,
		    root, basecomm);
    assert(cc == MPI_SUCCESS);

    if (rank == root) {
	printf("Split of lanes"
	       " (displayed by distinct colors assigned to ranks):\n");
	for (int d = 0; d < depth; d++) {
	    printf("color[level=%d]=", d);
	    for (int i = 0; i < nprocs; i++) {
		char col[20];
		if (allcolors[i].v[d] != -1) {
		    snprintf(col, sizeof(col), "%d", allcolors[i].v[d]);
		} else {
		    snprintf(col, sizeof(col), "-");
		}
		if (i == 0) {
		    printf("%s", col);
		} else {
		    printf(",%s", col);
		}
	    }
	    printf("\n");
	}
	fflush(0);
    }

    if (rank == root) {
	kmr_free(allcolors, (sizeof(struct kmr_lane_no) * (size_t)nprocs));
    } else {
	assert(allcolors == 0);
    }
}

static int kmr_check_lane_id(struct kmr_swf *wf, struct kmr_lane_no id,
			     _Bool admit_any);
static int kmr_color_subcommunicator(struct kmr_swf *wf, MPI_Comm subcomm,
				     MPI_Comm supercomm);
static int kmr_check_partitioning(struct kmr_swf *wf, int supercolor,
				  MPI_Comm subcomm);

/** Assigns a lane-number to a rank (wf->lane_id_on_proc)
    (at-all-ranks).  It calculates a lane-number from the set of split
    communicators.  It assumes the master-rank (the last rank) is
    excluded from the lanes. */

static void
kmr_resolve_lanes(struct kmr_swf *wf)
{
    assert(wf->base_comm != MPI_COMM_NULL);

    MPI_Comm basecomm = wf->base_comm;
    MPI_Comm *comms = wf->lane_comms;

    int cc;

    int colors[KMR_LANE_LEVELS];
    for (int level = 0; level < KMR_LANE_LEVELS; level++) {
	MPI_Comm supercomm = (level == 0 ? basecomm : comms[level - 1]);
	MPI_Comm subcomm = comms[level];
	if (supercomm == MPI_COMM_NULL) {
	    colors[level] = -1;
	} else {
	    int color = kmr_color_subcommunicator(wf, subcomm, supercomm);
	    colors[level] = color;
	}
    }

    for (int level = 0; level < KMR_LANE_LEVELS; level++) {
	MPI_Comm subcomm = comms[level];
	if (level > 0 && subcomm != MPI_COMM_NULL) {
	    int supercolor = colors[level - 1];
	    cc = kmr_check_partitioning(wf, supercolor, subcomm);
	    assert(cc == MPI_SUCCESS);
	}
	if (colors[level] == -1) {
	    wf->lane_id_on_proc.v[level] = KMR_NO_LANE;
	} else {
	    wf->lane_id_on_proc.v[level] = (short)colors[level];
	}
    }

    cc = kmr_check_lane_id(wf, wf->lane_id_on_proc, 0);
    assert(cc == MPI_SUCCESS);
}

/** Checks if a sub-communicator is a partitioning of a
    super-communicator (at-all-ranks).  A SUPERCOLOR gives a distinct
    color to each super-communicator, which should be identical in the
    sub-communicator. */

static int
kmr_check_partitioning(struct kmr_swf *wf, int supercolor, MPI_Comm subcomm)
{
    const int root = 0;
    int cc;

    int nprocs;
    int rank;
    cc = MPI_Comm_size(subcomm, &nprocs);
    assert(cc == MPI_SUCCESS);
    cc = MPI_Comm_rank(subcomm, &rank);
    assert(cc == MPI_SUCCESS);

    int *colors;
    if (rank == root) {
	colors = kmr_malloc(sizeof(int) * (size_t)nprocs);
	assert(colors != 0);
    } else {
	colors = 0;
    }
    cc = MPI_Gather(&supercolor, 1, MPI_INT, colors, 1, MPI_INT,
		    root, subcomm);
    assert(cc == MPI_SUCCESS);
    if (rank == root) {
	for (int i = 0; i < nprocs; i++) {
	    if (colors[i] != colors[0]) {
		kmr_error(wf->mr, ("Communicators are not partitioning"
				   " of upper ones"));
		abort();
		return MPI_ERR_SPAWN;
	    }
	}
	kmr_free(colors, (sizeof(int) * (size_t)nprocs));
    }
    return MPI_SUCCESS;
}

/** Checks well-formedness of a lane-number.  Slots after an any-lane
    must be an any-lane or a no-lane.  Slots after a no-lane must be a
    no-lane. */

static int
kmr_check_lane_id(struct kmr_swf *wf, struct kmr_lane_no id, _Bool admit_any)
{
    KMR *mr = wf->mr;
    int state;
    int level;
    state = 0;
    level = -1;
    for (int i = 0; i < KMR_LANE_LEVELS; i++) {
	if (!admit_any && id.v[i] == KMR_ANY_LANE) {
	    char ee[80];
	    snprintf(ee, sizeof(ee), "Bad lane-number (%s): any-lane appear",
		     kmr_lane_string(id, 1));
	    kmr_error(mr, ee);
	    abort();
	}
	int q = id.v[i];
	if (state == 0 && q == KMR_ANY_LANE) {
	    state = 1;
	    level = i;
	} else if (state == 0 && q == KMR_NO_LANE) {
	    state = 2;
	    assert(level == (i - 1));
	} else if (state == 0) {
	    level = i;
	} else if (state == 1 && q == KMR_ANY_LANE) {
	    level = i;
	} else if (state == 1 && q == KMR_NO_LANE) {
	    state = 2;
	    assert(level == (i - 1));
	} else if (state == 1) {
	    char ee[80];
	    snprintf(ee, sizeof(ee),
		     "Bad lane-number (%s): some follow any-lane",
		     kmr_lane_string(id, 1));
	    kmr_error(mr, ee);
	    abort();
	} else if (state == 2 && q == KMR_ANY_LANE) {
	    char ee[80];
	    snprintf(ee, sizeof(ee),
		     "Bad lane-number (%s): some follow no-lane",
		     kmr_lane_string(id, 1));
	    kmr_error(mr, ee);
	    abort();
	} else if (state == 2 && q == KMR_NO_LANE) {
	    /* OK. */
	} else if (state == 2) {
	    char ee[80];
	    snprintf(ee, sizeof(ee),
		     "Bad lane-number (%s): some follow no-lane",
		     kmr_lane_string(id, 1));
	    kmr_error(mr, ee);
	    abort();
	} else {
	    abort();
	}
    }
    return MPI_SUCCESS;
}

/** Colors sub-communicators distinctly in a super-communicator, and
    returns the color which names names a lane (at-all-ranks).  It
    enumerates the rank0 processes of the sub-communicators.  It
    returns -1 for the ranks with a null SUBCOMM. */

static int
kmr_color_subcommunicator(struct kmr_swf *wf, MPI_Comm subcomm,
			  MPI_Comm supercomm)
{
    assert(supercomm != MPI_COMM_NULL);

    const int root = 0;
    int cc;

    int nprocs;
    int rank;
    cc = MPI_Comm_size(supercomm, &nprocs);
    assert(cc == MPI_SUCCESS);
    cc = MPI_Comm_rank(supercomm, &rank);
    assert(cc == MPI_SUCCESS);

    int *subcommranks;
    int *colors;
    if (rank == root) {
	subcommranks = kmr_malloc(sizeof(int) * (size_t)nprocs);
	assert(subcommranks != 0);
	colors = kmr_malloc(sizeof(int) * (size_t)nprocs);
	assert(colors != 0);
    } else {
	subcommranks = 0;
	colors = 0;
    }

    int r;
    if (subcomm != MPI_COMM_NULL) {
	cc = MPI_Comm_rank(subcomm, &r);
	assert(cc == MPI_SUCCESS);
    } else {
	r = -1;
    }
    cc = MPI_Gather(&r, 1, MPI_INT, subcommranks, 1, MPI_INT, root, supercomm);
    assert(cc == MPI_SUCCESS);

    /* Determine the colors of rank=0 in the subcomm. */

    int ncolors;
    if (rank == root) {
	ncolors = 0;
	for (int i = 0; i < nprocs; i++) {
	    if (subcommranks[i] == 0) {
		colors[i] = ncolors;
		ncolors++;
	    } else {
		colors[i] = -1;
	    }
	}
    } else {
	ncolors = 0;
    }

    /* Tell its color to all ranks. */

    int color;
    cc = MPI_Scatter(colors, 1, MPI_INT, &color, 1, MPI_INT, root, supercomm);
    assert(cc == MPI_SUCCESS);
    if (subcomm != MPI_COMM_NULL) {
	cc = MPI_Bcast(&color, 1, MPI_INT, 0, subcomm);
	assert(cc == MPI_SUCCESS);
    }

    if (rank == root) {
	kmr_free(subcommranks, (sizeof(int) * (size_t)nprocs));
	kmr_free(colors, (sizeof(int) * (size_t)nprocs));
    }
    return color;
}

static struct kmr_lane_vector *
kmr_make_bottom_lanes(struct kmr_swf *wf, struct kmr_lane_no *laneids,
		      struct kmr_pair laneranks[][KMR_LANE_LEVELS]);
static void kmr_bond_all_lanes(struct kmr_swf *wf, struct kmr_lane_vector *v,
			       struct kmr_pair laneranks[][KMR_LANE_LEVELS]);
static void kmr_bond_sublanes(struct kmr_swf *wf, struct kmr_lane_state *sup,
			      struct kmr_lane_state *lanes[], int nlanes);
static int kmr_find_leader(struct kmr_swf *wf, struct kmr_lane_state *lane,
			   int level,
			   struct kmr_pair laneranks[][KMR_LANE_LEVELS]);
static int kmr_count_bottom_level_lanes(struct kmr_lane_state *lane);

/** Initializes thelanes at the master rank (at-all-ranks).  It
    collects lane-numbers of all ranks and makes the lane structures
    from the bottom and upwards. */

static void
kmr_make_lanes(struct kmr_swf *wf)
{
    const int master = wf->master_rank;
    const MPI_Comm basecomm = wf->base_comm;

    int cc;

    /* Collect lane-numbers of all ranks. */

    int sz0 = sizeof(struct kmr_lane_no);
    struct kmr_lane_no *laneids;
    if (wf->rank == master) {
	laneids = kmr_malloc((size_t)sz0 * (size_t)wf->nprocs);
	assert(laneids != 0);
    } else {
	laneids = 0;
    }
    cc = MPI_Gather(&wf->lane_id_on_proc, sz0, MPI_BYTE,
		    laneids, sz0, MPI_BYTE, master, basecomm);
    assert(cc == MPI_SUCCESS);

    if (0) {
	if (wf->rank == master) {
	    printf("Lanes of ranks:\n");
	    for (int i = 0; i < wf->nprocs; i++) {
		char *s = kmr_lane_string(laneids[i], 1);
		printf("lane[%d]=%s\n", i, s);
	    }
	    fflush(0);
	}
    }

    /* Collect rank numbers of the lane communicators. */

    struct kmr_pair ranks[KMR_LANE_LEVELS];
    for (int i = 0; i < KMR_LANE_LEVELS; i++) {
	if (wf->lane_comms[i] != MPI_COMM_NULL) {
	    int nprocs;
	    int rank;
	    cc = MPI_Comm_size(wf->lane_comms[i], &nprocs);
	    assert(cc == MPI_SUCCESS);
	    cc = MPI_Comm_rank(wf->lane_comms[i], &rank);
	    assert(cc == MPI_SUCCESS);
	    ranks[i].size= nprocs;
	    ranks[i].rank = rank;
	} else {
	    ranks[i].size = 0;
	    ranks[i].rank = -1;
	}
    }
    int sz1 = sizeof(struct kmr_pair [KMR_LANE_LEVELS]);
    struct kmr_pair (*laneranks)[KMR_LANE_LEVELS];
    if (wf->rank == master) {
	laneranks = kmr_malloc((size_t)sz1 * (size_t)wf->nprocs);
	assert(laneranks != 0);
    } else {
	laneranks = 0;
    }
    cc = MPI_Gather(ranks, sz1, MPI_BYTE, laneranks, sz1, MPI_BYTE,
		    master, basecomm);
    assert(cc == MPI_SUCCESS);

    if (wf->rank == master) {
	struct kmr_lane_vector *
	    v = kmr_make_bottom_lanes(wf, laneids, laneranks);
	kmr_bond_all_lanes(wf, v, laneranks);

	/* Check simply lane hierarchy. */

	int nbottoms = v->n;
	int count0 = kmr_count_bottom_level_lanes(wf->master.top_lane);
	assert(count0 == nbottoms);

	kmr_free(laneids, ((size_t)sz0 * (size_t)wf->nprocs));
	kmr_free(laneranks, ((size_t)sz1 * (size_t)wf->nprocs));
	kmr_free(v, (offsetof(struct kmr_lane_vector, lanes)
		     + sizeof(struct kmr_lane_state *) * (size_t)v->n));

	/* Check a list of lanes are OK. */

	struct kmr_lane_state *list = wf->master.list_of_all_lanes;
	int count1;
	count1 = 0;
	for (struct kmr_lane_state *p = list; p != 0; p = p->link) {
	    count1++;
	}
	assert(count1 == wf->master.top_lane->total_sublanes);
    }
}

/** Makes a lane structure (at-the-master).  It is a bottom lane if
    NPROCS is non-zero, or a superlane if NPROCS is zero.  The
    lane-number argument is ignored when creating a top-lane
    (level=-1). */

static struct kmr_lane_state *
kmr_allocate_lane(int level, struct kmr_lane_no id, int nprocs)
{
    assert(level != -1 || nprocs == 0);
    struct kmr_lane_state *lane = kmr_malloc(sizeof(struct kmr_lane_state));
    assert(lane != 0);
    memset(lane, 0, sizeof(struct kmr_lane_state));
    kmr_clear_lane_id(&(lane->lane_id));
    if (level != -1) {
	lane->lane_id = id;
    }
    lane->level = level;
    lane->leader_rank = -1;
    lane->total_sublanes = 1;
    lane->total_ranks = nprocs;
    lane->superlane = 0;
    lane->sublanes = 0;
    if (nprocs != 0) {
	lane->workers = kmr_make_rank_vector(nprocs);
	assert(lane->workers != 0);
    } else {
	lane->workers = 0;
    }

    lane->icomm = MPI_COMM_NULL;
    lane->queue_head.next = 0;
    lane->queue_head.item = 0;
    lane->queue_insertion_tail = 0;
    lane->current_work = 0;
    lane->yielding_to_superlane = 0;
    lane->n_joined_ranks = 0;
    lane->n_running_sublanes = 0;
    if (nprocs != 0) {
	lane->running_sublanes = kmr_malloc(sizeof(_Bool) * (size_t)nprocs);
	assert(lane->running_sublanes != 0);
    } else {
	lane->running_sublanes = 0;
    }

    return lane;
}

/** Makes lanes at the bottom levels (at-the-master).  The depths may
    be not eqaul.  The created bottom lanes are bonded to a superlane
    in kmr_bond_all_lanes(). */

static struct kmr_lane_vector *
kmr_make_bottom_lanes(struct kmr_swf *wf, struct kmr_lane_no *laneids,
		      struct kmr_pair laneranks[][KMR_LANE_LEVELS])
{
    assert(wf->master.lane_of_workers == 0);
    size_t qsz = (sizeof(struct kmr_lane_state *) * (size_t)wf->nprocs);
    wf->master.lane_of_workers = kmr_malloc(qsz);
    assert(wf->master.lane_of_workers != 0);
    memset(wf->master.lane_of_workers, 0, qsz);

    struct kmr_lane_state **lanes = kmr_malloc(qsz);
    assert(lanes != 0);
    memset(lanes, 0, qsz);
    int nlanes;
    nlanes = 0;
    for (int r = 0; r < wf->nprocs; r++) {
	assert(wf->master.lane_of_workers[r] == 0);
	struct kmr_lane_no id = laneids[r];
	int level = kmr_level_of_lane(id, 0);
	if (level == -1) {
	    /* Skip a null-lane. */
	} else {
	    int nprocsinlane = laneranks[r][level].size;
	    int rankinlane = laneranks[r][level].rank;
	    assert(nprocsinlane > 0 && rankinlane != -1);
	    assert(rankinlane < nprocsinlane);

	    /* Find a lane when it was already created. */

	    struct kmr_lane_state *lane;
	    {
		int q;
		for (q = 0; q < nlanes; q++) {
		    assert(lanes[q] != 0);
		    if (kmr_lane_eq(id, lanes[q]->lane_id, level)) {
			int l0 = kmr_level_of_lane(lanes[q]->lane_id, 0);
			assert(l0 == level);
			break;
		    }
		}
		if (q < nlanes) {
		    lane = lanes[q];
		} else {
		    assert(q == nlanes);
		    lane = kmr_allocate_lane(level, id, nprocsinlane);
		    assert(lane != 0);
		    lanes[nlanes] = lane;
		    nlanes++;
		}
	    }
	    assert(lane != 0);

	    wf->master.lane_of_workers[r] = lane;
	    assert(lane->workers->ranks[rankinlane] == -1);
	    lane->workers->ranks[rankinlane] = r;
	    if (rankinlane == 0) {
		assert(lane->leader_rank == -1);
		lane->leader_rank = r;
	    }
	}
    }

    struct kmr_lane_vector *v = kmr_make_lane_vector(nlanes, lanes);

    /* Check lane->workers->ranks are all set. */

    for (int q = 0; q < v->n; q++) {
	struct kmr_lane_state *lane = v->lanes[q];
	struct kmr_rank_vector *workers = lane->workers;
	for (int i = 0; i < workers->n; i++) {
	    assert(workers->ranks[i] != -1);
	}
    }

    return v;
}

/** Collects lanes to make a superlane, which build up to a single
    top-lane (at-the-master).  It destructively modifies the passed
    vector V.  (SLOW). */

static void
kmr_bond_all_lanes(struct kmr_swf *wf, struct kmr_lane_vector *v,
		   struct kmr_pair laneranks[][KMR_LANE_LEVELS])
{
    struct kmr_lane_no nulllane = {.v = {0}};
    struct kmr_lane_state *top = kmr_allocate_lane(-1, nulllane, 0);
    assert(top != 0);
    assert(wf->master.top_lane == 0);
    wf->master.top_lane = top;

    int nlanes = v->n;
    struct kmr_lane_state **lanes = v->lanes;

    for (int level = (KMR_LANE_LEVELS - 1); level >= 0; level--) {
	for (int q = 0; q < nlanes; q++) {
	    struct kmr_lane_state *lane = lanes[q];
	    assert(lane == 0 || lane->superlane == 0);
	    if (lane == 0) {
		/* Skip already handled entry. */
	    } else if (lane->level == level) {
		struct kmr_lane_state *sup;
		if (level == 0) {
		    sup = top;
		} else {
		    struct kmr_lane_no id;
		    id = lane->lane_id;
		    id.v[level] = KMR_NO_LANE;
		    sup = kmr_allocate_lane((level - 1), id, 0);
		}
		assert(sup != 0);
		kmr_bond_sublanes(wf, sup, lanes, nlanes);
		int leader = kmr_find_leader(wf, sup, sup->level, laneranks);
		assert((sup != top) == (leader != -1));
		assert(sup->leader_rank == -1);
		sup->leader_rank = leader;

		/* Replace a lane with its superlane. */

		assert(lanes[q] == 0);
		lanes[q] = sup;
	    }
	}
    }

    /* Add the top lane to the list of the all lanes. */

    top->link = wf->master.list_of_all_lanes;
    wf->master.list_of_all_lanes = top;
}

/** Builds a vector of sublanes for the superlane SUP (at-the-master).
    It destructively clears LANES[i] to null, when it is merged to a
    superlane. */

static void
kmr_bond_sublanes(struct kmr_swf *wf, struct kmr_lane_state *sup,
		  struct kmr_lane_state *lanes[], int nlanes)
{
    assert(sup->sublanes == 0);

    /* Collect sublanes. */

    struct kmr_lane_state *suptail;
    suptail = sup;
    int count;
    count = 0;
    for (int q = 0; q < nlanes; q++) {
	struct kmr_lane_state *lane = lanes[q];
	if (lane != 0
	    && (lane->level - 1) == sup->level
	    && kmr_lane_eq(lane->lane_id, sup->lane_id, sup->level)) {
	    lanes[q] = 0;
	    lane->superlane = sup;
	    assert(lane->link == 0);
	    suptail->link = lane;
	    suptail = lane;
	    sup->total_sublanes += lane->total_sublanes;
	    sup->total_ranks += lane->total_ranks;
	    count++;
	}
    }
    assert(count > 0);

    /* Make a sublanes vector. */

    struct kmr_lane_vector *v = kmr_make_lane_vector(count, 0);
    int i;
    i = 0;
    suptail = sup;
    while (suptail->link != 0) {
	v->lanes[i] = suptail->link;
	i++;
	suptail = suptail->link;
    }
    assert(i == count);

    assert(sup->running_sublanes == 0);
    sup->running_sublanes = kmr_malloc(sizeof(_Bool) * (size_t)count);
    assert(sup->running_sublanes != 0);

    /* List all lanes as a single list. */

    suptail->link = wf->master.list_of_all_lanes;
    wf->master.list_of_all_lanes = sup->link;

    sup->link = 0;
    sup->sublanes = v;
}

/** Searches a leader (rank=0) in a LANE at a LEVEL (at-the-master).
    It returns a rank in the base communicator or -1 if not found.  It
    returns -1 for the top lane, because the top lane is never used
    for work. (SLOW). */

static int
kmr_find_leader(struct kmr_swf *wf, struct kmr_lane_state *lane,
		int level, struct kmr_pair laneranks[][KMR_LANE_LEVELS])
{
    if (lane->level == -1) {
	return -1;
    } if (lane->sublanes == 0) {
	for (int i = 0; i < lane->workers->n; i++) {
	    int r = lane->workers->ranks[i];
#if 0 /*AHO*/
	    if (laneranks[r][level].rank == -1) {
		printf("AHO lane=%s i=%d r=%d level=%d w=%d\n",
		       kmr_lane_string(lane->lane_id, 1), i, r, level,
		       lane->workers->n);
		for (int j = 0; j < lane->workers->n; j++) {
		    printf("AHO ranks=%d\n", lane->workers->ranks[j]);
		}
		fflush(0);
		abort();
	    }
#endif
	    assert(laneranks[r][level].rank != -1);
	    if (laneranks[r][level].rank == 0) {
		return r;
	    }
	}
	return -1;
    } else {
	struct kmr_lane_vector *v = lane->sublanes;
	for (int i = 0; i < v->n; i++) {
	    int r = kmr_find_leader(wf, v->lanes[i], level, laneranks);
	    if (r != -1) {
		return r;
	    }
	}
	return -1;
    }
}

/** Counts the number of the bottom level lanes (at-the-master). */

static int
kmr_count_bottom_level_lanes(struct kmr_lane_state *lane)
{
    if (lane->sublanes == 0) {
	return 1;
    } else {
	int count;
	count = 0;
	for (int i = 0; i < lane->sublanes->n; i++) {
	    count += kmr_count_bottom_level_lanes(lane->sublanes->lanes[i]);
	}
	return count;
    }
}

static int kmr_dequeue_scattered_work(struct kmr_swf *wf,
				      struct kmr_lane_state *lane,
				      struct kmr_work_item *x);

/** Frees a lane and its sublanes, recursively (at-the-master). */

static void
kmr_free_lanes(struct kmr_swf *wf, struct kmr_lane_state *lane)
{

    /* Assert if some work-items remain. */

    assert(lane->icomm == MPI_COMM_NULL);
    if (lane->sublanes != 0) {
	struct kmr_lane_vector *v = lane->sublanes;
	for (int i = 0; i < v->n; i++) {
	    assert(v->lanes[i]->queue_head.next == 0);
	}
    }

    assert(lane->current_work == 0);
    assert(lane->yielding_to_superlane == 0);
    assert(lane->queue_insertion_tail == 0);

    int nsubs = ((lane->sublanes != 0) ? lane->sublanes->n : lane->workers->n);

    assert(lane->running_sublanes != 0);
    kmr_free(lane->running_sublanes, (sizeof(_Bool) * (size_t)nsubs));
    lane->running_sublanes = 0;

    if (lane->sublanes != 0) {
	struct kmr_lane_vector *v = lane->sublanes;
	for (int i = 0; i < v->n; i++) {
	    kmr_free_lanes(wf, v->lanes[i]);
	    v->lanes[i] = 0;
	}
	kmr_free(v, (offsetof(struct kmr_lane_vector, lanes)
		     + (sizeof(struct kmr_lane_state *) * (size_t)v->n)));
	lane->sublanes = 0;
    }
    if (lane->workers != 0) {
	struct kmr_rank_vector *u = lane->workers;
	kmr_free(u, (offsetof(struct kmr_rank_vector, ranks)
		     + (sizeof(int) * (size_t)u->n)));
	lane->workers = 0;
    }

    kmr_free(lane, sizeof(struct kmr_lane_state));
}

static void kmr_dump_sublanes(struct kmr_swf *wf, struct kmr_lane_state *lane);

/** Dumps lanes created by kmr_init_swf(). */

void
kmr_dump_swf_lanes(KMR *mr)
{
    kmr_err_when_swf_is_not_initialized(mr);
    struct kmr_swf *wf = mr->simple_workflow;
    if (wf->rank == wf->master_rank) {
	kmr_dump_sublanes(wf, wf->master.top_lane);
    }
}

static void
kmr_dump_sublanes(struct kmr_swf *wf, struct kmr_lane_state *lane)
{
    const int master = wf->master_rank;
    if (wf->rank == master) {
	if (lane->workers != 0) {
	    struct kmr_rank_vector *u = lane->workers;
	    printf("lane %s : ranks[%d]=(",
		   kmr_lane_string(lane->lane_id, 0), u->n);
	    for (int i = 0; i < u->n; i++) {
		char *separator = (i == 0 ? "" : ",");
		printf("%s%d", separator, u->ranks[i]);
	    }
	    printf(")\n");
	}
	if (lane->sublanes != 0) {
	    struct kmr_lane_vector *v = lane->sublanes;
	    for (int i = 0; i < v->n; i++) {
		kmr_dump_sublanes(wf, v->lanes[i]);
	    }
	}
    }
}

/* ================================================================ */

/* Scheduler of Workflow */

static struct kmr_work_item *
kmr_make_work_item(struct kmr_swf *wf, struct kmr_lane_no id,
		   const char *args, size_t argssize,
		   int seq, _Bool separatorspace);
static int kmr_enqueue_work(struct kmr_swf *wf, struct kmr_lane_state *lane,
			    struct kmr_work_item *x, _Bool multiple_any);
static int kmr_link_work(struct kmr_swf *wf, struct kmr_lane_state *lane,
			 struct kmr_work_item *x);
static void kmr_preset_lane_state(struct kmr_swf *wf, _Bool queuing);
static void kmr_check_work_queues_empty(struct kmr_swf *wf);

/** Maps with a simple workflow.  The ranks are configured as lanes,
    which should be initialized by kmr_init_swf() in advance.  The key
    part specifies the lane like "3.3.3", and the value part specifies
    the command-line arguments.  The work-items in a lane run in the
    FIFO order.  The lane specification can be an any-lane using a
    wildcard like "3.3.*".  The higher level lane blocks the sublanes,
    thus, for example, an entry with the lane "3.3" blocks the
    following entries with the lanes "3.3.*". */

int
kmr_map_swf(KMR_KVS *kvi, KMR_KVS *kvo, void *arg,
	    struct kmr_spawn_option opt, kmr_mapfn_t mapfn)
{
    kmr_assert_kvs_ok(kvi, kvo, 1, 0);
    assert(kvi->c.key_data == KMR_KV_OPAQUE
	   || kvi->c.key_data == KMR_KV_CSTRING);
    assert(kvi->c.value_data == KMR_KV_OPAQUE
	   || kvi->c.value_data == KMR_KV_CSTRING);
    assert(kvi->c.element_count <= INT_MAX);

    KMR * const mr = kvi->c.mr;
    kmr_err_when_swf_is_not_initialized(mr);
    struct kmr_swf *wf = mr->simple_workflow;
    _Bool tracing5 = (wf->mr->trace_map_spawn && (5 <= wf->mr->verbosity));

    if (wf->rank != wf->master_rank) {
	kmr_error(mr, "Non-master rank calls kmr_map_swf()");
	abort();
    }

    int cc;

    /* Clear the old history if it remains. */

    if (wf->master.history_head.next != 0) {
	assert(wf->master.record_history);
	assert(wf->master.history_insertion_tail != 0);
	kmr_free_work_list(wf, wf->master.history_head.next, 0, 0);
	wf->master.history_head.next = 0;
	wf->master.history_insertion_tail = 0;
    }

    wf->master.record_history = mr->swf_record_history;
    if (wf->master.record_history) {
	wf->master.history_insertion_tail = &wf->master.history_head;
    }

    /* Enqueue work-items. */

    if (tracing5) {
	fprintf(stderr, ";;KMR [%05d] kmr_map_swf:"
		" Queue work-items.\n", wf->rank);
	fflush(0);
    }

    kmr_preset_lane_state(wf, 1);

    {

	int count = (int)kvi->c.element_count;

	/* Scan key-value pairs and put them in the queues. */

	kvi->c.current_block = kvi->c.first_block;
	struct kmr_kvs_entry *e;
	e = kmr_kvs_first_entry(kvi, kvi->c.first_block);
	for (int i = 0; i < count; i++) {
	    struct kmr_kv_box kv = kmr_pick_kv(e, kvi);

	    if (kv.vlen >= (int)wf->args_size) {
		char ee[80];
		snprintf(ee, sizeof(ee),
			 "Command string (length=%d) too long (%s)",
			 kv.vlen, kv.v.p);
		kmr_error(mr, ee);
		abort();
		return MPI_ERR_SPAWN;
	    }

	    _Bool separatorspace = opt.separator_space;
	    struct kmr_lane_no id = kmr_name_lane(wf->mr, kv.k.p);
	    struct kmr_work_item *
		x = kmr_make_work_item(wf, id, kv.v.p, (size_t)kv.vlen,
				       i, separatorspace);
	    assert(x != 0);

	    cc = kmr_enqueue_work(wf, wf->master.top_lane, x, 1);
	    assert(cc == MPI_SUCCESS);

	    e = kmr_kvs_next(kvi, e, 0);
	}
    }

    /* Initialize lanes for start. */

    kmr_preset_lane_state(wf, 0);

    /* Start workers. */

    if (tracing5) {
	fprintf(stderr, ";;KMR [%05d] kmr_map_swf:"
		" Request workers to start.\n", wf->rank);
	fflush(0);
    }

    cc = kmr_activate_workers(wf, 0);
    assert(cc == MPI_SUCCESS);

    assert(wf->rank == wf->master_rank);
    for (;;) {
	cc = kmr_handle_worker_request(wf, 0);
	if (cc == MPI_SUCCESS) {
	    break;
	}
    }

    kmr_check_work_queues_empty(wf);
    kmr_free_kvs(kvi);

    if (tracing5) {
	fprintf(stderr, ";;KMR [%05d] Master finished"
		" (Workers will be in undefined state).\n", wf->rank);
	fflush(0);
    }

    return MPI_SUCCESS;
}

/* Allocates and fills a work-item.  It scans the arguments for
   "maxprocs=", and converts the separator of the arguments to a null
   character. */

static struct kmr_work_item *
kmr_make_work_item(struct kmr_swf *wf, struct kmr_lane_no id,
		   const char *args, size_t argssize,
		   int seq, _Bool separatorspace)
{
    char *name = "kmr_map_swf";
    struct kmr_lane_no nulllane;
    kmr_clear_lane_id(&nulllane);

    int cc;

    char *argsbuf = kmr_malloc(argssize);
    memcpy(argsbuf, args, argssize);

    int maxargc;
    cc = kmr_scan_argv_strings(wf->mr, argsbuf, argssize, 0,
			       &maxargc, 0, separatorspace, name);
    assert(cc == MPI_SUCCESS);

    char **argv0 = kmr_malloc(sizeof(char *) * (size_t)(maxargc + 1));
    memset(argv0, 0, (sizeof(char *) * (size_t)(maxargc + 1)));

    int argc;
    cc = kmr_scan_argv_strings(wf->mr, argsbuf, argssize, (maxargc + 1),
			       &argc, argv0, separatorspace, name);
    assert(cc == MPI_SUCCESS);
    assert(maxargc == argc);

    char **argv;
    argv = argv0;

    /* Check if the "MAXPROCS=" string in the arguments. */

    int nprocs;
    if (argc > 0 && strncmp("maxprocs=", argv[0], 9) == 0) {
	int v;
	cc = kmr_parse_int(&argv[0][9], &v);
	if (cc == 1 || v >= 0) {
	    nprocs = v;
	    argc--;
	    argv++;
	} else {
	    char ee[80];
	    snprintf(ee, sizeof(ee), "Bad maxprocs string (%s)", argv[0]);
	    kmr_error(wf->mr, ee);
	    abort();
	}
    } else {
	nprocs = 0;
    }

    size_t asz;
    {
	asz = 0;
	for (int i = 0; i < argc; i++) {
	    size_t sz = (strlen(argv[i]) + 1);
	    asz += sz;
	}
	/* Add the last empty string. */
	asz++;
	assert(asz <= (argssize + 1));
    }

    size_t msz = (offsetof(struct kmr_spawn_work, args) + asz);
    size_t xsz = (offsetof(struct kmr_work_item, work) + msz);
    struct kmr_work_item *x = kmr_malloc(xsz);
    assert(x != 0);

    /* Fill slots of a work-item. */

    x->req = KMR_SPAWN_WORK;
    x->requesting_lane = id;
    x->assigned_lane = nulllane;
    x->level = kmr_level_of_lane(id, 1);
    x->sequence_no = seq;

    x->work.req = KMR_SPAWN_WORK;
    x->work.protocol_version = KMR_SPAWN_MAGIC;
    x->work.message_size = (int)msz;
    x->work.subworld = x->level;
    x->work.color = kmr_color_of_lane(id);
    x->work.nprocs = nprocs;
    x->work.print_trace = 0;

    {
	char *p;
	p = x->work.args;
	for (int i = 0; i < argc; i++) {
	    size_t sz = (strlen(argv[i]) + 1);
	    memcpy(p, argv[i], sz);
	    p += sz;
	}
	*p = 0;
	p++;
	assert((size_t)(p - x->work.args) == asz);
    }

    if (0) {
	fprintf(stderr,
		(";;KMR [%05d] kmr_map_swf:"
		 " argc=%d siz=%d\n"), wf->rank, argc, (int)asz);
	for (int i = 0; i < argc; i++) {
	    fprintf(stderr, "%s\n", argv[i]);
	}
	fflush(0);
    }

    kmr_free(argv0, (sizeof(char *) * (size_t)(maxargc + 1)));
    kmr_free(argsbuf, argssize);

    return x;
}

/** Enqueues a work-item in some sublane of a LANE (at-the-master).
    Note it also puts the work-item in the sublanes below, which will
    block the lanes for yielding them for a superlane.  */

static int
kmr_enqueue_work(struct kmr_swf *wf, struct kmr_lane_state *lane,
		 struct kmr_work_item *x, _Bool multipleany)
{
    int cc;

    int queuing = (lane->level + 1);
    if (lane->sublanes == 0) {
	char ee[80];
	snprintf(ee, sizeof(ee),
		 ("Bad lane specified; nonexisting level"
		  " (lane=%s at level=%d)"),
		 kmr_lane_string(x->requesting_lane, 0), queuing);
	kmr_error(wf->mr, ee);
	abort();
	return MPI_ERR_SPAWN;
    } else {
	assert(x->level >= queuing);
	struct kmr_lane_vector *v = lane->sublanes;
	int q = x->requesting_lane.v[queuing];
	assert(q != KMR_NO_LANE);
	if (0 <= q && q < v->n && (x->level > queuing)) {
	    cc = kmr_enqueue_work(wf, v->lanes[q], x, multipleany);
	    return cc;
	} else if (0 <= q && q < v->n) {
	    assert(x->level == queuing);
	    cc = kmr_link_work(wf, v->lanes[q], x);
	    return cc;
	} else if (q == KMR_ANY_LANE && (x->level == queuing || multipleany)) {
	    for (int i = 0; i < v->n; i++) {
		cc = kmr_link_work(wf, v->lanes[i], x);
		if (cc != MPI_SUCCESS) {
		    break;
		}
	    }
	    return cc;
	} else if (q == KMR_ANY_LANE) {
	    assert(x->level > queuing && !multipleany);
	    char ee[80];
	    snprintf(ee, sizeof(ee),
		     ("Bad lane specified; multiple/non-tail any-lane"
		      " (lane=%s at level=%d)"),
		     kmr_lane_string(x->requesting_lane, 0), queuing);
	    kmr_error(wf->mr, ee);
	    abort();
	    return MPI_ERR_SPAWN;
	} else {
	    char ee[80];
	    snprintf(ee, sizeof(ee),
		     ("Bad lane specified; index exceeds size"
		      " (lane=%s at level=%d)"),
		     kmr_lane_string(x->requesting_lane, 0), queuing);
	    kmr_error(wf->mr, ee);
	    abort();
	    return MPI_ERR_SPAWN;
	}
    }
}

static int
kmr_link_work(struct kmr_swf *wf, struct kmr_lane_state *lane,
	      struct kmr_work_item *x)
{
    int cc;
    struct kmr_work_list *p = kmr_malloc(sizeof(struct kmr_work_list));
    assert(p != 0);
    p->next = 0;
    p->item = x;
    assert(lane->queue_insertion_tail != 0);
    assert(lane->queue_insertion_tail->next == 0);
    lane->queue_insertion_tail->next = p;
    lane->queue_insertion_tail = p;

    struct kmr_lane_vector *v = lane->sublanes;
    if (v != 0) {
	for (int i = 0; i < v->n; i++) {
	    cc = kmr_link_work(wf, v->lanes[i], x);
	    assert(cc == MPI_SUCCESS);
	}
    }
    return MPI_SUCCESS;
}

/* Sets the lanes as all working, so as to receive for an initial
   message which acts as a done message from the workers. */

static void
kmr_preset_lane_state(struct kmr_swf *wf, _Bool queuing)
{
    struct kmr_lane_state *h = wf->master.list_of_all_lanes;
    for (struct kmr_lane_state *lane = h; lane != 0; lane = lane->link) {
	assert((lane->sublanes != 0) || (lane->workers != 0));

	if (queuing) {
	    /* Open the work-item queue. */
	    assert(lane->queue_insertion_tail == 0);
	    lane->queue_insertion_tail = &lane->queue_head;
	} else {
	    /* Close the work-item queue, it will not be added. */
	    lane->queue_insertion_tail = 0;

	    /* Mark a lane as running and add a dummy work. */

	    assert(lane->running_sublanes != 0);
	    int nsubs = ((lane->sublanes != 0)
			 ? lane->sublanes->n : lane->workers->n);
	    lane->n_running_sublanes = nsubs;
	    for (int i = 0; i < nsubs; i++) {
		lane->running_sublanes[i] = 1;
	    }

	    if (lane->workers != 0) {
		struct kmr_lane_no id = lane->lane_id;

		size_t msz = (offsetof(struct kmr_spawn_work, args) + 0);
		size_t xsz = (offsetof(struct kmr_work_item, work) + msz);
		struct kmr_work_item *x = kmr_malloc(xsz);
		assert(x != 0);
		memset(x, 0, xsz);

		/* (Mark as a dummy with KMR_SPAWN_NONE). */
		x->req = KMR_SPAWN_NONE;
		x->requesting_lane = id;
		x->assigned_lane = id;
		x->level = kmr_level_of_lane(id, 1);
		x->work.req = KMR_SPAWN_NONE;
		x->work.protocol_version = KMR_SPAWN_MAGIC;
		x->work.message_size = (int)msz;
		x->work.subworld = x->level;
		x->work.color = kmr_color_of_lane(id);
		x->work.nprocs = lane->workers->n;
		x->work.print_trace = 0;

		assert(lane->current_work == 0);
		lane->current_work = x;
	    }
	}
    }
}

static int kmr_schedule_lanes(struct kmr_swf *wf, struct kmr_lane_state *lane);
static int kmr_start_lanes(struct kmr_swf *wf, struct kmr_lane_state *lane,
			   struct kmr_work_item *x);
static int kmr_finish_current_work(struct kmr_swf *wf,
				   struct kmr_lane_state *lane);
static struct kmr_work_item *kmr_dequeue_work(struct kmr_lane_state *lane);
static int kmr_ckeck_sublanes_empty(struct kmr_swf *wf,
				    struct kmr_lane_state *lane);
static void kmr_record_in_history(struct kmr_swf *wf, struct kmr_work_item *x);

/** Schedules a next work-item when a worker or a sublane finishes
    (at-the-master).  It returns MPI_SUCCESS when the all of its
    workers and sublanes finish, or MPI_ERR_PENDING when some
    work-items are running.  Scheduling of the lanes works in a
    bottom-up mannar, and is implemented by two functions
    kmr_yield_for_lane() and kmr_schedule_lanes().
    kmr_yield_for_lane() propagates the state upwards, and
    kmr_schedule_lanes() starts a work-item on the lane.
    kmr_yield_for_lane() is called when one of the workers or the
    sublanes finishes.  kmr_schedule_lanes() is called when the lane
    is free (no running workers nor running sublanes) to start the
    lane.  A call to kmr_yield_for_lane() first enters in a
    bottom-level lane and ascends its superlanes upwards.  Note that
    the current work-item can be nothing.  */

static int
kmr_yield_for_lane(struct kmr_swf *wf, struct kmr_lane_state *lane,
		   int sublaneindex)
{
    int cc;

    assert(lane->n_running_sublanes > 0);
    assert(lane->sublanes == 0 || (sublaneindex < lane->sublanes->n));
    assert(lane->workers == 0 || (sublaneindex < lane->workers->n));
    assert(lane->running_sublanes[sublaneindex] != 0);
    lane->running_sublanes[sublaneindex] = 0;

    lane->n_running_sublanes--;
    if (lane->n_running_sublanes != 0) {
	return MPI_ERR_PENDING;
    } else {
	cc = kmr_finish_current_work(wf, lane);
	assert(cc == MPI_SUCCESS);
	if (lane->superlane == 0) {
	    /* ALL DONE FOR THE TOP-LANE. */
	    return MPI_SUCCESS;
	} else {
	    cc = kmr_schedule_lanes(wf, lane);
	    if (cc == MPI_ERR_PENDING) {
		return MPI_ERR_PENDING;
	    } else {
		/* Propagate the free-state upwards. */
		assert(cc == MPI_SUCCESS);
		int s = kmr_find_sublane_index(wf, lane);
		cc = kmr_yield_for_lane(wf, lane->superlane, s);
		return cc;
	    }
	}
    }
}

/** Scehdules a lane for a next work-item (at-the-master).  There is
    no work-items currently running in the lane and its sublanes.  It
    is called by kmr_yield_for_lane(). */

static int
kmr_schedule_lanes(struct kmr_swf *wf, struct kmr_lane_state *lane)
{
    _Bool tracing5 = (wf->mr->trace_map_spawn && (5 <= wf->mr->verbosity));

    int cc;

    assert(lane->superlane != 0);
    assert(lane->n_running_sublanes == 0);
    assert(lane->current_work == 0);
    assert(lane->yielding_to_superlane == 0);

    struct kmr_work_item *x = kmr_dequeue_work(lane);
    if (x != 0) {
	assert(x->level <= lane->level);

	/* Confirm all sublanes have yielded. */

	struct kmr_lane_vector *v = lane->sublanes;
	if (v != 0) {
	    for (int i = 0; i < v->n; i++) {
		assert(x == v->lanes[i]->yielding_to_superlane);
		v->lanes[i]->yielding_to_superlane = 0;
	    }
	}

	if (x->level < lane->level) {
	    /* Yield this lane for the superlane. */
	    assert(lane->superlane != 0);
	    assert(lane->yielding_to_superlane == 0);
	    lane->yielding_to_superlane = x;
	    int s = kmr_find_sublane_index(wf, lane);
	    cc = kmr_yield_for_lane(wf, lane->superlane, s);
	    return cc;
	} else {
	    /* Start workers. */
	    if (tracing5) {
		fprintf(stderr, ";;KMR [%05d] Start a work (lane=%s).\n",
			wf->rank, kmr_lane_string(lane->lane_id, 0));
		fflush(0);
	    }
	    assert(x->level == lane->level);
	    x->assigned_lane = lane->lane_id;
	    x->work.color = kmr_color_of_lane(lane->lane_id);
	    x->work.print_trace = (tracing5 != 0);
	    cc = kmr_dequeue_scattered_work(wf, lane, x);
	    assert(cc == MPI_SUCCESS);
	    cc = kmr_start_lanes(wf, lane, x);
	    assert(cc == MPI_SUCCESS);
	    kmr_record_in_history(wf, x);
	    return MPI_ERR_PENDING;
	}
    } else {
	cc = kmr_ckeck_sublanes_empty(wf, lane);
	assert(cc == MPI_SUCCESS);

#if 0
	/* Schedule sublanes. */
	struct kmr_lane_vector *v = lane->sublanes;
	if (v != 0) {
	    for (int i = 0; i < v->n; i++) {
		cc = kmr_schedule_lanes(wf, v->lanes[i]);
		if (cc == MPI_ERR_PENDING) {
		    lane->n_running_sublanes++;
		}
	    }
	}
	if (lane->n_running_sublanes != 0) {
	    return MPI_ERR_PENDING;
	}
#endif

	if (tracing5) {
	    fprintf(stderr,
		    ";;KMR [%05d] workflow lane done (lane=%s).\n",
		    wf->rank, kmr_lane_string(lane->lane_id, 0));
	    fflush(0);
	}
	assert(lane->superlane != 0);
	return MPI_SUCCESS;
    }
}

static int
kmr_ckeck_sublanes_empty(struct kmr_swf *wf, struct kmr_lane_state *lane)
{
    int cc;

    struct kmr_work_list *h = lane->queue_head.next;
    if (h != 0) {
	return MPI_ERR_PENDING;
    } else if (lane->sublanes != 0) {
	struct kmr_lane_vector *v = lane->sublanes;
	for (int i = 0; i < v->n; i++) {
	    cc = kmr_ckeck_sublanes_empty(wf, v->lanes[i]);
	    if (cc == MPI_ERR_PENDING) {
		return MPI_ERR_PENDING;
	    }
	}
    }
    return MPI_SUCCESS;
}

static struct kmr_work_item *
kmr_dequeue_work(struct kmr_lane_state *lane)
{
    struct kmr_work_list *h = lane->queue_head.next;
    if (h == 0) {
	return 0;
    } else {
	lane->queue_head.next = h->next;
	struct kmr_work_item *x = h->item;
	kmr_free(h, sizeof(struct kmr_work_list));
	return x;
    }
}

static int kmr_remove_work(struct kmr_swf *wf, struct kmr_lane_state *lane,
			   struct kmr_work_item *x);

/** Removes all occurrences of a work-item (which may be scattered for
    an any-lane) from the all queues. */

static int
kmr_dequeue_scattered_work(struct kmr_swf *wf, struct kmr_lane_state *lane,
			   struct kmr_work_item *x)
{
    struct kmr_lane_no id = x->requesting_lane;
    int cc;
    struct kmr_lane_state *sup;
    sup = lane;
    while (sup->level >= 0 && id.v[sup->level] == KMR_ANY_LANE) {
	sup = lane->superlane;
    }
    cc = kmr_remove_work(wf, sup, x);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

static int
kmr_remove_work(struct kmr_swf *wf, struct kmr_lane_state *lane,
		struct kmr_work_item *x)
{
    struct kmr_lane_vector *v = lane->sublanes;
    if (v != 0) {
	for (int i = 0; i < v->n; i++) {
	    kmr_remove_work(wf, v->lanes[i], x);
	}
    }
    struct kmr_work_list *h = &(lane->queue_head);
    for (struct kmr_work_list *q = h; (q != 0 && q->next != 0); q = q->next) {
	struct kmr_work_list *p = q->next;
	if (p->item == x) {
	    q->next = p->next;
	    kmr_free(p, sizeof(struct kmr_work_list));
	}
    }
    return MPI_SUCCESS;
}

/** Requests workers to start a work for a lane and its sublanes, and
    then connects to workers (at-the-master). */

static int
kmr_start_lanes(struct kmr_swf *wf, struct kmr_lane_state *lane,
		struct kmr_work_item *x)
{
    assert(x->req == KMR_SPAWN_WORK);
    MPI_Comm basecomm = wf->base_comm;

    int cc;

    assert(lane->current_work == 0);
    lane->current_work = x;

    if (lane->sublanes != 0) {
	struct kmr_lane_vector *v = lane->sublanes;
	assert(lane->n_running_sublanes == 0);
	lane->n_running_sublanes = v->n;
	for (int i = 0; i < v->n; i++) {
	    cc = kmr_start_lanes(wf, v->lanes[i], x);
	    assert(cc == MPI_SUCCESS);
	    assert(lane->running_sublanes[i] == 0);
	    lane->running_sublanes[i] = 1;
	}
    } else {
	struct kmr_rank_vector *u = lane->workers;
	assert(lane->n_running_sublanes == 0);
	lane->n_running_sublanes = u->n;
	for (int i = 0; i < u->n; i++) {
	    cc = kmr_start_worker(&x->work, (size_t)x->work.message_size,
				  u->ranks[i], basecomm);
	    assert(cc == MPI_SUCCESS);
	    assert(lane->running_sublanes[i] == 0);
	    lane->running_sublanes[i] = 1;
	}
	assert(cc == MPI_SUCCESS);
    }

    _Bool mainlane = (lane->level == x->level);
    if (mainlane) {
	assert(lane->icomm == MPI_COMM_NULL);
	cc = kmr_join_to_workers(wf, lane);
	assert(cc == MPI_SUCCESS);
    }
    return MPI_SUCCESS;
}

/* Finishes the current work-item.  It is called when the all workers
   or the all sublanes finish.  Note that the current work-item can be
   nothing, when its sublanes are just scheduled. */

static int
kmr_finish_current_work(struct kmr_swf *wf, struct kmr_lane_state *lane)
{
    int cc;

    if (lane->current_work != 0) {
	struct kmr_work_item *x = lane->current_work;
	lane->current_work = 0;

	_Bool mainlane = (lane->level == x->level);
	if (mainlane) {
	    _Bool dummy_initial_work_item = (x->req == KMR_SPAWN_NONE);
	    if (dummy_initial_work_item) {
		size_t xsz = (offsetof(struct kmr_work_item, work)
			      + (size_t)x->work.message_size);
		kmr_free(x, xsz);
	    } else {
		assert(lane->icomm != MPI_COMM_NULL);
		cc = MPI_Comm_free(&lane->icomm);
		assert(cc == MPI_SUCCESS);

		/* A work-item cannot be freed, when it may be linked
		   from the history. */

		if (wf->master.history_insertion_tail == 0) {
		    assert(!wf->master.record_history);
		    size_t xsz = (offsetof(struct kmr_work_item, work)
				  + (size_t)x->work.message_size);
		    kmr_free(x, xsz);
		}
	    }
	}
    }
    return MPI_SUCCESS;
}

static void
kmr_free_work_list(struct kmr_swf *wf, struct kmr_work_list *h,
		   struct kmr_lane_state *lane, _Bool warn)
{
    assert(!warn || lane != 0);

    struct kmr_work_list *p;
    p = h;
    while (p != 0) {
	struct kmr_work_list *q = p;
	struct kmr_work_item *x = q->item;
	p = p->next;

	if (warn) {
	    char ee[80];
	    snprintf(ee, sizeof(ee),
		     "Some work-items remain queued (%s)",
		     x->work.args);
	    kmr_warning(wf->mr, 1, ee);
	}

	if (lane != 0) {
	    kmr_dequeue_scattered_work(wf, lane, x);
	}
	size_t xsz = (offsetof(struct kmr_work_item, work)
		      + (size_t)x->work.message_size);
	kmr_free(x, xsz);
	kmr_free(q, sizeof(struct kmr_work_list));
    }
}

static void
kmr_check_work_queues_empty(struct kmr_swf *wf)
{
    _Bool somenonempty;
    somenonempty = 0;
    struct kmr_lane_state *h = wf->master.list_of_all_lanes;
    for (struct kmr_lane_state *lane = h; lane != 0; lane = lane->link) {
	if (lane->queue_head.next != 0) {
	    somenonempty |= 1;
	    kmr_free_work_list(wf, lane->queue_head.next, lane, 1);
	    lane->queue_head.next = 0;
	}
    }
    if (somenonempty) {
	kmr_error(wf->mr, "Some work-items remain queued");
	abort();
    }
}

static void
kmr_record_in_history(struct kmr_swf *wf, struct kmr_work_item *x)
{
    if (wf->master.record_history) {
	assert(wf->master.history_insertion_tail != 0);
	struct kmr_work_list *p = kmr_malloc(sizeof(struct kmr_work_list));
	assert(p != 0);
	p->next = 0;
	p->item = x;
	assert(wf->master.history_insertion_tail->next == 0);
	wf->master.history_insertion_tail->next = p;
	wf->master.history_insertion_tail = p;
    }
}

/** Prints the history of kmr_map_swf(), which is the start ordering
    the work-items.  The work-items are given sequence numbers from
    zero in the order in the KVS. */

void
kmr_dump_swf_history(KMR *mr)
{
    kmr_err_when_swf_is_not_initialized(mr);
    struct kmr_swf *wf = mr->simple_workflow;
    if (wf->master.history_head.next == 0) {
	kmr_warning(wf->mr, 1, "Workflow history not recorded.");
    } else {
	struct kmr_work_list *p;
	p = wf->master.history_head.next;
	while (p != 0) {
	    struct kmr_work_list *q = p;
	    struct kmr_work_item *x = q->item;
	    p = p->next;
	    /* (Note kmr_lane_string() uses static buffer). */
	    printf("work=%d for lane=%s",
		   x->sequence_no, kmr_lane_string(x->requesting_lane, 0));
	    printf(" run in lane=%s\n",
		   kmr_lane_string(x->assigned_lane, 0));
	}
    }
    fflush(0);
}

/** Returns a list of start ordering of the work-items.  The
    work-items are given sequence numbers from zero in the order in
    the KVS, and the HISTORY vector is filled by them in the order of
    the starts of the work-items.  The COUNT specifies the allocated
    length of the history vector. */

void
kmr_dump_swf_order_history(KMR *mr, int *history, size_t count)
{
    kmr_err_when_swf_is_not_initialized(mr);
    struct kmr_swf *wf = mr->simple_workflow;
    if (wf->master.history_head.next == 0) {
	kmr_warning(wf->mr, 1, "Workflow history not recorded.");
    } else {
	int icount = (int)count;
	for (int i = 0; i < icount; i++) {
	    history[i] = -1;
	}
	int i;
	i = 0;
	struct kmr_work_list *p;
	p = wf->master.history_head.next;
	while (p != 0 && i < icount) {
	    struct kmr_work_list *q = p;
	    struct kmr_work_item *x = q->item;
	    p = p->next;
	    history[i] = x->sequence_no;
	    i++;
	}
    }
}

/** Clears the history recorded in kmr_map_swf().  The history is also
    automatically cleared when a next call to kmr_map_swf(). */

void
kmr_free_swf_history(KMR *mr)
{
    kmr_err_when_swf_is_not_initialized(mr);
    struct kmr_swf *wf = mr->simple_workflow;
    if (wf->master.history_head.next == 0) {
	kmr_warning(wf->mr, 1, "Workflow history not recorded.");
    } else {
	assert(wf->master.record_history);
	assert(wf->master.history_insertion_tail != 0);
	kmr_free_work_list(wf, wf->master.history_head.next, 0, 0);
	wf->master.history_head.next = 0;
	wf->master.history_insertion_tail = 0;
    }
}

/** (spawn-library-protocol) Handles requests from workers.  It
    returns MPI_ERR_PENDING when some workers not finish, or
    MPI_SUCCESS.  It blocks in receiving a new request. */

static int
kmr_handle_worker_request(struct kmr_swf *wf, _Bool joining)
{
    MPI_Comm comm = wf->base_comm;
    _Bool tracing5 = (wf->mr->trace_map_spawn && (5 <= wf->mr->verbosity));

    int cc;

    assert(wf->master.rpc_buffer != 0 && wf->master.rpc_size > 0);
    union kmr_spawn_rpc *mbuf = wf->master.rpc_buffer;
    int msz = (int)wf->master.rpc_size;
    mbuf->req = KMR_SPAWN_NONE;
    MPI_Status st;
    int len;
    cc = MPI_Recv(mbuf, msz, MPI_BYTE, MPI_ANY_SOURCE,
		  KMR_SPAWN_RPC_TAG, comm, &st);
    assert(cc == MPI_SUCCESS);
    cc = MPI_Get_count(&st, MPI_BYTE, &len);
    assert(cc == MPI_SUCCESS);
    size_t msglen = (size_t)len;
    int rank = st.MPI_SOURCE;
    assert(rank != wf->master_rank);

#if 0 /*AHOAHO*/
    struct ss {
	int MPI_SOURCE;
	int MPI_TAG;
	int MPI_ERROR;
	int _count;
	int _cancelled;
    } *xs = (struct ss *)&st;
    fprintf(stderr, "MPI_SOURCE=%d MPI_TAG=%d MPI_ERROR=%d _count=%d _cancelled=%d\n", xs->MPI_SOURCE, xs->MPI_TAG, xs->MPI_ERROR, xs->_count, xs->_cancelled); fflush(0);
    fprintf(stderr, "msz=%d ty=%lx\n", msz, MPI_BYTE); fflush(0);
#endif

    assert(MPI_SUCCESS != -1 && MPI_ERR_SPAWN != -1);
    cc = -1;
    switch (mbuf->req) {
    case KMR_SPAWN_NEXT: {
	assert(msglen == sizeof(struct kmr_spawn_next));
	struct kmr_spawn_next *w = &(mbuf->m0);
	struct kmr_lane_state *top = wf->master.top_lane;
	struct kmr_lane_state *lane = wf->master.lane_of_workers[rank];

	assert(lane == 0 || (lane->sublanes == 0 && lane->workers != 0));
	if (w->initial_message != 0) {
	    if (!joining) {
		char ee[80];
		snprintf(ee, sizeof(ee),
			 ("Unexpectedly receive a worker joining message"
			  " (from rank=%d)"), rank);
		kmr_error(wf->mr, ee);
		abort();
	    }
	    if (lane != 0) {
		lane->n_joined_ranks++;
		top->n_joined_ranks++;
		assert(lane->n_joined_ranks <= lane->workers->n);
		assert(top->n_joined_ranks <= top->total_ranks);
	    } else {
		wf->master.idle_ranks++;
	    }
	}

	if (tracing5) {
	    if (w->initial_message != 0) {
		fprintf(stderr,
			(";;KMR [%05d] rank=%d joined"
			 " (workers=%d/%d; idle=%d).\n"),
			wf->rank, rank,
			top->n_joined_ranks, top->total_ranks,
			wf->master.idle_ranks);
		fflush(0);
	    } else {
		fprintf(stderr, ";;KMR [%05d] rank=%d requesting a work.\n",
			wf->rank, rank);
		fflush(0);
	    }
	}

	if (joining) {
	    if (top->n_joined_ranks < top->total_ranks) {
		return MPI_ERR_PENDING;
	    } else {
		return MPI_SUCCESS;
	    }
	} else if (lane != 0) {
	    int s = kmr_find_worker_index(wf, lane, rank);
	    assert(s != -1);
	    cc = kmr_yield_for_lane(wf, lane, s);
	    if (cc == MPI_SUCCESS) {
		if (tracing5) {
		    fprintf(stderr, ";;KMR [%05d] Workflow finished.\n",
			    wf->rank);
		    fflush(0);
		}
	    }
	    return cc;
	} else {
	    return MPI_ERR_PENDING;
	}
	//break;
    }

    default: {
	cc = MPI_ERR_SPAWN;
	char ee[80];
	snprintf(ee, sizeof(ee),
		 "Bad RPC message request=0x%x length=%zd from rank=%d",
		 mbuf->req, msglen, rank);
	kmr_error(wf->mr, ee);
	abort();
	return cc;
    }
    }
}

/* (spawn-library-protocol) Starts processing on workers which are
   idle in the service loop.  With SHUTDOWN=true, it makes all
   workers leave the service loop. */

static int
kmr_activate_workers(struct kmr_swf *wf, _Bool shutdown)
{
    MPI_Comm comm = wf->base_comm;

    int cc;

    if (!shutdown) {
	struct kmr_spawn_work mm;
	struct kmr_spawn_work *mbuf = &mm;
	size_t msz = (offsetof(struct kmr_spawn_work, args) + 0);
	memset(mbuf, 0, msz);
	mbuf->req = KMR_SPAWN_WORK;
	mbuf->protocol_version = KMR_SPAWN_MAGIC;
	mbuf->message_size = (int)msz;
	mbuf->subworld = 0;
	mbuf->color = 0;
	mbuf->nprocs = 0;
	for (int i = 0; i < wf->nprocs; i++) {
	    if (i != wf->master_rank) {
		cc = MPI_Send(mbuf, (int)msz, MPI_BYTE, i,
			      KMR_SPAWN_RPC_TAG, comm);
		assert(cc == MPI_SUCCESS);
	    }
	}
    } else {
	struct kmr_spawn_none mm;
	struct kmr_spawn_none *mbuf = &mm;
	size_t msz = sizeof(struct kmr_spawn_none);
	memset(mbuf, 0, msz);
	mbuf->req = KMR_SPAWN_NONE;
	mbuf->protocol_version = KMR_SPAWN_MAGIC;
	for (int i = 0; i < wf->nprocs; i++) {
	    if (i != wf->master_rank) {
		cc = MPI_Send(mbuf, (int)msz, MPI_BYTE, i,
			      KMR_SPAWN_RPC_TAG, comm);
		assert(cc == MPI_SUCCESS);
	    }
	}
    }
    return MPI_SUCCESS;
}

/* (spawn-library-protocol).  Requests a worker to start a work.  A
   caller is responsible for ensuring that the all workers are idle.
   The started workers must be connected by kmr_spawn_connect(). */

static int
kmr_start_worker(struct kmr_spawn_work *w, size_t msglen,
		 int rank, MPI_Comm basecomm)
{
    int cc;

    int len = (int)msglen;
    cc = MPI_Send(w, len, MPI_BYTE, rank, KMR_SPAWN_RPC_TAG, basecomm);
    assert(cc == MPI_SUCCESS);
    return MPI_SUCCESS;
}

/* (spawn-library-protocol). Connects to workers corresponding to
   kmr_spawn_join_to_master(). */

static int
kmr_join_to_workers(struct kmr_swf *wf, struct kmr_lane_state *lane)
{
    MPI_Comm basecomm = wf->base_comm;
    _Bool tracing5 = (wf->mr->trace_map_spawn && (5 <= wf->mr->verbosity));

    int cc;

    if (tracing5) {
	fprintf(stderr, ";;KMR [%05d] Connect to workers (lane=%s).\n",
		wf->rank, kmr_lane_string(lane->lane_id, 0));
	fflush(0);
    }

    assert(lane->icomm == MPI_COMM_NULL);
    int leader = lane->leader_rank;
    int nranks = lane->total_ranks;
    cc = MPI_Intercomm_create(MPI_COMM_SELF, 0, basecomm, leader,
			      KMR_SPAWN_ICOMM_TAG, &lane->icomm);
    assert(cc == MPI_SUCCESS);

    int nprocs;
    cc = MPI_Comm_remote_size(lane->icomm, &nprocs);
    assert(cc == MPI_SUCCESS);
    if (nranks != nprocs) {
	char ee[80];
	snprintf(ee, sizeof(ee),
		 "Bad inter-communicator size (%d!=%d)", nprocs, nranks);
	kmr_error(wf->mr, ee);
	abort();
    }

    return MPI_SUCCESS;
}

/* (FAKE OF KMRSPAWN LIBRARY). */

#if 0 /*AHO*/
int
kmr_spawn_hookup_fake(struct kmr_spawn_hooks *hooks)
{
    if (kmr_fake_spawn_hooks != 0 && kmr_fake_spawn_hooks != hooks) {
	kmr_free(kmr_fake_spawn_hooks, sizeof(struct kmr_spawn_hooks));
    }
    kmr_fake_spawn_hooks = hooks;
    return MPI_SUCCESS;
}
#endif

/* (FAKE OF KMRSPAWN LIBRARY). */

#if 0
static int
kmr_run_command(struct kmr_spawn_hooks *hooks,
		struct kmr_spawn_work *w, size_t msglen)
{
    size_t asz = (msglen - offsetof(struct kmr_spawn_work, args));

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

    struct kmr_spawn_hooks *hooks = kmr_spawn_hooks;
    hooks->s.running_work = w;
    hooks->s.mpi_initialized = 1;

    if (hooks->s.print_trace) {
	char aa[80];
	kmr_make_printable_argv_string(aa, 45, argv);
	printf(";;KMR [%05d] EXEC: %s\n",
	       hooks->s.base_rank, aa);
	fflush(0);
    }

    hooks->s.running_work = 0;
    hooks->s.mpi_initialized = 0;

    return MPI_SUCCESS;
}
#endif

/*
Copyright (C) 2012-2016 RIKEN AICS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/
