/* kmrspawn.h (2016-09-04) -*-Coding: us-ascii;-*- */
/* Copyright (C) 2012-2016 RIKEN AICS */

/** \file kmrspawn.h Static-Spawning API. */

/* Parameters for Spawning.  KMR_SPAWN_MAGIC is a protocol version.
   KMR_SPAWN_SUBWORLDS is the maximum number of subworlds which are
   used as a world of spawning.  Subworlds need to be registered
   beforehand and one is selected at spawning.  KMR_SPAWN_ARGS_SIZE is
   the limit length of a sum of the strings of argv.
   KMR_SPAWN_RPC_TAG and KMR_SPAWN_ICOMM_TAG are message tags. */

#define KMR_SPAWN_MAGIC (20160904)
#define KMR_SPAWN_SUBWORLDS (20)
#define KMR_SPAWN_ARGS_SIZE (8 * 1024)
#define KMR_SPAWN_RPC_TAG (600)
#define KMR_SPAWN_ICOMM_TAG (601)

/* RPC Message Types. */

enum kmr_spawn_req {
    KMR_SPAWN_NONE, KMR_SPAWN_WORK, KMR_SPAWN_NEXT
};

/* RPC Message.  KMR_SPAWN_NEXT (worker-to-master) is a message to
   notify a finish of a work-item and to ask for a next one.  It is
   also initially sent as to notify a worker is ready.  */

struct kmr_spawn_next {
    enum kmr_spawn_req req;
    int protocol_version;
    int initial_message;
    int status;
};

/* RPC Message to Send a Work.  KMR_SPAWN_WORK (master-to-worker) is a
   message to start a work-item.  Note that the structure of a message
   is truncated by the size of the argument string.  The length set in
   MESSAGE_SIZE includes the whole structure.  SUBWORLD is an index to
   the vector of subworlds previously registered.  NPROCS is the
   number of ranks used.  It is just used to check the size of a
   communicator.  COLOR is a check code of a SUBWORLD index.  A color
   is compared to a value previously registered. */

struct kmr_spawn_work {
    enum kmr_spawn_req req;
    int protocol_version;
    int message_size;
    int subworld;
    unsigned long color;
    int nprocs;
    int print_trace;
    char args[64 /*KMR_SPAWN_ARGS_SIZE*/];
};

/* RPC Message.  KMR_SPAWN_NONE (master-to-worker) is a message to
   finish a worker.  */

struct kmr_spawn_none {
    enum kmr_spawn_req req;
    int protocol_version;
};

/* RPC Message between a Master and Workers.  RPC messages are sent by
   point-to-point because it does not yet established the
   inter-communicator used for spawning. */

union kmr_spawn_rpc {
    enum kmr_spawn_req req;
    struct kmr_spawn_next m0;
    struct kmr_spawn_work m1;
    struct kmr_spawn_none m2;
};

/* State of MPI Hooks and Spawner.  MPI_COMM_WORLD is a pointer to the
   world communicator (as being Open-MPI); SAVED_GENUINE_WORLD is a
   copy of the contents of the old world.  Note the world reference
   moves at link time, because it is a variable of static allocation
   in Open-MPI and thus it is of a copy-relocation. */

struct kmr_spawn_hooks {
    /* State of Spawner. */

    struct {
	/* State of Spawning API. */

	int master_rank;
	int base_rank;
	MPI_Comm base_comm;
	struct {
	    MPI_Comm comm;
	    unsigned long color;
	} subworlds[KMR_SPAWN_SUBWORLDS];
	int (*exec_fn)(struct kmr_spawn_hooks *, int, char **);

	void /*KMR*/ *mr;
	union kmr_spawn_rpc *rpc_buffer;
	size_t rpc_size;
	int service_count;
	_Bool print_trace;

	/* State of Running MPI. */

	MPI_Comm spawn_world;
	MPI_Comm spawn_parent;
	struct kmr_spawn_work *running_work;
	_Bool mpi_initialized;
	_Bool abort_when_mpi_abort;
    } s;

    /* State for KMR-LD. */

    struct {
	char **initial_argv;
	long options_flags;
	char *options_heap_bottom;
    } d;

    /* Hooks of MPI Library. */

    struct {
	void *saved_genuine_world;
	size_t size_of_comm_data;

	void (*exit)(int status);
	void (*raw_exit)(int status);
	int (*execve)(const char *file, char *const argv[],
		      char *const envp[]);

	int (*PMPI_Init)(int *argc, char ***argv);
	int (*PMPI_Init_thread)(int *argc, char ***argv, int required,
				int *provided);
	int (*PMPI_Finalize)(void);
	int (*PMPI_Abort)(MPI_Comm comm, int errorcode);
	int (*PMPI_Query_thread)(int *provided);

	/* (Used in this hook). */

	void *mpi_comm_world; /*ompi_mpi_comm_world*/
	void *mpi_byte; /*ompi_mpi_byte*/
	void *mpi_comm_null; /*ompi_mpi_comm_null*/

	int (*PMPI_Comm_get_parent)(MPI_Comm *parent);
	int (*PMPI_Comm_get_name)(MPI_Comm comm, char *name, int *len);
	int (*PMPI_Comm_set_name)(MPI_Comm comm, char *name);
	int (*PMPI_Comm_size)(MPI_Comm comm, int *size);
	int (*PMPI_Comm_rank)(MPI_Comm comm, int *rank);
	int (*PMPI_Comm_remote_size)(MPI_Comm comm, int *size);

	int (*PMPI_Intercomm_create)(MPI_Comm lcomm, int lleader,
				     MPI_Comm pcomm, int pleader,
				     int tag, MPI_Comm *newcomm);
	int (*PMPI_Comm_dup)(MPI_Comm comm, MPI_Comm *newcomm);
	int (*PMPI_Comm_free)(MPI_Comm *comm);
	int (*PMPI_Send)(void *buf, int count, MPI_Datatype dty,
			 int dst, int tag, MPI_Comm comm);
	int (*PMPI_Recv)(void *buf, int count, MPI_Datatype dty,
			 int src, int tag, MPI_Comm comm,
			 MPI_Status *status);
	int (*PMPI_Get_count)(MPI_Status *status, MPI_Datatype dty,
			      int *count);

	char world_name[MPI_MAX_OBJECT_NAME];
    } h;
};

/* API of Wokers. */

extern int kmr_spawn_hookup(struct kmr_spawn_hooks *hooks);
extern int kmr_spawn_setup(struct kmr_spawn_hooks *hooks,
			   MPI_Comm basecomm, int masterrank,
			   int (*execfn)(struct kmr_spawn_hooks *,
					 int, char **),
			   int nsubworlds,
			   MPI_Comm subworlds[], unsigned long colors[],
			   size_t argssize);
extern void kmr_spawn_set_verbosity(struct kmr_spawn_hooks *hooks, int level);
extern void kmr_spawn_service(struct kmr_spawn_hooks *hooks, int status);

/* (API of Wokers for dummy). */

extern int kmr_spawn_hookup_standin(struct kmr_spawn_hooks *hooks);
extern int kmr_spawn_setup_standin(struct kmr_spawn_hooks *hooks,
				   MPI_Comm basecomm, int masterrank,
				   int (*execfn)(struct kmr_spawn_hooks *,
						 int, char **),
				   int nsubworlds,
				   MPI_Comm subworlds[],
				   unsigned long colors[],
				   size_t argssize);
extern void kmr_spawn_set_verbosity_standin(struct kmr_spawn_hooks *hooks,
					    int level);
extern void kmr_spawn_service_standin(struct kmr_spawn_hooks *hooks,
				      int status);

/* Calling Unhooked Routines. */

extern void kmr_spawn_true_exit(int status);
extern int kmr_spawn_true_execve(const char *file, char *const argv[],
				  char *const envp[]);
extern int kmr_spawn_true_mpi_finalize(void);
extern int kmr_spawn_true_mpi_abort(MPI_Comm comm, int code);

/* Calling MPI Routines. */

extern int kmr_spawn_mpi_comm_size(MPI_Comm comm, int *size);
extern int kmr_spawn_mpi_comm_rank(MPI_Comm comm, int *rank);
extern int kmr_spawn_mpi_comm_remote_size(MPI_Comm comm, int *size);
extern int kmr_spawn_mpi_comm_get_name(MPI_Comm comm, char *name, int *len);
extern int kmr_spawn_mpi_comm_set_name(MPI_Comm comm, char *name);
extern int kmr_spawn_mpi_intercomm_create(MPI_Comm lcomm, int lleader,
					  MPI_Comm pcomm, int pleader,
					  int tag, MPI_Comm *icomm);
extern int kmr_spawn_mpi_comm_dup(MPI_Comm comm, MPI_Comm *newcomm);
extern int kmr_spawn_mpi_comm_free(MPI_Comm *comm);
extern int kmr_spawn_mpi_send(void *buf, int count, MPI_Datatype dty,
			      int dst, int tag, MPI_Comm comm);
extern int kmr_spawn_mpi_recv(void *buf, int count, MPI_Datatype dty,
			      int src, int tag, MPI_Comm comm,
			      MPI_Status *status);
extern int kmr_spawn_mpi_get_count(MPI_Status *status, MPI_Datatype dty,
				   int *count);

/*
Copyright (C) 2012-2016 RIKEN AICS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/
