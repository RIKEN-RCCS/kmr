/** \file kmrshell_mpi.c */
/* Copyright (C) 2012-2015 RIKEN AICS */

/** \file kmrshell_mpi.c
    kmrshell_mpi is command line version of KMR and it runs a MapReduce
    program whose mapper and reducers are user specified MPI programs.

    When kmrshell_mpi is used to run a MapReduce program, user should specify
    a simple program that generates key-value pairs from the output of mapper.
    The key-value generator program can be specified by '-k' option and
    can be implemented by reading outputs of mapper and then writing
    key-value pairs to the standard output.
    After shuffling the key-value paris, key-value pairs are written to files
    on each rank with 'key'-named text files whose line represents a key-value
    separated by a space.
    The file is passed to the reducer as the last parameter.

    kmrshell_mpi can run Map-only MapReduce where no reducer is run.
    This is very useful if you want to run multiple tasks as a single job.

    Options
    - \c -m mapper [Mandatory]\c
        - Specify a mapper program (MPI)
    - \c -k key_value_generator [Optional] \c
        - Specify a key-value pair generator program (serial)
    - \c -r reducer  [Optional]\c
        - Specify a reducer program (MPI)
    - \c -n m_num[:r_num]  [Optional] \c
        - Specify number of MPI processes to run mapper and reducers.
          When \c r_num \c is specified, each mapper runs with \c m_num \c
          processes and each reducer runs with \c r_num \c processes.
          When \c r_num \c is not specified each mapper and reducer runs
          with \c m_num \c processes.
          The default is 2.

    Usage
    \code
        $ mpiexec -machinefile machines -n 4 \
        ./kmrshell_mpi -n m_num[:r_num] -m mapper [-k kvgenerator] [-r reducer] \
        inputfile
    \endcode

    Examples
    \code
    e.g.1) Run MPI mapper and MPI reducer with 2 MPI processes each.
        $ mpiexec -np 2 ./kmrshell_mpi -n 2 -m "./mpi_pi.mapper" -k "./mpi_pi.kvgen.sh" -r "./mpi_pi.reducer" ./work

    e.g.2) Run MPI mapper with 2 MPI processes and MPI reducer with 4 processes.
        $ mpiexec -np 2 ./kmrshell_mpi -n 2:4 -m "./mpi_pi.mapper" -k "./mpi_pi.kvgen.sh" -r "./mpi_pi.reducer" ./work

    e.g.3) Only run MPI mapper with 2 MPI processes
        $ mpiexec -np 2 ./kmrshell2 -n 2 -m "./mpi_pi.mapper" ./work
    \endcode
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <dirent.h>
#include <unistd.h>
#include <mpi.h>
#include "kmr.h"

/* Maximum number of arguments to mapper and reducer programs. */
#define ARGSIZ 8

/* Buffer string size of arguments to mapper and reducer programs. */
#define ARGSTRLEN (8 * 1024)

/* Buffer string size of a command name */
#define COMMANDLEN 1024

/* Default number of MPI processes used in spawned program */
#define DEFAULT_MPI 2

/* Maximum length of line that represents a key-value */
#define LINELEN 32767

static void kmrsh_abort(int, const char *, ...);
static int add_command_kv(KMR_KVS *, int, char **, char *, int);
static int generate_mapcmd_kvs(const struct kmr_kv_box,
			       const KMR_KVS *, KMR_KVS *, void *, long);
static int run_kv_generator(const struct kmr_kv_box,
			    const KMR_KVS *, KMR_KVS *, void *, long);
static int write_kvs(const struct kmr_kv_box[], const long,
		     const KMR_KVS *, KMR_KVS *, void *);
static int generate_redcmd_kvs(const struct kmr_kv_box,
			       const KMR_KVS *, KMR_KVS *, void *, long);
static int sleep_wait(const struct kmr_kv_box, const KMR_KVS *, KMR_KVS *,
		      void *, long);
static int delete_file(const struct kmr_kv_box, const KMR_KVS *, KMR_KVS *,
		       void *, long);
static void parse_args(char *, char *[]);


/* A structure that stores command line information.
 */
struct cmdinfo {
    char **cmd_args;
    char *infile;
    int num_procs;
};


/* Abort function */
static void
kmrsh_abort(int rank, const char *format, ...)
{
    va_list arg;
    if (rank == 0) {
	va_start(arg, format);
	vfprintf(stderr, format, arg);
	va_end(arg);
    }
    MPI_Abort(MPI_COMM_WORLD, 1);
    exit(1);
}

/* This function create a key-value whose key is the specified id and
   value is command line, and then add it to the KVS.
*/
static int
add_command_kv(KMR_KVS *kvo, int id, char **cmd, char *infile, int n_procs)
{
    int i, cmdlen, vlen;
    char *cp, *np, *value;
    char maxprocs[32];

    /* set maxprocs parameter */
    snprintf(maxprocs, 31, "maxprocs=%d", n_procs);

    /* construct command line */
    for (cmdlen = 0, i = 0; i < ARGSIZ; i++) {
	if (cmd[i] == NULL) {
	    break;
	}
	cmdlen += (int)strlen(cmd[i]) + 1;
    }
    vlen = (int)strlen(maxprocs) + 1 + cmdlen + (int)strlen(infile) + 1;
    value = (char *)malloc((size_t)vlen * sizeof(char));
    memcpy(value, maxprocs, strlen(maxprocs));
    cp = value + strlen(maxprocs);
    *cp++ = ' ';
    for (i = 0; i < ARGSIZ; i++) {
	if (cmd[i] == NULL) {
	    break;
	}
	size_t len = strlen(cmd[i]);
	memcpy(cp, cmd[i], len);
	cp += len;
	*cp++ = ' ';
    }

    /* set input file */
    memcpy(cp, infile, strlen(infile));
    *(cp + strlen(infile)) = '\0';

    /* replace all ' ' by '\0' */
    cp = value;
    while (1) {
	if ((np = strchr((const char*)cp, ' ')) != NULL) {
	    *np++ = '\0';
	    cp = np;
	} else {
	    break;
	}
    }

    struct kmr_kv_box nkv = { .klen = sizeof(long),
			      .vlen = (size_t)vlen * sizeof(char),
			      .k.i  = id,
			      .v.p  = (void *)value };
    int ret = kmr_add_kv(kvo, nkv);
    free(value);
    return ret;
}

/* KMR map function
   It generates a KVS whose keys are numbers and values are command lines
   for mapper.
*/
static int
generate_mapcmd_kvs(const struct kmr_kv_box kv,
		    const KMR_KVS *kvi, KMR_KVS *kvo, void *p, long i_)
{
    int ret;
    struct cmdinfo *info = (struct cmdinfo *)p;
    char *path = info->infile;
    struct stat status;

    if (stat(path, &status) < 0) {
	fprintf(stderr, "File[%s] error\n", path);
	return -1;
    }
    if (!S_ISDIR(status.st_mode) && !S_ISREG(status.st_mode)) {
	fprintf(stderr, "File[%s] is not regular file or directory\n", path);
	return -1;
    }

    if (S_ISDIR(status.st_mode)) {  /* directory */
	size_t direntsz;
	long nmax = pathconf(path, _PC_NAME_MAX);
	if (nmax == -1) {
	    direntsz = (64 * 1024);
	} else {
	    direntsz = (offsetof(struct dirent, d_name) + (size_t)nmax + 1);
	}
	DIR *d;
	struct dirent *dentp;
	char b[direntsz];
	int id = 0;

	d = opendir(path);
	if (d == NULL) {
	    perror("opendir");
	    return -1;
	}
	while (readdir_r(d, (void *)b, &dentp) >= 0) {
	    struct stat substat;
	    char fullpath[MAXPATHLEN];
	    if (dentp == NULL) {
		break;
	    }

	    ret = snprintf(fullpath, sizeof(fullpath), "%s/%s",
			   path, dentp->d_name);
	    if (ret <= 0) {
		perror("snprintf");
		continue;
	    }

	    if (stat(fullpath, &substat) < 0) {
		continue;
	    }
	    if (S_ISREG(substat.st_mode)) {
		ret = add_command_kv(kvo, id, info->cmd_args, fullpath,
				     info->num_procs);
		if (ret != MPI_SUCCESS) {
		    return ret;
		}
		id += 1;
	    }
	}
	closedir(d);
	ret = MPI_SUCCESS;
    } else {  /* file */
	ret = add_command_kv(kvo, 0, info->cmd_args, path, info->num_procs);
    }

    return ret;
}

/* KMR map function
   It generates key-values for shuffling after mapper programs has been
   executed.
*/
static int
run_kv_generator(const struct kmr_kv_box kv,
		 const KMR_KVS *kvi, KMR_KVS *kvo, void *p, long i_)
{
    struct cmdinfo *info = (struct cmdinfo *)p;

    if (info->cmd_args[0] != NULL) {
	int ret, pipefd[2];

	ret = pipe(pipefd);
	if (ret < 0) {
	    perror("pipe for kv generator");
	    return ret;
	}

	pid_t pid = fork();
	if (pid < 0) {
	    perror("fork kv generator");
	    return -1;
	} else if (pid == 0) {
	    // child process
	    ret = close(pipefd[0]);
	    if (ret < 0) {
		perror("pipe close kv generator");
		return ret;
	    }
	    ret = dup2(pipefd[1], STDOUT_FILENO);
	    if (ret < 0) {
		perror("dup2 pipe kv generator");
		return ret;
	    }
	    ret = close(pipefd[1]);
	    if (ret < 0) {
		perror("pipe close kv generator");
		return ret;
	    }

	    // get the input filename from key-value
	    char *cp, *infile;
	    for (cp = (char *)kv.v.p; cp < kv.v.p + kv.vlen - 1; cp++) {
		if (*cp == '\0') {
		    infile = cp + 1;
		}
	    }

	    char *cmd_args[ARGSIZ+1] = { NULL };
	    int i;
	    for (i = 0; i <= ARGSIZ; i++) {
		if (info->cmd_args[i] != NULL) {
		    cmd_args[i] = info->cmd_args[i];
		} else {
		    cmd_args[i] = infile;
		    break;
		}
	    }

	    ret = execv(cmd_args[0], cmd_args);
	    if (ret < 0) {
		perror("execv kv generator");
		return ret;
	    }
	} else {
	    // parent process
	    ret = close(pipefd[1]);
	    if (ret < 0) {
		perror("pipe close kv generator");
		return ret;
	    }

	    char line[LINELEN];
	    long missingnl = 0;
	    long badlines = 0;
	    FILE* chld_out = fdopen(pipefd[0], "r");
	    while (fgets(line, sizeof(line), chld_out) != NULL) {
		char *cp = strchr(line, '\n');
		if (cp != NULL) {
		    assert(*cp == '\n');
		    *cp = '\0';
		} else {
		    missingnl++;
		}
		cp = strchr(line, ' ');
		if (cp == NULL) {
		    /* No value field. */
		    badlines++;
		    continue;
		}
		*cp = '\0';
		char *key = line;
		char *value = (cp + 1);
		struct kmr_kv_box nkv;
		nkv.klen = (int)strlen(key) + 1;
		nkv.vlen = (int)strlen(value) + 1;
		nkv.k.p = key;
		nkv.v.p = value;
		ret = kmr_add_kv(kvo, nkv);
		if (ret != MPI_SUCCESS) {
		    return ret;
		}

		if (missingnl) {
		    fprintf(stderr, ("warning: Line too long or "
				     "missing last newline.\n"));
		}
		if (badlines) {
		    fprintf(stderr, ("warning: Some lines have "
				     "no key-value pairs (ignored).\n"));
		}
	    }

	    ret = close(pipefd[0]);
	    if (ret < 0) {
		perror("pipe close kv generator");
		return ret;
	    }
	}
    }

    return MPI_SUCCESS;
}

/* KMR reduce function
   Write key-value pairs in KVS to key-named files.
*/
static int
write_kvs(const struct kmr_kv_box kv[], const long n,
	  const KMR_KVS *kvs, KMR_KVS *kvo, void *p)
{
    FILE *fp;
    int ret;

    if ((fp = fopen(kv[0].k.p, "w")) == NULL) {
	perror("open file with write mode");
	return -1;
    }
    for (long i = 0; i < n; i++) {
	fprintf(fp, "%s %s\n", kv[i].k.p, kv[i].v.p);
    }
    fclose(fp);

    // key is key, value is file path
    struct kmr_kv_box nkv;
    nkv.klen = kv[0].klen;
    nkv.k.p  = kv[0].k.p;
    nkv.vlen = kv[0].klen;
    nkv.v.p  = kv[0].k.p;
    ret = kmr_add_kv(kvo, nkv);
    if (ret != MPI_SUCCESS) {
	return ret;
    }
    return MPI_SUCCESS;
}

/* KMR map function
   It generates a KVS whose keys are numbers and values are command lines
   for reducer.
*/
static int
generate_redcmd_kvs(const struct kmr_kv_box kv,
		    const KMR_KVS *kvi, KMR_KVS *kvo, void *p, long i_)
{
    int ret;
    struct cmdinfo *info = (struct cmdinfo *)p;

    ret = add_command_kv(kvo, (int)i_, info->cmd_args, (char *)kv.k.p,
			 info->num_procs);
    return ret;
}

static int
sleep_wait(const struct kmr_kv_box kv,
	   const KMR_KVS *kvi, KMR_KVS *kvo, void *p, long i_)
{
    sleep(1);
    return MPI_SUCCESS;
}

static int
delete_file(const struct kmr_kv_box kv,
	    const KMR_KVS *kvi, KMR_KVS *kvo, void *p, long i_)
{
    char *file_name = (char*)kv.k.p;
    int ret = access(file_name, F_OK);
    if (ret == 0) {
	unlink(file_name);
    }
    return MPI_SUCCESS;
}


/* Parses command parameters given for mapper and reducer arguments.
   It scans an argument string like "mapper arg0 arg1" for the -m and
   -r options, and generates an argv array {"mapper", "arg0", "arg1",
   NULL}.  The separator is a whitespace.
   \param argstr string given for -m or -r options.
   \param argary array to be filled by argument strings. */
static void
parse_args(char *argstr, char *argary[])
{
    char *cp, *np;
    char **ap;

    ap = argary;
    cp = argstr;
    while (1) {
	*ap = cp;
	if ((np = strchr((const char*)cp, ' ')) != NULL) {
	    *np++ = '\0';
	}
	if (++ap >= &argary[ARGSIZ-1]) {
	    *ap = NULL;
	    break;
	} else {
	    if (np == NULL) {
		*ap = NULL;
		break;
	    }
	}
	while (*np == ' ') {
	    np++;
	}
	cp = np;
    }

    if (argary[0][0] != '.' || argary[0][0] != '/') {
	/* insert './' in front of command name */
	int len = (int)strlen(argary[0]) + 1;
	if (len + 2 > COMMANDLEN) {
	    fprintf(stderr, "command name is too long.\n");
	    MPI_Abort(MPI_COMM_WORLD, 1);
	}
	for (int i = len + 1; i >= 0; i--) {
	    argary[0][i+2] = argary[0][i];
	}
	argary[0][0] = '.';
	argary[0][1] = '/';
    }
}


int
main(int argc, char *argv[])
{
    int rank, ret, opt;
    char *mapper = NULL, *reducer = NULL, *infile = NULL;
    char *margv[ARGSIZ] = { NULL }, margbuf[ARGSTRLEN];
    char *rargv[ARGSIZ] = { NULL }, rargbuf[ARGSTRLEN];
    char *kargv[ARGSIZ] = { NULL }, kargbuf[ARGSTRLEN];
    int map_procs = DEFAULT_MPI, red_procs = DEFAULT_MPI;

    char *usage_msg =
	"Usage %s -n m_num[:r_num] -m mapper [-k kvgenerator] [-r reducer] "
	"inputfile\n";

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    while ((opt = getopt(argc, argv, "m:r:n:k:")) != -1) {
	switch (opt) {
	    size_t asz;
	case 'm':
	    asz = (strlen(optarg) + 1);
	    if (asz >= ARGSTRLEN) {
		kmrsh_abort(rank, "Argument too long for mapper (%s)\n",
			    optarg);
	    }
	    memcpy(margbuf, optarg, asz);
	    parse_args(margbuf, &margv[0]);
	    mapper = margv[0];
	    break;
	case 'r':
	    asz = (strlen(optarg) + 1);
	    if (asz >= ARGSTRLEN) {
		kmrsh_abort(rank, "Argument too long for reducer (%s)\n",
			    optarg);
	    }
	    memcpy(rargbuf, optarg, asz);
	    parse_args(rargbuf, &rargv[0]);
	    reducer = rargv[0];
	    break;
	case 'n':
	    asz = (strlen(optarg) + 1);
	    char para_arg[ARGSTRLEN], *cp;
	    memcpy(para_arg, optarg, asz);
	    cp = strchr(para_arg, ':');
	    if (cp == NULL) {
		/* use the same # of processes in map & reduce */
		map_procs = (int)strtol(para_arg, NULL, 10);
		red_procs = map_procs;
	    } else {
		/* use the different # of processes */
		*cp = '\0';
		char *np = cp + 1;
		map_procs = (int)strtol(para_arg, NULL, 10);
		red_procs = (int)strtol(np, NULL, 10);
	    }
	    break;
	case 'k':
	    asz = (strlen(optarg) + 1);
	    if (asz >= ARGSTRLEN) {
		kmrsh_abort(rank, "Argument too long for key-value "
			    "generator (%s)\n", optarg);
	    }
	    memcpy(kargbuf, optarg, asz);
	    parse_args(kargbuf, &kargv[0]);
	    break;
	default:
	    kmrsh_abort(rank, usage_msg, argv[0]);
	}
    }

    if ((argc - optind) != 1) {
	kmrsh_abort(rank, usage_msg, argv[0]);
    } else {
	infile = argv[optind];
	optind++;
    }

    if (mapper == NULL) {
	kmrsh_abort(rank, usage_msg, argv[0]);
    }

    kmr_init();
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);
    //mr->trace_map_spawn = 1;

    /* Assign mapper command lines to static processes */
    struct cmdinfo mapinfo = { margv, infile, map_procs };
    KMR_KVS *kvs_commands = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
    ret = kmr_map_once(kvs_commands, &mapinfo, kmr_noopt, 1,
		       generate_mapcmd_kvs);
    if (ret != MPI_SUCCESS) {
	kmrsh_abort(rank, "kmr_map_once failed.\n");
    }

    /* Run mapper */
    KMR_KVS *kvs_map = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    struct cmdinfo gkvinfo = { kargv, NULL, 0 };
    ret = kmr_map_processes(0, kvs_commands, kvs_map, &gkvinfo, MPI_INFO_NULL,
			    kmr_snoopt, run_kv_generator);
    if (ret != MPI_SUCCESS) {
	kmrsh_abort(rank, "executing mapper failed.\n");
    }

    if (reducer != NULL) {
	/* Shuffle key-value */
	KMR_KVS *kvs_red = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	ret = kmr_shuffle(kvs_map, kvs_red, kmr_noopt);
	if (ret != MPI_SUCCESS) {
	    kmrsh_abort(rank, "shuffling failed.\n");
	}

	/* Write key-values to files whose name is key */
	KMR_KVS *kvs_file = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
	ret = kmr_reduce(kvs_red, kvs_file, NULL, kmr_noopt, write_kvs);
	if (ret != MPI_SUCCESS) {
	    kmrsh_abort(rank, "writing key-values to files failed.\n");
	}

	/* Generate commands for reducer */
	struct cmdinfo redinfo = { rargv, NULL, red_procs };
	kvs_commands = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
	struct kmr_option kmr_inspect = { .inspect = 1 };
	ret = kmr_map(kvs_file, kvs_commands, &redinfo, kmr_inspect,
		      generate_redcmd_kvs);
	if (ret != MPI_SUCCESS) {
	    kmrsh_abort(rank, "kmr_map failed.\n");
	}

	/* Run reducer */
	ret = kmr_map_processes(0, kvs_commands, NULL, NULL, MPI_INFO_NULL,
				kmr_snoopt, sleep_wait);
	if (ret != MPI_SUCCESS) {
	    kmrsh_abort(rank, "executing reducer failed.\n");
	}

	/* Delete key files */
	ret = kmr_map(kvs_file, NULL, NULL, kmr_noopt, delete_file);
	if (ret != MPI_SUCCESS) {
	    kmrsh_abort(rank, "kmr_map failed.\n");
	}
    } else {
	kmr_free_kvs(kvs_map);
    }

    kmr_free_context(mr);
    kmr_fin();
    MPI_Finalize();
    return 0;
}

/*
NOTICE-NOTICE-NOTICE
*/
