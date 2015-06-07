/* kmrshell.c */
/* Copyright (C) 2012-2015 RIKEN AICS */

/** \file kmrshell.c KMR-Shell for Streaming.  It forks processes of a
    mapper, a shuffler, and a reducer, then, it reads a number of
    specified input files and passes their data to a mapper via a
    pipe.  A mapper, a shuffler, and a reducer are shell
    executables.  */

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/param.h>
#include <dirent.h>
#include <assert.h>

/** Maximum length of a line of data. */
#define LINELEN 32767

/** Maximum number of arguments to mapper and reducer programs. */
#define ARGSIZ 8

/** Buffer string size of arguments to mapper and reducer programs. */
#define ARGSTRLEN (8 * 1024)

/** Parameter for pipeops(). */
enum {
    PIPE_IN = 1,	/**< Attach stdin to pipe. */
    PIPE_OUT = 2,	/**< Attach stdout to pipe. */
};

static char *shuffler = "kmrshuffler";

static int pipeops(int *, int);
static int execute(char *, char *const [], pid_t *);
static void reapchild(int);
static void putfilecontents(char *);
static int filereader(char *);
static void parse_args(char *, char *[]);

/** Handles SIGCHLD signal. */

static void
reapchild(int exitstat)
{
    int status;
    //fprintf(stderr, "reapchild\n");
    waitpid(-1, &status, 0);
}

/** Sets up pipe states.  It attaches pipes to stdin or stdout.  */

static int
pipeops(int *pipefd, int direc)
{
    int ret;
    if (direc == PIPE_IN) {
        if ((ret = close(pipefd[1])) < 0) {
	    perror("close pipe");
	    return ret;
	}
	if ((ret = dup2(pipefd[0], STDIN_FILENO)) < 0) {
	    perror("dup2 pipe");
	    return ret;
	}
	if ((ret = close(pipefd[0])) < 0) {
	    perror("close pipe");
	    return ret;
	}
    }

    if (direc == PIPE_OUT) {
        if ((ret = close(pipefd[0])) < 0) {
	    perror("close pipe");
	    return ret;
	}
	if ((ret = dup2(pipefd[1], STDOUT_FILENO)) < 0) {
	    perror("dup2 pipe");
	    return ret;
	}
	if ((ret = close(pipefd[1])) < 0) {
	    perror("close pipe");
	    return ret;
	}
    }
    return 0;
}

/** Forks and execs a process.
    \param prog program path.
    \param args argments to program.
    \param cldpid pid of child (out). */

static int
execute(char *prog, char *const args[], pid_t *cldpid)
{
    pid_t pid;
    int ret, pipefd[2];
    struct sigaction schld, spipe;

    memset(&schld, 0, sizeof(struct sigaction));
    schld.sa_handler = reapchild;
    schld.sa_flags |= SA_RESTART;
    ret = sigaction(SIGCHLD, &schld, NULL);
    assert(ret >= 0);

    memset(&spipe, 0, sizeof(struct sigaction));
    spipe.sa_handler = SIG_IGN;
    ret = sigaction(SIGPIPE, &spipe, NULL);
    assert(ret >= 0);

    if ((ret = pipe(pipefd)) < 0) {
        perror("pipe");
	return ret;
    }

    pid = fork();

    if (pid < 0) {
        perror("fork");
	return -1;
    }

    if (pid > 0) {
        /* parent */
        if ((ret = pipeops(pipefd, PIPE_OUT)) < 0) {
	    return -1;
	}
	if (cldpid != NULL) {
	    *cldpid = pid;
	}
    }

    if (pid == 0) {
        /* child */
        if ((ret = pipeops(pipefd, PIPE_IN)) < 0) {
	    return ret;
	}
	if ((ret = execv(prog, args)) < 0) {
	    return ret;
	}

    }
    return 0;
}

/* Puts contents of a file to stdout.  It reads a line from a file and
   put it.
   \param path to file. */

static void
putfilecontents(char *path)
{
    FILE *fin;
    char line[LINELEN];

    fin = fopen(path, "r");
    if (fin == NULL) {
	perror("open");
	return;
    }

    while ((fgets(line, sizeof(line), fin)) != NULL) {
	if (fputs(line, stdout) == EOF) {
	    break;
	}
    }

    fclose(fin);
    return;
}

/** Reads files (file-reader).  It reads possibly multiple files and
    writes their contents to stdout.  If "path" is a directory, it
    enumerates the files under the directory and reads each file.
    \param path path to file (or directory). */

static int
filereader(char *path)
{
    struct stat status;

    /* file check */
    if (stat(path, &status) < 0) {
        fprintf(stderr, "file %s error\n", path);
        return -1;
    }

    if (!S_ISDIR(status.st_mode) && !S_ISREG(status.st_mode)) {
        fprintf(stderr, "file %s is not regular or directory\n", path);
        return -1;
    }

    if (S_ISDIR(status.st_mode)) {
	/* directory */
	/*char b[MAXGETDENTS_SIZE];*/
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

	d = opendir(path);
	if (d == NULL) {
	    perror("opendir");
	    return -1;
	}
	while (readdir_r(d, (void *)b, &dentp) >= 0) {
	    struct stat substat;
	    char fullpath[MAXPATHLEN];
	    int ret;
	    if (dentp == NULL) {
		break;
	    }

	    ret = snprintf(fullpath, sizeof(fullpath), "%s/%s", path, dentp->d_name);
	    if (ret <= 0) {
		perror("snprintf");
		continue;
	    }

	    if (stat(fullpath, &substat) < 0) {
		continue;
	    }

	    if (S_ISREG(substat.st_mode)) {
		putfilecontents(fullpath);
	    }
	}
	closedir(d);
    } else {
	putfilecontents(path);
    }
    return 0;
}

/** Parses command parameters given for mapper and reducer arguments.
    It scans an argument string like "mapper arg0 arg1" for the -m and
    -r options, and generates an argv array {"mapper", "arg0", "arg1",
    0}.  The separator is a whitespace.
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
	    **ap = '\0';
	    break;
	} else {
	    if (np == NULL) {
		**ap = '\0';
		break;
	    }
	}
	while (*np == ' ') {
	    np++;
	}
	cp = np;
    }
}

/** Starts map-reduce shell processes (for "streaming").  It
    forks and execs a mapper, a shuffler, and a reducer, which are
    connected via pipes.
    \arg \c -m \c mapper program.
    \arg \c -r \c reducer program.
    \arg \c input file or directory. */

int
main(int argc, char *argv[])
{
    int ret, opt;
    char *mapper = NULL, *reducer = NULL, *infile = NULL;
    char *margv[ARGSIZ], *rargv[ARGSIZ], *sargv[2];
    pid_t mapper_pid, shuffler_pid, reducer_pid;
    char margbuf[ARGSTRLEN];
    char rargbuf[ARGSTRLEN];

    while ((opt = getopt(argc, argv, "m:r:")) != -1) {
	switch (opt) {
	    size_t asz;
	case 'm':
	    asz = (strlen(optarg) + 1);
	    if (asz >= ARGSTRLEN) {
		fprintf(stderr, "Argument too long for mapper (%s)\n",
			optarg);
		exit(-1);
	    }
	    memcpy(margbuf, optarg, asz);
	    parse_args(margbuf, &margv[0]);
	    mapper = margv[0];
	    break;
	case 'r':
	    asz = (strlen(optarg) + 1);
	    if (asz >= ARGSTRLEN) {
		fprintf(stderr, "Argument too long for reducer (%s)\n",
			optarg);
		exit(-1);
	    }
	    memcpy(rargbuf, optarg, asz);
	    parse_args(rargbuf, &rargv[0]);
	    reducer = rargv[0];
	    break;
	default:
	    fprintf(stderr, "Usage %s -m mapper [-r reducer] inputfile\n", argv[0]);
	    exit(-1);
	}
    }

    if ((argc - optind) != 1) {
	fprintf(stderr, "Usage %s -m mapper [-r reducer] inputfile\n", argv[0]);
	exit(EXIT_FAILURE);
    } else {
	infile = argv[optind];
	optind++;
    }

    if (mapper == NULL) {
	fprintf(stderr, "Usage %s -m mapper [-r reducer] inputfile\n", argv[0]);
	exit(EXIT_FAILURE);
    }

    if (reducer != NULL) {
        /* Start reducer process. */
        ret = execute(reducer, rargv, &reducer_pid);
	if (ret < 0) {
	    fprintf(stderr, "execute %s failed\n", reducer);
	    exit(EXIT_FAILURE);
	}

	/* Start shuffler process. */
	sargv[0] = shuffler;
	sargv[1] = NULL;
	ret = execute(shuffler, sargv, &shuffler_pid);
	if (ret < 0) {
	    fprintf(stderr, "execute %s failed\n", shuffler);
	    exit(EXIT_FAILURE);
	}
    }

    /* Start mapper process. */
    ret = execute(mapper, margv, &mapper_pid);
    if (ret < 0) {
	fprintf(stderr, "execute %s failed\n", mapper);
	exit(EXIT_FAILURE);
    }

    /* Call filereader(). */
    ret = filereader(infile);
    if (ret < 0) {
	fprintf(stderr, "filereader failed\n");
    }
    fclose(stdout);

    if (reducer != NULL) {
        /* Wait for termination of shuffler. */
        waitpid(reducer_pid, NULL, 0);
    } else {
        /* Wait for termination of mapper. */
        waitpid(mapper_pid, NULL, 0);
    }

    exit(EXIT_SUCCESS);
}

/*
Copyright (C) 2012-2015 RIKEN AICS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/
