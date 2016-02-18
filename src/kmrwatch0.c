/* kmrwatch0.c (2014-02-04) */
/* Copyright (C) 2012-2016 RIKEN AICS */

/** \file kmrwatch0.c KMR Spawned Program Wrapper.  It wraps a command
    of a spawned process which runs independently and makes no
    communication to the parent (the spawner), so that it lets the
    parent notified about the end of the process.  It has two modes
    corresponding to kmr_map_processes() and
    kmr_map_serial_processes().  (MODE 0): It fork-execs an
    independent MPI program for kmr_map_processes(), and makes a
    socket to the parent to notify the end of the process via socket
    closure.  It uses a socket because MPI communication to the parent
    is unusable.  USAGE: kmrwatch0 "mpi" ipaddr magic "--" command...
    (MODE 1): It fork-execs a non-MPI program for
    kmr_map_serial_processes(), by surrounding it with calls to
    MPI_Init() and MPI_Finalize(), and sends a reply to the parent.
    See manual pages for more information. USAGE: kmrwatch0 "seq" any
    magic "--" command...\n To enable tracing, set 'T' as the last
    character in the magic argument. */

/* For "kmrwatch mpi" the following script suffices.  NOTE: A file
   descriptor number 20 is used in it because the use of 9 failed with
   "MPICH2" (it only matters when the wrapper would use a fixed
   number).
   "#!/bin/bash"
   "# KMRWATCH, notifies process end via socket closure."
   "# Usage: kmrwatch.sh mpi hostport magic -- command..."
   "if [ \"$#\" -lt 5 ]; then"
   "\techo \"KMRWATCH.SH: bad number of arguments.\"; exit 1"
   "fi"
   "seqmpi=$1; hostport=$2; magic=$3; separator=$4"
   "shift; shift; shift; shift"
   "if [ \"$separator\" != \"--\" ]; then"
   "\techo \"KMRWATCH.SH: bad argument separator.\"; exit 1"
   "fi"
   "exec 20<> \"/dev/tcp/$hostport\""
   "echo -e \"$magic\" >&20"
   "$*"  */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>

#include "kmr.h"

union SOCKADDR {
    struct sockaddr sa;
    struct sockaddr_in in4;
    struct sockaddr_in6 in6;
    struct sockaddr_storage ss;
};

int
main(int argc, char **argv)
{
    int cc;

    if (argc < 5) {
	fprintf(stderr, (";;KMR kmrwatch0 error:"
			 " too few arguments: argc=%d\n"), argc);
	exit(255);
    }

    char *mode = argv[1];
    char *hostport = argv[2];
    char *magic = argv[3];
    char *separator = argv[4];

    int version = -1;
    _Bool tracing = 0;
    _Bool quitwithoutfinalize = 0;
    int lm = (int)strlen(magic);
    for (int i = 0; i < lm; i++) {
	if (magic[i] == 'V' && i < (lm - 1)) {
	    version = magic[i + 1] - '0';
	}
	if (magic[i] == 'T') {
	    tracing = 1;
	}
	if (magic[i] == 'X') {
	    quitwithoutfinalize = 1;
	}
    }

    if (version != 0) {
	fprintf(stderr, (";;KMR kmrwatch0 error:"
			 " version mismatch: %s\n"), magic);
	exit(255);
    }

    if (!(strcmp(mode, "seq") == 0 || (strcmp(mode, "mpi") == 0))) {
	fprintf(stderr, (";;KMR kmrwatch0 error:"
			 " bad mode (be seq or mpi): %s\n"), mode);
	exit(255);
    }
    if (strcmp(separator, "--") != 0) {
	fprintf(stderr, (";;KMR kmrwatch0 error:"
			 " bad separator, -- needed: %s\n"), separator);
	exit(255);
    }

    if (tracing) {
	int pid = getpid();
	fprintf(stderr, ";;KMR kmrwatch0: pid=%d\n", pid);
	fflush(0);
    }

    /* Non-MPI means the wrapper has to behave as an MPI process. */

    _Bool nonmpi = (mode[0] == 's');

    int fd = -1;

    int nprocs = -1;
    int rank = -1;
    MPI_Comm parent = MPI_COMM_NULL;

    if (nonmpi) {
	if (tracing) {
	    fprintf(stderr, (";;KMR kmrwatch0: initializing MPI\n"));
	    fflush(0);
	}

#if 0
	cc = MPI_Init(&argc, &argv);
	assert(cc == MPI_SUCCESS);
#else
	int lev;
	cc = MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &lev);
	assert(cc == MPI_SUCCESS);
#endif
	cc = MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
	assert(cc == MPI_SUCCESS);
	cc = MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	assert(cc == MPI_SUCCESS);
	cc = MPI_Comm_get_parent(&parent);
	assert(cc == MPI_SUCCESS);
	if (parent == MPI_COMM_NULL) {
	    fprintf(stderr, ("KMR kmrwatch0 error:"
			     " MPI_Comm_get_parent() failed\n"));
	    exit(255);
	}
    }

#if 0
    if (tracing) {
	fprintf(stderr, (";;KMR kmrwatch0:"
			 " starting a process\n"));
	fflush(0);
    }
#endif

    int pid = fork();
    if (pid == -1) {
	char *m = strerror(errno);
	fprintf(stderr, ("KMR kmrwatch0 error:"
			 " fork() failed: %s\n"), m);
    } else {
	if (pid == 0) {
	    if (fd != -1) {
		close(fd);
		fd = -1;
	    }

	    argv[argc] = 0;
	    cc = execvp(argv[5], &argv[5]);
	    if (cc == -1) {
		char *m = strerror(errno);
		fprintf(stderr, ("KMR kmrwatch0 error:"
				 " execvp(%s) failed: %s\n"),
			argv[5], m);
	    } else {
		fprintf(stderr, ("KMR kmrwatch0 error:"
				 " execvp() returned: cc=%d\n"),
			cc);
	    }
	    exit(255);
	}
    }

    if (pid != -1 && !nonmpi) {
	/* Connect to the spawner */
	assert(pid != 0);
	char host[NI_MAXHOST + 6];
	int preferip = 0;

	if (tracing) {
	    fprintf(stderr, (";;KMR kmrwatch0:"
			     " connecting a socket to the spawner\n"));
	    fflush(0);
	}

	int len = ((int)strlen(hostport) + 1);
	if (len > (int)sizeof(host)) {
	    fprintf(stderr, ("KMR kmrwatch0 error:"
			     " bad host/port pair (too long): %s\n"),
		    hostport);
	    exit(255);
	}
	memcpy(host, hostport, (size_t)len);
	char *port = 0;
	int portno = 0;
	{
	    char gomi[4];
	    char *s = rindex(host, '/');
	    if (s == 0) {
		fprintf(stderr,
			("KMR kmrwatch0 error:"
			 " bad host/port pair (no slash): %s\n"), hostport);
		exit(255);
	    }
	    *s = 0;
	    port = (s + 1);
	    cc = sscanf(port, "%d%c", &portno, (char *)&gomi);
	    if (cc != 1) {
		fprintf(stderr,
			("KMR kmrwatch0 error:"
			 " bad host/port pair (bad port): %s\n"), hostport);
	    }
	}

	struct addrinfo hints;
	memset(&hints, 0, sizeof(hints));
	hints.ai_flags = (AI_ADDRCONFIG);
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = IPPROTO_TCP;
	if (preferip == 4) {
	    hints.ai_family = AF_INET;
	} else if (preferip == 6) {
	    hints.ai_family = AF_INET6;
	} else {
	    hints.ai_family = AF_UNSPEC;
	    /*AI_V4MAPPED*/
	}
	struct addrinfo *ai;
	cc = getaddrinfo(host, port, &hints, &ai);
	if (cc != 0) {
	    char const *m = gai_strerror(cc);
	    fprintf(stderr, ("KMR kmrwatch0 error:"
			     "getaddrinfo(%s,%s) failed: %s.\n"),
		    host, port, m);
	}

	struct {char *s; struct addrinfo *a; int e;} errs[10];
	int addresstries = 0;
	for (struct addrinfo *p = ai; p != 0; p = p->ai_next) {
	    union SOCKADDR *sa = (void *)p->ai_addr;
	    char *fm = 0;
	    if (p->ai_family == AF_INET) {
		assert(ntohs(sa->in4.sin_port) == portno);
		fm = "AF_INET";
	    } else if (p->ai_family == AF_INET6) {
		assert(ntohs(sa->in6.sin6_port) == portno);
		fm = "AF_INET6";
	    } else {
		continue;
	    }
	    addresstries++;
	    if (addresstries >= (int)(sizeof(errs) / sizeof(*errs))) {
		break;
	    }
	    errs[addresstries - 1].s = 0;

	    fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
	    if (fd < 0) {
		errs[addresstries - 1].s = "socket";
		errs[addresstries - 1].a = p;
		errs[addresstries - 1].e = errno;
		char *m = strerror(errno);
		if (tracing) {
		    fprintf(stderr, ("KMR kmrwatch0 error:"
				     "socket(%s) failed: %s\n"),
			    fm, m);
		    fflush(0);
		}
		fd = -1;
		continue;
	    }
	    int one = 1;
	    cc = setsockopt(fd, SOL_SOCKET, SO_REUSEADDR,
			    &one, sizeof(one));
	    if (cc != 0) {
		char *m = strerror(errno);
		fprintf(stderr, ("KMR kmrwatch0 error:"
				 "setsockopt(SO_REUSEADDR) failed"
				 " (ignored): %s\n"), m);
	    }
	    cc = connect(fd, p->ai_addr, p->ai_addrlen);
	    if (cc < 0) {
		errs[addresstries - 1].s = "connect";
		errs[addresstries - 1].a = p;
		errs[addresstries - 1].e = errno;
		/*(errno == ETIMEDOUT || errno == ECONNREFUSED)*/
		if (tracing) {
		    char *m = strerror(errno);
		    fprintf(stderr,
			    (";;KMR kmrwatch0:"
			     " connect(%s): %s\n"), hostport, m);
		    fflush(0);
		}
		close(fd);
		fd = -1;
		continue;
	    }
	    break;
	}
	if (fd == -1) {
	    if (addresstries == 0) {
		fprintf(stderr, ("KMR kmrwatch0 error:"
				 "No address found for %s %s\n"),
			host, port);
	    } else {
		for (int i = 0; i < addresstries; i++) {
		    assert(errs[i].s != 0);
		    struct addrinfo *p = errs[i].a;
		    union SOCKADDR *sa = (void *)p->ai_addr;
		    void *addr;
		    char *fm = 0;
		    if (p->ai_family == AF_INET) {
			addr = &(sa->in4.sin_addr);
			fm = "AF_INET";
		    } else if (p->ai_family == AF_INET6) {
			addr = &sa->in6.sin6_addr;
			fm = "AF_INET6";
		    } else {
			assert(0);
		    }
		    char peer[INET6_ADDRSTRLEN];
		    inet_ntop(p->ai_family, addr, peer, sizeof(peer));
		    char *m = strerror(errs[i].e);
		    if (strcmp(errs[i].s, "socket") == 0) {
			fprintf(stderr, ("KMR kmrwatch0 error:"
					 " socket(%s) failed: %s\n"),
				fm, m);
		    } else if (strcmp(errs[i].s, "connect") == 0) {
			fprintf(stderr,
				("KMR kmrwatch0 error:"
				 " connect(%s/%s) failed: %s\n"),
				peer, port, m);
		    } else {
			assert(0);
		    }
		}
	    }
	    exit(255);
	}
	freeaddrinfo(ai);

	/* Handshake between server and client */
	int val;
	ssize_t rsize = read(fd, &val, sizeof(int));
	if (rsize < 0) {
	    char *m = strerror(errno);
	    fprintf(stderr, ("KMR kmrwatch0 error:"
			     "read failed: %s\n"), m);
	    close(fd);
	    exit(255);
	}
	assert(rsize == sizeof(int));
	ssize_t wsize = write(fd, &val, sizeof(int));
	if (wsize < 0) {
	    char *m = strerror(errno);
	    fprintf(stderr, ("KMR kmrwatch0 error:"
			     "write failed: %s\n"), m);
	    close(fd);
	    exit(255);
	}
	assert(wsize == sizeof(int));
	if (val == 0) {
	    close(fd);
	    fd = -1;
	}
    }

    if (tracing) {
	fprintf(stderr, (";;KMR kmrwatch0:"
			 " waiting for a process"
			 " to finish (pid=%d)\n"),
		(int)pid);
	fflush(0);
    }
#if 0
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = (SA_RESETHAND);
    sa.sa_handler = 0;
    cc = sigaction(SIGALRM, &sa, 0);
    if (cc == -1) {
	char *m = strerror(errno);
	fprintf(stderr, ("KMR kmrwatch0 error:"
			 " sigaction(%d): %s\n", SIGALRM, m));
    }
#endif
    /*alarm(30);*/
    int st;
    cc = waitpid(pid, &st, 0);
    if (cc == -1) {
	if (errno == EINTR) {
	    fprintf(stderr, ("KMR kmrwatch0 error:"
			     " waitpid() interrupted\n"));
	} else {
	    char *m = strerror(errno);
	    fprintf(stderr, ("KMR kmrwatch0 error:"
			     " waitpid() failed: %s\n"), m);
	}
    }
    /*alarm(0);*/

    if (0) {
	fprintf(stderr, (";;KMR kmrwatch0: detected a process done\n"));
	fflush(0);
    }

    if (nonmpi) {
	if (tracing) {
	    fprintf(stderr, (";;KMR kmrwatch0:"
			     " sending a reply for done-notification\n"));
	    fflush(0);
	}

	int peer = 0;
	cc = MPI_Send(0, 0, MPI_BYTE, peer,
		      KMR_TAG_SPAWN_REPLY, parent);
	assert(cc == MPI_SUCCESS);

	if (quitwithoutfinalize) {
	    if (tracing) {
		fprintf(stderr, (";;KMR kmrwatch0:"
				 " force quit without finalizing MPI\n"));
		fflush(0);
	    }
	    _exit(0);
	}

	if (tracing) {
	    fprintf(stderr, (";;KMR kmrwatch0: finalizing MPI\n"));
	    fflush(0);
	}

	cc = MPI_Finalize();
	assert(cc == MPI_SUCCESS);
    } else {
	if (tracing) {
	    fprintf(stderr, (";;KMR kmrwatch0:"
			     " closing a socket for done-notification\n"));
	    fflush(0);
	}

	if (fd != -1) {
	    close(fd);
	    fd = -1;
	}
    }

    if (tracing) {
	fprintf(stderr, (";;KMR kmrwatch0: done\n"));
	fflush(0);
    }

    return 0;
}

/*
Copyright (C) 2012-2016 RIKEN AICS
This library is distributed WITHOUT ANY WARRANTY.  This library can be
redistributed and/or modified under the terms of the BSD 2-Clause License.
*/
