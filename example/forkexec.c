/* forkexec.c (2020-11-07) -*-Coding: us-ascii-unix;-*- */

/* NOT KMR.  It is a part of platform testing.  It is a library just
   to fork-exec a sequence of shell commands (echo;sleep;echo).  It is
   for checking fork-execing works. */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <errno.h>
#include <wait.h>
#include <assert.h>
#if 0
#include <execinfo.h>
#endif

#ifdef _OPENMP
#include <omp.h>
#endif

#define USEPTHREAD

#ifdef USEPTHREAD
#include <pthread.h>
#endif

#if 0
void print_backtrace(void);

void
print_backtrace(void)
{
    printf("BACKTRACE:\n");
    void *array[100];
    int n = backtrace(array, 100);
    char **strings = backtrace_symbols(array, n);
    if (strings != 0) {
	printf("stack frames n=%d.\n", n);
	for (int i = 0; i < n; i++)
	    printf("%s\n", strings[i]);
    }
    free(strings);
    fflush(0);
}
#endif

#ifdef USEPTHREAD
void *nothing(void *a);

void *nothing(void *a)
{
    fprintf(stdout, "do nothing in threads.\n");
    fflush(0);
    return 0;
}
#endif

/* BUFLOC=0: data, 1: stack, 2: heap */

const int BUFLOC = 2;

char argbuf_d[4096];
char *args_d[256];

char *execstrings[] = {
    "sh", "-c",
    "echo start a subprocess.; sleep 3; echo a process done.",
    0
};

int forkexec(void);

int
forkexec()
{
    char argbuf_s[4096];
    char *args_s[256];
    char *argbuf_h = 0;
    char **args_h = 0;

    char *argbuf;
    char **args;
    if (BUFLOC == 0) {
	argbuf = argbuf_d;
	args = args_d;
    } else if (BUFLOC == 1) {
	argbuf = argbuf_s;
	args = args_s;
    } else if (BUFLOC == 2) {
	argbuf_h = malloc(4096);
	args_h = malloc(sizeof(char *) * (size_t)256);
	assert(argbuf_h != 0);
	assert(args_h != 0);
	argbuf = argbuf_h;
	args = args_h;
    } else if (BUFLOC == 3) {
	argbuf = 0;
	args = execstrings;
    }
    assert(args != 0);

    if (args != execstrings) {
	char *q;
	q = argbuf;
	for (int i = 0; execstrings[i] != 0; i++) {
	    args[i] = q;
	    args[i + 1] = 0;
	    strcpy(q, execstrings[i]);
	    q += (strlen(execstrings[i]) + 1);
	}
    }

    int nprocs, rank;
    /*MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &thlv);*/
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

#ifdef _OPENMP
    _Pragma("omp parallel")
	{
	    int tid = omp_get_thread_num();
	    assert(tid >= 0);
	}
#endif

#ifdef USEPTHREAD
    {
	pthread_t thr;
	int cc = pthread_create(&thr, 0, nothing, 0);
	if (cc != 0) {
	    fprintf(stderr, "pthread_create failed: %s\n", strerror(cc));
	    fflush(0);
	    exit(1);
	}
    }
#endif

    MPI_Comm newworld;
    MPI_Comm_dup(MPI_COMM_WORLD, &newworld);

    MPI_Barrier(newworld);
    usleep(50 * 1000);

    int ACK = 88;
    int REQ = 99;
    int req[5];
    if (rank == 0) {
	for (int peer = 1; peer < nprocs; peer++) {
	    MPI_Status st;
	    MPI_Recv(req, 5, MPI_INT, peer, ACK, newworld, &st);
	    MPI_Send(req, 5, MPI_INT, peer, REQ, newworld);
	}
    } else {
	int peer = 0;
	MPI_Status st;
	req[0] = 10; req[1] = 11; req[2] = 12; req[3] = 13; req[4] = 14;
	MPI_Send(req, 5, MPI_INT, peer, ACK, newworld);
	MPI_Recv(req, 5, MPI_INT, peer, REQ, newworld, &st);
    }
    printf("fork-execing a process...\n");
    fflush(0);

    /*print_backtrace();*/

    int pid = fork();
    if (pid == -1) {
	perror("fork");
	fflush(0);
    } else {
	if (pid == 0) {
	    for (int fd = 3; fd < 1024; fd++) {
		close(fd);
	    }

	    int cc = execvp(args[0], args);
	    if (cc == -1) {
		perror("execvp");
		fflush(0);
	    } else {
		fprintf(stderr, "execvp() returned with cc=%d\n", cc);
		fflush(0);
	    }
	} else {
	    int waitstatus = 0;
	    int cc = 0;
	    for (;;) {
		cc = waitpid(pid, &waitstatus, 0);
		if (cc == -1) {
		    if (errno == EINTR) {
			fprintf(stderr, "waitpid() interrupted\n");
			fflush(0);
			continue;
		    } else {
			perror("waitpid");
			fflush(0);
			break;
		    }
		} else {
		    break;
		}
	    }
	    if (cc == -1) {
		;
	    } else if (WIFEXITED(waitstatus) != 0) {
		int n = WEXITSTATUS(waitstatus);
		fprintf(stderr, "waitpid() status exit=%d\n", n);
		fflush(0);
	    } else if (WIFSIGNALED(waitstatus)) {
		int n = WTERMSIG(waitstatus);
		fprintf(stderr, "waitpid() status signaled=%d\n", n);
		fflush(0);
	    } else if (WIFSTOPPED(waitstatus)) {
		/* (never happens). */
		int n = WSTOPSIG(waitstatus);
		fprintf(stderr, "waitpid() status stopped=%d\n", n);
		fflush(0);
	    } else {
		/* (never happens). */
		fprintf(stderr, "waitpid() bad status (?)\n");
		fflush(0);
	    }
	}
    }

    MPI_Barrier(newworld);
    usleep(50 * 1000);
    if (rank == 0) {printf("OK\n");}
    fflush(0);

    MPI_Comm_free(&newworld);

    /*MPI_Finalize();*/

    if (argbuf_h != 0) {
	free(argbuf_h);
    }
    if (args_h != 0) {
	free(args_h);
    }
    return 0;
}
