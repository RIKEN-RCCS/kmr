/* check-exec-in-mpi-process.c (2020-11-07) -*-Coding: us-ascii-unix;-*- */

/* NOT KMR.  It checks fork-execing works from mpi proceeses. */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <errno.h>
#include <wait.h>
#include <assert.h>
#ifdef _OPENMP
#include <omp.h>
#endif

#define USEPTHREAD

#ifdef USEPTHREAD
#include <pthread.h>
#endif

#ifdef USEPTHREAD
void *nothing(void *a)
{
    fprintf(stdout, "do nothing.\n");
    fflush(0);
    return 0;
}
#endif

/* BUFLOC=0: data, 1: stack, 2: heap */

#define BUFLOC 2

#if (BUFLOC == 0)
char argbuf[4096];
char *args[256];
#endif

int
main(int argc, char *argv[])
{
    char *execstrings[] = {
	"sh", "-c",
	"echo start a subprocess.; sleep 3; echo a process done.",
	0
    };

#if (BUFLOC == 0)
    /*empty*/
#elif (BUFLOC == 1)
    char argbuf[4096];
    char *args[256];
#elif (BUFLOC == 2)
    char *argbuf = malloc(4096);
    char **args = malloc(sizeof(char *) * (size_t)256);
    assert(argbuf != 0);
    assert(args != 0);
#elif (BUFLOC == 3)
    char **args = execstrings;
#endif

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

    int nprocs, rank, thlv;
    /*MPI_Init(&argc, &argv);*/
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &thlv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

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

#ifdef _OPENMP
_Pragma("omp parallel")
    {
	int tid = omp_get_thread_num();
	assert(tid >= 0);
    }
#endif

    if (rank == 0) {
	printf("fork-execing a process...\n");
    }
    fflush(0);

    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);

    int pid = fork();
    if (pid == -1) {
	perror("fork");
	fflush(0);
    } else {
	if (pid == 0) {
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

    MPI_Barrier(MPI_COMM_WORLD);
    usleep(50 * 1000);
    if (rank == 0) {printf("OK\n");}
    fflush(0);

    MPI_Finalize();
    return 0;
}
