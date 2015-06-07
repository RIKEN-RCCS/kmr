/* testdp.c (2014-02-04) */

/* Test for kmrdp.cpp. */

/* RUN THIS WITH "mpirun -np 4 a.out -tb testdp.table -ot 3". */

#include <mpi.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>

char xargs[4 * 1024];

extern int application(int argc, char *argv[]);
int init(int argc, char *argv[]);
int (*application_init)(int argc, char *argv[]) = &init;
int fin(int argc, char *argv[]);
int (*application_fin)(int argc, char *argv[]) = &fin;

int
init(int argc, char *argv[])
{
    printf("testdp:init() called.\n");
    fflush(0);
    return 0;
}

int
fin(int argc, char *argv[])
{
    printf("testdp:fin() called.\n");
    fflush(0);
    return 0;
}

int
application(int argc, char *argv[])
{
    int nprocs, rank;
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int n = (int)sizeof(xargs);
    int p = 0;
    for (int i = 0; i < argc; i++) {
	int cc = snprintf(&xargs[p], (n - p), " %s", argv[i]);
	p += cc;
	if ((n - p) < 1) {
	    break;
	}
    }
    time_t ct = time(0);
    char *cs = ctime(&ct);
    char clock[20];
    if (cs != 0) {
	snprintf(clock, sizeof(clock), "%s", &cs[11]);
	clock[8] = '\0';
    } else {
	snprintf(clock, sizeof(clock), "(time unknown)");
    }
    printf("Starting application on rank %04d (creates a file %s) %s...\n"
	   "Args:%s\n", rank, argv[7], clock, xargs);

    sleep(3);

    char buf[256];
    snprintf(buf, 256, "touch %s", argv[7]);
    system(buf);
    return 0;
}
