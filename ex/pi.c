/* pi.c (Classic MPI Example; Not KMR) */

#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <math.h>

const double PI = 3.141592653589793238462643;

int
main(int argc, char *argv[])
{
    int nprocs, rank;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    printf("Running PI on rank=%d/%d\n", rank, nprocs);
    fflush(0);

    int n;
    if (rank == 0) {
	n = 1000;
    }
    MPI_Bcast(&n, 1, MPI_INT, 0, MPI_COMM_WORLD);

    int c;
    c = 0;
    for (int i = 0; i < n; i++) {
	double x = drand48();
	double y = drand48();
	if (x*x + y*y < 1e0) {
	    c++;
	}
    }
    double pi0 = (4e0 * ((double)c / (double)n));
    double tot;
    MPI_Reduce(&pi0, &tot, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    if (rank == 0) {
	double pi = ((double)tot / (double)nprocs);
	printf("PI is pi=%e (error=%e)\n", pi, fabs(pi - PI));
	fflush(0);
	sleep(1);
    } else {
	sleep(1);
    }

    MPI_Finalize();

    exit(0);
    return 0;
}
