/** \file mpi_pi.mapper.c
    \brief Example for KMRRUN.  It is a mapper for PI calculation
    implemented using MPI.

    How to run.
    1. create input files in a directory.
         work/
            000
            001
            002
            ...

       Each file have one line which represents number of points to plot.
         $ cat work/000
         100000

    2. run by kmrrun
         $ mpirun -np 2 ./kmrrun --mpi-proc 4 --m-mpi "./mpi_pi.mapper" \
         -k "./mpi_pi.kvgen.sh" --r-mpi "./mpi_pi.reducer" ./work
*/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <mpi.h>

#define LINELEN 80

/** \brief Main function.
    Read the number of points to plot from the specified input file,
    plot points randomly, and count the number of points plotted in
    a circle.
    The output is written to a file that has a line formatted in "num1/num2"
    where num1 is number of points plotted in a circle and "num2"
    is the total points. */
int
main(int argc, char *argv[])
{
    int rank, size;
    char line[LINELEN];
    FILE *ifp, *ofp;
    int points, mine, i, count, total_count;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    srand((unsigned int)rank);

    if (argc != 2) {
        if (rank == 0) {
            fprintf(stderr, "specify an input file\n");
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    if (rank == 0) {
        ifp = fopen(argv[1], "r");
        if (fgets(line, sizeof(line), ifp) == NULL) {
            fprintf(stderr, "failed to read a file\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        fclose(ifp);
        points = atoi(line);
    }

    MPI_Bcast(&points, 1, MPI_INT, 0, MPI_COMM_WORLD);
    mine = points / size;
    if (rank == size - 1) {
        mine += points % size;
    }

    count = 0;
    for (i = 0; i < mine; i++) {
        float x = (float)rand() / ((float)RAND_MAX + 1.0F);
        float y = (float)rand() / ((float)RAND_MAX + 1.0F);
        if ( x * x + y * y < 1.0) {
            count += 1;
        }
    }
    MPI_Reduce(&count, &total_count, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        char *ofilename = malloc(strlen(argv[1]) + 5);
        strncpy(ofilename, argv[1], strlen(argv[1]) + 1);
        strncat(ofilename, ".out", 4);

        ofp = fopen(ofilename, "w");
        fprintf(ofp, "%d/%d\n", total_count, points);
        fclose(ofp);
    }

    MPI_Finalize();
    return 0;
}
