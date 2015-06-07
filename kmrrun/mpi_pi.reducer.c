/** \file mpi_pi.reducer.c
    \brief Example for KMRRUN.  It is a reducer for PI calculation
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
    Read a file which has key-values separated by lines.
    One line is like this.

       0 7932/10000

    '0' is key and '7932/10000' is value.
    7932 is number of points plotted in a circle and 10000 is
    total number of points plotted.
    By reading these numbers, it calculates pi and writes result
    to a file. */
int
main(int argc, char *argv[])
{
    char line[LINELEN];
    int rank;
    FILE *ifp, *ofp;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (argc != 2) {
        if (rank == 0) {
            fprintf(stderr, "specify an input file\n");
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    if (rank == 0) {
        int sum_count = 0;
        int sum_point = 0;
        ifp = fopen(argv[1], "r");
        while (fgets(line, sizeof(line), ifp) != NULL) {
            char *count_s, *point_s;
            char *cp = line;
            int len = (int)strlen(line);

            // chomp
            if (cp[len-1] == '\n') {
                cp[len-1] = '\0';
            }

            // find In count position
            cp = strchr(line, ' ');
            count_s = cp + 1;

            // find Total point position
            cp = strchr(line, '/');
            point_s = cp + 1;
            cp[0] = '\0';

            sum_count += atoi(count_s);
            sum_point += atoi(point_s);
        }
        fclose(ifp);

        double pi = 4.0 * sum_count / sum_point;

        ofp = fopen("mpi_pi.out", "w");
        fprintf(ofp, "%f\n", pi);
        fclose(ofp);
    }

    MPI_Finalize();
    return 0;
}
