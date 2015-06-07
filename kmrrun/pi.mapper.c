/** \file pi.mapper.c
    \brief Example for KMRRUN.  It is a mapper for PI calculation.

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
         $ mpirun -np 2 ./kmrrun -m "./pi.mapper" \
         -k "./pi.kvgen.sh" -r "./pi.reducer" ./work
*/


#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

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
    char line[LINELEN];
    FILE *ifp, *ofp;
    int points, i, count;

    srand((unsigned int)getpid());

    if (argc != 2) {
        fprintf(stderr, "specify an input file\n");
        return 1;
    }

    ifp = fopen(argv[1], "r");
    if (fgets(line, sizeof(line), ifp) == NULL) {
        fprintf(stderr, "failed to read the input file\n");
        return 1;
    }
    fclose(ifp);
    points = atoi(line);

    count = 0;
    for (i = 0; i < points; i++) {
        float x = (float)rand() / ((float)RAND_MAX + 1.0F);
        float y = (float)rand() / ((float)RAND_MAX + 1.0F);
        if ( x * x + y * y < 1.0) {
            count += 1;
        }
    }

    char *ofilename = malloc(strlen(argv[1]) + 5);
    strncpy(ofilename, argv[1], strlen(argv[1]) + 1);
    strncat(ofilename, ".out", 4);

    ofp = fopen(ofilename, "w");
    fprintf(ofp, "%d/%d\n", count, points);
    fclose(ofp);

    return 0;
}
