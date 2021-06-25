/* flexdicemain.c (2014-02-04) -*-Coding: us-ascii;-*- */

/* FlexDice (main routine).  See "flexdice.c". */

/* ARGUMENTS (example):
   % flexdice 3 indata outdir 4 5 7 403
   1: dimension of input data (3)
   2: input data file name (indata)
   3: output directory name (outdir)
   4: lower bound of the number of objects in dense cells (4)
   5: dense cell factor (larger value requires higher density) (5)
   6: limit of the number of layers (7)
   7: hashtable size (403) */

#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <math.h>
#include <assert.h>
#include "flexdice.h"

static void make_data(int *data, int range, int dim, int m, int n);

int
main(int argc, char **argv)
{
    struct INPARA para_, *para = &para_;

    int datadash = (argc != 1);
    if (datadash) {
	assert(argc == 8);
	para->dim = atoi(argv[1]);
	para->dmin = atoi(argv[4]);
	para->dfac = atoi(argv[5]);
	para->nlayers = atoi(argv[6]);
	para->hashsize = atoi(argv[7]);
	para->infile = argv[2];
	para->outdir = argv[3];
	para->plot_cut_off = 10;
	para->plot_noise = 1;
    } else {
	para->dim = 3;
	para->dmin = 4;
	para->dfac = 5;
	para->nlayers = 7;
	para->hashsize = 403;
	para->infile = "indata";
	para->outdir = "outdir";
	para->plot_cut_off = 0;
	para->plot_noise = 0;
    }

    init_flexdice(argc, argv, para);
    print_parameters(para);
    struct CELL *input = create_cell(0, 0);
    if (datadash) {
	read_input(input, para);
    } else {
	int range = 10000;
	int dim = para->dim;
	int m = 20;
	int n = 100;
	int *data = malloc(sizeof(int) * (dim * m * n));
	assert(data != 0);
	make_data(data, range, dim, m, n);
	set_input(input, para, data, (m * n));
	free(data);
    }

    flexdice(input, para);

    output_clusters();

    printf("CPU TIME: Phase1=%.3f Phase2=%.3f Total=%.3f (sec)\n",
	   (fxd.t[1] - fxd.t[0]), (fxd.t[2] - fxd.t[1]),
	   (fxd.t[2] - fxd.t[0]));

    printf("#clusters=%ld #dense-cells=%ld #noise-objects=%ld\n",
	   fxd.n_clusters, fxd.n_dense_cells, fxd.n_noise_objects);

    return 0;
}

static double
RND()
{
    double u, v, s;
    do {
	u = 2.0 * drand48() - 1.0;
	v = 2.0 * drand48() - 1.0;
	s = u * u + v * v;
    } while (s >= 1.0 || s == 0.0);
    double m = sqrt(-2.0 * log(s) / s);
    return (u * m);
}

#define BND(X,R) ((X < -R) ? -R : ((X > R) ?  R : X))

/* Generates M clusters of N data each. */

static void
make_data(int *data, int range, int dim, int m, int n)
{
    int p[dim];
    double sdev = (0.01 * range);
    assert(sdev * 10.0 < 1.0 * INT_MAX);
    for (int j = 0; j < m; j++) {
	for (int attr = 0; attr < dim; attr++) {
	    int v = (int)(range * (2.0 * drand48() - 1.0));
	    p[attr] = BND(v, range);
	}
	for (int i = 0; i < n; i++) {
	    for (int attr = 0; attr < dim; attr++) {
		int v = (int)((RND() * sdev) + p[attr]);
		data[dim * (n * j + i) + attr] = BND(v, range);
	    }
	}
    }
}
