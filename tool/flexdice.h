/* flexdice.h (2014-02-04) -*-Coding: us-ascii;-*- */
/* Copyright (C) 1901-2014 Tomotake Nakamura */

/* FlexDice.  See "flexdice.c". */

#include <stdbool.h>

/* Data types: DATA=DATA_Z for integer, DATA=DATA_R for real. */

#define DATA_Z 1
#define DATA_R 2
#define DATA DATA_R

#if (DATA == DATA_Z)
#define DATATYPE int
#elif (DATA == DATA_R)
#define DATATYPE double
#else
#define DATATYPE error
#endif

/* Cell state.  Undetermined-cells, sparse-cells, middle-cells,
   dense-cells. */

enum CELLDENSITY {MADA = 0, SPARSE, MIDDLE, DENSE, DEBRIS};

extern struct FLEXDICE fxd;

/* Parameters to FlexDice.  DIM: The number of attributes (dimension).
   DMIN: The lower bound of the number of objects in a dense cell.
   DFAC: The factor for the lower bound of the number of child cells
   in a dense cell.  NLAYERS: The limit of the number of layers
   (bottom cells can have levels of this value minus one).
   PLOT_CUT_OFF: Print threshold; only clusters holding objects more
   than this value is printed. */

struct INPARA {
    int dim;
    int dmin;
    double dfac;
    int nlayers;
    int hashsize;
    char *infile;
    char *outdir;
    int plot_cut_off;
    int plot_noise;
};

struct OBJECT {
    struct OBJECT *next;
    int id;
    DATATYPE *value;
};

/* Object Holder. */

struct OBJECTQ {
    struct OBJECT *head;
    struct OBJECT *tail;
    long count;
};

/* List of cells (for hashtables).  It links CELLs by the D_NEXT
   field. */

struct CELLS {
    struct CELL *head;
    long count;
};

/* Cell of objects.  C_NEXT links cells in CHILDREN (thus at the same
   level), Q_NEXT for QUEUE, D_NEXT for dense cells, noise cells, and
   hash-bin, X_NEXT for cells in clusters.  PATH is a relative path
   from its parent.  CLUSTER is one it belongs to (in phase2).
   TOTAL_OBJECT_COUNT is the number of objects in this and below.  It
   equals to the sum of the COUNTS of the all children for a cell with
   children. */

struct CELL {
    struct CELL *c_next;
    struct CELL *q_next;
    struct CELL *d_next;
    struct CELL *x_next;
    int level;
    enum CELLDENSITY density;
    int child_count;
    long total_object_count;
    struct CELL *parent;
    unsigned int *path;
    struct CLUSTER *cluster;
    struct CELL *children;
    struct CELLS *hashed_children;
    struct CELL **adjacents;
    DATATYPE *min;
    DATATYPE *max;
    DATATYPE *cen;
    struct OBJECTQ objects;
};

/* FIFO queue of cells. */

struct CELLQ {
    struct CELL *head;
    struct CELL *tail;
    long count;
};

/* Clusters of cells.  ACTIVE indicates it is not merged to another
   cluster (and are representatives at the end).  V_NEXT links all
   clusters created.  N_NEXT links neighboring clusters as a cyclic
   list.  Cells in CELLS are linked by X_NEXT. */

struct CLUSTER {
    struct CLUSTER *v_next;
    struct CLUSTER *n_next;
    struct CELL *cells;
    int id;
    bool active;
    long object_count;
};

/* Information of each middle layer.  It records the number of middle
   cells, the total number of the child cells of them, and the number
   of objects in middle cells. */

struct LAYER {
    long cell_count;
    long child_cell_count;
    long object_count;
};

/* State of FlexDice.  CODE_SIZE=((DIM-1)/INT_BITS+1).  DEPTH_LIMITS:
   The limit of depth for each attribute.  NOISE_CELLS: List of noise
   (sparse) cells, linked thru D_NEXT.  LAYERS: Info at each layer. */

struct FLEXDICE {
    struct INPARA *para;
    int DIM;
    int DIM_UPDN;
    int CODE_SIZE;

    int *depth_limits;

    long n_objects;
    long n_clusters;
    long n_cells;
    long n_middle_cells;
    long n_dense_cells;
    long n_noise_cells;
    long n_debris_cells;
    long n_noise_objects;

    struct CLUSTER *clusters;
    long cluster_count;

    struct CELL *noise_cells;
    struct CELL *dense_cells;
    struct LAYER *layers;

    struct CELLQ queue;

    struct CELL *top;
    double t[3];
};

extern void init_flexdice(int argc, char **argv, struct INPARA *);
extern void fin_flexdice(void);
extern struct CELL *create_cell(struct CELL *parent, unsigned int *path);
extern void read_input(struct CELL *top, struct INPARA *para);
extern void set_input(struct CELL *top, struct INPARA *para,
		      int *data, long count);
extern int flexdice(struct CELL *top, struct INPARA *para);
extern void print_parameters(struct INPARA *para);
extern void print_input(struct CELL *top);
extern void output_clusters(void);
