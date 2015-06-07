/* flexdice.c (2014-02-04) -*-Coding: us-ascii;-*- */
/* Copyright (C) 1901-2014 Tomotake Nakamura */

/* "FlexDice" - a clustering method which groups data objects in an
   adjacent dense data space.  Code is rewritten for KMR example.  All
   bugs are imputed to rewriters. */

/* REFERENCES:
   (1) "A Clustering Method using an Irregular Size Cell Graph",
   RIDE-SDMA 2005, IEEE, 2005.
   http://ieeexplore.ieee.org/xpl/articleDetails.jsp?arnumber=1498227
   (2) "FlexDice: A Fast Clustering Method for Large High Dimensional
   Data Sets", IPSJ, 2005 (in Japanese).
   http://ci.nii.ac.jp/naid/110002977727. */

#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <float.h>
#include <math.h>
#include <string.h>
#include <sys/resource.h>
#include <errno.h>
#include <assert.h>
#include "flexdice.h"

struct FLEXDICE fxd;

static const int INT_BITS = (8 * sizeof(int));

static void divide_cell_and_objects(struct CELL *oya);
static void set_direct_adjacency(struct CELL *cell);
static void undo_divide_cell(struct CELL *cell);
static void set_cell_range(struct CELL *cell, struct CELL *oya);
static void make_clusters(void);
static void finish_clusters(void);

static void calculate_cell_path(unsigned int *, struct CELL *,
				struct OBJECT *);
static void merge_clusters(struct CLUSTER *x0, struct CLUSTER *y0);
static void link_cells_to_cluster(struct CLUSTER *cluster, struct CELL *cell);

static void setup_flexdice(struct CELL *top);
static void print_depth_limit(void);
static void output_statistics(void);
static double my_clock(void);

#define BITSET(V,I) ((V)[(I)/INT_BITS] |= (1U << ((I)%INT_BITS)))
#define BITXOR(V,I) ((V)[(I)/INT_BITS] ^= (1U << ((I)%INT_BITS)))
#define BITGET(V,I) (((V)[(I)/INT_BITS] >> ((I)%INT_BITS)) & 1)
#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

static void *
malloc_safe(size_t s)
{
    void *p = malloc(s);
    if (p == NULL) {
	fprintf(stderr, "Insufficient Memory.\n");
	exit(1);
    }
    memset(p, 0, s);
    return p;
}

static bool
bits_equal(unsigned int *a, unsigned int *b)
{
    for (int i = 0; i < fxd.CODE_SIZE; i++) {
	if (a[i] != b[i])
	    return false;
    }
    return true;
}

static void
bits_clear(unsigned int *a)
{
    for (int i = 0; i < fxd.CODE_SIZE; i++) {
	a[i] = 0;
    }
}

static void
bits_copy(unsigned int *a, unsigned int *b)
{
    for (int i = 0; i < fxd.CODE_SIZE; i++) {
	a[i] = b[i];
    }
}

static int
hash_bits(unsigned int *p, int m, struct INPARA *para)
{
    return (p[0] % m);
}

double
my_clock(void)
{
    struct rusage ru;
    int cc = getrusage(RUSAGE_SELF, &ru);
    assert(cc == 0);
    return (ru.ru_utime.tv_sec + (double)ru.ru_utime.tv_usec * 1e-6);
}

static int
log2i(int n)
{
    int i = 0;
    int v = n;
    while (v > 1) {
	v /= 2;
	i++;
    }
    return i;
}

/* Merges the second object-list to the first, and empties the
   second. */

static void
relink_object_lists(struct OBJECTQ *dst, struct OBJECTQ *src)
{
    if (src->head != NULL) {
	assert(src->tail != NULL && src->count != 0);
	src->tail->next = dst->head;
	dst->head = src->head;
	if (dst->tail == NULL) {
	    assert(dst->count == 0);
	    dst->tail = src->tail;
	}
	dst->count += src->count;
	src->count = 0;
	//src->total_count = 0;
	src->head = NULL;
	src->tail = NULL;
    }
}

static void
setup_object(struct OBJECT *o, int id)
{
    o->next = NULL;
    o->value = malloc_safe(sizeof(DATATYPE) * fxd.DIM);
    o->id = id;
}

static void
setup_celllist(struct CELLS *cs)
{
    cs->count = 0;
    cs->head = NULL;
}

static void
setup_object_queue(struct OBJECTQ *data)
{
    data->count = 0;
    //data->total_count = 0;
    data->head = NULL;
    data->tail = NULL;
}

static void
put_cluster(struct CLUSTER *c)
{
    c->v_next = fxd.clusters;
    fxd.clusters = c;
    fxd.cluster_count++;
}

static void
setup_cluster(struct CLUSTER *cluster, int id)
{
    //setup_object_queue(&cluster->objects);
    cluster->v_next = NULL;
    cluster->n_next = cluster;
    cluster->id = id;
    cluster->object_count = 0;
    cluster->active = true;
    put_cluster(cluster);
}

static void
setup_layer(struct LAYER *layer)
{
    layer->object_count = 0;
    layer->cell_count = 0;
    layer->child_cell_count = 0;
}

/* Takes an object in a cell, or returns null. */

static struct OBJECT *
take_object_from_cell(struct CELL *cell)
{
    struct OBJECT *o = cell->objects.head;
    if (o != NULL) {
	assert(cell->objects.count > 0);
	cell->objects.head = cell->objects.head->next;
	if (cell->objects.head == NULL) {
	    assert(cell->objects.count == 1);
	    cell->objects.tail = NULL;
	}
	cell->objects.count--;
	o->next = NULL;
    }
    return o;
}

static void
update_layer_stat(struct LAYER *layer, struct CELL *c)
{
    layer->cell_count++;
    layer->child_cell_count += c->child_count;
    layer->object_count += c->total_object_count;
}

static void
setup_cell_queue(struct CELLQ *q)
{
    q->head = NULL;
    q->tail = NULL;
    q->count = 0;
}

/* Puts a cell to the FIFO queue. */

static void
put_cell_in_queue(struct CELL *c)
{
    assert(c->q_next == NULL);
    if (fxd.queue.count == 0) {
	assert(fxd.queue.head == NULL);
	fxd.queue.head = c;
    } else {
	fxd.queue.tail->q_next = c;
    }
    fxd.queue.tail = c;
    fxd.queue.count++;
}

/* Takes a cell from the FIFO queue.  */

static struct CELL *
take_cell_from_queue(void)
{
    assert(fxd.queue.count != 0);
    struct CELL *c = fxd.queue.head;
    fxd.queue.head = c->q_next;
    fxd.queue.count--;
    return c;
}

static void
link_cell_list(struct CELLS *h, struct CELL *cell)
{
    cell->d_next = h->head;
    h->head = cell;
    h->count++;
}

static void
put_object_in_cell(struct OBJECTQ *data, struct OBJECT *o)
{
    assert(o->next == NULL);
    if (data->head == NULL) {
	assert(data->tail == NULL && data->count == 0);
	data->head = o;
    } else {
	assert(data->tail != NULL && data->count != 0);
	data->tail->next = o;
    }
    data->tail = o;
    data->count++;
}

static void
put_cell_in_cluster(struct CLUSTER *cluster, struct CELL *p)
{
    assert(p->cluster == NULL && p->x_next == NULL);
    p->cluster = cluster;
    p->x_next = cluster->cells;
    cluster->cells = p;
}

static struct CELLS *
make_hashtable()
{
    int hashsize = fxd.para->hashsize;
    struct CELLS *h = malloc_safe(sizeof(struct CELLS) * hashsize);
    for (int i = 0; i < hashsize; i++) {
	setup_celllist(&h[i]);
    }
    return h;
}

/* Finds an adjacent cell along with the given relative PATH of the
   attributes.	It returns NULL if nothing found. */

static struct CELL *
find_child(struct CELL *oya, unsigned int *path)
{
    int hv = hash_bits(path, fxd.para->hashsize, fxd.para);
    struct CELLS *hashbin = &(oya->hashed_children[hv]);
    struct CELL *cell;
    for (cell = hashbin->head; cell != NULL; cell = cell->d_next) {
	assert(cell->density != DEBRIS);
	if (bits_equal(path, cell->path)) {
	    break;
	}
    }
    return cell;
}

static void
put_child(struct CELL *oya, unsigned int *path, struct CELL *cell)
{
    int hv = hash_bits(path, fxd.para->hashsize, fxd.para);
    struct CELLS *hashbin = &(oya->hashed_children[hv]);
    link_cell_list(hashbin, cell);
    cell->c_next = oya->children;
    oya->children = cell;
    oya->child_count++;
}

static void
setup_cell(struct CELL *cell, unsigned int *path)
{
    cell->c_next = NULL;
    cell->q_next = NULL;
    cell->d_next = NULL;
    cell->x_next = NULL;
    cell->level = 0;
    cell->density = MADA;
    cell->child_count = 0;
    cell->total_object_count = 0;
    cell->parent = NULL;
    cell->path = malloc_safe(sizeof(unsigned int) * fxd.CODE_SIZE);
    if (path == NULL) {
	bits_clear(cell->path);
    } else {
	bits_copy(cell->path, path);
    }
    cell->cluster = NULL;
    cell->children = NULL;
    cell->hashed_children = NULL;
    cell->adjacents = malloc_safe(sizeof(struct CELL *) * 2 * fxd.DIM);
    for (int dir = 0; dir < fxd.DIM_UPDN; dir++) {
	cell->adjacents[dir] = NULL;
    }
    cell->min = malloc_safe(sizeof(DATATYPE) * fxd.DIM);
    cell->max = malloc_safe(sizeof(DATATYPE) * fxd.DIM);
    cell->cen = malloc_safe(sizeof(DATATYPE) * fxd.DIM);
    for (int attr = 0; attr < fxd.DIM; attr++) {
	cell->min[attr] = 0;
	cell->max[attr] = 0;
	cell->cen[attr] = 0;
    }
    setup_object_queue(&cell->objects);
}

/* Makes a cell for PATH for the PARENT.  PARENT and PATH can be
   NULL. */

struct CELL *
create_cell(struct CELL *oya, unsigned int *path)
{
    struct CELL *cell = malloc_safe(sizeof(struct CELL));
    setup_cell(cell, path);
    fxd.n_cells++;
    if (oya != NULL) {
	cell->parent = oya;
	cell->level = (oya->level + 1);
	bits_copy(cell->path, path);
	put_child(oya, path, cell);
    }
    return cell;
}

static void
put_cell_to_dense_list(struct CELL *c)
{
    assert(c->d_next == NULL);
    c->d_next = fxd.dense_cells;
    fxd.dense_cells = c;
    fxd.n_dense_cells++;
}

static void
put_cell_to_noise_list(struct CELL *c)
{
    assert(c->d_next == NULL);
    c->d_next = fxd.noise_cells;
    fxd.noise_cells = c;
    fxd.n_noise_cells++;
    fxd.n_noise_objects += c->objects.count;
}

/* Runs FlexDice.  Call it after "init_flexdice" and "read_input". */

int
flexdice(struct CELL *top, struct INPARA *para)
{
#if 0
    init_flexdice(argv, para);
    print_parameters(para);
    struct CELL *top = create_cell(NULL, NULL);
    read_input(top, para);
#endif

    top->density = MADA;
    setup_flexdice(top);

    print_depth_limit();

    fxd.t[0] = my_clock();

    /* Move initial cells to the processing queue. */

    put_cell_in_queue(top);

    /* Phase 1: Start. */

    int level = 0;
    while (fxd.queue.count != 0) {
	int count = 0;
	for (struct CELL *p = fxd.queue.head; p != NULL; p = p->q_next) {
	    count++;
	}
	assert(count == fxd.queue.count);
	assert(fxd.layers[level].cell_count == 0);

	printf("layer-info: "
	       "level=%d #cells-at-level=%ld #dense-cells=%ld\n",
	       level, fxd.queue.count, fxd.n_dense_cells);

	for (struct CELL *p = fxd.queue.head; p != NULL; p = p->q_next) {
	    assert((p->level == level) && (p->density == MADA));
	    if (p->objects.count < fxd.para->dmin) {
		p->density = SPARSE;
	    } else if (level == (fxd.para->nlayers - 1)) {
		p->density = DENSE;
	    } else {
		p->density = MIDDLE;
		divide_cell_and_objects(p);
	    }
	    update_layer_stat(&fxd.layers[level], p);
	}
	assert(fxd.layers[level].cell_count == fxd.queue.count);

	/* Work after all siblings at the level are divided. */

	assert(fxd.layers[level].cell_count != 0);
	double ratio = (1.0 * fxd.layers[level].child_cell_count
			/ fxd.layers[level].cell_count);
	int threshold = (int)(fxd.para->dfac * ratio);

	int n = fxd.queue.count;
	for (int i = 0; i < n; i++) {
	    struct CELL *p = take_cell_from_queue();
	    assert(p->level == level);
	    set_direct_adjacency(p);
	    if (p->child_count > threshold) {
		p->density = DENSE;
		undo_divide_cell(p);
	    }
	    assert(p->density != MADA);
	    if (p->density == SPARSE) {
		put_cell_to_noise_list(p);
	    } else if (p->density == DENSE) {
		put_cell_to_dense_list(p);
	    } else {
		assert(p->density == MIDDLE);
		fxd.n_middle_cells++;
		for (struct CELL *c = p->children; c != NULL; c = c->c_next) {
		    put_cell_in_queue(c);
		}
	    }
	}

	level++;
    }

    /* Phase 1: End. */

    /* Check the number of objects. */

    {
	int no = 0;
	int nc = 0;
	for (struct CELL *c = fxd.noise_cells; c != NULL; c = c->d_next) {
	    assert(c->objects.count != 0 && c->child_count == 0);
	    no += c->objects.count;
	    nc++;
	}
	int oo = 0;
	int dc = 0;
	for (struct CELL *c = fxd.dense_cells; c != NULL; c = c->d_next) {
	    assert(c->density == DENSE);
	    assert(c->objects.count != 0);
	    oo += c->objects.count;
	    dc++;
	}
	assert(fxd.n_objects == (oo + no));
	assert(fxd.n_noise_objects == no);
	assert(fxd.n_noise_cells == nc);
	assert(fxd.n_dense_cells == dc);
	assert(fxd.n_cells == (fxd.n_middle_cells + fxd.n_debris_cells
			       + nc + dc));
    }

    fxd.t[1] = my_clock();

    printf("result: " "#dense-cells=%ld #noise-objects=%ld\n",
	   fxd.n_dense_cells, fxd.n_noise_objects);

    if (fxd.dense_cells == NULL) {
	fprintf(stderr, "All objects are noise.\n");
	exit(1);
    }

    /* Phase 2: Start. */

    /* Construct subclusters. */

    make_clusters();
    finish_clusters();

    /* Phase 2: End. */

    /* Check the number of objects. */

    {
	int oo = 0;
	int dc = 0;
	for (struct CLUSTER *p = fxd.clusters; p != NULL; p = p->v_next) {
	    if (!p->active) {
		continue;
	    }
	    oo += p->object_count;
	    {
		struct CLUSTER *q = p;
		do {
		    for (struct CELL *c = q->cells; c != NULL; c = c->x_next) {
			if (c->density == DENSE) {
			    dc++;
			}
		    }
		    q = q->n_next;
		} while (q != p);
	    }
	}
	assert(fxd.n_objects == (oo + fxd.n_noise_objects));
	assert(fxd.n_dense_cells == dc);
    }

    fxd.t[2] = my_clock();

    return 0;
}

static void
delete_cell(struct CELL *c)
{
    free(c->path);
    free(c->min);
    free(c->max);
    free(c->cen);
    free(c->adjacents);
    //free(c->children);
    if (c->hashed_children != NULL) {
	free(c->hashed_children);
    }
}

/*NOTYET*/

static void
delete_flexdice()
{
    free(fxd.depth_limits);
    free(fxd.layers);
    //free(fxd.noise_cells);
}

void
init_flexdice(int argc, char **argv, struct INPARA *para)
{
    fxd.para = para;
    fxd.DIM = fxd.para->dim;
    fxd.DIM_UPDN = (2 * fxd.para->dim);
    fxd.CODE_SIZE = ((fxd.para->dim - 1) / INT_BITS) + 1;
}

void
fin_flexdice(void)
{
    delete_flexdice();
}

void
read_input(struct CELL *input, struct INPARA *para)
{
    int cc;
    DATATYPE max[fxd.DIM];
    DATATYPE min[fxd.DIM];
    input->level = 0;
    input->parent = NULL;
    for (int dir = 0; dir < fxd.DIM_UPDN; dir++) {
	input->adjacents[dir] = NULL;
    }
    for (int attr = 0; attr < fxd.DIM; attr++) {
#if (DATA == DATA_Z)
	max[attr] = INT_MIN;
	min[attr] = INT_MAX;
#else
	max[attr] = -DBL_MAX;
	min[attr] = DBL_MAX;
#endif
    }

    FILE *f = fopen(para->infile, "r");
    assert(f != NULL);
    int nobjects = 0;

    {
	struct OBJECT *o = NULL;
	int attr = 0;
	DATATYPE val;
	errno = 0;
#if (DATA == DATA_Z)
	#define FMT "%d"
#else
	#define FMT "%lf"
#endif
	for (;;) {
	    cc = fscanf(f, FMT, &val);
	    if (cc == EOF) {
		break;
	    }
	    if (cc == 0) {
#define BAD_DATA_IN_INPUT 0
		assert(BAD_DATA_IN_INPUT);
	    }
	    if (attr == 0) {
		o = malloc_safe(sizeof(struct OBJECT));
		setup_object(o, nobjects);
		nobjects++;
	    }
	    o->value[attr] = val;
	    max[attr] = MAX(max[attr], val);
	    min[attr] = MIN(min[attr], val);
	    if (attr == (fxd.DIM - 1)) {
		put_object_in_cell(&(input->objects), o);
		attr = 0;
	    } else {
		attr++;
	    }
	}
	assert(errno == 0);
    }
    cc = fclose(f);
    assert(cc == 0);

    assert(input->objects.count == nobjects);
    for (int attr = 0; attr < fxd.DIM; attr++) {
	input->max[attr] = max[attr];
	input->min[attr] = min[attr];
	input->cen[attr] = (min[attr] + (max[attr] - min[attr]) / 2);
    }
}

/* Sets input with integer data DATA[COUNT][DIM]. */

void
set_input(struct CELL *input, struct INPARA *para, int *data, long count)
{
    assert(DATA == DATA_Z);
    int max[fxd.DIM];
    int min[fxd.DIM];
    input->level = 0;
    input->parent = NULL;
    for (int dir = 0; dir < fxd.DIM_UPDN; dir++) {
	input->adjacents[dir] = NULL;
    }
    for (int attr = 0; attr < fxd.DIM; attr++) {
	max[attr] = INT_MIN;
	min[attr] = INT_MAX;
    }
    for (long i = 0; i < count; i++) {
	struct OBJECT *o = malloc_safe(sizeof(struct OBJECT));
	setup_object(o, i);
	for (int attr = 0; attr < fxd.DIM; attr++) {
	    int val = data[(fxd.DIM * i) + attr];
	    o->value[attr] = val;
	    max[attr] = MAX(max[attr], val);
	    min[attr] = MIN(min[attr], val);
	}
	put_object_in_cell(&(input->objects), o);
    }
    for (int attr = 0; attr < fxd.DIM; attr++) {
	assert(max[attr] != INT_MIN);
	assert(min[attr] != INT_MAX);
    }
    for (int attr = 0; attr < fxd.DIM; attr++) {
	input->max[attr] = max[attr];
	input->min[attr] = min[attr];
	input->cen[attr] = (min[attr] + (max[attr] - min[attr]) / 2);
    }
}

/* Makes files ("datamax" and "datamin") holding min. and max. of
   the input data. */

void
output_statistics(void)
{
    int cc;
    char name[100];

    cc = snprintf(name, sizeof(name), "%s/datamax", fxd.para->outdir);
    assert(cc < (int)sizeof(name));
    FILE *f0 = fopen(name, "w");
    assert(f0 != 0);

    struct CELL *top = fxd.top;
    for (int attr = 0; attr < fxd.DIM; attr++) {
#if (DATA == DATA_Z)
	cc = fprintf(f0, "%s%d", (attr == 0 ? "" : " "), top->max[attr]);
	assert(cc >= 0);
#else
	cc = fprintf(f0, "%s%f", (attr == 0 ? "" : " "), top->max[attr]);
	assert(cc >= 0);
#endif
    }
    cc = fprintf(f0, "\n");
    assert(cc >= 0);
    cc = fclose(f0);
    assert(cc == 0);

    cc = snprintf(name, sizeof(name), "%s/datamin", fxd.para->outdir);
    assert(cc < (int)sizeof(name));
    FILE *f1 = fopen(name, "w");
    assert(f1 != 0);
    for (int attr = 0; attr < fxd.DIM; attr++) {
#if (DATA == DATA_Z)
	cc = fprintf(f1, "%s%d", (attr == 0 ? "" : " "), top->min[attr]);
	assert(cc >= 0);
#else
	cc = fprintf(f1, "%s%f", (attr == 0 ? "" : " "), top->min[attr]);
	assert(cc >= 0);
#endif
    }
    cc = fprintf(f1, "\n");
    assert(cc >= 0);
    cc = fclose(f1);
    assert(cc == 0);
}

static void
output_object(FILE *f, struct OBJECT *o)
{
    int cc;
    cc = fprintf(f, "%d ", o->id);
    assert(cc >= 0);
    for (int attr = 0; attr < fxd.DIM; attr++) {
#if (DATA == DATA_Z)
	cc = fprintf(f, " %d", o->value[attr]);
	assert(cc >= 0);
#else
	cc = fprintf(f, " %f", o->value[attr]);
	assert(cc >= 0);
#endif
    }
    cc = fprintf(f, "\n");
    assert(cc >= 0);
}

void
output_clusters(void)
{
    int cc;
    char name[50];
    char command[2048];

    int digits = ((int)log10((double)fxd.n_clusters) + 1);

    cc = snprintf(command, sizeof(command), "mkdir -p %s", fxd.para->outdir);
    assert(cc < (int)sizeof(command));
    cc = system(command);
    assert(cc != -1);

    output_statistics();

    int count = 0;
    for (struct CLUSTER *p = fxd.clusters; p != NULL; p = p->v_next) {
	if (!p->active) {
	    continue;
	}
	cc = snprintf(name, sizeof(name), "%s/C%0*d",
		      fxd.para->outdir, digits, count);
	assert(cc < (int)sizeof(name));
	FILE *f0 = fopen(name, "w");
	assert(f0 != 0);
	for (struct CELL *c = p->cells; c != NULL; c = c->x_next) {
	    for (struct OBJECT *o = c->objects.head; o != NULL; o = o->next) {
		output_object(f0, o);
	    }
	}
	cc = fclose(f0);
	assert(cc == 0);
	count++;
    }
    assert(count == fxd.n_clusters);

    _Bool noise[fxd.para->nlayers];
    for (int i = 0; i < fxd.para->nlayers; i++) {
	noise[i] = 0;
    }

    for (int i = 0; i < fxd.para->nlayers; i++) {
	cc = snprintf(name, sizeof(name), "%s/NL%d", fxd.para->outdir, i);
	assert(cc < (int)sizeof(name));
	FILE *f1 = fopen(name, "w");
	assert(f1 != 0);
	for (struct CELL *c = fxd.noise_cells; c != NULL; c = c->d_next) {
	    if (c->level != i) {
		continue;
	    }
	    noise[i] = 1;
	    for (struct OBJECT *o = c->objects.head; o != NULL; o = o->next) {
		output_object(f1, o);
	    }
	}
	cc = fclose(f1);
	assert(cc == 0);
    }

#if 0
    if (fxd.para->dim == 2) {
	cc = snprintf(name, sizeof(name), "%s/plot", fxd.para->outdir);
	assert(cc < (int)sizeof(name));
	FILE *f2 = fopen(name, "w");
	assert(f2 != 0);
	cc = fprintf(f2, "plot 'NL'notitle");
	assert(cc >= 0);
	int cnt2 = 0;
	for (struct CLUSTER *p = fxd.clusters; p != NULL; p = p->v_next) {
	    if (p->active && (fxd.para->plot_cut_off < p->object_count)) {
		cc = fprintf(f2, ",'C%0*d'notitle", digits, cnt2);
		assert(cc >= 0);
	    }
	    cnt2++;
	}
	cc = fclose(f2);
	assert(cc == 0);
    }
#endif

    if (fxd.para->dim >= 2) {
	cc = snprintf(name, sizeof(name), "%s/plot2d", fxd.para->outdir);
	assert(cc < (int)sizeof(name));
	FILE *f3 = fopen(name, "w");
	assert(f3 != 0);
	cc = fprintf(f3, "plot ");
	assert(cc >= 0);
	char *fsep = "";
	int cnt3 = 0;
	for (struct CLUSTER *p = fxd.clusters; p != NULL; p = p->v_next) {
	    if (p->active && (p->object_count > fxd.para->plot_cut_off)) {
		cc = fprintf(f3, "%s'C%0*d' u 2:3 w d", fsep, digits, cnt3);
		assert(cc >= 0);
		fsep = ", ";
	    }
	    cnt3++;
	}
	assert(fsep[0] != 0);
	if (fxd.para->plot_noise) {
	    for (int i = 1; i < fxd.para->nlayers; i++) {
		if (noise[i]) {
		    cc = fprintf(f3, "%s'NL%d' u 2:3 w d", fsep, i);
		    assert(cc >= 0);
		}
	    }
	}
	cc = fclose(f3);
	assert(cc == 0);
    }
    if (fxd.para->dim >= 3) {
	cc = snprintf(name, sizeof(name), "%s/plot3d", fxd.para->outdir);
	assert(cc < (int)sizeof(name));
	FILE *f4 = fopen(name, "w");
	assert(f4 != 0);
	cc = fprintf(f4, "splot ");
	assert(cc >= 0);
	char *fsep = "";
	int cnt4 = 0;
	for (struct CLUSTER *p = fxd.clusters; p != NULL; p = p->v_next) {
	    if (p->active && (p->object_count > fxd.para->plot_cut_off)) {
		cc = fprintf(f4, "%s'C%0*d' u 2:3:4 w d", fsep, digits, cnt4);
		assert(cc >= 0);
		fsep = ", ";
	    }
	    cnt4++;
	}
	assert(fsep[0] != 0);
	if (fxd.para->plot_noise) {
	    for (int i = 1; i < fxd.para->nlayers; i++) {
		if (noise[i]) {
		    cc = fprintf(f4, "%s'NL%d' u 2:3:4 w d", fsep, i);
		    assert(cc >= 0);
		}
	    }
	}
	cc = fclose(f4);
	assert(cc == 0);
    }

    /* Make "inputdata.data" for calculating precision (Ec). */

    {
	cc = snprintf(name, sizeof(name), "%s/inputdata.dat",
		      fxd.para->outdir);
	assert(cc < (int)sizeof(name));
	FILE *f5 = fopen(name, "w");
	assert(f5 != 0);
	int cnt5 = 0;
	for (struct CLUSTER *p = fxd.clusters; p != NULL; p = p->v_next) {
	    if (p->active && fxd.para->plot_cut_off < p->object_count) {
		cc = fprintf(f5, "C%0*d\n", digits, cnt5);
		assert(cc >= 0);
	    }
	    cnt5++;
	}
	cc = fprintf(f5, "NL\n");
	assert(cc >= 0);
	cc = fclose(f5);
	assert(cc == 0);
    }
}

/* Sets the depth-limit of each attribute.  It errs if the parameter
   value is too large. */

static void
setup_flexdice(struct CELL *top)
{
    fxd.depth_limits = malloc_safe(sizeof(int) * fxd.para->dim);
    int botmax = INT_MIN;
    for (int attr = 0; attr < fxd.DIM; attr++) {
#if (DATA == DATA_Z)
	DATATYPE w = (top->max[attr] - top->min[attr]);
	fxd.depth_limits[attr] = log2i(w + 1);
#else
	fxd.depth_limits[attr] = (fxd.para->nlayers - 1);
#endif
	assert(fxd.depth_limits[attr] > 0);
	botmax = MAX(botmax, fxd.depth_limits[attr]);
    }
    if (botmax < (fxd.para->nlayers - 1)) {
	fprintf(stderr, "input parameter error; layers be %d.\n", botmax);
	assert(0);
	exit(1);
    }

    fxd.n_cells = 1;
    fxd.n_objects = top->objects.count;
    fxd.n_middle_cells = 0;
    fxd.n_dense_cells = 0;
    fxd.n_clusters = 0;
    fxd.n_noise_cells = 0;
    fxd.n_debris_cells = 0;
    fxd.n_noise_objects = 0;

    fxd.layers = malloc_safe(sizeof(struct LAYER) * fxd.para->nlayers);
    for (int i = 0; i < fxd.para->nlayers; i++) {
	setup_layer(&fxd.layers[i]);
    }
    fxd.layers[0].object_count = top->objects.count;
    fxd.layers[0].cell_count = 0;
    fxd.layers[0].child_cell_count = 0;

    fxd.clusters = NULL;
    fxd.cluster_count = 0;
    fxd.t[0] = 0.0;
    fxd.t[1] = 0.0;
    fxd.t[2] = 0.0;

    setup_cell_queue(&(fxd.queue));

    fxd.noise_cells = NULL;
    fxd.dense_cells = NULL;

    fxd.top = top;
}

/* *** PHASE 1 *** */

/* Distributes data objects to child cells, making child cells if
   needed. */

static void
divide_cell_and_objects(struct CELL *cell)
{
    unsigned int path[fxd.CODE_SIZE];
    assert(cell->child_count == 0 && cell->hashed_children == NULL);
    cell->total_object_count = cell->objects.count;
    cell->hashed_children = make_hashtable();
    int nchildren = 0;
    struct OBJECT *o;
    while ((o = take_object_from_cell(cell)) != NULL) {
	calculate_cell_path(path, cell, o);
	struct CELL *c = find_child(cell, path);
	if (c == NULL) {
	    c = create_cell(cell, path);
	    set_cell_range(c, cell);
	    nchildren++;
	}
	put_object_in_cell(&(c->objects), o);
    }
    assert(cell->objects.count == 0);
    assert(cell->child_count == nchildren);
}

/* Sets the value range of a cell. */

static void
set_cell_range(struct CELL *cell, struct CELL *oya)
{
    for (int attr = 0; attr < fxd.DIM; attr++) {
	if (oya->level < fxd.depth_limits[attr]) {
	    /* Divisible dimension. */
	    int updn = BITGET(cell->path, attr);
	    if (updn == 0) {
		cell->min[attr] = oya->min[attr];
		cell->max[attr] = oya->cen[attr];
	    } else {
		cell->min[attr] = oya->cen[attr];
		cell->max[attr] = oya->max[attr];
	    }
	    cell->cen[attr] = (cell->min[attr]
			       + ((cell->max[attr] - cell->min[attr]) / 2));
	} else {
	    /* Not-divisible dimension. */
	    cell->min[attr] = oya->min[attr];
	    cell->max[attr] = oya->max[attr];
	    cell->cen[attr] = oya->cen[attr];
	}
    }
}

static void
undo_divide_cell(struct CELL *cell)
{
    assert(cell->objects.count == 0);
    for (struct CELL *c = cell->children; c != NULL; c = c->c_next) {
	assert(c->density == MADA);
	relink_object_lists(&(cell->objects), &(c->objects));
	c->density = DEBRIS;
	fxd.n_debris_cells++;
    }
}

/* Calculates a relative path to the cell for an object.  It is a bit
   vector on attributes, indicating up(1) or down(0) from the
   parent. */

static void
calculate_cell_path(unsigned int *path, struct CELL *oya, struct OBJECT *obj)
{
    bits_clear(path);
    for (int attr = 0; attr < fxd.DIM; attr++) {
	if (oya->level < fxd.depth_limits[attr]) {
	    /* Dimension is divisible. */
	    if (oya->cen[attr] < obj->value[attr]) {
		BITSET(path, attr);
	    }
	}
    }
}

static void
flip_path(unsigned int *xpath, unsigned int *path, int attr)
{
    bits_copy(xpath, path);
    BITXOR(xpath, attr);
}

/* Makes a direct adjacency of a CELL for each direction.  The
   flipping the bit of the path makes the new path to the near side of
   the sibling cells. */

static void
set_direct_adjacency(struct CELL *cell)
{
    unsigned int xpath[fxd.CODE_SIZE];
    struct CELL *oya = cell->parent;
    if (oya != NULL) {
	for (int dir = 0; dir < fxd.DIM_UPDN; dir++) {
	    assert(cell->adjacents[dir] == NULL);
	    int attr = (dir / 2);
	    int updn = BITGET(cell->path, attr);
	    if ((oya->level < fxd.depth_limits[attr]) && updn != (dir & 1)) {
		/* Take the other half of the cell. */
		flip_path(xpath, cell->path, attr);
		struct CELL *c = find_child(oya, xpath);
		if (c != NULL && c->density != SPARSE) {
		    cell->adjacents[dir] = c;
		}
	    } else {
		/* Take the adjacent cell of the parent. */
		struct CELL *ac = oya->adjacents[dir];
		if (ac != NULL) {
		    switch (ac->density) {
		    case MADA:
			assert(ac->density != MADA);
			break;
		    case SPARSE:
			/*nothing*/
			break;
		    case DENSE:
			cell->adjacents[dir] = ac;
			break;
		    case MIDDLE:
			if (oya->level < fxd.depth_limits[attr]) {
			    flip_path(xpath, cell->path, attr);
			} else {
			    bits_copy(xpath, cell->path);
			}
			struct CELL *c = find_child(ac, xpath);
			if (c != NULL && c->density != SPARSE) {
			    cell->adjacents[dir] = c;
			}
			break;
		    case DEBRIS:
			assert(ac->density != DEBRIS);
			break;
		    default:
			assert(false);
			break;
		    }
		}
	    }
	}
    }
}

/* *** PHASE 2 *** */

/* Creates clusters of the dense cells, and unions clusters among the
   adjacent ones. */

static void
make_clusters(void)
{
    int id = 0;
    for (struct CELL *p = fxd.dense_cells; p != NULL; p = p->d_next) {
	if (p->cluster == NULL) {
	    struct CLUSTER *cluster = malloc_safe(sizeof(struct CLUSTER));
	    setup_cluster(cluster, id);
	    link_cells_to_cluster(cluster, p);
	    id++;
	}
    }
}

static void
link_cells_to_cluster(struct CLUSTER *cluster, struct CELL *cell)
{
    assert(cell->cluster == NULL);
    assert(cell->density == DENSE);
    assert(cell->density != DENSE || cell->objects.count != 0);
    put_cell_in_cluster(cluster, cell);
    for (int dir = 0; dir < fxd.DIM_UPDN; dir++) {
	struct CELL *ac = cell->adjacents[dir];
	assert(ac == NULL || (ac->density != MADA && ac->density != DEBRIS));
	if (ac == NULL) {
	    /*nothing*/ ;
	} else if (ac->density == SPARSE || ac->density == MIDDLE) {
	    /*nothing*/ ;
	} else if (ac->cluster == NULL) {
	    link_cells_to_cluster(cluster, ac);
	} else {
	    merge_clusters(cluster, ac->cluster);
	}
    }
}

static void
merge_clusters(struct CLUSTER *x0, struct CLUSTER *y0)
{
    /* Do nothing if x0 and y0 are the same cluster. */
    for (struct CLUSTER *p = x0->n_next; p != x0; p = p->n_next) {
	if (p == y0) {
	    return;
	}
    }
    struct CLUSTER *x1 = x0->n_next;
    struct CLUSTER *y1 = y0->n_next;
    /* (x1->..->x0->y1->..->y0->) */
    x0->n_next = y1;
    y0->n_next = x1;
}

/* Marks representatives of clusters active, and others inactive.  It
   also counts the number of objects and clusters.  Clusters are
   active initially. */

static void
finish_clusters(void)
{
    int nclusters = 0;
    for (struct CLUSTER *pp = fxd.clusters; pp != NULL; pp = pp->v_next) {
	if (!pp->active) {
	    continue;
	}
	nclusters++;
	int oo = 0;
	{
	    struct CLUSTER *q = pp;
	    do {
		assert(q->active);
		if (q != pp) {
		    q->active = false;
		}
		for (struct CELL *c = q->cells; c != NULL; c = c->x_next) {
		    oo += c->objects.count;
		}
		q = q->n_next;
	    } while (q != pp);
	}
	assert(pp->object_count == 0);
	pp->object_count = oo;
    }
    assert(fxd.n_clusters == 0);
    fxd.n_clusters = nclusters;
}

/* *** PRINTERS *** */

static void
print_depth_limit(void)
{
    printf("bottom-levels:");
    for (int attr = 0; attr < fxd.DIM; attr++) {
	printf(" [%d]=%d", attr, fxd.depth_limits[attr]);
    }
    printf("\n");
}

void
print_parameters(struct INPARA *para)
{
    printf("input-file: %s\n", para->infile);
    printf("parameters:"
	   " dense-min=%d dense-factor=%.2f bottom-level=%d hash-size=%d\n",
	   para->dmin , para->dfac, para->nlayers, para->hashsize);
}

void
print_input(struct CELL *top)
{
    printf("-----Input Cell information-----\n");
    printf("The number of data objects = %ld\n", top->objects.count);
    printf("top->nighbor.dl\n");
    printf("(attribute#, min, center, max)\n");
    for (int i = 0; i < fxd.DIM; i++) {
	printf("(%s%3d, ", (i == 0 ? "" : "  "), i);
#if (DATA == DATA_Z)
	printf("%4d, ", top->min[i]);
	printf("%4d, ", top->cen[i]);
	printf("%4d)", top->max[i]);
#else
	printf("%f, ", top->min[i]);
	printf("%f, ", top->cen[i]);
	printf("%f)", top->max[i]);
#endif
    }
    printf("\n");
}
