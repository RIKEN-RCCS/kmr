#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "kmr.h"
#include "kmrimpl.h"
#include <ctype.h>

#ifdef _OPENMP
#include <omp.h>
#endif


#define BUF_MAX 512
#define DAMPING_FACTOR 0.85


#define NEVERHERE 0

typedef struct {
    char *filename;
    int separate;
} FILE_INFO;

typedef struct {
    long node_id;
    double pagerank;
} NODE_PAGERANK;

typedef struct {
    enum {FROM_ID, TO_ID, TO_IDS, PAGERANK} type;
    long size;
} DATA_HEADER;


static void
print_usage(void)
{
    fprintf(stdout, "Usage: ./a.out [options] inputfile\n");
    fprintf(stdout, "Options:\n");
    fprintf(stdout, "\t-n number( > 2 )\n");
    fprintf(stdout, "\t\tnumber of file separation\n");
    fprintf(stdout, "\t\tif you use this option, you should set indexed files\n");
    fprintf(stdout, "\t\texample: [inputfile].000000, [inputfile].000001,..., [inputfile].[number]\n");
}

static int 
isdigits(const char *str)
{
    unsigned int i;
    for(i = 0; i < strlen(str); i++){
        if(isdigit(str[i]) == 0){
            return 0;
        }
    }
    return 1;
}

static int
parse_opt(int argc, char **argv, char *filename, int *sep)
{
    int ret = 0;
    int result = 0;

    *sep = -1;

    while((result = getopt(argc, argv, "n:")) != -1){
        switch(result){
        case 'n':
            if(isdigits(optarg)){
                sscanf(optarg, "%d", sep);
                break;
            }
        default:
            ret = 1;
        }
    }

    if(0 <= *sep && *sep < 2){
        ret = 1;
    }

    // get filename
    if(argv[optind] != NULL){
        strncpy(filename, argv[optind], BUF_MAX);
    }

    return ret;
}

static int
read_ids_from_a_file(const struct kmr_kv_box kv0,
                     const KMR_KVS *kvi, KMR_KVS *kvo, void *p, const long i)
{
    // Map function
    assert(kvi == 0 && kv0.klen == 0 && kv0.vlen == 0 && kvo != 0);
   
    FILE_INFO *fi = (FILE_INFO *)p;
    char buf[BUF_MAX];
    int rank = kvo->c.mr->rank;

    if(!(fi->separate == -1 || rank < fi->separate)){
        // this rank does not have to read files.
        return MPI_SUCCESS;
    }

    printf("i am %d, i read %s, sep = %d\n", rank, fi->filename, fi->separate);
    fflush(0);

    FILE *f = fopen(fi->filename, "r");

    if (f == NULL) {
        fprintf(stderr, "Cannot open a file \"%s\"; fopen(%s)\n", (char *)p, (char *)p);
        MPI_Abort(MPI_COMM_WORLD, 1);
        return 1;
    }
    
    while(fgets(buf, BUF_MAX, f) != NULL){
        int FromNodeId, ToNodeId;
        // skip comment or space or something...
        if(!isdigit(buf[0])){
            continue;
        }
        // read FromNodeId and ToNodeId
        sscanf(buf, "%d\t%d", &FromNodeId, &ToNodeId);
        struct kmr_kv_box kv = {
            .klen = sizeof(long),
            .k.i = FromNodeId,
            .vlen = sizeof(long),
            .v.i = ToNodeId
        };
        kmr_add_kv(kvo, kv);
    }
    
    fclose(f);
    return MPI_SUCCESS;
}


int
main(int argc, char **argv)
{
    int nprocs, rank, thlv;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &thlv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if(argc < 2){
        if(rank == 0){ print_usage(); }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    char filename[BUF_MAX];
    int sep = -1; // -1: no separate
    
    if(parse_opt(argc, argv, filename, &sep) != 0){
        // invaild option
        if(rank == 0){
            fprintf(stderr, "number has invaild digits\n");
            print_usage();
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    if( sep > nprocs ){
        if(rank == 0){
            fprintf(stderr, "MPI process is less than number of file separation!!");
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    if(sep != -1 && rank < sep){
        char tmp[BUF_MAX];
        strncpy(tmp, filename, BUF_MAX);
        // indexed filename example: web-graph.txt.000000
        snprintf(filename, BUF_MAX, "%s.%06d", tmp, rank);
    }

    // set FILE_INFO structure
    FILE_INFO fi = {.filename = filename, .separate = sep};

    kmr_init();
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {printf("Ranking pages...\n");}

    //// initialize ////
    // load web-graph from file
    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    kmr_map_once(kvs0, &fi, kmr_noopt, sep == -1 ? 1:0, read_ids_from_a_file);

    MPI_Barrier(MPI_COMM_WORLD);

    // share local kvs to all node
    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    mykmr_shuffle(kvs0, kvs1, kmr_noopt);


    // finish
    kmr_free_context(mr);
    kmr_fin();
    MPI_Finalize();
    return 0;
}
