#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "kmr.h"
#include <ctype.h>

#ifdef _OPENMP
#include <omp.h>
#endif

#define BUF_MAX 512
#define DAMPING_FACTOR 0.85

typedef struct {
    char *filename;
    int separate;
} FILE_INFO;

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

typedef struct {
    long node_id;
    double pagerank;
} NODE_PAGERANK;

typedef struct {
    enum {FROM_ID, TO_ID, TO_IDS, PAGERANK} type;
    long size;
} DATA_HEADER;


static int
sum_toids_for_a_fromid(const struct kmr_kv_box kv[], const long n,
                       const KMR_KVS *kvs, KMR_KVS *kvo, void *p)
{
    // Reduce function

    // make key: [url, pagerank]
    NODE_PAGERANK np = {
        .node_id = kv[0].k.i,
        .pagerank = 1.0
    };    

    // make val: outlink_list
    long *ToNodeIds = malloc((size_t)((long)sizeof(long) * n));
    for(int i = 0;i < n; i++){
        ToNodeIds[i] = kv[i].v.i;
    }

    // key: [url, pagerank], value: outlink_list 
    struct kmr_kv_box nkv = {
        .klen = sizeof(NODE_PAGERANK),
        .k.p  = (char *)&np,
        .vlen = (int)sizeof(long) * (int)n,
        .v.p  = (char *)ToNodeIds
    };
    kmr_add_kv(kvo, nkv);

    free(ToNodeIds);

    return MPI_SUCCESS;
}

static int
calc_pagerank_drain(const struct kmr_kv_box kv0,
                    const KMR_KVS *kvi, KMR_KVS *kvo, void *p, const long i)
{
    // Map function
    // get key: [url, pagerank], value: outlink_list 
    long  n = kv0.vlen / (long)sizeof(long);
    NODE_PAGERANK *np = (NODE_PAGERANK *)(kv0.k.p);

    for(int j = 0; j < n; j++){
        double pagerank_drain = np->pagerank / (double)n;
        long ToNodeId = ((long *)(kv0.v.p))[j];
        int vlen = sizeof(DATA_HEADER) + sizeof(double);
        DATA_HEADER *drain_header = malloc((size_t)vlen);
        drain_header->type = PAGERANK;
        drain_header->size = sizeof(double);
        *((double *)(drain_header + 1)) = pagerank_drain;

        // emit( key: outlink, value: pagerank/size(outlink_list) )
        struct kmr_kv_box kv = {
            .klen = sizeof(long),
            .k.i = (long)ToNodeId,
            .vlen = vlen,
            .v.p = (char *)drain_header
        };
        kmr_add_kv(kvo, kv);
        free(drain_header);
    }

    // emit( key: url, value: outlink_list )
    long FromNodeId = np->node_id;

    int vlen = (int)sizeof(DATA_HEADER) + (int)kv0.vlen;
    DATA_HEADER *header = malloc((size_t)vlen);
    header->type = TO_IDS;
    header->size = kv0.vlen;
    memcpy((char *)(header + 1), kv0.v.p, (size_t)kv0.vlen);

    struct kmr_kv_box kv = {
        .klen = sizeof(long),
        .k.i = (long)FromNodeId,
        .vlen = vlen,
        .v.p = (char *)header
    };
    kmr_add_kv(kvo, kv);
    free(header);

    return MPI_SUCCESS;
}

static int
sum_pagerank_for_a_toid(const struct kmr_kv_box kv[], const long n,
                        const KMR_KVS *kvs, KMR_KVS *kvo, void *p)
{
    // Reduce function
    double pagerank = 0.0;
    NODE_PAGERANK np = {.node_id = 0, .pagerank = 0.0};
    struct kmr_kv_box kvn = {.klen = 0};
    np.node_id = kv[0].k.i;

    for(int i = 0; i < n; i++){
        DATA_HEADER *h = (DATA_HEADER *)(kv[i].v.p);
        if(h->type == TO_IDS){
            kvn.vlen = (int)h->size;
            kvn.v.p  = (char *)(h + 1);
        }
        else{
            // pagerank
            pagerank += *((double *)(h+1));
        }
    }
    
    pagerank = 1.0 - DAMPING_FACTOR + ( DAMPING_FACTOR * pagerank );
    
    np.pagerank = pagerank;
    kvn.klen = sizeof(NODE_PAGERANK);
    kvn.k.p  = (char *)&np;

    kmr_add_kv(kvo, kvn);

    return MPI_SUCCESS;
}


static int
convert_kvs_pagerank_and_fromid(const struct kmr_kv_box kv0,
                                const KMR_KVS *kvi, KMR_KVS *kvo, void *p, const long i)
{
    NODE_PAGERANK *np = (NODE_PAGERANK *)kv0.k.p;
    struct kmr_kv_box kv = {
        .klen = sizeof(double),
        .k.d  = np->pagerank * (-1.0), // for descending order sort
        .vlen = sizeof(long),
        .v.i  = np->node_id
    };
    kmr_add_kv(kvo, kv);

    return MPI_SUCCESS;
}


static int
print_top_five(const struct kmr_kv_box kv0,
               const KMR_KVS *kvi, KMR_KVS *kvo, void *p, const long i)
{
    int rank = kvi->c.mr->rank;
    if (rank == 0 && i < 5) {
        printf("NodeId: %5ld = %lf\n", kv0.v.i, kv0.k.d * (-1.0));
        fflush(0);
    }
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
    kmr_shuffle(kvs0, kvs1, kmr_noopt);

    // convert kvs[key:FromId, val:ToId] to [key:[FromId, pagerank], val: ToId list]
    KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    kmr_reduce(kvs1, kvs2, 0, kmr_noopt, sum_toids_for_a_fromid);


    //// calculate pagerank ////
    KMR_KVS *kvs3;

    // get start time
    MPI_Barrier(MPI_COMM_WORLD);
    double stime = MPI_Wtime();

    for(int i = 1; i <= 100; i++){
        if(rank == 0){ fprintf(stdout, "progress %d / 100\n", i); }
        // set pagerank drain
        kvs3 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
        kmr_map(kvs2, kvs3, 0, kmr_noopt, calc_pagerank_drain);

        // share local kvs to all node
        KMR_KVS *kvs4 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE);
        kmr_shuffle(kvs3, kvs4, kmr_noopt);

        // renew pagegraph
        kvs2 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
        kmr_reduce(kvs4, kvs2, 0, kmr_noopt, sum_pagerank_for_a_toid);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    double etime = MPI_Wtime();
    if(rank == 0){
        printf("///// Information /////\n");
        printf("MPI nprocs\t\t\t: %d\n", nprocs);
#ifdef _OPENMP
        printf("OMP_MAX_THREADS\t\t: %d\n", (int)omp_get_max_threads());
#endif
        printf("calculation time\t: %lf seconds\n", etime - stime);
        printf("\n");
        printf("///// Top 5 pagerank /////\n");
    }
  
    //// print result ////
    // convert [key:[FromId, pagerank], val: ToId list] to [key: pagerank, val: FromId]
    KMR_KVS *kvs_fin = kmr_create_kvs(mr, KMR_KV_FLOAT8, KMR_KV_INTEGER);
    kmr_map(kvs2, kvs_fin, 0, kmr_noopt, convert_kvs_pagerank_and_fromid);

    // sort
    KMR_KVS *kvs_sorted_fin = kmr_create_kvs(mr, KMR_KV_FLOAT8, KMR_KV_INTEGER);
    kmr_sort(kvs_fin, kvs_sorted_fin, kmr_noopt);

    // show top5 pagerank and FromId
    struct kmr_option kmr_noth = {.nothreading = 1};
    kmr_map(kvs_sorted_fin, 0, 0, kmr_noth, print_top_five);


    // finish
    kmr_free_context(mr);
    kmr_fin();
    MPI_Finalize();
    return 0;
}
