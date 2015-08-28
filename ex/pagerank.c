#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "kmr.h"
#include <ctype.h>

#define BUF_MAX 512
#define DAMPING_FACTOR 0.85

static int
read_ids_from_a_file(const struct kmr_kv_box kv0,
                     const KMR_KVS *kvi, KMR_KVS *kvo, void *p, const long i)
{
    // Map function
    assert(kvi == 0 && kv0.klen == 0 && kv0.vlen == 0 && kvo != 0);
    char buf[BUF_MAX];
    FILE *f = fopen((char *)p, "r");
    //  FILE *f = fopen("simple_graph.txt", "r");
    if (f == 0) {
        fprintf(stderr, "Cannot open a file \"%s\"; fopen(%s)\n", (char *)p, (char *)p);
        MPI_Abort(MPI_COMM_WORLD, 1);
        return 0;
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

    if(argc != 2){
        if(rank == 0){ fprintf(stderr, "Please set web-graph filename.\n"); }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    char filename[BUF_MAX];
    strncpy(filename, argv[1], BUF_MAX);

    kmr_init();
    KMR *mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL, 0);

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {printf("Ranking words...\n");}

    //// initialize ////
    // load web-graph from file
    KMR_KVS *kvs0 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    kmr_map_once(kvs0, filename, kmr_noopt, 1, read_ids_from_a_file);

    // share local kvs to all node
    KMR_KVS *kvs1 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_INTEGER);
    kmr_shuffle(kvs0, kvs1, kmr_noopt);

    // convert kvs[key:FromId, val:ToId] to [key:[FromId, pagerank], val: ToId list]
    KMR_KVS *kvs2 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE);
    kmr_reduce(kvs1, kvs2, 0, kmr_noopt, sum_toids_for_a_fromid);



    //// calculate pagerank ////
    KMR_KVS *kvs3;

    for(int i = 0; i < 100; i++){
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

  
    //// print result ////
    // convert [key:[FromId, pagerank], val: ToId list] to [key: pagerank, val: FromId]
    KMR_KVS *kvs_fin = kmr_create_kvs(mr, KMR_KV_FLOAT8, KMR_KV_INTEGER);
    kmr_map(kvs2, kvs_fin, 0, kmr_noopt, convert_kvs_pagerank_and_fromid);

    /* KMR_KVS *kvs_shuffled_fin = kmr_create_kvs(mr, KMR_KV_FLOAT8, KMR_KV_INTEGER); */
    /* kmr_shuffle(kvs_fin, kvs_shuffled_fin, kmr_noopt); */

    // sort
    KMR_KVS *kvs_sorted_fin = kmr_create_kvs(mr, KMR_KV_FLOAT8, KMR_KV_INTEGER);
    //    kmr_sort(kvs_shuffled_fin, kvs_sorted_fin, kmr_noopt);
    kmr_sort(kvs_fin, kvs_sorted_fin, kmr_noopt);

    // show top5 pagerank and FromId
    kmr_map(kvs_sorted_fin, 0, 0, kmr_noopt, print_top_five);


    // finish
    kmr_free_context(mr);
    kmr_fin();
    MPI_Finalize();
    return 0;
}
