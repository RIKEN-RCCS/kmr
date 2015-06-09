# K-Means (2014-11-19)

# An example of K-Means implementation.
# This program can be run as follow.
#   $ mpiexec -n 4 python ./kmeans.py

import random
from mpi4py import MPI
import kmr4py as KMR

class K_Means:
    def __init__(self):
        # Change the following variables
        self.n_iteration = 10
        self.grid_size   = 1000
        self.dim         = 3
        self.n_points    = 10000
        self.n_means     = 100
        self.means       = None
        self.points      = None

    def __str__(self):
        ostr =  '#### Configuration ###########################\n'
        ostr += 'Iteration           = %d\n' % (self.n_iteration)
        ostr += 'Grid size           = %d\n' % (self.grid_size)
        ostr += 'Dimension           = %d\n' % (self.dim)
        ostr += 'Number of clusters  = %d\n' % (self.n_means)
        ostr += 'Number of points    = %d\n' % (self.n_points)
        ostr += '##############################################'
        return ostr

    def init_means(self):
        self.means = []
        self._fill_randoms(self.means, self.n_means)

    def init_points(self):
        self.points = []
        self._fill_randoms(self.points, self.n_points)

    def _fill_randoms(self, tlst, count):
        for _ in range(0, count):
            lst = []
            for _ in range(0, self.dim):
                lst.append(random.randint(0, self.grid_size - 1))
            tlst.append(lst)


#### Function
def calc_sq_dist(v1, v2):
    sum = 0
    for (x, y) in zip(v1, v2):
        sum += (x - y) * (x - y)
    return sum


#### Mapper

# Emit Key:id of point(integer), Value:point(list of integer)
def load_points(kv, kvi, kvo, kmeans, i):
    for (idp, point) in enumerate(kmeans.points):
        kvo.add_kv((idp, point))


# Emit Key:id of nearest group, Value:point(list of integer)
def calc_cluster(kv, kvi, kvo, kmeans, i):
    min_id = 0
    min_dst = kmeans.grid_size * kmeans.grid_size
    for (idm, mean) in enumerate(kmeans.means):
        dst = calc_sq_dist(kv.val, mean)  # val is point
        if dst < min_dst:
            min_id = idm
            min_dst = dst
    kvo.add_kv((min_id, kv.val))


# Emit nothing
def copy_center(kv, kvi, kvo, kmeans, i):
    kmeans.means[kv[0]] = kv[1]


#### Reducer

# Emit Key:id of mean(integer),
#      Value:coordinates of center of the mean(list of integer)
def update_cluster(kves, n, kvi, kvo, data):
    sum = []
    for d in range(0, kmeans.dim):
        sum.append(0)
    for kv in kves:
        point = kv[1]
        for d in range(0, kmeans.dim):
            sum[d] += point[d]
    avg = [x / (len(kves)) for x in sum]
    kvo.add_kv(kves[0][0], avg)


#### main
comm = MPI.COMM_WORLD
mr = KMR.MapReduce(comm)
kmeans = K_Means()
random.seed()

if comm.rank == 0:
    print 'Number of processes = %d' % (comm.size)
    print kmeans
    kmeans.init_means()
kmeans.means = comm.bcast(kmeans.means, root=0)
kmeans.init_points()

# TODO: consider if 'kvo_key_type' can be removed
for _ in range(0, kmeans.n_iteration):
    # Map: load points to KVS
    kvs0 = mr.map_once(lambda a,b,c,d : load_points(a,b,c,kmeans,d),
                       kvo_key_type=KMR.integer)

    # Map: calculate cluster of each point
    kvs1 = kvs0.map(lambda a,b,c,d : calc_cluster(a,b,c,kmeans,d),
                    kvo_key_type=KMR.integer)

    # Shuffle
    kvs2 = kvs1.shuffle(kvo_key_type=KMR.integer)

    # Reduce: update cluster centers
    kvs3 = kvs2.reduce(lambda a,b,c,d : update_cluster(a,b,c,d,kmeans),
                       kvo_key_type=KMR.integer)

    # Share result
    kvs4 = kvs3.replicate(kvo_key_type=KMR.integer)

    # Copy cluster centers to kmeans class
    kvs4.map(lambda a,b,c,d : copy_center(a,b,c,kmeans,d))

    if comm.rank == 0:
        print 'Cluster coordinates'
        for mean in kmeans.means:
            print mean

mr.fin()
