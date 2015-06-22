# K-Means (2015-06-15)

## An example of K-Means implementation.
## It can be run under MPI as follows:
##   $ mpiexec -n 4 python kmeanspy.py

import random
from mpi4py import MPI
import kmr4py

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

def calc_sq_dist(v1, v2):
    sum_ = 0
    for (x, y) in zip(v1, v2):
        sum_ += (x - y) * (x - y)
    return sum_

# Emit Key:id of point(integer), Value:a point(list of integer)
def load_points(kv, kvi, kvo, i):
    del kv, kvi, i
    for (idp, point) in enumerate(kmeans.points):
        kvo.add(idp, point)

# Emit Key:id of nearest group, Value:a point(list of integer)
def calc_cluster((k, v), kvi, kvo, i):
    del k, kvi, i
    min_id = 0
    min_dst = kmeans.grid_size * kmeans.grid_size
    for (idm, mean) in enumerate(kmeans.means):
        dst = calc_sq_dist(v, mean)
        if dst < min_dst:
            min_id = idm
            min_dst = dst
    kvo.add(min_id, v)

# Emit nothing
def copy_center((k, v), kvi, kvo, i):
    del kvi, kvo, i
    kmeans.means[k] = v

# Emit Key:id of group(integer),
#      Value:coordinates of center of the group(list of integer)
def update_cluster(kvvec, kvi, kvo):
    del kvi
    sum_ = []
    for d in range(0, kmeans.dim):
        sum_.append(0)
    for (_, v) in kvvec:
        for d in range(0, kmeans.dim):
            sum_[d] += v[d]
    avg = [x / (len(kvvec)) for x in sum_]
    kvo.add_kv(kvvec[0][0], avg)


#### main
comm = MPI.COMM_WORLD
kmr = kmr4py.KMR("world")
kmeans = K_Means()
random.seed(1)

if comm.rank == 0:
    print 'Number of processes = %d' % (comm.size)
    print kmeans
    kmeans.init_means()
kmeans.means = comm.bcast(kmeans.means, root=0)
kmeans.init_points()

for _ in range(0, kmeans.n_iteration):
    kvs0 = kmr.emptykvs.map_once(False, load_points, key="integer")
    kvs1 = kvs0.map(calc_cluster, key="integer")
    kvs2 = kvs1.shuffle()
    kvs3 = kvs2.reduce(update_cluster, key="integer")
    kvs4 = kvs3.replicate()
    kvs4.map(copy_center)

    if comm.rank == 0:
        print 'Cluster coordinates'
        for m in kmeans.means:
            print m

kmr.dismiss()
kmr4py.fin()
