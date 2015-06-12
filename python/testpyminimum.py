## testpyminimum.py

### Very Basics Test

## TESTED: add(), add_kv_done(), get_element_count(),
## local_element_count(), map(), map_once(), reverse(), shuffle(),
## reduce(), reduce_as_one(), replicate(), distribute(),
## sort_locally(), sort(), concatenate(), map_rank_by_rank()

## UNTESTED: map_for_some(), map_ms(), map_ms_commands(),
## map_via_spawn(), map_processes(), reduce_for_some(),
## read_files_reassemble(), read_file_by_segments(), save(), restore()

from mpi4py import MPI
import kmr4py
import time
import sys

### SETTINGS

THREADS = True

kmr0 = kmr4py.KMR(1)
kmr0.set_option("single_thread", ("0" if THREADS else "1"))

NPROCS = kmr0.nprocs
RANK = kmr0.rank

if (RANK == 0): print "CHECK options..."

kmr4py._check_passing_options()

# SAMPLE LIST; IT SHOULD BE SORTED LIST:

XX = [(0, "re"), (1, "he"), (2, "fu"), (3, "me"),
       (4, "yo"), (5, "its"), (6, "moo"), (7, "nana"),
       (8, "ya"), (9, "kono"), (10, "toe")]
LL = len(XX)

YY = []
for i in range(0, NPROCS):
    YY.extend(XX)

k00 = kmr0.make_kvs(key="integer")
for (k, v) in XX:
    k00.add(k, v)
k00.add_kv_done()

s00 = kmr4py.listify(k00)
assert (s00 == XX)
k00.free()

k01 = kmr0.make_kvs(key="opaque")
for (k, v) in XX:
    k01.add(k, v)
k01.add_kv_done()

s01 = kmr4py.listify(k01)
assert (s01 == XX)

sys.stdout.flush()
sys.stderr.flush()
if (RANK == 0): print "CHECK unfreed kvs message..."
sys.stdout.flush()

time.sleep(1)
kmr0.dismiss()
time.sleep(1)

### MAPPING/REDUCING

kmr1 = kmr4py.KMR(1)
kmr1.set_option("single_thread", ("0" if THREADS else "1"))

NN = NPROCS

if (RANK == 0): print "RUN map_once()..."

def k10add(kv, kvi, kvo, data, i):
    for (k, v) in XX:
        kvo.add(k, v)
    return 0

k10 = kmr1.make_kvs().map_once(False, k10add, key="integer")
s10 = kmr4py.listify(k10)
assert (s10 == XX)
assert (k10.local_element_count() == LL)

if (RANK == 0): print "RUN replicate()..."

k11 = k10.replicate()
assert (k11.local_element_count() == NN * LL)
s11 = kmr4py.listify(k11)
assert (s11 == YY)

if (RANK == 0): print "RUN map()..."

def k12add((k, v), kvi, kvo, data, i):
    kvo.add(k, v)
    return 0

k12 = k11.map(k12add, key="integer")
assert (k12.local_element_count() == NN * LL)

# (NOTE: Order in k12 can change.)

if (RANK == 0): print "RUN distribute()..."

k13 = k12.distribute(True)
assert (k13.get_element_count() == NN * LL * NPROCS)

if (RANK == 0): print "RUN shuffle()..."

k14 = k13.shuffle()
assert (k14.get_element_count() == NN * LL * NPROCS)

if (RANK == 0): print "RUN reduce()..."

def k15add(kvvec, n, kvi, kvo, data):
    assert (n == (NN * NPROCS))
    for v in kvvec:
        assert (v == kvvec[0])
    for (k, v) in kvvec:
        kvo.add(k, v)
    return 0

k15 = k14.reduce(k15add, key="integer")
assert (k15.get_element_count() == NN * LL * NPROCS)

if (RANK == 0): print "RUN shuffle()..."

def k16add(kvvec, n, kvi, kvo, data):
    (k, v) = kvvec[0]
    kvo.add(k, v)
    return 0

k16 = k15.shuffle().reduce(k16add, key="integer")
k17 = k16.replicate().sort_locally(False)

s17 = kmr4py.listify(k17)
assert (s17 == XX)

k17.free()
kmr1.dismiss()

### REDUCE_AS_ONE/SORT

kmr2 = kmr4py.KMR(1)
kmr2.set_option("single_thread", ("0" if THREADS else "1"))

k20 = kmr2.emptykvs.map_once(False, k10add, key="integer")
s20 = kmr4py.listify(k20)
assert (s20 == XX)

if (RANK == 0): print "RUN reduce_as_one()..."

def k21add(kvvec, n, kvi, kvo, data):
    assert (n == LL)
    for (k, v) in kvvec:
        kvo.add(k, v)
    return 0

k21 = k20.reduce_as_one(k21add, key="integer")
s21 = kmr4py.listify(k21)
assert (s21 == XX)

if (RANK == 0): print "RUN sort()..."

k22 = k21.sort()
assert (k22.get_element_count() == LL * NPROCS)

s22 = kmr4py.listify(k22)
for i in range(1, len(s22)):
    (k0, v0) = s22[i - 1]
    (k1, v1) = s22[i]
    assert(k0 <= k1)

k22.free()
kmr2.dismiss()

### CONCATENATE

kmr3 = kmr4py.KMR(1)
kmr3.set_option("single_thread", ("0" if THREADS else "1"))

k30 = kmr3.make_kvs(key="integer")
k30.add_kv_done()

if (RANK == 0): print "RUN concatenate()..."

k31 = []
for i in range(0, NPROCS):
    k31.append(kmr3.emptykvs.map_once(False, k10add, key="integer"))
    s31 = kmr4py.listify(k31[i])
    assert (s31 == XX)

k32 = k30.concatenate(*k31)
s32 = kmr4py.listify(k32)
assert(s32 == YY)

k32.free()
kmr3.dismiss()

### REVERSE

kmr4 = kmr4py.KMR(1)
kmr4.set_option("single_thread", ("0" if THREADS else "1"))

k40 = kmr4.emptykvs.map_once(False, k10add, key="integer")
s40 = kmr4py.listify(k40)
assert (s40 == XX)

if (RANK == 0): print "RUN reverse()..."

k41 = k40.reverse()
k42 = k41.reverse()
k43 = k42.sort_locally(False)
s43 = kmr4py.listify(k43)
assert (s43 == XX)
k43.free()

k44 = kmr4.make_kvs().map_once(False, k10add, key="integer")
s44 = kmr4py.listify(k44)
assert (s44 == XX)

if (RANK == 0): print "RUN map_rank_by_rank()..."

k45 = k44.map_rank_by_rank(k12add, key="integer")
k46 = k45.sort_locally(False)
s46 = kmr4py.listify(k46)
assert (s46 == XX)

k46.free()
kmr4.dismiss()

sys.stdout.flush()
sys.stderr.flush()
if (RANK == 0):
    print "testminimum OK"
sys.stdout.flush()
