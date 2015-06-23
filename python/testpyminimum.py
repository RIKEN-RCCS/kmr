#!/usr/bin/env python
## testpyminimum.py

### Very Basics Test

## TESTED: add(), add_kv_done(), get_element_count(),
## local_element_count(), map(), map_once(), reverse(), shuffle(),
## reduce(), reduce_as_one(), replicate(), distribute(),
## sort_locally(), sort(), concatenate(), map_rank_by_rank(),
## map_for_some(), reduce_for_some(), save(), restore()

## NOT TESTED HERE: map_ms(), map_ms_commands(), map_via_spawn(),
## map_processes(), read_files_reassemble(), read_file_by_segments()

from mpi4py import MPI
import kmr4py
import time
import sys
import ctypes

### SETTINGS

THREADS = True
kmr4py.print_backtrace_in_map_fn = False

kmr0 = kmr4py.KMR("world")
kmr0.set_option("single_thread", ("0" if THREADS else "1"))

NPROCS = kmr0.nprocs
RANK = kmr0.rank

if (RANK == 0): print "CHECK configurations..."

kmr4py._check_ctypes_values()
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

k01 = kmr0.make_kvs(key="float8")
for (k, v) in XX:
    k01.add(float(k), v)
k01.add_kv_done()
sxx = map((lambda (k, v): (float(k), v)), XX)
s01 = kmr4py.listify(k01)
assert (s01 == sxx)
k01.free()

k02 = kmr0.make_kvs(key="opaque", value="cstring")
for (k, v) in XX:
    k02.add(k, v)
k02.add_kv_done()
s02 = kmr4py.listify(k02)
assert (s02 == XX)

sys.stdout.flush()
sys.stderr.flush()
if (RANK == 0): print "CHECK printing exceptions..."
sys.stdout.flush()

## k02exception raises an exception by mismatch of the number of
## parameters:

def k02exception():
    pass

time.sleep(1)
k02.map(k02exception, inspect=True)

time.sleep(1)
sys.stdout.flush()
sys.stderr.flush()
if (RANK == 0): print "CHECK unfreed kvs message..."
sys.stdout.flush()

## k02 is left unfreed and issues a warning message:

time.sleep(1)
kmr0.dismiss()
time.sleep(1)

### MAPPING/REDUCING

kmr1 = kmr4py.KMR("world")
kmr1.set_option("single_thread", ("0" if THREADS else "1"))

NN = NPROCS

if (RANK == 0): print "RUN map_once()..."

def putXXlist(kv, kvi, kvo, i, *_data):
    for (k, v) in XX:
        kvo.add(k, v)
    return 0

k10 = kmr1.make_kvs().map_once(False, putXXlist, key="integer")
s10 = kmr4py.listify(k10)
assert (s10 == XX)
assert (k10.local_element_count() == LL)

if (RANK == 0): print "RUN replicate()..."

k11 = k10.replicate()
assert (k11.local_element_count() == NN * LL)
s11 = kmr4py.listify(k11)
assert (s11 == YY)

if (RANK == 0): print "RUN map()..."

def identitymap((k, v), kvi, kvo, i, *_data):
    kvo.add(k, v)
    return 0

k12 = k11.map(identitymap, key="integer")
assert (k12.local_element_count() == NN * LL)

# (NOTE: Order in k12 can change.)

if (RANK == 0): print "RUN distribute()..."

k13 = k12.distribute(True)
assert (k13.get_element_count() == NN * LL * NPROCS)

if (RANK == 0): print "RUN shuffle()..."

k14 = k13.shuffle()
assert (k14.get_element_count() == NN * LL * NPROCS)

if (RANK == 0): print "RUN reduce()..."

def identityred(keycheck, countn):
    def identity_reduce_with_check_count(kvvec, kvi, kvo, *_data):
        assert (len(kvvec) == countn)
        if (keycheck):
            (k0, _0) = kvvec[0]
            for (k, _1) in kvvec:
                assert (k == k0)
        for (k, v) in kvvec:
            kvo.add(k, v)
        return 0
    return identity_reduce_with_check_count

k15 = k14.reduce(identityred(True, (NN * NPROCS)), key="integer")
assert (k15.get_element_count() == NN * LL * NPROCS)

if (RANK == 0): print "RUN shuffle()..."

def addonered(kvvec, kvi, kvo, *_data):
    (k, v) = kvvec[0]
    kvo.add(k, v)
    return 0

k16 = k15.shuffle().reduce(addonered, key="integer")
k17 = k16.replicate().sort_locally(False)
s17 = kmr4py.listify(k17)
assert (s17 == XX)

k17.free()
kmr1.dismiss()

### REDUCE_AS_ONE/SORT

kmr2 = kmr4py.KMR("world")
kmr2.set_option("single_thread", ("0" if THREADS else "1"))

k20 = kmr2.emptykvs.map_once(False, putXXlist, key="integer")
s20 = kmr4py.listify(k20)
assert (s20 == XX)

if (RANK == 0): print "RUN reduce_as_one()..."

k21 = k20.reduce_as_one(identityred(False, LL), key="integer")
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

kmr3 = kmr4py.KMR("world")
kmr3.set_option("single_thread", ("0" if THREADS else "1"))

k30 = kmr3.make_kvs(key="integer")
k30.add_kv_done()

if (RANK == 0): print "RUN concatenate()..."

k31 = []
for i in range(0, NPROCS):
    k31.append(kmr3.emptykvs.map_once(False, putXXlist, key="integer"))
    s31 = kmr4py.listify(k31[i])
    assert (s31 == XX)

k32 = k30.concatenate(*k31)
s32 = kmr4py.listify(k32)
assert(s32 == YY)

k32.free()
kmr3.dismiss()

### REVERSE

kmr4 = kmr4py.KMR("world")
kmr4.set_option("single_thread", ("0" if THREADS else "1"))

k40 = kmr4.emptykvs.map_once(False, putXXlist, key="integer")
s40 = kmr4py.listify(k40)
assert (s40 == XX)

if (RANK == 0): print "RUN reverse()..."

k41 = k40.reverse()
k42 = k41.reverse()
k43 = k42.sort_locally(False)
s43 = kmr4py.listify(k43)
assert (s43 == XX)
k43.free()

k44 = kmr4.make_kvs().map_once(False, putXXlist, key="integer")
s44 = kmr4py.listify(k44)
assert (s44 == XX)

if (RANK == 0): print "RUN map_rank_by_rank()..."

k45 = k44.map_rank_by_rank(identitymap, key="integer")
k46 = k45.sort_locally(False)
s46 = kmr4py.listify(k46)
assert (s46 == XX)

if (RANK == 0): print "RUN map_for_some()..."

c46 = k46.get_element_count()
k47 = k46.map_for_some(identitymap, inspect=True)
c47 = k47.get_element_count()
assert (c47 > 0 and c47 <= c46)
k47.free()

if (RANK == 0): print "RUN reduce_for_some()..."

k48 = k46.reduce_for_some(identityred(True, 1), inspect=True)
c48 = k48.get_element_count()
assert (c48 > 0 and c48 <= c46)
k48.free()

k46.free()
kmr4.dismiss()

### SAVE/RESTORE

kmr5 = kmr4py.KMR("world")
kmr5.set_option("single_thread", ("0" if THREADS else "1"))

k50 = kmr5.emptykvs.map_once(False, putXXlist, key="integer")
s50 = kmr4py.listify(k50)
assert (s50 == XX)

if (RANK == 0): print "RUN map() with c-function..."

k51 = k50.map(kmr4py.kmrso.kmr_reverse_fn, inspect=True,
              key="opaque", value="integer")
k52 = k51.sort()
k53 = k50.reverse(inspect=True)
k54 = k53.sort()
s52 = kmr4py.listify(k52)
s54 = kmr4py.listify(k54)
assert (s52 == s54)
k52.free()
k54.free()

if (RANK == 0): print "RUN save() and restore()..."

b50 = k50.save()

k53 = kmr5.emptykvs.restore(b50)
s53 = kmr4py.listify(k53)
assert (s53 == XX)

k50.free()
k53.free()
kmr5.dismiss()

sys.stdout.flush()
sys.stderr.flush()
if (RANK == 0):
    print "testpyminimum OK"
sys.stdout.flush()
