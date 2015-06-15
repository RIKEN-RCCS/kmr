#!/usr/bin/env python
# testpymapms.py

### Master-Slave Mappers Test

## TESTED: map_ms(), map_ms_commands(), map_via_spawn(),
## map_processes()

from mpi4py import MPI
import kmr4py
import time
import sys
import ctypes

THREADS = True
kmr4py.print_backtrace_in_map_fn = False

kmr0 = kmr4py.KMR(1)
kmr0.set_option("single_thread", ("0" if THREADS else "1"))
kmr0.set_option("trace_map_spawn", "1")

NPROCS = kmr0.nprocs
RANK = kmr0.rank

PHONETIC = ["_asahi", "_iroha", "_ueno", "_eigo", "_osaka",
            "kawase", "kite", "kurabu", "kesiki", "kodomo",
            "sakura", "sinbun", "suzume", "sekai", "soroban",
            "tabako", "tidori", "turukame", "tegami", "tokyo",
            "nagoya", "nihon", "numazu", "nezumi", "nohara",
            "hagaki", "hikoki", "hujisan", "hewa", "hoken",
            "machi", "mikasa", "musen", "meiji", "momiji",
            "yamato", "yumiya", "yosino",
            "rajio", "ringo", "rusui", "renge", "roma",
            "warabi", "osimai"]

def identitymap((k, v), kvi, kvo, i, *_data):
    ##print ("add @rank=%d k=%s v=%s" % (RANK, k, v))
    kvo.add(k, v)
    return 0

def run_map_ms():
    if (RANK == 0): print "RUN map_ms()..."

    k00 = kmr0.make_kvs()
    if (RANK == 0):
        for w in PHONETIC:
            k00.add(w[0:2], w)
    k00.add_kv_done()
    k01 = k00.sort()
    s01 = kmr4py.listify(k01)

    ## (NOTHREADING IS NEEDED???; HANGS OFTEN IF OMIT)

    k02 = k01.map_ms(identitymap, nothreading=True)
    k03 = k02.sort()
    assert (kmr4py.listify(k03) == s01)
    k03.free()
    return 0

def run_map_ms_commands():
    if (RANK == 0): print "RUN map_ms_commands()..."

    k10 = kmr0.make_kvs(value="cstring")
    if (RANK == 0):
        for i in range(0, 20):
            k10.add(("index=" + str(i)), ("sleep 3"));
    k10.add_kv_done()
    k11 = k10.sort()
    s11 = kmr4py.listify(k11)
    k12 = k11.map_ms_commands(identitymap, nothreading=True,
                              separator_space=True, value="cstring")
    k13 = k12.sort()
    assert (kmr4py.listify(k13) == s11)
    k13.free()

## TEST MAIN

sys.stdout.flush()
sys.stderr.flush()
time.sleep(1)

if (NPROCS == 1):
    print "map_ms() needs more than one ranks; test skipped"
else:
    run_map_ms()

if (NPROCS == 1):
    print "map_ms_commands() needs more than one ranks; test skipped"
else:
    run_map_ms_commands()

sys.stdout.flush()
sys.stderr.flush()
if (RANK == 0):
    print "testpymapms OK"
sys.stdout.flush()
