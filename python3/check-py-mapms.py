#!/usr/bin/env python3
# -*-coding: utf-8;-*-

## check-py-mapms.py

### Master-Worker Mappers Test.

## TESTED: map_ms(), map_ms_commands().

from mpi4py import MPI
import kmr4py
import time
import sys

kmr4py.kmrso_name = "../src/libkmr.so." + kmr4py.kmrversion

THREADS = False
FORKEXEC = False
kmr4py.print_backtrace_in_map_fn = False

kmr0 = kmr4py.KMR("world")
kmr0.set_option("single_thread", ("0" if THREADS else "1"))
kmr0.set_option("trace_map_ms", "1")
kmr0.set_option("map_ms_use_exec", ("1" if FORKEXEC else "0"))
kmr0.set_option("map_ms_abort_on_signal", "1")

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

def identitymap(kv, kvi, kvo, i, *_data):
    (k, v) = kv
    ##print ("add @rank=%d k=%s v=%s" % (RANK, k, v))
    kvo.add(k, v)
    return 0

def run_map_ms():
    if (RANK == 0):
        print("RUN map_ms()...")
        sys.stdout.flush()

    k00 = kmr0.make_kvs()
    if (RANK == 0):
        for w in PHONETIC:
            k00.add(w[0:2], w)
    k00.add_kv_done()
    k01 = k00.sort()
    s01 = kmr4py.listify(k01)

    ## (NOTHREADING IS NEEDED???; HANGS OFTEN IF OMITTED)

    k02 = k01.map_ms(identitymap, nothreading=True)
    k03 = k02.sort()
    assert (kmr4py.listify(k03) == s01)
    k03.free()
    return 0

def run_map_ms_commands():
    if (RANK == 0):
        print("RUN map_ms_commands()...")
        sys.stdout.flush()

    if (FORKEXEC):
        command = ("sh\0-c\0echo start a subprocess.;"
        +" /bin/sleep 3; echo a process done.; exit 10")
    else:
        command = ("echo start a subprocess.;"
                   +" /bin/sleep 3; echo a process done.; exit 10")

    k10 = kmr0.make_kvs(value="cstring")
    if (RANK == 0):
        for i in range(0, 20):
            k10.add(("index=" + str(i)), command)
    k10.add_kv_done()
    k11 = k10.sort()
    s11 = kmr4py.listify(k11)
    k12 = k11.map_ms_commands(identitymap, nothreading=True,
                              separator_space=False, value="cstring")
    k13 = k12.sort()
    assert (kmr4py.listify(k13) == s11)
    k13.free()

## TEST MAIN.

sys.stdout.flush()
sys.stderr.flush()
time.sleep(1)

if (NPROCS == 1):
    print("map_ms() needs more than one ranks; test skipped")
    sys.stdout.flush()
else:
    run_map_ms()

if (NPROCS == 1):
    print("map_ms_commands() needs more than one ranks; test skipped")
    sys.stdout.flush()
else:
    run_map_ms_commands()

sys.stdout.flush()
sys.stderr.flush()
if (RANK == 0):
    print("check-py-mapms OK")
    sys.stdout.flush()
