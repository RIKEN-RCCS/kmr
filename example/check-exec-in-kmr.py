#!/usr/bin/env python3
# -*-coding: utf-8;-*-

## check-exec-in-kmr.py

### Master-Worker Mappers Test.

## TESTED: map_ms_commands().

from mpi4py import MPI
import kmr4py
import time
import sys

kmr4py.kmrso_name = "../src/libkmr.so." + kmr4py.kmrversion

kmr4py.print_backtrace_in_map_fn = False

kmr0 = kmr4py.KMR("world")
kmr0.set_option("single_thread", "1")
kmr0.set_option("trace_map_ms", "1")
kmr0.set_option("map_ms_abort_on_signal", "1")

NPROCS = kmr0.nprocs
RANK = kmr0.rank

## TEST MAIN.

sys.stdout.flush()
sys.stderr.flush()
time.sleep(1)

if (RANK == 0):
    print("RUN exec_test()...")
    sys.stdout.flush()

kmr4py.kmrso.kmr_fork_exec_test_(kmr0._ckmr)

sys.stdout.flush()
sys.stderr.flush()
if (RANK == 0):
    time.sleep(1)
    print("check-exec-in-kmr OK")
    sys.stdout.flush()
