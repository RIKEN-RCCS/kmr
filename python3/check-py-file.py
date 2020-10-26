#!/usr/bin/env python3
# -*-coding: utf-8;-*-

## check-py-file.py

### File Reader Test.

## TESTED: read_files_reassemble(), read_file_by_segments().

from mpi4py import MPI
import kmr4py
import time
import sys

kmr4py.kmrso_name = "../src/libkmr.so." + kmr4py.kmrversion

THREADS = True
kmr4py.print_backtrace_in_map_fn = False

kmr0 = kmr4py.KMR("world")
kmr0.set_option("single_thread", ("0" if THREADS else "1"))
kmr0.set_option("trace_map_ms", "1")

NPROCS = kmr0.nprocs
RANK = kmr0.rank

def run_read_files_reassemble():
    filename = "../LICENSE"
    color = 0
    offset = 0
    bytes = 100
    data0 = kmr0.emptykvs.read_files_reassemble(filename, color, offset, bytes)
    ss0 = data0.decode("latin-1")

    f = open(filename)
    ss1 = f.read()
    f.close()
    ss2 = ss1[offset : (offset + bytes)]

    assert (len(ss0) == bytes * NPROCS)
    for i in range(0, RANK):
        assert (ss0[(bytes * i) : (bytes * i + bytes)] == ss2)
    return None

def read_file_by_segments():
    filename = "../LICENSE"
    color = 0
    data = kmr0.emptykvs.read_file_by_segments(filename, color)
    ss0 = data.decode("latin-1")

    f = open(filename)
    ss1 = f.read()
    f.close()

    assert (ss0 == ss1)
    return None

run_read_files_reassemble()
read_file_by_segments()

if (RANK == 0):
    time.sleep(3)
    print("check-py-file OK")
    sys.stdout.flush()
