#!/usr/bin/env python
# testpyfiles.py

### File Reader Test

## TESTED: read_files_reassemble(), read_file_by_segments()

from mpi4py import MPI
import kmr4py
import time
import sys

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
    bs0 = kmr0.emptykvs.read_files_reassemble(filename, color, offset, bytes)

    f = open(filename)
    bs1 = f.read()
    f.close()
    assert (str(bs0) == bs1[offset : (offset + bytes)])

def run_read_files_reassemble():
    filename = "../LICENSE"
    color = 0
    bs0 = kmr0.emptykvs.read_file_by_segments(filename, color)

    f = open(filename)
    bs1 = f.read()
    f.close()
    assert (str(bs0) == bs1)

run_read_files_reassemble()
run_read_files_reassemble()

if (RANK == 0):
    print ("testpyfiles OK")
