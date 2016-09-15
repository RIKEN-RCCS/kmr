#!/usr/bin/env python

# wfrun.pi.py - Sample script for WFRUN.  It runs "pi" (calculates pi)
# many times.  This is the master part WFRUN.  It is interpreted by a
# Python interpreter invoked by "wfrun", and the interpreter marker at
# the first line (#!) is meaningless.  For running this script, see
# the comment in "wfrun.c".  Pass this script as an argument to
# "wfrun" ("mpiexec wfrun wfrun.pi.py").

from mpi4py import MPI
import sys
import kmr4py

kmr0 = kmr4py.KMR("world")
NPROCS = kmr0.nprocs
RANK = kmr0.rank

if (NPROCS < 5):
    print "USAGE: mpiexec -n N ./wfrun wfrun.pi.py (with N>=5)"
    sys.exit(0)

masterrank = NPROCS - 1

# Makes an array of strings of "I:N", where I for lane number and N
# for the number of ranks in the lane.

def make_lane_description():
    print "NPROCS=" + str(NPROCS)
    lanes = ["0.0:2", "0.1:2"]
    return lanes

# Makes a KVS filled with key-value pairs (LANE,COMMAND), where LANE
# is 0 to (|lanes|-1), and COMMAND is "pi".

def make_works():
    nlanes = 2
    k00 = kmr0.make_kvs(key="cstring", value="cstring")
    for i in range(5):
        k00.add("0.0", "pi with-2-ranks")
        k00.add("0.1", "pi with-2-ranks")
    k00.add("0", "pi with-4-ranks")
    for i in range(5):
        k00.add("0.0", "pi with-2-ranks")
        k00.add("0.1", "pi with-2-ranks")
    k00.add("0", "pi with-4-ranks")
    k00.add_kv_done()
    return k00

## MAIN

sys.stdout.flush()
sys.stderr.flush()

kmr0.set_option("trace_map_spawn", "true")
kmr0.set_option("swf_record_history", "true")

lanes = make_lane_description()
splitcomms = kmr0.split_swf_lanes(masterrank, lanes, True)
kmr0.init_swf(splitcomms, masterrank)
kmr0.dump_swf_lanes()

kmr0.detach_swf_workers()

k20 = make_works()
k20.map_swf(None, separator_space=True, output=False)

kmr0.stop_swf_workers()
kmr0.dismiss()

sys.stdout.flush()
sys.stderr.flush()
