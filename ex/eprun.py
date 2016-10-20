#!/usr/bin/env python

# eprun.py - Invokes shell commands on ranks (except rank=0).  It
# reads a file containing lines of shell commands (one per line) on
# rank=0.  It runs mulitple commands on each node (one per core) with
# option "-m".  USAGE: "mpiexec -n N python ./eprun.py
# ./file-of-commands"

from mpi4py import MPI
import kmr4py
from optparse import OptionParser
from optparse import HelpFormatter
from optparse import IndentedHelpFormatter
import time
import sys
import re

kmr0 = kmr4py.KMR("world")
NPROCS = kmr0.nprocs
RANK = kmr0.rank

class NullHelpFormatter(HelpFormatter):
    """Suppress helps on except rank=0."""

    def __init__(self,
                 indent_increment=0,
                 max_help_position=24,
                 width=None,
                 short_first=0):
        HelpFormatter.__init__(
            self, indent_increment, max_help_position, width, short_first)

    def format_option(self, option):
        return ""

    def format_heading(self, heading):
        return ""

    def format_text(self, text):
        return ""

    def format_usage(self, usage):
        return ""

if (RANK == 0):
    options = OptionParser()
else:
    options = OptionParser(formatter=NullHelpFormatter())

options.add_option("-t", "--trace",
                   dest="trace", action="store_true", default=False,
                   help="prints traces of invoking commands to stderr")
options.add_option("-m", "--per-core",
                   dest="percore", action="store_true", default=False,
                   help="invokes commands per core")

def read_commands(arg):
    k00 = kmr0.make_kvs(value="cstring")
    if (RANK == 0):
        f = open(arg)
        lines = f.readlines()
        f.close()
        for i, line in zip(range(len(lines)), lines):
            k00.add(i, "sh\0-c\0" + line.rstrip())
    k00.add_kv_done()
    return k00

def identitymap((k, v), kvi, kvo, i, *_data):
    kvo.add(k, v)
    return 0

## MAIN

(opts, args) = options.parse_args()

if (NPROCS == 1):
    sys.stderr.write("eprun needs more than one rank; abort.\n")
    sys.exit(1)

if (len(args) != 1):
    if (RANK == 0):
        sys.stderr.write("Usage: python eprun.py [options] input-file.\n")
    sys.exit(1)

if (opts.trace):
    kmr0.set_option("trace_map_ms", "1")
threading = (opts.percore)

sys.stdout.flush()
sys.stderr.flush()

k20 = read_commands(args[0])
k21 = k20.map_ms_commands(identitymap, nothreading=(not threading),
                          separator_space=False, value="cstring")

k21.free()
kmr0.dismiss()

sys.stdout.flush()
sys.stderr.flush()
#time.sleep(1)
#if (RANK == 0):
#    print "eprun OK"
#sys.stdout.flush()
