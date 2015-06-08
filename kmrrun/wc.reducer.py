#! /usr/bin/python
#
# wc.reducer.py (2014-10-31)
#
# The combination of wc.mapper.py, wc.kvgen.sh and wc.reducer.py performs
# word counting of files in a specified directory.
#
# How to run this program.
#
# 1. Prepare input files
#    $ mkdir ./inp
#    $ cp ../file1 ./inp
#    $ cp ../file2 ./inp
#
#    There are two files so that two mappers will be run to process them.
#
# 2. Execute kmrrun
#    $ mpiexec -machinefile machines -np 2 ./kmrrun \
#    -m ./wc.mapper.py -k ./wc.kvgen.sh -r ./wc.reducer.py ./inp
#

import sys
import os

if __name__ == "__main__":
    argv = sys.argv
    if (len(argv) != 2):
        sys.stderr.write("Specify an input file.\n")
        sys.exit(1)

    key = None
    count = 0
    rf = open(argv[1])
    line = rf.readline()
    while line:
        (k,ns) = line.split()
        if (key == None):
            key = k
        n = int(ns)
        count += n
        line = rf.readline()
    rf.close()

    os.remove(argv[1])
    print "%s %d" % (key, count)
