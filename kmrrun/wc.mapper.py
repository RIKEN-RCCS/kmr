#! /usr/bin/python
#
# wc.mapper.py (2014-10-31)
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
import re

if __name__ == "__main__":
    argv = sys.argv
    if (len(argv) != 2):
        sys.stderr.write("Specify an input file.\n")
        sys.exit(1);

    rf = open(argv[1])
    wf = open(argv[1] + ".out", 'w')
    line = rf.readline()
    while line:
        words = re.split('[\s/]+', line)
        for w in words:
            if (w == ''):
                continue
            wf.write("%s 1\n" % (w))
        line = rf.readline()
    rf.close
    wf.close
