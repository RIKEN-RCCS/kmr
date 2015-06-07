#! /bin/sh
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

file=$1.out

cat $file
rm $file
