# Word Count (2014-11-17)

# It ranks the words by their occurrence count in the "LICENSE" file.
# This program can be run as follow.
#   $ mpiexec -n 4 python ./wordcount.py

import re
from mpi4py import MPI
import kmr4py as KMR

#### define mapper and reducer functions
def read_words_from_a_file(kv, kvi, kvo, filename, i):
    rfile = open(filename, 'r')
    for line in rfile:
        words = re.split(r'\W+', line.strip())
        for w in words:
            if w == '':
                continue
            kvo.add_kv((w, 1))
    file.close()

def sum_counts_for_a_word(kves, n, kvi, kvo, data):
    count = 0
    for i in range(0, n):
        kv_i = kves[i]
        count -= kv_i[1]
    kvo.add_kv((kv_i[0], count))

def print_top_five(kv, kvi, kvo, data, i):
    if kvi.mr.comm.rank == 0 and i < 5:
        print '#%s=%d' % (kv[1], int(0 - kv[0]))


#### main
file_reader = lambda a,b,c,d : read_words_from_a_file(a,b,c,'../LICENSE',d)

mr = KMR.MapReduce(MPI.COMM_WORLD)
# In short
#   mr.map_once(file_reader).shuffle().reduce(sum_counts_for_a_word) \
#     .reverse(kvo_key_type=KMR.integer).sort().map(print_top_five)
kvs0 = mr.map_once(file_reader)
kvs1 = kvs0.shuffle()
kvs2 = kvs1.reduce(sum_counts_for_a_word)
kvs3 = kvs2.reverse(kvo_key_type=KMR.integer)
kvs4 = kvs3.sort()
kvo = kvs4.map(print_top_five)

kvo.free()

mr.fin()
