# Word Count (2015-06-13)

## This ranks the words by their occurrence count in the "../LICENSE"
## file.  It can be run under MPI as follows:
##   $ mpiexec -n 4 python wordcountpy.py

from mpi4py import MPI
import kmr4py
import re

file_name = "../LICENSE"

kmr = kmr4py.KMR(1)

def read_words_from_a_file(kv, kvi, kvo, i, *_data):
    file_ = open(file_name, "r")
    for line in file_:
        words = re.split(r"\W+", line.strip())
        for w in words:
            if (w != ""):
                kvo.add(w, 1)
    file_.close()

def print_top_five((k, v), kvi, kvo, i, *_data):
    ## (NO FIELD VALUE IN KMR.MR BECAUSE IT IS A DUMMY).
    if (kmr.rank == 0 and i < 5):
        print "#%s=%d" % (v, int(0 - k))

def sum_counts_for_a_word(kvvec, kvi, kvo, *_data):
    count = 0
    (k0, _) = kvvec[0]
    for (_, v) in kvvec:
        count += v
    kvo.add(k0, -count)

if (kmr.rank == 0): print "Ranking words..."

kvs0 = kmr.emptykvs.map_once(False, read_words_from_a_file, key="cstring")
kvs1 = kvs0.shuffle()
kvs2 = kvs1.reduce(sum_counts_for_a_word, key="cstring", value="integer")
kvs3 = kvs2.reverse()
kvs4 = kvs3.sort()
kvs4.map(print_top_five, output=False, nothreading=True)

kmr.dismiss()
kmr4py.fin()
