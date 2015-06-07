#!/bin/sh -x
#PJM --rsc-list "elapse=00:10:00"
#PJM --rsc-list "rscgrp=small"
#PJM --rsc-list "node=12"
#PJM --mpi "shape=4"
#PJM -S
#PJM --mpi "use-rankdir"
#PJM --stgin "rank=* ./a.out %r:./"
#PJM --stg-transfiles "all"
. /work/system/Env_base
mpiexec ./a.out
