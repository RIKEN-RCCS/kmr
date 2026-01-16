#!/bin/sh -x
#PJM --rsc-list "elapse=00:10:00"
#PJM --rsc-list "node=12"
#PJM --mpi "shape=4"
#PJM -S
. /work/system/Env_base
mpiexec ./a.out
