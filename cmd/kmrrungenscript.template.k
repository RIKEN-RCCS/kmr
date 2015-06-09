#!/bin/bash
#
#PJM --rsc-list "rscgrp=%(QUEUE)s"
#PJM --rsc-list "elapse=%(RSCTIME)s"
#PJM --rsc-list "node=%(NODE)s"
#PJM --mpi "shape=%(SHAPE)s"
#PJM --mpi "proc=%(PROC)s"
#PJM --stg-transfiles "all"
#PJM --stgin "%(KMRRUN)s ./kmrrun"
%(DATASTGIN)s
%(DATASTGOUT)s
# !!PJM --stgout "./core* ./"
#PJM -S

. /work/system/Env_base

%(EXEC)s
