#!/usr/bin/env python3
# -*-coding: utf-8;-*-

# run-many-pi-by-spawn.py -- This example runs multiple MPI
# applications ("pi" for instance) via kmr_map_processes().  This
# example is rather meaningless as "pi" just runs independently.  Note
# that kmr_map_processes() is to run independent MPI applications,
# while kmr_map_via_spawn() is to run KMR applications which will
# communicate with the parent process in KMR.

# To use kmr4py, use the following setting.
#
# | export LD_LIBRARY_PATH=${KMRHOME}/lib:${LD_LIBRARY_PATH}
# | export PYTHONPATH=${KMRHOME}/lib:{PYTHONPATH}

# To use mpi4py on Fugaku, it requires weird setting.
#
# | module load Python3-CN
# | pytorch=/home/apps/oss/PyTorch-1.7.0
# | export PATH=${pytorch}/bin:${PATH}
# | export LD_LIBRARY_PATH=${pytorch}/lib:${LD_LIBRARY_PATH}
# | export PYTHONPATH=${pytorch}/lib/python3.8/site-packages:${PYTHONPATH}
# | export XOS_MMM_L_HPAGE_TYPE=none
# | export FLIB_CNTL_BARRIER_ERR=FALSE
# | export LD_PRELOAD=/usr/lib/FJSVtcs/ple/lib64/libpmix.so
#
# See https://www.r-ccs.riken.jp/fugaku/docs/user-guide/sys-use/user-guide-lang-1.37/build/ja/ScriptingLanguage/index.html
#
# or the below one may be added (???)
# | export LD_PRELOAD=libtcmalloc.so:${LD_PRELOAD}

# To use dynamic processes (spawning) on Fugaku, it needs setting in a
# job-script on the number processes: "proc" is for static processes
# and "node" for total processes (static+dynamic).
#
# | #PJM -L "node=16"
# | #PJM --mpi "proc=1"
#
# See https://www.r-ccs.riken.jp/fugaku/docs/user-guide/sys-use/user-guide-use-1.52/build/ja/MPI/DynamicProcessCreation%28MPI_Comm_spawn%29.html

# An example job script on Fugaku.
#
# | #!/bin/bash -x
# | #PJM -g groupname??
# | #PJM -L "elapse=00:10:00"
# | #PJM -L "rscgrp=small"
# | #PJM -L "node=16"
# | #PJM --mpi "proc=1"
# | #PJM -x PJM_LLIO_GFSCACHE=/vol0000??
# | #PJM -S
# | module load Python3-CN
# | export LD_LIBRARY_PATH=${pytorch}/lib:${LD_LIBRARY_PATH}
# | export LD_LIBRARY_PATH=$KMRHOME/lib:${LD_LIBRARY_PATH}
# | export PYTHONPATH=${pytorch}/lib/python3.8/site-packages:${PYTHONPATH}
# | export PYTHONPATH=$KMRHOME/lib:{PYTHONPATH}
# | export XOS_MMM_L_HPAGE_TYPE=none
# | export FLIB_CNTL_BARRIER_ERR=FALSE
# | export LD_PRELOAD=/usr/lib/FJSVtcs/ple/lib64/libpmix.so
# | mpiexec -n 1 python3 run-many-pi-via-spawn.py

import time
import sys
import os

n_procs = 4
n_spawns = 16

## SPAWNER.

def run_map_processes(kmr0):
    if (RANK == 0):
        print("Run many mpi-pi...")
        sys.stdout.flush()
        pass

    if (RANK == 0):
        print("Spawn %d-proc work %d times using %d dynamic processes.\n"
              % (n_procs, n_spawns, 0))
        sys.stdout.flush()
        pass

    k00 = kmr0.make_kvs(value="cstring")
    if (RANK == 0):
        for _ in range(0, n_spawns):
            k00.add("key", f"maxprocs={n_procs} ./pi")
            pass
        pass
    k00.add_kv_done()

    def called_at_app_finish(kv, kvi, kvo, index):
        comm = kmr0.get_spawner_communicator(index)
        print(f"App finishes (index={index}, comm={comm}).")
        print("Maybe, check the output from the app.")
        sys.stdout.flush()
        return 0

    k01 = k00.map_processes(False, called_at_app_finish, separator_space=1)
    k01.free()
    return

from mpi4py import MPI
import kmr4py

kmr4py.kmrso_name = "../src/libkmr.so." + kmr4py.kmrversion
kmr0 = kmr4py.KMR("world")
kmr0.set_option("single_thread", "1")
kmr0.set_option("trace_map_spawn", "1")
#kmr0.set_option("spawn_max_processes", "4")

NPROCS = kmr0.nprocs
RANK = kmr0.rank

if (RANK == 0):
    usize1 = MPI.INFO_ENV.get("maxprocs")
    usize2 = MPI.COMM_WORLD.Get_attr(MPI.UNIVERSE_SIZE)
    print("Running this test needs a number of dynamic processes.")
    print(f"Universe is maxprocs={usize1} and usize={usize2}.")
    sys.stdout.flush()
    pass

run_map_processes(kmr0)

kmr0.dismiss()

if (RANK == 0):
    time.sleep(3)
    print("DONE.")
    sys.stdout.flush()
    pass
