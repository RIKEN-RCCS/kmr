#!/usr/bin/env python3
# -*-coding: utf-8;-*-

## NOT KMR.  It checks fork-execing works from mpi proceeses.

import ctypes
import pickle
import os
from mpi4py import MPI

rank = MPI.COMM_WORLD.Get_rank()
os.system("echo echo from shell rank=" + str(rank) + ".")

so = ctypes.CDLL("./libforkexec.so")
so.forkexec()
