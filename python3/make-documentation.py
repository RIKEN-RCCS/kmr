#!/usr/bin/env python3
# -*-coding: latin-1;-*-

## Runs pdoc on "kmr4py" and generates "index.html".  It imports a
## dummy "mpi4py", in order to load "kmr4py" without starting MPI.

import types
import sys
import pdoc

m = types.ModuleType("dummy_mpi4py")
sys.modules["mpi4py"] = m

mod = pdoc.import_module("kmr4py")
doc = pdoc.Module(mod)
s = doc.html()

f = open("index.html", "wt", encoding="utf-8")
f.write(s)
f.close()
