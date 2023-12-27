# KMR - Kobe Map-Reduce

Copyright (C) 2012-2020 RIKEN R-CCS

## Overview

KMR is a high-performance map-reduce library in C/C++, Fortran, and
Python.  It simplifies data-processing by hiding the low-level details
of using MPI (the Message Passing Interface).  See
"http://riken-rccs.github.io/kmr" for API usage.

KMR provides a set of map-reduce operations in MPI (Message Passing
Interface), especially with a hope to be useful in Fortran.  It is
designed to work in-memory by exploiting the large amount of memory
available on supercomputers.  Data exchanges in KMR are, thus,
performed by message passing instead of remote file operations that
are the norm in popular map-reduce systems.

KMR includes functions found in fancy old data-parallel languages such
as Connection-Machine Lisp.  In the hindsight, the API design for
Fortran hindered the usefulness of such fancy functions.

## INSTALLATION

Building KMR requires a C compiler and an MPI library.  The Python
binding requires Python 3.4 and higher and the mpi4py package.  KMR
does not need any other uncommon libraries.  However, a few routines
have a restriction on the choice of MPI library (that is related to
the behaviour of spawning processes).  Note that recent KMR releases
are only lightly tested.

KMR can be installed by just typing "configure", "make", and "make
install".  To change the installation directory, specify the
"--prefix" option to the configure script.

    $ ./configure --prefix=PATH_TO_INSTALL
    $ make
    $ make install

To build KMR documents, type "make htmldoc".  It requres packages
Doxygen, Python's pdoc, and GNU Troff.  The documents are generated in
the "./html" directory.

    $ make htmldoc

To install KMR documents, type "make install-htmldoc" after installing
KMR binaries.

    $ make install-htmldoc

## INSTALLATION (for kudpc @kyoto-u.ac.jp)

Care should be taken in the Cray environment to use KMR with Python3.
KMR needs to be built with the same MPI library as mpi4py (the
Python-MPI binding).  But, the default environment is not the one used
to build the mpi4py (for example, at the sites like
kudpc.kyoto-u.ac.jp).  It needs to switch the compiler and the MPI
library appropriately before running the configure script.

    $ module switch PrgEnv-cray PrgEnv-gnu
    $ ./confugire ......

## INSTALLATION (for Fugaku)

Care should be taken to use KMR with Python3.  It needs CFLAGS=-Nclang
and CXXFLAGS=-Nclang in configuring KMR, to match the configuration of
mpi4py.  Also, setting some environment variables is needed to use
mpi4py.  See the documents of Fugaku.  It is recommended to start
Python3 with environment variable XOS_MMM_L_HPAGE_TYPE=none.

## FILES

The "src" directory contains the source code.  The "cmd" directory
contains the source code of the commands.  The "kmrrun" directory
contains the source code for shell command pipelining (or
"streaming").  The "python3" directory contains the source code for the
Pything binding.  The "shell" directory contains the source code of
the old shell command pipelining, which was mostly replaced by the
"kmrrun" command.  The "ex" directory contains a few examples.

The file "gensort-1.2.tar.gz" is a 3rd-party software of the data
generator of TeraSort.  The file "tpch_2_17_0.zip" is also a 3rd-party
software of the data generator of TPC-H.  These files are not part of
KMR, but they are needed for running some examples.

## EXPLORE FILES

* [CHANGELOG.md](CHANGELOG.md)
* Examples: [ex](ex)
* Tools: [tool](tool)

## COPYRIGHTS other than RIKEN R-CCS

The files in "src" directory include the materials copyrighted by
Akiyama Lab., Tokyo Institute of Technology (titec) (code from the
GHOST Project) and the materials copyrighted by The Regents of the
University of California (qsort code from NetBSD-5.1.2).  The files in
"ex" directory include the materials copyrighted by Stanford
University (code from Phoenix MapReduce Library), and the materials
copyrighted by Sandia Corporation (code from MapReduce-MPI Library).
The files in "gensort-1.2.tar.gz" are copyrighted by Chris Nyberg
(chris.nyberg@ordinal.com).  The files in "tpch_2_17_0.zip" are
copyrighted by the Transaction Processing Performance Council (TPC).

## ACKNOWLEDGEMENT

KMR is copyrighted by RIKEN R-CCS and distributed under the BSD
2-Clause License.  Part of the results is obtained by using Fugaku at
RIKEN R-CCS.
