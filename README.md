# KMR - Kobe Map-Reduce

Copyright (C) 2012-2018 RIKEN R-CCS

KMR comes with ABSOLUTELY NO WARRANTY.

This is KMR, a high-performance map-reduce library.  It simplifies
coding for parallel processing in C or Fortran with MPI (the Message
Passing Interface).  See "http://mt.r-ccs.riken.jp/kmr" for the
information.  See "http://riken-rccs.github.io/kmr" for an overview
and API usage.

## FILES

The "src" directory contains the source code.  The "cmd" directory
contains the source code of the commands.  The "kmrrun" directory
contains the source code for shell command pipelining (or
"streaming").
The "python" directory contains the source code for the Pything binding.
The "shell" directory contains the source code of the old shell command pipelining,
which was mostly replaced by the "kmrrun" command.
The "ex" directory contains a few examples.

The file "gensort-1.2.tar.gz" is a 3rd-party software of the data
generator of TeraSort.  The file "tpch_2_17_0.zip" is also a 3rd-party
software of the data generator of TPC-H.  These files are not part of
KMR, but they are needed for running some examples.

## INSTALLATION

Building KMR requires a C compiler that supports the C99 standard and an
MPI library that supports MPI 2.2.  However, the command line tool
"kmrrun" requires Open MPI or Fujitsu MPI.  Python binding requires
2.6.x or higher (Python 3.x are unsupported).  KMR does not need
any unusual libraries.  KMR is developed and tested mainly on the
following environments.

* CentOS 6.5 Linux x86_64, GCC 4.4.7, Open MPI 1.8.3
* K Computer/FX10, Fujitsu Compiler, Fujitsu MPI (latest stable)

Note that recent releases are only lightly tested.

KMR can be installed by just typing "configure", "make", and "make
install".  To change the installation directory, specify the
"--prefix" option to the configure script.

    $ ./configure --prefix=PATH_TO_INSTALL
    $ make
    $ make install

To build KMR documents, type "make htmldoc".  It depends on Doxygen,
Epydoc and GNU Troff.  The documents are generated in the "./html"
directory.

    $ make htmldoc

To install KMR documents, type "make install-htmldoc" after installing
KMR binaries.

    $ make install-htmldoc

## INSTALLATION (for kudpc @kyoto-u.ac.jp)

Care should be taken in the Cray environment to use KMR Python API.
KMR needs to be built with the same MPI library as the Python-MPI
binding (mpi4py).  But, the default environment is not the one used to
build the Python-MPI binding (for example, at the sites like
kudpc.kyoto-u.ac.jp).  It needs to switch the compiler and the MPI
library appropriately before running the configure script.

    $ module switch PrgEnv-cray PrgEnv-gnu
    $ ./confugire ......

## COPYRIGHTS

The files in "src" directory include the materials copyrighted by
Akiyama Lab., Tokyo Institute of Technology (titec) (code from the
GHOST Project) and the materials copyrighted by The Regents of the
University of California (code from NetBSD-5.1.2).  The files in "ex"
directory include the materials copyrighted by Stanford University
(code from Phoenix MapReduce Library), and the materials copyrighted
by Sandia Corporation (code from MapReduce-MPI Library).  The files in
"gensort-1.2.tar.gz" are copyrighted by Chris Nyberg
(chris.nyberg@ordinal.com).  The files in "tpch_2_17_0.zip" are
copyrighted by the Transaction Processing Performance Council (TPC).
All others are copyrighted by RIKEN R-CCS, and all rights reserved
except for the grants by the license.

## LICENSE TERMS

KMR is free software licensed under the BSD 2-Clause License.  See
the file LICENSE for more details.
