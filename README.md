KMR
===

Copyright (C) 2012-2015 RIKEN AICS

KMR comes with ABSOLUTELY NO WARRANTY.

This is KMR, a high-performance map-reduce library.  See
"http://mt.aics.riken.jp/kmr" for the project information.  See
"http://pf-aics-riken.github.io/kmr-manual/" for an overview and
API usage of the current stable release of KMR.

FILES
-----

The "src" directory contains the source code.  The "cmd" directory
contains the source code of the commands.  The "kmrrun" directory
contains the source code for shell command pipelining (or
"streaming").  The "ex" directory contains a few examples.
The "shell" directory contains the former commands of shell command
pipelining.

The file "gensort-1.2.tar.gz" is a 3rd-party software of the data
generator of TeraSort.  The file "tpch_2_17_0.zip" is also a 3rd-party
software of the data generator of TPC-H.  These files are not part of
KMR, but they are needed for running some examples.

INSTALL
-------

Building KMR requires a C compiler that supports C99 standard and an
MPI library that supports MPI 2.2.  However the command line tool
'kmrrun' supports only OpenMPI and Fujitsu MPI.  KMR does not depend
on any other libraries.  KMR is developed and tested mainly on the
following environments.

* CentOS 6.5 Linux x86_64, GCC 4.4.7, OpenMPI 1.8.3
* K Computer/FX10, Fujitsu Compiler, Fujitsu MPI (latest stable)

KMR can be installed by just typing 'configure', 'make' and
'make install'.  To change the installation directory, specify
'--prefix' option to the configure script.

    $ ./configure --prefix=PATH_TO_INSTALL_DIR
    $ make
    $ make install

To build KMR documents, type 'make htmldoc'. It depends on Doxygen,
Epydoc and Gnu troff.  The documents are generated under './html'
directory.

    $ make htmldoc

To install KMR documents, type 'make install-htmldoc' after installing
KMR binaries.

    $ make install-htmldoc

COPYRIGHTS
----------

The files in "src" directory include the materials copyrighted by
AKIYAMA Lab., Tokyo Institute of Technology (titec) (code from the
GHOST Project) and the materials copyrighted by The Regents of the
University of California (code from NetBSD-5.1.2).  Some distributions
may not contain the materials copyrighted by AKIYAMA Lab.  The files
in "ex" directory include the materials copyrighted by Stanford
University (code from Phoenix MapReduce Library), and the materials
copyrighted by Sandia Corporation (code from MapReduce-MPI Library).
The files in "gensort-1.2.tar.gz" are copyrighted by Chris Nyberg
(chris.nyberg@ordinal.com).  The files in "tpch_2_17_0.zip" are
copyrighted by the Transaction Processing Performance Council (TPC).
All others are copyrighted by RIKEN AICS, and all rights reserved
except for the grants by the license.

LICENSE TERMS
-------------

KMR is free software licensed under the BSD 2-Clause License.  See
LICENSE for more details.
