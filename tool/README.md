# KMR Tools

Copyright (C) 2021-2021 RIKEN R-CCS

KMR comes with ABSOLUTELY NO WARRANTY.

Stupid tools.  Those are maybe not related to KMR, but they are
included for user benefits.

## FILES (Not KMR)

### User benefits

* "Flexdice": A cluster analysis program.  It is particularly suited
  to outlier detection.  A user of KMR uses Flexdice, and it is
  included here.  Files are: [flexdice.c](flexdice.c),
  [flexdice.h](flexdice.h), and [flexdicemain.c](flexdicemain.c).
* [splicezip.c](splicezip.c): A zip concatenator.  It concatenates zip
  files into a single one without expanding the contents.  For usage:
  see "splicezip.1" (with "nroff -man splicezip.1").
* [fugaku-nodeid.scm](fugaku-nodeid.scm): A calculator of a node ID
  from a Tofu coordinate of Fugaku (it is a Scheme code).

## INSTALLATION

No convenience scripts for building/installation are provided.

## COPYRIGHTS

## LICENSE TERMS

KMR is free software licensed under the BSD 2-Clause License.  See
the file LICENSE for more details.
