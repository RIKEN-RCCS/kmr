README for KMR Developers
=========================

This document is for KMR developers.

Coding Style
------------

C source codes in KMR adopt the BSD coding style with 4-spaces
indent size.  Python source code in KMR adopt PEP 0008 coding standard.

Update Configure Script
-----------------------

When you update autoconf related files (configure.ac, ax_openmp.m4 and
ax_mpi.m4), run 'make configure'.

    $ make configure

When Releasing a New Version
----------------------------

If you release a new version of KMR, follow the following instructions.

1. Commit and push the all changes to the master branch

2. Define release number (hereafter v1.6)

3. Modify 'KMR_H' macro defined at l.7 in src/kmr.h to the release date
   (hereafter 20150401)

       #define KMR_H  20150401

4. Modify l.5 in src/kmr-overview.html to include the version and date
   string

       <p>(KMR Version: v1.6(20150401))</p>

5. Modify l.7 in ex/kmrdp-help.html to include the version and date string.

       <p>(KMR Version: v1.6(20150401))</p>

6. Commit and push changes to src/kmr.h and src/kmr-overview.html

7. Tag the head of the master branch as the version number and push it

       $ git tag -m 'KMR release v1.6' v1.6
       $ git push origin v1.6

8. Make html documents locally

       $ make htmldoc

9. Publish the htmldoc on http://pf-aics-riken.github.io/kmr-manual/
