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

2. Define release version and date (hereafter v1.7 and 20150622)

3. Write changes in this release to CHANGELOG.md

4. Edit configure.ac to include the release version and date
   (line 21 and 22)

       version="1.7"
       release_date="20150622"

5. Run 'make configure' to update configure script

       $ make configure

6. Run 'make update-version' to update version string in files.

       $ ./configure
       $ make update-version

   It will update the following files.

       src/kmr.h              (line 7)
       src/kmr-overview.html  (line 5)
       python/kmr4py.py       (line 21)
       ex/kmrdp-help.html     (line 7)

7. Commit and push changes to the all updated files

8. Tag the head of the master branch as the version number and push it

       $ git tag -m 'KMR release v1.7' v1.7
       $ git push origin v1.7

9. Make html documents locally

       $ make htmldoc

10. Publish the htmldoc on http://pf-aics-riken.github.io/kmr-manual/
