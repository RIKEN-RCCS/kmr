# README for KMR Developers

This document is for KMR developers.

## Coding Style

KMR uses the BSD coding style with 4-spaces indent for C source code
and the PEP 0008 coding standard for Python source code.

## Updating Configure Script

When you modify autoconf related files (configure.ac, ax_openmp.m4 and
ax_mpi.m4), run "make configure".

    $ make configure

## Releasing a New Version

When you want to release a new version, follow the instructions below.

1. Commit and push the all changes to the master branch, and write
   changes in CHANGELOG.md.

2. Define the version and the date of a release (e.g., v1.7 and
   20150622) in "configure.ac" (line 21 and 22).

        version="1.7"
        release_date="20150622"

3. Run "make configure" to update the configure script.

        $ make configure

4. Run "make update-version" which updates version strings in source
   files.

        $ ./configure
        $ make update-version

   It will update the following files.

        src/kmr.h              (line 7)
        src/kmr-overview.html  (line 5)
        python/kmr4py.py       (line 21)
        ex/kmrdp-help.html     (line 7)

5. Commit and push changes again.

6. Tag the head of the master branch as that version and push it.

        $ git tag -m 'KMR release v1.7' v1.7
        $ git push origin v1.7

7. Make html documents locally.

        $ make htmldoc

8. Publish the htmldoc on http://riken-rccs.github.io/kmr-manual/.
