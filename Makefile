# Makefile (2014-02-04)

.PHONY: htmldoc

INSTALL=install

all:
	cd src; make all
	cd shell; make all
	cd kmrrun; make all
	cd cmd; make all

install:
	cd src; make install
	cd shell; make install
	cd kmrrun; make install
	cd cmd; make install
	cd python; make install
	cd man; make install

install-htmldoc: install htmldoc
	cd src; make install-htmldoc

configure: configure.ac ax_openmp.m4 ax_mpi.m4
	cat ax_openmp.m4 ax_mpi.m4 > aclocal.m4
	autoconf

htmldoc::
	rm -fr ./html
	doxygen doxyfile
	cp -p ./kmr.jpg ./html/
	groff -man -Thtml man/kmrshell.1 > html/kmrshell_81.html
	groff -man -Thtml man/kmrshell_mpi.1 > html/kmrshell_mpi_81.html
	groff -man -Thtml man/kmrshuffler.1 > html/kmrshuffler_81.html
	groff -man -Thtml man/kmrrun.1 > html/kmrrun_81.html
	groff -man -Thtml man/kmrfsplit.1 > html/kmrfsplit_81.html
	groff -man -Thtml man/kmrgenscript.1 > html/kmrgenscript_81.html
	groff -man -Thtml man/kmrrungenscript.1 > html/kmrrungenscript_81.html
	groff -man -Thtml man/kmrwrapper.1 > html/kmrwrapper_81.html
	groff -man -Thtml man/kmrckptdump.1 > html/kmrckptdump_81.html
	groff -man -Thtml man/kmrwatch0.1 > html/kmrwatch0_81.html

distclean:: clean
	cd src; make distclean
	cd shell; make distclean
	cd kmrrun; make distclean
	cd cmd; make distclean
	cd ex; make distclean
	rm -f config.status config.log aclocal.m4
	rm -rf autom4te.cache
	rm -f config.make config.h

clean::
	rm -f doxygen_entrydb_*.tmp doxygen_objdb_*.tmp
	cd src; make clean
	cd shell; make clean
	cd kmrrun; make clean
	cd cmd; make clean
	cd python; make clean
	cd ex; make clean
