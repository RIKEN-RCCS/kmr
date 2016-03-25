# Makefile (2014-02-04)

include ../config.make

INSTALL=install

mpicc=$(MPICC) $(OPENMP_CFLAGS) $(CFLAGS)

KMRINC=-I../src
KMRLIB=../src/libkmr.a

SRCS=
OBJS=

all: kmrckptdump kmrgenscript.py kmrrungenscript.py

kmrckptdump: kmrckptdump.c
	$(mpicc) $(KMRINC) kmrckptdump.c -o kmrckptdump $(LIBS) $(KMRLIB)

kmrgenscript.py: kmrgenscript.in.py
	sed -e "s,@KMRHOME@,$(prefix)," < kmrgenscript.in.py > kmrgenscript.py
	chmod +x kmrgenscript.py

kmrrungenscript.py: kmrrungenscript.in.py
	sed -e "s,@KMRHOME@,$(prefix)," < kmrrungenscript.in.py > kmrrungenscript.py
	chmod +x kmrrungenscript.py

install::
	$(INSTALL) -d $(BINDIR)
	$(INSTALL) -d $(LIBDIR)
	$(INSTALL) -m 755 kmrckptdump $(BINDIR)
	$(INSTALL) -m 755 kmrfsplit.py $(BINDIR)
	$(INSTALL) -m 755 kmrgenscript.py $(BINDIR)
	$(INSTALL) -m 755 kmrwrapper.py $(BINDIR)
	$(INSTALL) -m 755 kmrrungenscript.py $(BINDIR)
	$(INSTALL) -m 644 kmrgenscript.template $(LIBDIR)
	$(INSTALL) -m 644 kmrrungenscript.template.k $(LIBDIR)
	$(INSTALL) -m 644 kmrrungenscript.template.focus $(LIBDIR)

distclean:: clean

clean::
	rm -f kmrckptdump kmrgenscript.py kmrrungenscript.py
	rm -f a.out core core.*
	rm -rf *.dSYM
