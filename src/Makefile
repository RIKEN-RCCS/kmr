# Makefile (2014-02-04)

include ../config.make

.PHONY : kmr_c kmr_f

INSTALL=install

mpicc=$(MPICC) $(OPENMP_CFLAGS) $(CFLAGS)
mpicxx=$(MPICXX) $(OPENMP_CXXFLAGS) $(CXXFLAGS)
mpif90=$(MPIFC) $(OPENMP_FFLAGS) $(FCFLAGS)

SRCS_C=kmrbase.c kmrutil.c kmrmapms.c kmrmoreops.c kmrisort.c kmratoa.c \
	kmrckpt.c kmrfefs.c kmrfiles.c kmraltkvs.c kmriolb.c
OBJS_C=kmrbase.o kmrutil.o kmrmapms.o kmrmoreops.o kmrisort.o kmratoa.o \
	kmrckpt.o kmrfefs.o kmrfiles.o  kmraltkvs.o kmriolb.o
SRCS_F=kmrf.F90
OBJS_F=kmrf.o

all:: libkmr.a

libkmr.a: $(KMRTARGET)

kmr_c.a: kmr.h kmrimpl.h kmrckpt.h $(SRCS_C) $(KMRWATCH)
	$(mpicc) -c $(SRCS_C)
	ar csr kmr_c.a $(OBJS_C) $(KMRWATCH)
	cp kmr_c.a libkmr.a

kmr_f.a: kmr.h kmrimpl.h kmrckpt.h $(SRCS_C) $(SRCS_F) $(KMRWATCH)
	$(mpicc) -c $(SRCS_C)
	$(mpif90) -c $(SRCS_F)
	ar csr kmr_f.a $(OBJS_C) $(OBJS_F) $(KMRWATCH)
	cp kmr_f.a libkmr.a

kmrwatch0.bin.o: kmrwatch0.c
	$(CC) $(CFLAGS) -o kmrwatch0 kmrwatch0.c $(LIBS)
	./bindat.sh kmrwatch0
	$(CC) $(CFLAGS) -c kmrwatch0.bin.c

install::
	$(INSTALL) -d $(BINDIR)
	$(INSTALL) -d $(LIBDIR)
	$(INSTALL) -d $(INCDIR)
	$(INSTALL) -d $(ETCDIR)
	$(INSTALL) -m 644 kmr.h $(INCDIR)
	if [ -f kmrf.mod ]; then $(INSTALL) -m 644 kmrf.mod $(INCDIR); fi
	$(INSTALL) -m 644 libkmr.a $(LIBDIR)
	$(INSTALL) -m 755 kmrwatch0 $(LIBDIR)
	$(INSTALL) -m 644 ../README $(ETCDIR)
	$(INSTALL) -m 644 ../LICENSE $(ETCDIR)
	$(INSTALL) -m 644 ../config.log $(ETCDIR)
	$(INSTALL) -m 644 ../config.make $(ETCDIR)

install-htmldoc::
	$(INSTALL) -d $(DOCDIR)
	cp -rp ../html $(DOCDIR)/

check::
	@if ./ck.semicolons.sh ; then echo "Bad OMP directive line" ; fi

test0: libkmr.a
	$(mpicc) -g test0.c libkmr.a $(LIBS)

test1: libkmr.a
	$(mpicc) -g test1.c libkmr.a $(LIBS)

test2: libkmr.a
	$(mpicc) -g test2.c libkmr.a $(LIBS)

test3: libkmr.a
	$(mpicc) -g test3.c libkmr.a $(LIBS)

test4: libkmr.a
	$(mpicc) -g test4.c libkmr.a $(LIBS)

test5: libkmr.a
	$(mpicc) -g test5.c libkmr.a $(LIBS)

testckpt1: libkmr.a
	$(mpicc) -g testckpt1.c libkmr.a $(LIBS)

testckpt2: libkmr.a
	$(mpicc) -g testckpt2.c libkmr.a $(LIBS)

testckpt3: libkmr.a
	$(mpicc) -g testckpt3.c libkmr.a $(LIBS)

testckpt4: libkmr.a
	$(mpicc) -g testckpt4.c libkmr.a $(LIBS)

testiolb1: libkmr.a
	$(mpicc) -g testiolb1.c libkmr.a $(LIBS)

testiolb2: libkmr.a
	$(mpicc) -g testiolb2.c libkmr.a $(LIBS)

testcxx: libkmr.a testcxx.cpp
	$(mpicxx) -std=gnu++0x -g testcxx.cpp libkmr.a $(LIBS)

testf: libkmr.a testf.f90
	$(mpif90) -I . -g testf.f90 libkmr.a $(LIBS)

testmisc:
	$(mpicc) -g testmisc.c libkmr.a $(LIBS)

distclean:: clean

clean::
	rm -f kmr_c.a kmr_f.a *.o libkmr.a *.mod kmrwatch0 kmrwatch0.bin.c
	rm -f properties option mpidp.log out.* dat.*
	rm -rf dir.*
	rm -f a.out core core.*
	rm -fr ckptdir00* tmp_ckptdir00* kmeans_ckpt*
