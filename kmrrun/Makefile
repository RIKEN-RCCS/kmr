# Makefile (2014-02-04)

include ../config.make

INSTALL=install

mpicc=$(MPICC) $(OPENMP_CFLAGS) $(CFLAGS)

KMRINC=-I../src
KMRLIB=../src/libkmr.a

SRCS=
OBJS=

all: kmrrun

kmrrun: kmrrun.c
	$(mpicc) $(KMRINC) kmrrun.c -o kmrrun $(LIBS) $(KMRLIB)

install::
	$(INSTALL) -d $(BINDIR)
	$(INSTALL) -d $(LIBDIR)
	$(INSTALL) -m 755 kmrrun $(LIBDIR)

mpi_pi.mapper: mpi_pi.mapper.c
	$(mpicc) $(CFLAGS) -o mpi_pi.mapper mpi_pi.mapper.c $(LIBS)

mpi_pi.reducer: mpi_pi.reducer.c
	$(mpicc) $(CFLAGS) -o mpi_pi.reducer mpi_pi.reducer.c $(LIBS)

pi.mapper: pi.mapper.c
	$(CC) $(CFLAGS) -o pi.mapper pi.mapper.c $(LIBS)

pi.reducer: pi.reducer.c
	$(CC) $(CFLAGS) -o pi.reducer pi.reducer.c $(LIBS)

distclean:: clean

clean::
	rm -f kmrrun mpi_pi.mapper mpi_pi.reducer
	rm -f pi.mapper pi.reducer
	rm -f *.o a.out core core.*
	rm -f pi.out mpi_pi.out
	rm -rf *.dSYM
