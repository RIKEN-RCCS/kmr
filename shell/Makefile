# Makefile (2014-02-04)

include ../config.make

INSTALL=install

mpicc=$(MPICC) $(OPENMP_CFLAGS) $(CFLAGS)

KMRINC=-I../src
KMRLIB=../src/libkmr.a

SRCS=
OBJS=

all: kmrshell kmrshuffler

kmrshell: kmrshell.c
	$(CC) $(CFLAGS) kmrshell.c -o kmrshell $(LIBS)

kmrshuffler: kmrshuffler.c
	$(mpicc) $(KMRINC) kmrshuffler.c -o kmrshuffler $(LIBS) $(KMRLIB)

kmrshell_mpi: kmrshell_mpi.c
	$(mpicc) $(KMRINC) kmrshell_mpi.c -o kmrshell_mpi $(LIBS) $(KMRLIB)

install::
	$(INSTALL) -d $(BINDIR)
	$(INSTALL) -d $(LIBDIR)
	$(INSTALL) -m 755 kmrshell $(LIBDIR)
	$(INSTALL) -m 755 kmrshuffler $(LIBDIR)

wc.mapper: wc.mapper.c
	$(CC) $(CFLAGS) -o wc.mapper wc.mapper.c $(LIBS)

wc.reducer: wc.reducer.c
	$(CC) $(CFLAGS) -o wc.reducer wc.reducer.c $(LIBS)

distclean:: clean

clean::
	rm -f kmrshell kmrshuffler wc.mapper wc.reducer
	rm -f kmrshell_mpi
	rm -f *.o a.out core core.*
