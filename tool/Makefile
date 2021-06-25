## Makefile (2021-06-02)

include ../config.make

mpicc=$(MPICC) $(OPENMP_CFLAGS) $(CFLAGS)
mpicxx=$(MPICXX) $(OPENMP_CXXFLAGS) $(CXXFLAGS)
mpif90=$(MPIFC) $(OPENMP_FFLAGS) $(FCFLAGS)

LIBS += ../src/libkmr.a
INCL=-I ../src/

all::
	@echo 'make one of flexdice, splicezip, ...'

flexdice:
	gcc -fopenmp -m64 -std=gnu99 -Wall -Wextra -pedantic -Wno-long-long -Wno-unused-parameter -Wmissing-prototypes -Wshadow -g flexdice.c flexdicemain.c -lm

splicezip::
	gcc -m64 -std=gnu99 -Wall -Wextra -g splicezip.c

tarczf: tarczf.c
	mpicc -o tarczf tarczf.c

zip-test-files::
	sh -x make-zip-test-files.sh

check-splicezip::
	sh -x check-splicezip.sh

update-version::

distclean:: clean

clean::
	rm -rf tarczf splicezip
