# Makefile (2014-02-04)

include ../config.make

mpicc=$(MPICC) $(OPENMP_CFLAGS) $(CFLAGS)
mpicxx=$(MPICXX) $(OPENMP_CXXFLAGS) $(CXXFLAGS)
mpif90=$(MPIFC) $(OPENMP_FFLAGS) $(FCFLAGS)

LIBS += ../src/libkmr.a
INCL=-I ../src/

all::
	@echo 'make one of wordcount, pagerank, kmeans, graysort, tpch, mm, wordfreq, rmat, phoenix-kmeans'

wordcount:
	$(mpicc) $(INCL) -g wordcount.c $(LIBS)

pagerank:
	$(mpicc) $(INCL) -g pagerank.c $(LIBS)

leenpagerank:
	$(mpicc) $(INCL) -g leenpagerank.c $(LIBS)


mykmr_shuffle:
	$(mpicc) $(INCL) -g mykmr_shuffle.c $(LIBS)

kmeans:
	$(mpicc) $(INCL) -g kmeans-kmr.c $(LIBS) -lm

graysort:
	$(mpicc) $(INCL) -g graysort.c $(LIBS)

mm:
	$(mpicc) $(INCL) -g phoenix-matrix-multiply.c $(LIBS)

wordfreq:
	$(mpicc) $(INCL) -g mrmpi-wordfreq.c $(LIBS)

rmat:
	$(mpicc) $(INCL) -g mrmpi-crmat.c $(LIBS)

phoenix-kmeans:
	$(mpicc) $(INCL) -g phoenix-kmeans.c $(LIBS)

wordcountf:
	$(mpif90) $(INCL) -cpp -g wordcountf.f90 $(LIBS)

tpch:
	$(mpicc) $(INCL) -g tpch.c $(LIBS)

flexdice:
	gcc -fopenmp -m64 -std=gnu99 -Wall -Wextra -pedantic -Wno-long-long -Wno-unused-parameter -Wmissing-prototypes -Wshadow -g flexdice.c flexdicemain.c -lm

update-version::
	cp kmrdp-help.html _kmrdp-help.html
	sed -e \
	"s/\(KMR Version: v\)[0-9\.]*([0-9]*)/\1$(KMRVER)($(KMRVERDATE))/" \
	< _kmrdp-help.html > kmrdp-help.html
	rm -f _kmrdp-help.html

distclean:: clean

clean::
	rm -f matrix_file_A.txt matrix_file_B.txt
	rm -f *.o a.out core core.*
