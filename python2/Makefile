# Makefile

include ../config.make

INSTALL = install

all::

install:
	${INSTALL} -m 644 kmr4py.py $(LIBDIR)

update-version::
	cp kmr4py.py _kmr4py.py
	sed -e "s/^\(__version__ = \)\"[0-9]*\"/\1\"$(KMRVERDATE)\"/" \
	< _kmr4py.py > kmr4py.py
	rm -f _kmr4py.py


clean::
	rm -f *.pyc
