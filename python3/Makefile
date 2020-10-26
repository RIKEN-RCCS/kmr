## Makefile

include ../config.make

INSTALL = install

all::

install:
	${INSTALL} -m 644 kmr4py.py $(LIBDIR)

htmldoc::
	python3 make-documentation.py

update-version::
	cp kmr4py.py _kmr4py.py
	sed -e "s/^\(__version__ = \)\"[0-9]*\"/\1\"$(KMRVERDATE)\"/" \
	    -e "s/^\(kmrversion = \)\"[0-9.]*\"/\1\"$(KMRVER)\"/" \
	< _kmr4py.py > kmr4py.py
	rm -f _kmr4py.py

clean::
	rm -fr __pycache__
