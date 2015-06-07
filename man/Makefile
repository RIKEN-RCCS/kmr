# Makefile (2014-02-04)

include ../config.make

INSTALL=install

all:

install::
	$(INSTALL) -d $(MANDIR)/man1
	$(INSTALL) -m 644 kmrshell.1 $(MANDIR)/man1
	$(INSTALL) -m 644 kmrshell_mpi.1 $(MANDIR)/man1
	$(INSTALL) -m 644 kmrshuffler.1 $(MANDIR)/man1
	$(INSTALL) -m 644 kmrrun.1 $(MANDIR)/man1
	$(INSTALL) -m 644 kmrfsplit.1 $(MANDIR)/man1
	$(INSTALL) -m 644 kmrgenscript.1 $(MANDIR)/man1
	$(INSTALL) -m 644 kmrwrapper.1 $(MANDIR)/man1
	$(INSTALL) -m 644 kmrrungenscript.1 $(MANDIR)/man1
	$(INSTALL) -m 644 kmrwatch0.1 $(MANDIR)/man1
	$(INSTALL) -m 644 kmrckptdump.1 $(MANDIR)/man1

distclean:: clean

clean::
	rm -f *.o a.out core core.*
