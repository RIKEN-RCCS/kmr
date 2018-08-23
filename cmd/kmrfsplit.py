#!/usr/bin/python
# Copyright (C) 2012-2018 RIKEN R-CCS

## \file kmrfsplit.py KMR-Shell File Splitter.

import sys
import os
import re
from optparse import OptionParser

##  Write part of the inputfile to outputfile.
#   @param  ipath  inputfile path.
#   @param  opath  outputfile path.
#   @startpos  start position of part.
#   @endpos    end position of part.

def writefile(ipath, opath, startpos, endpos) :
    bufsize = 0x8000000
    # read buffer size is 128Mbyte
    try:
        fin = open(ipath, "r")
    except IOError:
       print 'Error: could not open "%s".' % ipath
       sys.exit()

    try:
        fout = open(opath, "w")
    except IOError:
       print 'Error: could not open "%s".' % opath
       sys.exit()

    fin.seek(startpos, 0)
    remain = endpos - startpos
    while remain > 0 :
        # bufferd read/write.
        if bufsize > remain :
            bufsize = remain
        buf = fin.read(bufsize)
        fout.write(buf)
        remain -= len(buf)
    fin.close()
    fout.close()

##  Caluculate cutting point of file.
#   Search separator string in proper position of input file
#   and return cutting point of file.
#   If separator string not found, print error message and exit.
#
#   @param ipath     input file path.
#   @param sep       separator string. (regular expression)
#   @param startpos  start position of separate.
#   @param endpos    end position of separate.

def getcuttingpoint(ipath, sep, startpos, partsize) :
    bufsize = 0x8000000
    # read buffer size is 128Mbyte
    filesize = os.path.getsize(ipath)
    if startpos + partsize > filesize :
        # remain size of file is smaller than partition size.
        endpos = filesize
    else:
        endpos = startpos + partsize
        if endpos + bufsize > filesize :
            bufsize = filesize - endpos
        try:
            f = open(ipath, "r")
        except IOError:
            print 'Error: could not open "%s".' % ipath
            sys.exit()
        f.seek(endpos, 0)
        # read size of buffer.
        buf = f.read(bufsize)
        f.close()
        # search separator string in the buffer.
        p = re.compile(sep)
        ret = p.search(buf)
        if ret is None:
            print "Separator not found in proper position.\n"
            sys.exit()
        endpos += ret.end()
    return endpos

##  Split a file using separator string.
#
#   @param nums    number of part to split.
#   @param sep     separator string. (regular expression)
#   @param odir    output directory of splitted files.
#   @param opref   output file prefix of splitted files.
#   @param infile  input file path.

def splitfile(nums, sep, odir, opref, infile) :
    startpos = 0
    filesize = os.path.getsize(infile)
    partsize = filesize / nums

    print "Splitting file: ",
    for i in range(nums-1) :
        endpos = getcuttingpoint(infile, sep, startpos, partsize)

        # compose output file name.
        # ex: partXXXXXX, where XXXXXX is number of part.
        suffix = "%06d" % i
        opath = os.path.join(odir, (opref + suffix))
        # output cutted part of input file.
        writefile(infile, opath, startpos, endpos)
        startpos = endpos
        sys.stdout.write('.')
	sys.stdout.flush()

    # output remain part of input file.
    suffix = "%06d" % (nums-1)
    opath = os.path.join(odir, (opref + suffix))
    writefile(infile, opath, startpos, filesize)
    print "done."

## kmrfsplit main routine.
#  It works on Python 2.4 or later.

if __name__ == "__main__":

    usage = "usage: %prog [options] inputfile"
    parser = OptionParser(usage)

    parser.add_option("-n",
                      "--num-separate",
                      dest="nums",
                      type="int",
                      help="number of file separation",
                      metavar="number",
                      default=1)

    parser.add_option("-s",
                      "--separator",
                      dest="sep",
                      type="string",
                      help="separator string",
                      metavar="'string'",
                      default='\n')

    parser.add_option("-d",
                      "--output-directory",
                      dest="odir",
                      type="string",
                      help="output directory",
                      metavar="'string'",
                      default="./")

    parser.add_option("-p",
                      "--output-file-prefix",
                      dest="opref",
                      type="string",
                      help="output filename prefix",
                      metavar="'string'",
                      default="part")

    parser.add_option("-f",
                      "--force",
                      dest="force",
                      action="store_true",
                      help="force option",
                      default=False)

    (options, args) = parser.parse_args()

    # parameter check.
    if len(args) <> 1 :
        parser.error("missing parameter")
        sys.exit()

    inputfile = args[0]

    if not os.path.exists(inputfile) :
        print 'Error: inputfile %s is not exist.' % inputfile
        sys.exit()

    if os.path.exists(options.odir) :
        if not os.path.isdir(options.odir) :
            print 'Error: "%s" is not directory.' % options.odir
            sys.exit()
    else:
        if options.force :
            try:
                os.mkdir(options.odir)
            except IOError:
                print 'Error: could not create "%s".' % options.odir
                sys.exit()
        else:
            print 'Error: directory "%s" is not exist. create it or use -f option.' % options.odir
            sys.exit()

    splitfile(options.nums, options.sep, options.odir, options.opref, inputfile)

# Copyright (C) 2012-2018 RIKEN R-CCS
# This library is distributed WITHOUT ANY WARRANTY.  This library can be
# redistributed and/or modified under the terms of the BSD 2-Clause License.
