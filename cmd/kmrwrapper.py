#!/usr/bin/env python3
# -*-coding: utf-8;-*-

## Copyright (C) 2012-2018 RIKEN R-CCS

## \file kmrwrapper.py KMR-Shell File Spliter and Job-Script Generator.

import sys
import os
from optparse import OptionParser
from kmrfsplit import *
from kmrgenscript import *

## Wrapper script of kmrfsplit and kmrgenscript.
#  It works on Python 2.4 or later.

if __name__ == "__main__":

    usage = "usage: %prog [options] -m mapper -r reducer inputfile"
    parser = OptionParser(usage)

    parser.add_option("-n",
                      "--number-of-separation",
                      dest="nums",
                      type="int",
                      help="number of file separate",
                      metavar="number",
                      default=1)

    parser.add_option("-e",
                      "--number-of-exec-nodes",
                      dest="nodes",
                      type="int",
                      help="number of execute nodes",
                      metavar="number",
                      default=1)

    parser.add_option("-s",
                      "--separator",
                      dest="sep",
                      type="string",
                      help="separator string",
                      metavar="'string'",
                      default='\n')

    parser.add_option("-p",
                      "--separate-file-prefix",
                      dest="prefix",
                      type="string",
                      help="separate filename prefix",
                      metavar="'string'",
                      default='part')

    parser.add_option("-o",
                      "--outputfile",
                      dest="outfile",
                      type="string",
                      help="output filename prefix",
                      metavar="'string'",
                      default='output')

    parser.add_option("-d",
                      "--workdir",
                      dest="workdir",
                      type="string",
                      help="work directory",
                      metavar="'string'",
                      default='./work')

    parser.add_option("-O",
                      "--outputdir",
                      dest="outdir",
                      type="string",
                      help="output directory",
                      metavar="'string'",
                      default='./outdir')

    parser.add_option("-t",
                      "--resource-time",
                      dest="rsctime",
                      type="string",
                      help="resource time",
                      metavar="'string'",
                      default='00:10:00')

    parser.add_option("-m",
                      "--mapper",
                      dest="mapper",
                      type="string",
                      help="mapper path",
                      metavar="'string'")

    parser.add_option("-r",
                      "--reducer",
                      dest="reducer",
                      type="string",
                      help="reducer path",
                      metavar="'string'")

    parser.add_option("-S",
                      "--scheduler",
                      dest="sched",
                      type="string",
                      help="scheduler (default is 'K')",
                      metavar="'string'",
                      default='K')

    parser.add_option("-w",
                      "--write-scriptfile",
                      dest="scrfile",
                      type="string",
                      help="script filename",
                      metavar="'string'")

    parser.add_option("-f",
                      "--force",
                      dest="force",
                      action="store_true",
                      help="force option",
                      default=False)

    (options, args) = parser.parse_args()

    # check parameters.

    if len(args) != 1 :
        parser.error("missing parameter")
        sys.exit()

    inputfile = args[0]

    if options.nodes > options.nums :
        print('Error: number of execute nodes must be less than or equal to number of file separation.')
        sys.exit()

    # If number of nodes < number of input files,
    #  set M option (attach multi files to a mapper) to True.
    if options.nodes < options.nums :
        multi = True
    else :
        multi = False

    if not os.path.exists(inputfile) :
        print('Error: inputfile %s is not exist.' % inputfile)
        sys.exit()

    if os.path.exists(options.workdir) :
        if not os.path.isdir(options.workdir) :
            print('Error: "%s" is not directory.' % options.workdir)
            sys.exit()
    else:
        if options.force :
            try:
                os.mkdir(options.workdir)
            except IOError:
                print('Error: could not create "%s".' % options.workdir)
                sys.exit()
        else:
            print('Error: directory "%s" is not exist. create it or use -f option.' % options.workdir)
            sys.exit()

    splitfile(options.nums, options.sep, options.workdir, options.prefix, inputfile)

    selectscheduler(options.nodes, options.prefix, options.outfile,
                    options.workdir, options.outdir, options.rsctime,
                    options.mapper, options.reducer, multi,
                    options.sched, options.scrfile)

# Copyright (C) 2012-2018 RIKEN R-CCS
# This library is distributed WITHOUT ANY WARRANTY.  This library can be
# redistributed and/or modified under the terms of the BSD 2-Clause License.
