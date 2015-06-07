#!/usr/bin/python
# Copyright (C) 2012-2015 RIKEN AICS

## \file kmrgenscript.in.py KMR-Shell Job-Script Generator.

import sys
import os
from optparse import OptionParser

kmrhome = '@KMRHOME@'

## Checks file existence.
#  If file does not exist, it prints an error message and exit.
#  @param path  file path for check.

def checkexist (path) :
    if not os.path.exists(path) :
        print 'Error: file or dir "%s" is not exist.' % path
        sys.exit()

## Checks path is a directory.
#  If path does not exist, it prints an error message and exit.  Or,
#  it creates a directory with force option.

def checkdir (path, force) :
    if os.path.exists(path) :
        if not os.path.isdir(path) :
            print 'Error: "%s" is not directory.' % path
            sys.exit()
    else :
        if force :
            try :
                os.mkdir(path)
            except IOError :
                print 'Error: could not create "%s".' % path
                sys.exit()
        else :
            print 'Error: directory "%s" is not exist. create it or use -f option.' % path
            sys.exit()

## Generates job-script for K.
#  @param node     number of node to execute.
#  @param infile   input file pathname.
#  @param outfile  output file prefix.
#  @param indir    directory path name that has input files.
#  @param outdir   directory path name that has result output files.
#  @param rsctime  resource time limit.
#  @param mapper   pathname of mapper program.
#  @param reducer  pathname of reducer program.
#  @param multi    multiple input file for one mapper process.
#  @param scrfile  output script file name.

def k_scheduler(node, infile, outfile, indir, outdir, rsctime, mapper, reducer, multi, scrfile) :

    # Read template file.
    template = ''
    if template == '' :
        try :
            template = open('kmrgenscript.template').read()
        except IOError :
            pass
    if template == '' :
        try :
            dir0 = os.path.dirname(os.path.realpath(__file__))
            dir1 = os.path.realpath(dir0 + '/../lib')
            #template = open(dir1 + '/kmrgenscript.template').read()
        except IOError :
            pass
    if template == '' :
        try :
            dir2 = os.path.realpath(kmrhome + '/lib')
            template = open(dir2 + '/kmrgenscript.template').read()
        except IOError :
            pass
    if template == '' :
       print 'Error: could not open job-script template.'
       sys.exit()

    # Stage in section
    ncol = len(str(node -1))
    stginstr = ""
    # Stage in reducer if specified
    if reducer :
        stginstr += '#PJM --stgin "rank=* %s %%r:./"' \
                    % ('./' + os.path.basename(reducer))
    if multi :
        files = os.listdir(indir)
        rank = 0
        for file in files :
            ipath = os.path.join(indir, file)
            if len(stginstr) :
                stginstr += '\n'
            stginstr += '#PJM --stgin "rank=%s %s %%r:./work/"' % (rank, ipath)
            rank = rank +1
            if rank >= node :
                rank = 0
    else :
        if len(stginstr) :
            stginstr += '\n'
        ipath = os.path.join(indir, infile)
        stginstr += '#PJM --stgin "rank=* %s%%0%sr %%r:./input"' % (ipath, ncol)

    # Stage out section
    opath = os.path.join(outdir, outfile)
    stgoutstr = '#PJM --stgout "rank=* %%r:./output.%%r %s.%%0%sr"' % (opath, ncol)

    # program execute section
    if not multi :
        if not reducer :
            execstr = 'mpiexec -n %s -of-proc output ./kmrshell -m %s ./input'\
                 % (node, './' + os.path.basename(mapper))
        else :
            execstr = 'mpiexec -n %s -of-proc output ./kmrshell -m %s -r %s ./input'\
                 % (node, './' + os.path.basename(mapper), './' + os.path.basename(reducer))
    else :
        if not reducer :
            execstr = 'mpiexec -n %s -of-proc output ./kmrshell -m %s ./work'\
                 % (node, './' + os.path.basename(mapper))
        else :
            execstr = 'mpiexec -n %s -of-proc output ./kmrshell -m %s -r %s ./work'\
                 % (node, './' + os.path.basename(mapper), './' + os.path.basename(reducer))


    # replace template keyword using parameter.
    script = template % {'NODE': node, 'RSCTIME': rsctime, 'MAPPER': mapper, 'DATASTGIN': stginstr, 'DATASTGOUT': stgoutstr, 'EXEC': execstr, 'KMRHOME': kmrhome}

    # output script
    if scrfile is None :
        print script
    else :
        out = open(scrfile, "w")
        print >> out, script
        out.close()

## Selects job-scheduler.
#  @param node     number of node to execute.
#  @param infile   input file pathname.
#  @param outfile  output file prefix.
#  @param indir    directory path name that has input files.
#  @param outdir   directory path name that has result output files.
#  @param rsctime  resource time limit.
#  @param mapper   pathname of mapper program.
#  @param reducer  pathname of reducer program.
#  @param multi    multiple input file for one mapper process.
#  @param sched    scheduler.
#  @param scrfile  output script file name.

def selectscheduler(node, infile, outfile, indir, outdir, rsctime, mapper, reducer, multi, sched, scrfile) :
    if sched == 'K' :
        k_scheduler(node, infile, outfile, indir, outdir, rsctime, mapper, reducer, multi, scrfile)
    # for other schedulers...

## kmrgenscript main routine.
#  It works on Python 2.4 or later.

if __name__ == "__main__" :

    usage = "usage: %prog [options] -m mapper [-r reducer]"
    parser = OptionParser(usage)

    parser.add_option("-e",
                      "--number-of-exec-node",
                      dest="node",
                      type="int",
                      help="number of execute node",
                      metavar="number",
                      default=1)

    parser.add_option("-p",
                      "--input-file-prefix",
                      dest="infile",
                      type="string",
                      help="input filename prefix",
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
                      "--inputdir",
                      dest="indir",
                      type="string",
                      help="input directory",
                      metavar="'string'",
                      default='./')

    parser.add_option("-O",
                      "--outputdir",
                      dest="outdir",
                      type="string",
                      help="output directory",
                      metavar="'string'",
                      default='./')

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

    parser.add_option("-M",
                      "--multi-input",
                      dest="multi",
                      action="store_true",
                      help="multi input files to one node",
                      default=False)

    parser.add_option("-f",
                      "--force",
                      dest="force",
                      action="store_true",
                      help="force option",
                      default=False)

    (options, args) = parser.parse_args()

    # check parameters.

    if len(args) > 1 :
        parser.error("missing parameter")
        sys.exit()

    if not options.mapper :
        print "mapper not specified\n"
        sys.exit()

    checkexist(options.indir)
    checkdir(options.outdir, options.force)

    if options.multi :
        if options.indir == "./" :
            print "-M option needs -d (input directory) option.\n"
            sys.exit()
        files = os.listdir(options.indir)
        if len(files) < options.node :
            print 'Node number is greater than number of files in %s.\n' % options.indir
            sys.exit()

    selectscheduler(options.node, options.infile, options.outfile,
                    options.indir, options.outdir, options.rsctime,
                    options.mapper, options.reducer, options.multi,
                    options.sched, options.scrfile)

# NOTICE-NOTICE-NOTICE-SH
