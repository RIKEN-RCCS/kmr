#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright (C) 2012-2015 RIKEN AICS

## \file kmrrungenscript.in.py KMRRUN Job-Script Generator.

import sys
import os
import re
from optparse import OptionParser

kmrhome = '@KMRHOME@'

## Checks file existence.
#  If file does not exist, it prints an error message and exit.
#  @param path  file path for check.

def checkexist (path) :
    if not os.path.exists(path) :
        print 'Error: file or dir "%s" is not exist.' % path
        sys.exit()


## Check restart mode.
#  If node > number of checkpoint file, error.

def checkrestart (procstr, restart_basenae) :
    files = os.listdir('./')
    count = 0
    for sfile in files :
        if sfile.split(".")[0] == os.path.basename(restart_basenae) :
            count = count +1
    fname = os.path.basename(restart_basenae) + '.00000/nprocs'
    if (not os.path.exists(fname)) :
        print >> sys.stderr, \
            'Error: Checkpoint nproc file %s not exit.\n' % fname
        sys.exit()
    preprocstr = open(fname).read()
    preproc = preprocstr.split("=")[1]
    if count != int(preproc) :
        print >> sys.stderr, \
            'Error: Do not match number of checkpoint file and ' \
            'executed process. ***\n'
        sys.exit()
    proc = k_node_to_int(procstr)
    if proc > int(preproc) :
        print >> sys.stderr, \
            'Error: On restart, increasing number of process is ' \
            'not supported. ***\n'
        sys.exit()
    if count > proc :
        sys.stderr.write("*** Reduction mode. ***\n")


## Update command path.

def update_execpath(cmd) :
    if cmd :
        m = re.match("^\./.+", cmd)
        if not m :
            m = re.match("^/.+", cmd)
            if m :
                print >> sys.stderr, \
                    'Error: A program should be specified by a relative ' \
                    'path from the current directory.'
                sys.exit()
            else :
                cmd = './' + cmd
    return cmd


## Parse K node declaration into an integer.

def k_node_to_int(shape_str) :
    m = re.match("(\d+)x?(\d+)?x?(\d+)?(:strict)?", shape_str)
    prdct = 1
    for mstr in m.groups()[0:3] :
        if mstr :
            prdct *= int(mstr)
    return prdct


## Generates job-script for K.
#  @param node             number of node to execute.
#  @param shape            mpi process shape
#  @param proc             number of execute proc
#  @param indir            directory where inputs are located(staged-in)
#  @param rsctime          resource time limit
#  @param taskproc         number of processes to run each mapper/reducer
#  @param mapper           mapper program
#  @param kvgen            kv generator program
#  @param reducer          reducer program
#  @param ckpt             enable checkpoint
#  @param restart_basename prefix of checkpoint directory nam
#  @param scrfile          output job script file name

def k_scheduler(node, shape, proc, indir, rsctime, taskproc, mapper, kvgen,
                reducer, ckpt, restart_basename, scrfile) :
    # Read template file.
    template = ''
    cmdpath = ''
    if template == '' :
        try :
            template = open('kmrrungenscript.template').read()
            cmdpath = 'kmrrun'
        except IOError :
            pass
    if template == '' :
        try :
            dir0 = os.path.dirname(os.path.realpath(__file__))
            dir1 = os.path.realpath(dir0 + '/../lib')
            template = open(dir1 + '/kmrrungenscript.template').read()
            cmdpath = dir1 + '/kmrrun'
        except IOError :
            pass
    if template == '' :
        try :
            dir2 = os.path.realpath(kmrhome + '/lib')
            template = open(dir2 + '/kmrrungenscript.template').read()
            cmdpath = dir2 + '/kmrrun'
        except IOError :
            pass
    if template == '' :
       print >> sys.stderr, 'Error: could not open job-script template.'
       sys.exit()

    # Stage in section
    stginstr = ""
    if mapper :
        mapper_cmd  = mapper.split()[0]
        stginstr += '#PJM --stgin "%s %s"' % (mapper_cmd, mapper_cmd)
    if kvgen :
        if len(stginstr) :
            stginstr += '\n'
        kvgen_cmd   = kvgen.split()[0]
        stginstr += '#PJM --stgin "%s %s"' % (kvgen_cmd, kvgen_cmd)
    if reducer :
        if len(stginstr) :
            stginstr += '\n'
        reducer_cmd = reducer.split()[0]
        stginstr += '#PJM --stgin "%s %s"' % (reducer_cmd, reducer_cmd)
    if len(stginstr) :
        stginstr += '\n'
    indir_stgin = './' + os.path.basename(indir)
    stginstr += '#PJM --stgin "%s/* %s/"' % (indir, indir_stgin)
    # Stage in ckpt files
    if restart_basename :
        fname = os.path.basename(restart_basename) + '.00000/nprocs'
        nproc = int(open(fname).read().split("=")[1])
        for rank in range(nproc) :
            stginstr += '\n'
            stginstr += '#PJM --stgin "./%s.%05d/* ./ckptdir%05d/"' \
                        % (restart_basename, rank, rank)

    # Stage out section
    stgoutstr = "#\n# !!WRITE STGOUT HERE!!\n#"
    # Stage out ckpt files
    if ckpt or restart_basename :
        for rank in range(k_node_to_int(proc)) :
            stgoutstr += '\n'
            stgoutstr += '#PJM --stgout "./ckptdir%05d/* ' \
                         './ckptdir_%%j.%05d/"' % (rank, rank)

    # program execute section
    execstr = ''
    optstring = ''
    if taskproc :
        optstring += '-n %s ' % (taskproc)
    if mapper :
        optstring += '-m "%s" ' % (mapper)
    if kvgen :
        optstring += '-k "%s" ' % (kvgen)
    if reducer :
        optstring += '-r "%s" ' % (reducer)
    if ckpt or restart_basename :
        optstring += '--ckpt '
    optstring += indir_stgin

    execstr += 'mpirun -np %d ./kmrrun %s' % (k_node_to_int(proc), optstring)

    # replace template keyword using parameter.
    script = template % {'NODE': node, 'RSCTIME': rsctime, 'SHAPE': shape,
                         'PROC': proc, 'KMRRUN': cmdpath,
                         'DATASTGIN': stginstr, 'DATASTGOUT': stgoutstr,
                         'EXEC': execstr, 'KMRHOME': kmrhome}

    # output script
    if scrfile is None :
        print script
    else :
        out = open(scrfile, "w")
        print >> out, script
        out.close()


## Selects job-scheduler.
#  @param node            number of node to execute.
#  @param shape           mpi process shape
#  @param proc            number of execute proc
#  @param indir           directory where inputs are located(staged-in)
#  @param rsctime         resource time limit
#  @param taskproc        number of processes to run each mapper/reducer
#  @param mapper          mapper program
#  @param kvgen           kv generator program
#  @param reducer         reducer program
#  @param ckpt            enable checkpoint
#  @param restart_basenae prefix of checkpoint directory nam
#  @param scrfile         output job script file name
#  @param sched           scheduler

def select_scheduler(node, shape, proc, indir, rsctime, taskproc, mapper,
                     kvgen, reducer, ckpt, restart_basenae, scrfile, sched) :
    if sched == 'K' :
        k_scheduler(node, shape, proc, indir, rsctime, taskproc, mapper,
                    kvgen, reducer, ckpt, restart_basenae, scrfile)
    # for other schedulers...


## kmrgenscript main routine.
#  It works on Python 2.4 or later.

if __name__ == "__main__" :

    usage = "usage: %prog [options] -m mapper [-k keygener -r reducer]"
    parser = OptionParser(usage)

    parser.add_option("-t",
                      "--resource-time",
                      dest="rsctime",
                      type="string",
                      help="job execution time (default is '00:10:00')",
                      metavar="'string'",
                      default='00:10:00')

    parser.add_option("-e",
                      "--number-of-node",
                      dest="node",
                      type="string",
                      help="number of node (default is '12')",
                      metavar="'string'",
                      default='12')

    parser.add_option("-s",
                      "--shape",
                      dest="shape",
                      type="string",
                      help="mpi process shape (default is '1')",
                      metavar="'string'",
                      default='1')

    parser.add_option("-p",
                      "--proc",
                      dest="proc",
                      type="string",
                      help="number of mpi processes (default is '8')",
                      metavar="'string'",
                      default='8')

    parser.add_option("-d",
                      "--inputdir",
                      dest="indir",
                      type="string",
                      help="input directory on K global storage that is "
                      "staged-in (default is './input')",
                      metavar="'string'",
                      default='./input')

    parser.add_option("-n",
                      "--task-proc",
                      dest="taskproc",
                      type="string",
                      help="number of processes to run each mapper/reducer "
                      "(default is 1)",
                      metavar="number",
                      default=1)

    parser.add_option("-m",
                      "--mapper",
                      dest="mapper",
                      type="string",
                      help="mapper command path and its arguments",
                      metavar="'string'")

    parser.add_option("-k",
                      "--kvgen",
                      dest="kvgen",
                      type="string",
                      help="kv generator command path and its arguments",
                      metavar="'string'")

    parser.add_option("-r",
                      "--reducer",
                      dest="reducer",
                      type="string",
                      help="reducer command path and its arguments",
                      metavar="'string'")

    parser.add_option("-C",
                      "--ckpt",
                      dest="ckpt",
                      action="store_true",
                      help="save checkpoint (default is false)",
                      default=False)

    parser.add_option("-R",
                      "--restart",
                      dest="restart",
                      type="string",
                      help="restart using checkpoint. "
                      "Specify prefix of checkpoint directory",
                      metavar="'string'")

    parser.add_option("-w",
                      "--write-scriptfile",
                      dest="scrfile",
                      type="string",
                      help="output job script filename",
                      metavar="'string'")


    (options, args) = parser.parse_args()

    # check parameters.
    if len(args) <> 0 :
        parser.error("Error: Missing parameter")
        sys.exit()
    if not options.mapper :
        print >> sys.stderr, "Error: Mapper is not specified\n"
        sys.exit()
    if options.reducer and not options.kvgen :
        print >> sys.stderr, \
            "Error: Specify kv generator when reducer is specified\n"
        sys.exit()

    checkexist(options.indir)
    if options.restart :
        checkrestart(options.proc, options.restart)

    select_scheduler(options.node, options.shape, options.proc,
                     options.indir, options.rsctime, options.taskproc,
                     update_execpath(options.mapper),
                     update_execpath(options.kvgen),
                     update_execpath(options.reducer),
                     options.ckpt, options.restart,
                     options.scrfile, 'K')

    message = """
#########################################################################
Don't forget to write stage-out directives for MapReduce output files.
"""[1:-1]
    if options.ckpt or options.restart :
        message += """
A job script generated by this program stages-out only checkpoint files.
"""[0:-1]

    message += """
#########################################################################
"""
    print >> sys.stderr, message


# Copyright (C) 2012-2015 RIKEN AICS
# This library is distributed WITHOUT ANY WARRANTY.  This library can be
# redistributed and/or modified under the terms of the BSD 2-Clause License.
