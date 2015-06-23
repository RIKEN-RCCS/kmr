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

def checkexist(path):
    if not os.path.exists(path):
        print >> sys.stderr, 'Error: file or dir "%s" is not exist.' % path
        sys.exit()


## Check restart mode.
#  If node > number of checkpoint file, error.
#  @param restart_basename  prefix of checkpoint directory name
#  @param procstr           string that represents process number
#  @param sched             string that represents scheduler type

def checkrestart(restart_basename, procstr, sched):
    if sched.upper() == 'K':
        if restart_basename is None: return
        ckpt_prefix = restart_basename + '.'
    else:
        ckpt_prefix = 'ckptdir'
    repatter = re.compile(r'^%s\d+$' % ckpt_prefix)
    files = os.listdir('./')
    count = 0
    for file_ in files:
        if repatter.match(file_):
            count += 1
    if count == 0: return

    nprocs_file = ckpt_prefix + '00000/nprocs'
    if not os.path.exists(nprocs_file):
        print >> sys.stderr, \
            'Error: Checkpoint nproc file %s not exit.\n' % nprocs_file
        sys.exit()
    preprocstr = open(nprocs_file).read()
    preproc = preprocstr.split("=")[1]
    if count != int(preproc):
        print >> sys.stderr, \
            'Error: Do not match number of checkpoint file and ' \
            'executed process. ***\n'
        sys.exit()
    proc = k_node_to_int(procstr)
    if proc > int(preproc):
        print >> sys.stderr, \
            'Error: On restart, increasing number of process is ' \
            'not supported. ***\n'
        sys.exit()
    if count > proc:
        sys.stderr.write("*** Reduction mode. ***\n")


## Update command path.
#  @param cmd     path to a command

def update_execpath(cmd):
    if cmd:
        m = re.match(r"^\./.+", cmd)
        if not m:
            m = re.match("^/.+", cmd)
            if m:
                print >> sys.stderr, \
                    'Error: A program should be specified by a relative ' \
                    'path from the current directory.'
                sys.exit()
            else:
                cmd = './' + cmd
    return cmd


## Parse K node declaration into an integer.
#  @param shape_str       string that represents K node shape

def k_node_to_int(shape_str):
    m = re.match(r"(\d+)x?(\d+)?x?(\d+)?(:strict)?", shape_str)
    prdct = 1
    for mstr in m.groups()[0:3]:
        if mstr:
            prdct *= int(mstr)
    return prdct


## Generates job-script for K.
#  @param name               name of the job
#  @param queue              queue to submit job
#  @param rsctime            resource time limit
#  @param node               number of node to execute.
#  @param kmrrun_path        path to kmrrun command
#  @param kmrrun_parameter   parameter for kmrrun
#  @param template_path      path for template file
#  @param shape              mpi process shape
#  @param proc               number of execute proc
#  @param mapper             mapper program
#  @param kvgen              kv generator program
#  @param reducer            reducer program
#  @param indir              directory where inputs are located(staged-in)
#  @param ckpt               enable checkpoint
#  @param restart_basename   prefix of checkpoint directory name

def k_scheduler(name, queue, rsctime, node, kmrrun_path, kmrrun_parameter,
                template_path, shape, proc, mapper, kvgen, reducer, indir,
                ckpt, restart_basename):
    # Stage in section
    stginstr = ""
    if mapper:
        mapper_cmd = mapper.split()[0]
        stginstr += '#PJM --stgin "%s %s"' % (mapper_cmd, mapper_cmd)
    if kvgen:
        if len(stginstr):
            stginstr += '\n'
        kvgen_cmd = kvgen.split()[0]
        stginstr += '#PJM --stgin "%s %s"' % (kvgen_cmd, kvgen_cmd)
    if reducer:
        if len(stginstr):
            stginstr += '\n'
        reducer_cmd = reducer.split()[0]
        stginstr += '#PJM --stgin "%s %s"' % (reducer_cmd, reducer_cmd)
    if len(stginstr):
        stginstr += '\n'
    indir_stgin = './' + os.path.basename(indir)
    stginstr += '#PJM --stgin "%s/* %s/"' % (indir, indir_stgin)
    # Stage in ckpt files
    if restart_basename:
        fname = os.path.basename(restart_basename) + '.00000/nprocs'
        nproc = int(open(fname).read().split("=")[1])
        for rank in range(nproc):
            stginstr += '\n'
            stginstr += '#PJM --stgin "./%s.%05d/* ./ckptdir%05d/"' \
                        % (restart_basename, rank, rank)

    # Stage out section
    stgoutstr = "#\n# !!WRITE STGOUT HERE!!\n#"
    # Stage out ckpt files
    if ckpt or restart_basename:
        for rank in range(k_node_to_int(proc)):
            stgoutstr += '\n'
            stgoutstr += '#PJM --stgout "./ckptdir%05d/* ' \
                         './ckptdir_%%j.%05d/"' % (rank, rank)

    execstr = 'mpiexec -n %d ./kmrrun %s' % (k_node_to_int(proc), kmrrun_parameter)

    template = open(template_path).read()
    return template % {'NAME': name, 'QUEUE': queue, 'NODE': node,
                       'RSCTIME': rsctime, 'KMRRUN': kmrrun_path,
                       'SHAPE': shape, 'PROC': proc, 'DATASTGIN': stginstr,
                       'DATASTGOUT': stgoutstr, 'EXEC': execstr}


## Generates job-script for FOCUS supercomputer
#  @param name               name of the job
#  @param queue              queue to submit job
#  @param rsctime            resource time limit
#  @param node               number of MPI processes to use
#  @param kmrrun_path        path to kmrrun command
#  @param kmrrun_parameter   parameter for kmrrun
#  @param template_path      path for template file

def focus_scheduler(name, queue, rsctime, node, kmrrun_path, kmrrun_parameter,
                    template_path):
    template = open(template_path).read()
    return template % {'NAME': name, 'QUEUE': queue, 'NODE': node,
                       'RSCTIME': rsctime, 'KMRRUN': kmrrun_path,
                       'KMRRUN_PARAM': kmrrun_parameter}


## Selects job-scheduler.
#  @param opts  Options to the generator
#  @param sched           scheduler

def select_scheduler(opts, sched):
    # find kmrrun and its job-scheduler templates
    template_dir = kmrhome + '/lib'
    kmrrun_path = template_dir + '/kmrrun'
    if not os.path.exists(kmrrun_path):
        # kmrrun does not exist in the install directory.  In this case,
        # We assume that we are working in KMRSRC/cmd directory.
        template_dir = '.'
        kmrrun_path = template_dir + '/../kmrrun/kmrrun'
        if not os.path.exists(kmrrun_path):
            # error exit
            print >> sys.stderr, 'Error: could not find kmrrun utility.'
            sys.exit()

    # set parameters
    queue    = opts.queue
    node     = opts.node
    rsctime  = options.rsctime
    mapper   = update_execpath(opts.mapper)
    kvgen    = update_execpath(opts.kvgen)
    reducer  = update_execpath(opts.reducer)
    kmrrun_parameter = ''
    if opts.taskproc:
        kmrrun_parameter += '-n %s ' % (opts.taskproc)
    if opts.mapper:
        kmrrun_parameter += '-m "%s" ' % (mapper)
    if opts.kvgen:
        kmrrun_parameter += '-k "%s" ' % (kvgen)
    if opts.reducer:
        kmrrun_parameter += '-r "%s" ' % (reducer)
    if opts.ckpt or opts.restart:
        kmrrun_parameter += '--ckpt '
    kmrrun_parameter += './' + os.path.basename(opts.indir)
    name = 'kmrrun_job'
    if opts.scrfile:
        name = opts.scrfile

    if sched.upper() == 'K':
        script = k_scheduler(name, queue, rsctime, node, kmrrun_path,
                             kmrrun_parameter,
                             template_dir + '/kmrrungenscript.template.k',
                             opts.shape, opts.proc, mapper, kvgen, reducer,
                             opts.indir, opts.ckpt, opts.restart)
    elif sched.upper() == 'FOCUS':
        script = focus_scheduler(name, queue, rsctime, node, kmrrun_path,
                                 kmrrun_parameter,
                                 template_dir + '/kmrrungenscript.template.focus')
    # for other schedulers...
    else:
        print >> sys.stderr, 'Unknown scheduler'
        sys.exit()

    # output script
    if opts.scrfile is None:
        print script
    else:
        out = open(opts.scrfile, "w")
        print >> out, script
        out.close()


## Warn to write Stage-out section.
#  @param opts  Options to the generator

def warn_stageout(opts):
    if opts.sched != 'K':
        return

    message = """
#########################################################################
Don't forget to write stage-out directives for MapReduce output files.
"""[1:-1]
    if opts.ckpt or opts.restart:
        message += """
A job script generated by this program stages-out only checkpoint files.
"""[0:-1]

    message += """
#########################################################################
"""
    print >> sys.stderr, message


## kmrgenscript main routine.
#  It works on Python 2.4 or later.

if __name__ == "__main__":

    usage = "usage: %prog [options] -m mapper [-k keygener -r reducer]"
    parser = OptionParser(usage)

    parser.add_option("-q",
                      "--queue",
                      dest="queue",
                      type="string",
                      help="queue to submit your job",
                      metavar="'string'",
                      default='None')

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
                      help="mpi process shape. "
                      "Valid only on K scheduler. (default is '1')",
                      metavar="'string'",
                      default='1')

    parser.add_option("-p",
                      "--proc",
                      dest="proc",
                      type="string",
                      help="number of mpi processes. "
                      "Valid only on K scheduler. (default is '8')",
                      metavar="'string'",
                      default='8')

    parser.add_option("-d",
                      "--inputdir",
                      dest="indir",
                      type="string",
                      help="input file directory. "
                      "When used on K computer, this directory should be one "
                      "located in K global storage that is staged-in. "
                      "(default is './input')",
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
                      help="enable Checkpoint/Restart (default is false)",
                      default=False)

    parser.add_option("-R",
                      "--restart-filename",
                      dest="restart",
                      type="string",
                      help="specify prefix of directories where checkpoint "
                      "files are located. "
                      "This option should be given when restarting on "
                      "a system that requires staging. "
                      "Valid only on K scheduler.",
                      metavar="'string'")

    parser.add_option("-S",
                      "--scheduler",
                      dest="sched",
                      type="string",
                      help="scheduler type. "
                      "Specify Scheduler 'K' or 'FOCUS'. "
                      "'K' supports K computer/FX10 and 'FOCUS' supports "
                      "Focus supercomputer. (default is 'K')",
                      metavar="'string'",
                      default='K')

    parser.add_option("-w",
                      "--write-scriptfile",
                      dest="scrfile",
                      type="string",
                      help="output job script filename",
                      metavar="'string'")

    (options, args) = parser.parse_args()

    # check parameters.
    if len(args) <> 0:
        parser.error("Error: Missing parameter")
        sys.exit()
    if not options.mapper:
        print >> sys.stderr, "Error: Mapper is not specified\n"
        sys.exit()
    if options.reducer and not options.kvgen:
        print >> sys.stderr, \
            "Error: Specify kv generator when reducer is specified\n"
        sys.exit()

    checkexist(options.indir)
    if options.ckpt:
        if options.sched == 'K':
            checkrestart(options.restart, options.proc, 'K')
        else:
            checkrestart(options.restart, '1', options.sched)

    select_scheduler(options, options.sched)
    warn_stageout(options)


# Copyright (C) 2012-2015 RIKEN AICS
# This library is distributed WITHOUT ANY WARRANTY.  This library can be
# redistributed and/or modified under the terms of the BSD 2-Clause License.
