#!/usr/bin/env python3
# -*-coding: utf-8;-*-

## Copyright (C) 2012-2018 RIKEN R-CCS

## \file kmrrungenscript.in.py KMRRUN Job-Script Generator.

import sys
import os
import re
from optparse import OptionParser

kmrhome = '@KMRHOME@'

## Checks file existence.
#  If file does not exist, it prints an error message and exit.
#  @param path  file path for check.

def _check_exist(path):
    if not os.path.exists(path):
        print('Error: file or dir "%s" is not exist.' % path, file=sys.stderr)
        sys.exit()
    return path


## Check if command in the specified command line exists.
#  @param cmdline  a command line to be executed
#  @param sched    string that represents scheduler type

def check_cmdline(cmdline, sched):
    _check_exist(cmdline.split()[0])
    if sched.upper() == 'K':
        cmdlns = cmdline.split()
        cmdlns[0] = './' + os.path.basename(cmdlns[0])
        return ' '.join(cmdlns)
    else:
        return cmdline

## Check if command in the specified command line exists.
#  @param dirname  name of directory where input files are located
#  @param sched    string that represents scheduler type

def check_indir(dirname, sched):
    _check_exist(dirname)
    if sched.upper() == 'K':
        _dirname = dirname.rstrip().rstrip('/')
        return './' + os.path.basename(_dirname)
    else:
        return dirname


## Check restart mode.
#  If node > number of checkpoint file, error.
#  @param restart_basename  prefix of checkpoint directory name
#  @param procstr           string that represents process number
#  @param sched             string that represents scheduler type

def check_restart(restart_basename, procstr, sched):
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
        print('Error: Checkpoint nproc file %s not exit.\n' % nprocs_file,
              file=sys.stderr)
        sys.exit()
    preprocstr = open(nprocs_file).read()
    preproc = preprocstr.split("=")[1]
    if count != int(preproc):
        print('Error: Do not match number of checkpoint file and ' \
              'executed process. ***\n', file=sys.stderr)
        sys.exit()
    proc = k_node_to_int(procstr)
    if proc > int(preproc):
        print('Error: On restart, increasing number of process is ' \
              'not supported. ***\n', file=sys.stderr)
        sys.exit()
    if count > proc:
        sys.stderr.write("*** Reduction mode. ***\n")


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
#  @param mapper             mapper command line
#  @param kvgen              kv generator command line
#  @param reducer            reducer command line
#  @param indir              directory where inputs are located(staged-in)
#  @param ckpt               enable checkpoint
#  @param restart_basename   prefix of checkpoint directory name

def k_scheduler(name, queue, rsctime, node, kmrrun_path, kmrrun_parameter,
                template_path, shape, proc, mapper, kvgen, reducer, indir,
                ckpt, restart_basename):
    # Stage in section
    stginstr = ''
    if mapper:
        mapper_cmd = mapper.split()[0]
        mapper_cmd_base = os.path.basename(mapper_cmd)
        stginstr += '#PJM --stgin "%s %s"' % (mapper_cmd, mapper_cmd_base)
    if kvgen:
        if len(stginstr):
            stginstr += '\n'
        kvgen_cmd = kvgen.split()[0]
        kvgen_cmd_base = os.path.basename(kvgen_cmd)
        stginstr += '#PJM --stgin "%s %s"' % (kvgen_cmd, kvgen_cmd_base)
    if reducer:
        if len(stginstr):
            stginstr += '\n'
        reducer_cmd = reducer.split()[0]
        reducer_cmd_base = os.path.basename(reducer_cmd)
        stginstr += '#PJM --stgin "%s %s"' % (reducer_cmd, reducer_cmd_base)
    if len(stginstr):
        stginstr += '\n'
    indir_stgin = './' + os.path.basename(indir.rstrip().rstrip('/'))
    stginstr += '#PJM --stgin "%s/* %s/"' % (indir, indir_stgin)
    # Stage in ckpt files
    if restart_basename:
        fname = os.path.basename(restart_basename) + '.00000/nprocs'
        nproc = int(open(fname).read().split('=')[1])
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
            print('Error: could not find kmrrun utility.', file=sys.stderr)
            sys.exit()

    # set parameters
    queue    = opts.queue
    node     = opts.node
    rsctime  = options.rsctime
    mapper   = check_cmdline(opts.mapper, sched)
    kvgen    = check_cmdline(opts.kvgen, sched)
    reducer  = check_cmdline(opts.reducer, sched)
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
    kmrrun_parameter += check_indir(opts.indir, sched)
    name = 'kmrrun_job'
    if opts.scrfile:
        name = opts.scrfile

    if sched.upper() == 'K':
        script = k_scheduler(name, queue, rsctime, node, kmrrun_path,
                             kmrrun_parameter,
                             template_dir + '/kmrrungenscript.template.k',
                             opts.shape, opts.proc, opts.mapper, opts.kvgen,
                             opts. reducer, opts.indir,
                             opts.ckpt, opts.restart)
    elif sched.upper() == 'FOCUS':
        script = focus_scheduler(name, queue, rsctime, node, kmrrun_path,
                                 kmrrun_parameter,
                                 template_dir + '/kmrrungenscript.template.focus')
    # for other schedulers...
    else:
        print('Unknown scheduler', file=sys.stderr)
        sys.exit()

    # output script
    if opts.scrfile is None:
        print(script)
    else:
        out = open(opts.scrfile, "w")
        print(script, file=out)
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
    print(message, file=sys.stderr)


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
    if len(args) != 0:
        parser.error("Error: Missing parameter")
        sys.exit()
    if not options.mapper:
        print("Error: Mapper is not specified\n", file=sys.stderr)
        sys.exit()
    if options.reducer and not options.kvgen:
        print("Error: Specify kv generator when reducer is specified\n",
              file=sys.stderr)
        sys.exit()

    if options.ckpt:
        if options.sched == 'K':
            check_restart(options.restart, options.proc, 'K')
        else:
            check_restart(options.restart, '1', options.sched)

    select_scheduler(options, options.sched)
    warn_stageout(options)


# Copyright (C) 2012-2018 RIKEN R-CCS
# This library is distributed WITHOUT ANY WARRANTY.  This library can be
# redistributed and/or modified under the terms of the BSD 2-Clause License.
