## 1.7.1 (2015-06-29)

- BUGFIX: kmr_take_one()
- BUGFIX: kmrrungenscript.py

## 1.7 (2015-06-22)

- Change license to BSD
- Strictly check options passed to kmr functions
- Add support for FOCUS supercomputer in kmrrungenscript.py
- Add Python API

## 1.6 (2015-04-01)

- Disable keep_open option in kmr functions except mapper functions
- Support no-fsync mode in Checkpoint/Restart
- Improve performance of KMRRUN when many input files are given
- BUGFIX: Checkpoint/Restart
- EXPERIMENTAL: Support selective mode that reduces IO overhead in
  Checkpoint/Restart

## 1.5 (2015-02-04)

- Reduce overhead of KMRRUN
- Temporal files for reducers are generated in subdirectories when
  using KMRRUN
- WORKAROUND: Abort on calling kmr_read_file_xxx with some MPI
  implementations
- WORKAROUND: Abort on calling kmr_map_processes and using KMRRUN
  with some Open MPI versions (1.6.3 - 1.8.1)

## 1.4 (2014-12-25)

- Resolved a scalability issue of KMRRUN and map functions that
  spawns child programs

## 1.3.2 (2014-11-07)

- Improve job script generator for KMRRUN

## 1.3.1 (2014-11-04)

- Add a word counting sample of KMRRUN
- BUGFIX: KMRRUN command line option parsing

## 1.3 (2014-09-17)

- Add a new function for affinity-aware file assignment

## 1.2 (2014-06-27)

- Support Checkpoint/Restart
- Add KMRRUN command that executes simple MapReduce workflow

## 1.1 (2013-09-20)

- 1.1 is a minor update

## 1.0 (2013-04-26)

- First release
