! kmrf.F90 (2014-02-04) -*-Mode: F90;-*-
! Copyright (C) 2012-2016 RIKEN AICS

!> \file kmrf.F90 KMR Fortran Binding.  KMR mixes-up integers,
!> doubles, and pointers in passing key-value pairs as all occupy 8
!> bytes, and KMR defines bad-mannered converters.  See the definition
!> kmr_kv_box in C, where it uses union, but just long in the Fortran
!> binding.  Options to mapper/shuffler/reducer are integers (c_int)
!> in Fortran, instead of structures in C.  So, zero means no options.
!> Integer options are converted to long-integers (c_int to c_long)
!> before passing to C routines.  This conversion is needed because
!> structure options are compatible to long-integers.  Conversion is
!> done by kmr_fixopt() defined here.

! NOTE: Excessive "bind(c)" are added to procedure dummy arguments
! (i.e., parameters of procedure types), although their associated
! type definitions are already with bind(c).  It is illegally required
! by some versions of gcc-4.4.x (OK for gcc-4.7, 4.8, and 4.9).

! MEMO: It does not assume MPI Fortran-binding is by modules.  So,
! some paramters are just c_int instread of specific kinds.

! MEMO: c_sizeof is only defined in F2008, not F2003.

module kmrf
  !use, intrinsic :: iso_c_binding
  use iso_c_binding
  implicit none

  public

  !> Data presentation of keys and values (see ::kmr_kv_field in C).

  integer(c_int), parameter :: &
       kmr_kv_opaque = 1, &
       kmr_kv_cstring = 2, &
       kmr_kv_integer = 3, &
       kmr_kv_float8 = 4, &
       kmr_kv_pointer_owned = 5, &
       kmr_kv_pointer_unmanaged = 6

  !> Option bits (OR'ed) to mapping and reduction (see ::kmr_option).

  integer(c_int), parameter :: &
       kmr_nothreading = ishft(1,0), &
       kmr_inspect = ishft(1,1), &
       kmr_keep_open = ishft(1,2), &
       kmr_key_as_rank = ishft(1,3), &
       kmr_rank_zero = ishft(1,4)

  !> Option bits (OR'ed) to mapping on files (see ::kmr_file_option).

  integer(c_int), parameter :: &
       kmr_each_rank = ishft(1,0), &
       kmr_subdirectories = ishft(1,1), &
       kmr_list_file = ishft(1,2), &
       kmr_shuffle_names = ishft(1,3)

  !> Option bits (OR'ed) to mapping by spawning (see ::kmr_spawn_option).

  integer(c_int), parameter :: &
       kmr_separator_space = ishft(1,0), &
       kmr_reply_each = ishft(1,1), &
       kmr_reply_root = ishft(1,2), &
       kmr_one_by_one = ishft(1,3)

  !> struct kmr_kv_box {int klen; int vlen; unit_sized k; unit_sized v;};

  type, bind(c) :: kmr_kv_box
     !sequence
     integer(c_int) :: klen, vlen
     integer(c_long) :: k, v
  end type kmr_kv_box

  type, bind(c) :: kmr_spawn_info
     type(c_ptr) :: maparg
     integer(c_long) :: icomm_cc
     integer(c_int) :: icomm
     integer(c_int) :: reply_root
  end type kmr_spawn_info

  !> Type of map-function.

  abstract interface
     integer(c_int) function kmr_mapfn(kv, kvi, kvo, p, i) bind(c)
       use iso_c_binding
       import kmr_kv_box
       implicit none
       type(kmr_kv_box), value, intent(in) :: kv
       type(c_ptr), value, intent(in) :: kvi, kvo
       type(c_ptr), value, intent(in) :: p
       integer(c_long), value, intent(in) :: i
     end function kmr_mapfn
  end interface

  !> Type of reduce-function.

  abstract interface
     integer(c_int) function kmr_redfn(kv, n, kvi, kvo, p) bind(c)
       use iso_c_binding
       import kmr_kv_box
       implicit none
       type(kmr_kv_box), intent(in) :: kv(*)
       integer(c_long), value, intent(in) :: n
       type(c_ptr), value, intent(in) :: kvi, kvo
       type(c_ptr), value, intent(in) :: p
     end function kmr_redfn
  end interface

  !> Converts a string to a C pointer (ill-mannered).

  interface kmr_strptr
     type(c_ptr) function kmr_strptr(s) bind(c, name='kmr_strptr_ff')
       use iso_c_binding
       implicit none
       character(kind=c_char,len=1) :: s
     end function kmr_strptr
  end interface kmr_strptr

  !> Converts in reverse of kmr_strptr

  interface kmr_ptrstr
     character(kind=c_char,len=1) function kmr_ptrstr(s) &
          bind(c, name='kmr_ptrstr_ff')
       use iso_c_binding
       implicit none
       type(c_ptr), value :: s
     end function kmr_ptrstr
  end interface kmr_ptrstr

  !> Converts a pointer to a long for key/value (ill-mannered).

  interface kmr_ptrint
     integer(c_long) function kmr_ptrint(p) bind(c, name='kmr_ptrint_ff')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: p
     end function kmr_ptrint
  end interface kmr_ptrint

  !> Converts in reverse of kmr_ptrint.

  interface kmr_intptr
     type(c_ptr) function kmr_intptr(p) bind(c, name='kmr_intptr_ff')
       use iso_c_binding
       implicit none
       integer(c_long), value, intent(in) :: p
     end function kmr_intptr
  end interface kmr_intptr

  !> Converts a double to a long for key/value (ill-mannered).

  interface kmr_dblint
     integer(c_long) function kmr_dblint(v) bind(c, name='kmr_dblint_ff')
       use iso_c_binding
       implicit none
       real(c_double), value, intent(in) :: v
     end function kmr_dblint
  end interface kmr_dblint

  !> Converts in reverse of kmr_dblint.

  interface kmr_intdbl
     real(c_double) function kmr_intdbl(v) bind(c, name='kmr_intdbl_ff')
       use iso_c_binding
       implicit none
       integer(c_long), value, intent(in) :: v
     end function kmr_intdbl
  end interface kmr_intdbl

  !> Converts a character array to a (pointer value) integer for
  !> key/value (it is casting in C).

  interface kmr_strint
     integer(c_long) function kmr_strint(s) bind(c, name='kmr_strint_ff')
       use iso_c_binding
       implicit none
       character(kind=c_char,len=1) :: s
     end function kmr_strint
  end interface kmr_strint

  !> Converts in reverse of kmr_strint().  It fills the character
  !> array (s) by the contents at the pointer value integer (p) by the
  !> length (n).

  interface kmr_intstr
     integer(c_int) function kmr_intstr(p, s, n) &
          bind(c, name='kmr_intstr_ff')
       use iso_c_binding
       implicit none
       integer(c_long), value :: p
       character(kind=c_char,len=1) :: s
       integer(c_int), value :: n
     end function kmr_intstr
  end interface kmr_intstr

  !> Fixes little-endian bits used in Fortran to host-endian.

  interface kmr_fix_bits_endian_ff
     integer(c_long) function kmr_fix_bits_endian_ff(b) &
          bind(c, name='kmr_fix_bits_endian_ff')
       use iso_c_binding
       implicit none
       integer(c_long), value, intent(in) :: b
     end function kmr_fix_bits_endian_ff
  end interface kmr_fix_bits_endian_ff

  !> Gets MPI rank from a key-value stream.

  interface kmr_get_rank
     integer(c_int) function kmr_get_rank(kvs) &
          bind(c, name='kmr_get_rank_ff')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: kvs
     end function kmr_get_rank
  end interface kmr_get_rank

  !> Gets MPI nprocs from a key-value stream.

  interface kmr_get_nprocs
     integer(c_int) function kmr_get_nprocs(kvs) &
          bind(c, name='kmr_get_nprocs_ff')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: kvs
     end function kmr_get_nprocs
  end interface kmr_get_nprocs

  interface kmr_get_key_type
     integer(c_int) function kmr_get_key_type(kvs) &
          bind(c, name='kmr_get_key_type_ff')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: kvs
     end function kmr_get_key_type
  end interface kmr_get_key_type

  interface kmr_get_value_type
     integer(c_int) function kmr_get_value_type(kvs) &
          bind(c, name='kmr_get_value_type_ff')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: kvs
     end function kmr_get_value_type
  end interface kmr_get_value_type

  !> Returns the element count local on each node.

  interface kmr_local_element_count
     integer(c_int) function kmr_local_element_count(kvs, v) &
          bind(c, name='kmr_local_element_count')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: kvs
       integer(c_long), intent(out) :: v
     end function kmr_local_element_count
  end interface kmr_local_element_count

  interface kmr_init_ff
     integer(c_int) function kmr_init_ff(kf, opt, fopt) &
          bind(c, name='kmr_init_ff')
       use iso_c_binding
       implicit none
       integer(c_int), value, intent(in) :: kf
       integer(c_long), value, intent(in) :: opt
       integer(c_long), value, intent(in) :: fopt
     end function kmr_init_ff
  end interface kmr_init_ff

  !> (See ::kmr_fin() in C).

  interface kmr_fin
     integer(c_int) function kmr_fin() bind(c, name='kmr_fin')
       use iso_c_binding
       implicit none
     end function kmr_fin
  end interface kmr_fin

  interface kmr_create_context_ff
     type(c_ptr) function kmr_create_context_ff(comm, info, name) &
          bind(c, name='kmr_create_context_ff')
       use iso_c_binding
       implicit none
       integer(c_int), value, intent(in) :: comm
       integer(c_int), value, intent(in) :: info
       type(c_ptr), intent(in) :: name
     end function kmr_create_context_ff
  end interface kmr_create_context_ff

  !> (See ::kmr_free_context() in C).

  interface kmr_free_context
     integer(c_int) function kmr_free_context(mr) &
          bind(c, name='kmr_free_context')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: mr
     end function kmr_free_context
  end interface kmr_free_context

  !> (See ::kmr_get_context_of_kvs() in C).

  interface kmr_get_context_of_kvs
     type(c_ptr) function kmr_get_context_of_kvs(kvs) &
          bind(c, name='kmr_get_context_of_kvs')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: kvs
     end function kmr_get_context_of_kvs
  end interface kmr_get_context_of_kvs

  interface kmr_create_kvs7_ff
     type(c_ptr) function kmr_create_kvs7_ff(mr, kf, vf, opt, f, l, n) &
          bind(c, name='kmr_create_kvs7')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: mr
       integer(c_int), value, intent(in) :: kf, vf
       integer(c_long), value, intent(in) :: opt
       type(c_ptr), value, intent(in) :: f
       integer(c_int), value, intent(in) :: l
       type(c_ptr), value, intent(in) :: n
     end function kmr_create_kvs7_ff
  end interface kmr_create_kvs7_ff

  !> (See ::kmr_free_kvs() in C).

  interface kmr_free_kvs
     integer(c_int) function kmr_free_kvs(kvs) bind(c, name='kmr_free_kvs')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: kvs
     end function kmr_free_kvs
  end interface kmr_free_kvs

  !> (See ::kmr_add_kv() in C).

  interface kmr_add_kv
     integer(c_int) function kmr_add_kv(kvs, kv) bind(c, name='kmr_add_kv')
       use iso_c_binding
       import kmr_kv_box
       implicit none
       type(c_ptr), value, intent(in) :: kvs
       type(kmr_kv_box), value, intent(in) :: kv;
     end function kmr_add_kv
  end interface kmr_add_kv

  !> (See ::kmr_add_kv_done() in C).

  interface kmr_add_kv_done
     integer(c_int) function kmr_add_kv_done(kvs) &
          bind(c, name='kmr_add_kv_done')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: kvs
     end function kmr_add_kv_done
  end interface kmr_add_kv_done

  interface kmr_map_ff
     integer(c_int) function kmr_map_ff(s, kvi, kvo, p, opt, m, g, l, f) &
          bind(c, name='kmr_map9')
       use iso_c_binding
       implicit none
       integer(c_int), value, intent(in) :: s
       type(c_ptr), value, intent(in) :: kvi, kvo
       type(c_ptr), value, intent(in) :: p
       integer(c_long), value, intent(in) :: opt
       type(c_funptr), value, intent(in) :: m
       type(c_ptr), value, intent(in) :: g
       integer(c_int), value, intent(in) :: l
       type(c_ptr), value, intent(in) :: f
     end function kmr_map_ff
  end interface kmr_map_ff

  interface kmr_map_on_rank_zero_ff
     integer(c_int) function kmr_map_on_rank_zero_ff(kvo, p, opt, m) &
          bind(c, name='kmr_map_on_rank_zero')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: kvo
       type(c_ptr), value, intent(in) :: p
       integer(c_long), value, intent(in) :: opt
       type(c_funptr), value, intent(in) :: m
     end function kmr_map_on_rank_zero_ff
  end interface kmr_map_on_rank_zero_ff

  interface kmr_map_once_ff
     integer(c_int) function kmr_map_once_ff(kvo, p, opt, rankzeroonly, m) &
          bind(c, name='kmr_map_once')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: kvo
       type(c_ptr), value, intent(in) :: p
       integer(c_long), value, intent(in) :: opt
       logical(c_bool), value, intent(in) :: rankzeroonly
       type(c_funptr), value, intent(in) :: m
     end function kmr_map_once_ff
  end interface kmr_map_once_ff

  interface kmr_map_ms_ff
     integer(c_int) function kmr_map_ms_ff(kvi, kvo, p, opt, m) &
          bind(c, name='kmr_map_ms')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: kvi, kvo
       type(c_ptr), value, intent(in) :: p
       integer(c_long), value, intent(in) :: opt
       type(c_funptr), value, intent(in) :: m
     end function kmr_map_ms_ff
  end interface kmr_map_ms_ff

  interface kmr_map_via_spawn_ff
     integer(c_int) function kmr_map_via_spawn_ff(kvi, kvo, p, &
          info, opt, m) &
          bind(c, name='kmr_map_via_spawn_ff')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: kvi, kvo
       type(c_ptr), value, intent(in) :: p
       integer(c_int), value, intent(in) :: info
       integer(c_long), value, intent(in) :: opt
       type(c_funptr), value, intent(in) :: m
     end function kmr_map_via_spawn_ff
  end interface kmr_map_via_spawn_ff

  interface kmr_get_spawner_communicator_ff
     integer(c_int) function kmr_get_spawner_communicator_ff(mr, &
          ii, comm) bind(c, name='kmr_get_spawner_communicator_ff')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: mr
       integer(c_long), value, intent(in) :: ii
       integer(c_int), intent(out) :: comm
     end function kmr_get_spawner_communicator_ff
  end interface kmr_get_spawner_communicator_ff

  interface kmr_sort_locally_ff
     integer(c_int) function kmr_sort_locally_ff(kvi, kvo, shuffling, opt) &
          bind(c, name='kmr_sort_locally')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: kvi, kvo
       logical(c_bool), value, intent(in) :: shuffling
       integer(c_long), value, intent(in) :: opt
     end function kmr_sort_locally_ff
  end interface kmr_sort_locally_ff

  interface kmr_shuffle_ff
     integer(c_int) function kmr_shuffle_ff(kvi, kvo, opt) &
          bind(c, name='kmr_shuffle')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: kvi, kvo
       integer(c_long), value, intent(in) :: opt
     end function kmr_shuffle_ff
  end interface kmr_shuffle_ff

  interface kmr_replicate_ff
     integer(c_int) function kmr_replicate_ff(kvi, kvo, opt) &
          bind(c, name='kmr_replicate')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: kvi, kvo
       integer(c_long), value, intent(in) :: opt
     end function kmr_replicate_ff
  end interface kmr_replicate_ff

  interface kmr_reduce_ff
     integer(c_int) function kmr_reduce_ff(s, kvi, kvo, p, opt, r, g, l, f) &
          bind(c, name='kmr_reduce9')
       use iso_c_binding
       implicit none
       integer(c_int), value, intent(in) :: s
       type(c_ptr), value, intent(in) :: kvi, kvo
       type(c_ptr), value, intent(in) :: p
       integer(c_long), value, intent(in) :: opt
       type(c_funptr), value, intent(in) :: r
       type(c_ptr), value, intent(in) :: g
       integer(c_int), value, intent(in) :: l
       type(c_ptr), value, intent(in) :: f
     end function kmr_reduce_ff
  end interface kmr_reduce_ff

  !> (See ::kmr_dump_kvs() in C).

  interface kmr_dump_kvs
     integer(c_int) function kmr_dump_kvs(kvi, f) &
          bind(c, name='kmr_dump_kvs')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: kvi
       integer(c_int), value, intent(in) :: f
     end function kmr_dump_kvs
  end interface kmr_dump_kvs

  !> (See ::kmr_get_element_count() in C).

  interface kmr_get_element_count
     integer(c_int) function kmr_get_element_count(kvs, v) &
          bind(c, name='kmr_get_element_count')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: kvs
       integer(c_long), intent(out) :: v
     end function kmr_get_element_count
  end interface kmr_get_element_count

  interface kmr_sort_ff
     integer(c_int) function kmr_sort_ff(kvi, kvo, opt) &
          bind(c, name='kmr_sort')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: kvi, kvo
       integer(c_long), value, intent(in) :: opt
     end function kmr_sort_ff
  end interface kmr_sort_ff

  interface kmr_reverse_ff
     integer(c_int) function kmr_reverse_ff(kvi, kvo, opt) &
          bind(c, name='kmr_reverse')
       use iso_c_binding
       implicit none
       type(c_ptr), value, intent(in) :: kvi, kvo
       integer(c_long), value, intent(in) :: opt
     end function kmr_reverse_ff
  end interface kmr_reverse_ff

contains

  !> Asserts the expression to be true.

  subroutine kmr_assert(v, expr)
    include "mpif.h"
    logical, intent(in) :: v
    character(len=*), intent(in) :: expr
    if (v) return
    print *, "Assertion failed: ", trim(expr)
    call mpi_abort(MPI_COMM_WORLD, 1)
    !call exit_with_status(1)
  end subroutine kmr_assert

  !> Places null function.  Use kmr_nullmapfn for a C null function.
  !> It is a placeholder and never called.

  integer(c_int) function kmr_nullmapfn(kv, kvi, kvo, p, i) bind(c) result(zz)
    use iso_c_binding
    implicit none
    type(kmr_kv_box), value, intent(in) :: kv
    type(c_ptr), value, intent(in) :: kvi, kvo
    type(c_ptr), value, intent(in) :: p
    integer(c_long), value, intent(in) :: i
    integer(c_long) :: z
    ! JUST SUPPRESS WARNINGS
    call kmr_assert(c_associated(kvi) .and. c_associated(kvo) &
         .and. c_associated(p), &
         "c_associated(kvi) .and. c_associated(kvo) .and. c_associated(p)")
    z = (kv%vlen + i)
    zz = 0
  end function kmr_nullmapfn

  ! Fixes null functions to zero for C.

  !type(c_funptr) function kmr_fixfun(m) result(zz)
  !  type(c_funptr), value, intent(in) :: m
  !  if (c_associated(m, c_funloc(kmr_nullmapfn))) then
  !     zz = c_null_funptr
  !  else
  !     zz = m
  !  end if
  !end function kmr_fixfun

  !> Fixes bits-endian of option bits.

  integer(c_long) function kmr_fixopt(b) result(zz)
    integer(c_int), value, intent(in) :: b
    zz = kmr_fix_bits_endian_ff(int(b, c_long))
  end function kmr_fixopt

  !> (See ::kmr_init() in C).

  integer(c_int) function kmr_init() result(zz)
    call kmr_assert(c_int > 0, 'c_int > 0')
    call kmr_assert(c_long > 0, 'c_long > 0')
    !call kmr_assert(c_ptr > 0, 'c_ptr > 0')
    !call kmr_assert(c_sizeof(v) == 8, 'c_sizeof(c_long) == 8')
    zz = kmr_init_ff(kmr_kv_pointer_unmanaged, &
         kmr_fixopt(kmr_rank_zero), kmr_fixopt(kmr_shuffle_names))
  end function kmr_init

  !> (See ::kmr_create_context() in C).

  type(c_ptr) function kmr_create_context(comm, info) result(zz)
    integer, value, intent(in) :: comm
    integer, value, intent(in) :: info
    zz = kmr_create_context_ff(int(comm, c_int), &
         int(info, c_int), c_null_ptr)
  end function kmr_create_context

  !> (See ::kmr_create_kvs() in C).

  type(c_ptr) function kmr_create_kvs(mr, kf, vf) result(zz)
    type(c_ptr), value, intent(in) :: mr
    integer(c_int), value, intent(in) :: kf, vf
    zz = kmr_create_kvs7_ff(mr, kf, vf, int(0, c_long), &
         c_null_ptr, 0, c_null_ptr)
  end function kmr_create_kvs

  !> (See ::kmr_map() in C).

  integer(c_int) function kmr_map(kvi, kvo, p, opt, m) result(zz)
    type(c_ptr), value, intent(in) :: kvi, kvo
    type(c_ptr), value, intent(in) :: p
    integer(c_int), value, intent(in) :: opt
    procedure(kmr_mapfn), bind(c) :: m
    zz = kmr_map_ff(0, kvi, kvo, p, kmr_fixopt(opt), c_funloc(m), &
         c_null_ptr, 0, c_null_ptr)
  end function kmr_map

  !> (See ::kmr_map_on_rank_zero() in C).

  integer(c_int) function kmr_map_on_rank_zero(kvo, p, opt, m) result(zz)
    type(c_ptr), value, intent(in) :: kvo
    type(c_ptr), value, intent(in) :: p
    integer(c_int), value, intent(in) :: opt
    procedure(kmr_mapfn), bind(c) :: m
    !print *, "kmr_map_on_rank_zero:kvo=", kmr_ptrint(kvo)
    !print *, "kmr_map_on_rank_zero:m=", kmr_ptrint(c_funloc(m))
    call kmr_assert(c_associated(kvo), 'c_associated(kvo)')
    zz = kmr_map_on_rank_zero_ff(kvo, p, kmr_fixopt(opt), c_funloc(m))
  end function kmr_map_on_rank_zero

  !> (See ::kmr_map_once() in C).

  integer(c_int) function kmr_map_once(kvo, p, opt, rankzeroonly, m) result(zz)
    type(c_ptr), value, intent(in) :: kvo
    type(c_ptr), value, intent(in) :: p
    integer(c_int), value, intent(in) :: opt
    logical, value, intent(in) :: rankzeroonly
    procedure(kmr_mapfn), bind(c) :: m
    logical(c_bool) :: bb
    call kmr_assert(c_associated(kvo), 'c_associated(kvo)')
    if (rankzeroonly) then
       bb = .true.
    else
       bb = .false.
    end if
    zz = kmr_map_once_ff(kvo, p, kmr_fixopt(opt), bb, c_funloc(m))
  end function kmr_map_once

  !> (See ::kmr_map_ms() in C).

  integer(c_int) function kmr_map_ms(kvi, kvo, p, opt, m) result(zz)
    type(c_ptr), value, intent(in) :: kvi, kvo
    type(c_ptr), value, intent(in) :: p
    integer(c_int), value, intent(in) :: opt
    procedure(kmr_mapfn), bind(c) :: m
    call kmr_assert(c_associated(kvi), 'c_associated(kvi)')
    zz = kmr_map_ms_ff(kvi, kvo, p, kmr_fixopt(opt), c_funloc(m))
  end function kmr_map_ms

  !> (See ::kmr_map_via_spawn() in C).

  integer(c_int) function kmr_map_via_spawn(kvi, kvo, p, info, opt, m) &
       result(zz)
    type(c_ptr), value, intent(in) :: kvi, kvo
    type(c_ptr), value, intent(in) :: p
    integer, value, intent(in) :: info
    integer(c_int), value, intent(in) :: opt
    procedure(kmr_mapfn), bind(c) :: m
    type(c_funptr) :: fp
    call kmr_assert(c_associated(kvi), 'c_associated(kvi)')
    !fp = kmr_fixfun(c_funloc(m))
    if (c_associated(c_funloc(m), c_funloc(kmr_nullmapfn))) then
       fp = c_null_funptr
    else
       fp = c_funloc(m)
    end if
    zz = kmr_map_via_spawn_ff(kvi, kvo, p, int(info, c_int), &
         kmr_fixopt(opt), fp)
  end function kmr_map_via_spawn

  !> (See ::kmr_get_spawner_communicator() in C).
  !> MPI_Comm_free() cannot be used on the returned communicator in
  !> the Fortran binding.

  integer(c_int) function kmr_get_spawner_communicator(mr, ii, comm) &
       result(zz)
    type(c_ptr), value, intent(in) :: mr
    integer(c_long), value, intent(in) :: ii
    integer, intent(out) :: comm
    zz = kmr_get_spawner_communicator_ff(mr, ii, comm)
  end function kmr_get_spawner_communicator

  !> (See ::kmr_sort_locally() in C).

  integer(c_int) function kmr_sort_locally(kvi, kvo, shuffling, opt) result(zz)
    type(c_ptr), value, intent(in) :: kvi, kvo
    logical(c_bool), value, intent(in) :: shuffling
    integer(c_int), value, intent(in) :: opt
    call kmr_assert(c_associated(kvi), 'c_associated(kvi)')
    call kmr_assert(c_associated(kvo), 'c_associated(kvo)')
    zz = kmr_sort_locally_ff(kvi, kvo, shuffling, kmr_fixopt(opt))
  end function kmr_sort_locally

  !> (See ::kmr_shuffle() in C).

  integer(c_int) function kmr_shuffle(kvi, kvo, opt) result(zz)
    type(c_ptr), value, intent(in) :: kvi, kvo
    integer(c_int), value, intent(in) :: opt
    call kmr_assert(c_associated(kvi), 'c_associated(kvi)')
    call kmr_assert(c_associated(kvo), 'c_associated(kvo)')
    zz = kmr_shuffle_ff(kvi, kvo, kmr_fixopt(opt))
  end function kmr_shuffle

  !> (See ::kmr_replicate() in C).

  integer(c_int) function kmr_replicate(kvi, kvo, opt) result(zz)
    type(c_ptr), value, intent(in) :: kvi, kvo
    integer(c_int), value, intent(in) :: opt
    call kmr_assert(c_associated(kvi), 'c_associated(kvi)')
    call kmr_assert(c_associated(kvo), 'c_associated(kvo)')
    zz = kmr_replicate_ff(kvi, kvo, kmr_fixopt(opt))
  end function kmr_replicate

  !> (See ::kmr_reduce() in C).

  integer(c_int) function kmr_reduce(kvi, kvo, p, opt, r) result(zz)
    type(c_ptr), value, intent(in) :: kvi, kvo
    type(c_ptr), value, intent(in) :: p
    integer(c_int), value, intent(in) :: opt
    procedure(kmr_redfn), bind(c) :: r
    call kmr_assert(c_associated(kvi), 'c_associated(kvi)')
    zz = kmr_reduce_ff(0, kvi, kvo, p, kmr_fixopt(opt), c_funloc(r), &
         c_null_ptr, 0, c_null_ptr)
  end function kmr_reduce

  !> (See ::kmr_sort() in C).

  integer(c_int) function kmr_sort(kvi, kvo, opt) result(zz)
    type(c_ptr), value, intent(in) :: kvi, kvo
    integer(c_int), value, intent(in) :: opt
    call kmr_assert(c_associated(kvi), 'c_associated(kvi)')
    call kmr_assert(c_associated(kvo), 'c_associated(kvo)')
    zz = kmr_sort_ff(kvi, kvo, kmr_fixopt(opt))
  end function kmr_sort

  !> (See ::kmr_reverse() in C).

  integer(c_int) function kmr_reverse(kvi, kvo, opt) result(zz)
    type(c_ptr), value, intent(in) :: kvi, kvo
    integer(c_int), value, intent(in) :: opt
    call kmr_assert(c_associated(kvi), 'c_associated(kvi)')
    call kmr_assert(c_associated(kvo), 'c_associated(kvo)')
    zz = kmr_reverse_ff(kvi, kvo, kmr_fixopt(opt))
  end function kmr_reverse

  !! BELOWS ARE TO BE ADDED SOON

  !!kmr_add_string

  !!kmr_concatenate_kvs
  !!kmr_local_element_count
  !!kmr_dump_kvs_stats
  !!kmr_save_kvs
  !!kmr_restore_kvs
  !!kmr_retrieve_kvs_entries

  !!kmr_map_rank_by_rank
  !!kmr_map_for_some
  !!kmr_map_ms_commands

  !!kmr_reply_to_spawner
  !!kmr_map_processes
  !!kmr_send_kvs_to_spawner
  !!kmr_receive_kvs_from_spawned_fn

  !!kmr_reduce_as_one
  !!kmr_reduce_for_some

  !!kmr_distribute

  !!kmr_read_files_reassemble
  !!kmr_read_file_by_segments

end module kmrf

! Copyright (C) 2012-2016 RIKEN AICS
! This library is distributed WITHOUT ANY WARRANTY.  This library can be
! redistributed and/or modified under the terms of the BSD 2-Clause License.
