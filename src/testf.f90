! testf.f90 (2014-02-04)

module testfn
  implicit none

  integer, target :: foobar

  type :: tuple2
     integer :: a0, a1
  end type tuple2

contains

  function upcase(s) result(ss)
    use iso_c_binding
    implicit none
    character(kind=c_char,len=*), intent(in) :: s
    character(kind=c_char,len=len(s)) :: ss
    integer :: n, i
    character(*), parameter :: UC = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    character(*), parameter :: LC = "abcdefghijklmnopqrstuvwxyz"
    ss = s
    do i = 1, len_trim(s)
       n = index(LC, s(i:i))
       if (n /= 0) ss(i:i) = UC(n:n)
    end do
  end function upcase

  integer(c_int) function addfivekeysfn(kv, kvi, kvo, p, i) bind(c) result(zz)
    use iso_c_binding
    use kmrf
    implicit none
    type(kmr_kv_box), value, intent(in) :: kv
    type(c_ptr), value, intent(in) :: kvi, kvo
    type(c_ptr), value, intent(in) :: p
    integer(c_long), value, intent(in) :: i
    type(kmr_kv_box) :: nkv
    integer :: j
    character(kind=c_char,len=1) :: strj
    character(kind=c_char,len=5), target :: k
    character(kind=c_char,len=7), target :: v
    !print *, "mapfn:kvo=", kmr_ptrint(kvo)
    do j = 0, 4
       nkv%klen = 5
       nkv%vlen = 7
       write(strj, "(I1)") j
       k = (c_char_"key" // strj // C_NULL_CHAR)
       v = (c_char_"value" // strj // C_NULL_CHAR)
       nkv%k = kmr_strint(k)
       nkv%v = kmr_strint(v)
       zz = kmr_add_kv(kvo, nkv)
    end do
    zz = 0
  end function addfivekeysfn

  integer(c_int) function replacevaluefn(kv, kvi, kvo, p, i) bind(c) result(zz)
    use iso_c_binding
    use kmrf
    implicit none

    type(kmr_kv_box), value, intent(in) :: kv
    type(c_ptr), value, intent(in) :: kvi, kvo
    type(c_ptr), value, intent(in) :: p
    integer(c_long), value, intent(in) :: i
    type(kmr_kv_box) :: nkv
    character(kind=c_char,len=kv%vlen), target :: v0
    character(kind=c_char,len=kv%vlen), target :: v1
    integer(c_int) :: n
    nkv%klen = kv%klen
    nkv%vlen = kv%vlen
    nkv%k = kv%k
    n = kmr_intstr(kv%v, v0, kv%vlen)
    v1 = upcase(v0)
    nkv%v = kmr_strint(v1)
    zz = kmr_add_kv(kvo, nkv)
  end function replacevaluefn

  integer(c_int) function starttaskfn(kv, kvi, kvo, p, i) bind(c) result(zz)
    use iso_c_binding
    use kmrf
    implicit none
    type(kmr_kv_box), value, intent(in) :: kv
    type(c_ptr), value, intent(in) :: kvi, kvo
    type(c_ptr), value, intent(in) :: p
    integer(c_long), value, intent(in) :: i
    character(kind=c_char,len=kv%klen), target :: k
    character(kind=c_char,len=kv%vlen), target :: v
    integer(c_int) :: n, rank
    n = kmr_intstr(kv%k, k, kv%klen)
    n = kmr_intstr(kv%v, v, kv%vlen)
    rank = kmr_get_rank(kvi)
    print "(A,A,A,A,A,A,I0)", "Start master/worker task...", &
         " key=", k(1:kv%klen-1), &
         " value=", v(1:kv%vlen-1), " rank=", rank
    zz = kmr_add_kv(kvo, kv)
  end function starttaskfn

  integer(c_int) function printpairsfn(kv, n, kvi, kvo, p) bind(c) result(zz)
    use iso_c_binding
    use kmrf
    implicit none
    type(kmr_kv_box), intent(in) :: kv(*)
    integer(c_long), value, intent(in) :: n
    type(c_ptr), value, intent(in) :: kvi, kvo
    type(c_ptr), value, intent(in) :: p
    character(kind=c_char,len=5), target :: k
    character(kind=c_char,len=7), target :: v
    integer(c_long) :: i
    integer(c_int) :: nn, rank
    rank = kmr_get_rank(kvi);
    print "(A,I0,A,I0)", "Reducing count=", n, " rank=", rank
    do i = 1, n
       nn = kmr_intstr(kv(i)%k, k, kv(i)%klen)
       nn = kmr_intstr(kv(i)%v, v, kv(i)%vlen)
       print "(A,I0,A,A,A,A)", "index=", i, " key=", k(1:kv(i)%klen-1), &
            " value=", v(1:kv(i)%vlen-1)
    end do
    zz = 0
  end function printpairsfn

  integer(c_int) function addstructfn(kv, kvi, kvo, p, i) bind(c) result(zz)
    use iso_c_binding
    use kmrf
    implicit none
    type(kmr_kv_box), value, intent(in) :: kv
    type(c_ptr), value, intent(in) :: kvi, kvo
    type(c_ptr), value, intent(in) :: p
    integer(c_long), value, intent(in) :: i
    type(kmr_kv_box) :: nkv
    character(kind=c_char,len=3) :: stra0
    character(kind=c_char,len=3) :: stra1
    character(kind=c_char,len=7), target :: k
    character(kind=c_char,len=9), target :: v
    type(tuple2), pointer :: ptr
    call c_f_pointer(p, ptr)
    write(stra0, "(I3)") ptr%a0
    write(stra1, "(I3)") ptr%a1
    k = (c_char_"key" // stra0 // C_NULL_CHAR)
    v = (c_char_"value" // stra1 // C_NULL_CHAR)
    nkv%k = kmr_strint(k)
    nkv%v = kmr_strint(v)
    nkv%klen = 7
    nkv%vlen = 9
    zz = kmr_add_kv(kvo, nkv)
  end function addstructfn

  integer(c_int) function addcommandsfn(kv, kvi, kvo, p, ii) bind(c) result(zz)
    use iso_c_binding
    use kmrf
    implicit none
    type(kmr_kv_box), value, intent(in) :: kv
    type(c_ptr), value, intent(in) :: kvi, kvo
    type(c_ptr), value, intent(in) :: p
    integer(c_long), value, intent(in) :: ii
    type(kmr_kv_box) :: nkv
    character(kind=c_char,len=4), target :: k
    character(kind=c_char,len=100), target :: v
    character(kind=c_char,len=1), target :: reply
    integer :: i
    if (c_associated(p)) then
       reply = c_char_"1"
    else
       reply = c_char_"0"
    end if
    nkv%klen = 4
    nkv%vlen = (11 + 8 + 2 + 3 + 3 + 3)
    k = (c_char_"key" // C_NULL_CHAR)
    v = (c_char_"maxprocs=2" // c_char_" " &
         // c_char_"./a.out" // c_char_" " &
         // reply // c_char_" " &
         // c_char_"a0" // c_char_" " &
         // c_char_"a1" // c_char_" " &
         // c_char_"a2" // C_NULL_CHAR)
    nkv%k = kmr_strint(k)
    nkv%v = kmr_strint(v)
    do i = 1, 4
       zz = kmr_add_kv(kvo, nkv)
    end do
    zz = 0
  end function addcommandsfn

  integer(c_int) function waitprocfn(kv, kvi, kvo, p, ii) bind(c) result(zz)
    use iso_c_binding
    use kmrf
    implicit none
    include "mpif.h"
    type(kmr_kv_box), value, intent(in) :: kv
    type(c_ptr), value, intent(in) :: kvi, kvo
    type(c_ptr), value, intent(in) :: p
    integer(c_long), value, intent(in) :: ii
    type(c_ptr) :: mr
    integer :: ic
    integer :: ierr
    mr = kmr_get_context_of_kvs(kvi)
    ierr = kmr_get_spawner_communicator(mr, ii, ic)
    print "(A,(I0),A)", "(waitprocfn sleeping(3)... icomm=", ic, ")"
    !call mpi_comm_free(ic, ierr)
    call sleep(3)
    zz = 0
  end function waitprocfn

end module testfn

program main
  use iso_c_binding
  use kmrf
  use testfn
  implicit none
  include "mpif.h"

  integer :: argc, ierr, rank, nprocs, thlv
  character(len=128) argi
  type(c_ptr) :: mr
  type(c_ptr) :: kvs0, kvs1, kvs2, kvs3, kvs4, kvs5
  type(c_ptr) :: kvs10
  type(c_ptr) :: kvs20, kvs21, kvs22, kvs23, kvs24, kvs25
  integer(c_long) :: cnt0, cnt1, cnt2, cnt3, cnt5
  integer(c_int) :: opt
  character(len=128) keepstack
  integer :: parent, sz, peernprocs
  logical :: maybespawned, needreply
  integer :: iargc
  character(len=MPI_MAX_PROCESSOR_NAME) :: name
  integer :: namelen
  type(tuple2), target :: tuple
  integer :: sig

  ! Disable backtracing (which unwinds stack) on ILL/ABRT/SEGV

  call getenv("KEEPSTACK", keepstack)
  if (keepstack(1:2) /= "") then
     call signal(4, 1, sig)
     call signal(6, 1, sig)
     call signal(11, 1, sig)
  end if

  ! PARENT BE WITHOUT ARGUMENTS; CHILD BE WITH ARGUMENTS.

  argc = iargc()
  if (argc >= 1) then
     maybespawned = .true.
  else
     maybespawned = .false.
  end if

  call mpi_init_thread(MPI_THREAD_SERIALIZED, thlv, ierr);
  call mpi_comm_rank(mpi_comm_world, rank, ierr)
  call mpi_comm_size(mpi_comm_world, nprocs, ierr)
  call mpi_get_processor_name(name, namelen, ierr)

  ! CHILD PART:

  if (maybespawned) then
     call mpi_comm_get_parent(parent, ierr)
     if (parent == MPI_COMM_NULL) then
        print *, "NO PARENTS"
        call mpi_abort(1);
     end if
     call mpi_comm_remote_size(parent, peernprocs, ierr)
     call kmr_assert(peernprocs == 1, "peernprocs == 1")
     print *, "Spawned process runs (", trim(name), ")..."
     call getarg(1, argi)
     if (argi(1:1) == "0") then
        needreply = .false.
     else
        needreply = .true.
     end if
     call sleep(1)
     if (needreply) then
        sz = 0
        call mpi_send(sz, 0, mpi_byte, 0, 500, parent, ierr)
        print *, "(spawned process send reply (", trim(name), "))"
     end if
     call mpi_comm_free(parent, ierr)
     call mpi_finalize(ierr)
     print *, "Spawned process runs (", trim(name), ") DONE"
     call exit(0)
  end if

  ! NORMAL (PARENT) PART:

  ierr = kmr_init()

  mr = kmr_create_context(mpi_comm_world, mpi_info_null)

  kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE)
  ierr = kmr_map_on_rank_zero(kvs0, c_null_ptr, 0, addfivekeysfn)

  ierr = kmr_get_element_count(kvs0, cnt0)
  !print *, "cnt0=", cnt0
  !ierr = kmr_dump_kvs(kvs0, 0)
  call kmr_assert(cnt0 == 5, "cnt0 == 5")

  kvs1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE)
  ierr = kmr_replicate(kvs0, kvs1, 0);

  ierr = kmr_get_element_count(kvs1, cnt1)
  !print *, "cnt1=", cnt1
  !ierr = kmr_dump_kvs(kvs1, 0)
  call kmr_assert(cnt1 == (5 * nprocs), "cnt1 == (5 * nprocs)")

  kvs2 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE)
  ierr = kmr_map(kvs1, kvs2, c_null_ptr, 0, replacevaluefn)

  ierr = kmr_get_element_count(kvs2, cnt2)
  if (rank == 0) print "(A,I0)", "cnt2=", cnt2
  !ierr = kmr_dump_kvs(kvs2, 0)
  call kmr_assert(cnt2 == (5 * nprocs), "cnt2 == (5 * nprocs)")

  kvs3 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE)
  opt = kmr_rank_zero
  ierr = kmr_replicate(kvs2, kvs3, opt);
  !ierr = kmr_dump_kvs(kvs3, 0)

  ierr = kmr_local_element_count(kvs3, cnt3)
  if (rank == 0) then
     call kmr_assert(cnt3 == (5 * nprocs), "cnt3 == (5 * nprocs)")
  else
     call kmr_assert(cnt3 == 0, "cnt3 == 0")
  end if

  kvs4 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE)
  opt = kmr_nothreading
  ierr = 7
  do while (ierr /= 0)
     ierr = kmr_map_ms(kvs3, kvs4, c_null_ptr, opt, starttaskfn);
  end do

  kvs5 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE)
  ierr = kmr_shuffle(kvs4, kvs5, 0);

  !ierr = kmr_dump_kvs(kvs5, 0)
  ierr = kmr_get_element_count(kvs5, cnt5)
  if (rank == 0) print "(A,I0)", "cnt5=", cnt5
  call kmr_assert(cnt5 == (5 * nprocs), "cnt5 == (5 * nprocs)")

  call sleep(1)

  ierr = kmr_reduce(kvs5, c_null_ptr, c_null_ptr, 0, printpairsfn);

  call sleep(1)

  ! PASS STRUCTURE POINTERS

  tuple%a0 = 333
  tuple%a1 = 555

  kvs10 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE)
  ierr = kmr_map_on_rank_zero(kvs10, c_loc(tuple), 0, addstructfn)
  ierr = kmr_dump_kvs(kvs10, 0)
  ierr = kmr_free_kvs(kvs10)

  call sleep(5)
  if (rank == 0) print *, "Run spawn test with at least 4 dynamic processes"

  ! SPAWN WAITING IN MAP-FN

  call sleep(5)
  if (rank == 0) print *, "SPAWN WAITING IN MAP-FN"

  kvs20 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE)
  ierr = kmr_map_on_rank_zero(kvs20, c_null_ptr, 0, addcommandsfn)

  kvs21 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE)
  opt = ior(kmr_one_by_one, kmr_separator_space)
  ierr = kmr_map_via_spawn(kvs20, kvs21, c_loc(foobar), &
       mpi_info_null, opt, waitprocfn);
  ierr = kmr_free_kvs(kvs21)

  call sleep(5)
  call mpi_barrier(mpi_comm_world, ierr);

  ! SPAWN WAITING WITH REPLY_EACH

  call sleep(5)
  if (rank == 0) print *, "SPAWN WAITING WITH REPLY_EACH"

  kvs22 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE)
  ierr = kmr_map_on_rank_zero(kvs22, mr, 0, addcommandsfn)

  kvs23 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE)
  opt = ior(kmr_reply_each, kmr_separator_space)
  ierr = kmr_map_via_spawn(kvs22, kvs23, c_null_ptr, &
       mpi_info_null, opt, kmr_nullmapfn);
  ierr = kmr_free_kvs(kvs23)

  call sleep(5)
  call mpi_barrier(mpi_comm_world, ierr);

  ! SPAWN NO WAITING

  if (rank == 0) print *, "SPAWN NO WAITING"
  if (rank == 0) print *, "THIS PART FAILS WHEN LESS THAN 8 DYNAMIC PROCESSES"

  kvs24 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE)
  ierr = kmr_map_on_rank_zero(kvs24, c_null_ptr, 0, addcommandsfn)

  kvs25 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_OPAQUE)
  opt = kmr_separator_space
  ierr = kmr_map_via_spawn(kvs24, kvs25, c_null_ptr, &
       mpi_info_null, opt, kmr_nullmapfn);
  ierr = kmr_free_kvs(kvs25)

  call sleep(5)
  call mpi_barrier(mpi_comm_world, ierr);

  if (rank == 0) print *, "TEST DONE"

  ierr = kmr_free_context(mr)
  ierr = kmr_fin()

  call mpi_finalize(ierr)
  if (rank == 0) print *, "OK"
end program main
