! Word Count (2015-05-29)

! It ranks the words by their occurrence count in the "LICENSE" file.
! Copy the file in the current directory and run it.

module wcfn

  implicit none

  !integer, target :: foobar

  !type :: tuple2
  !integer :: a0, a1
  !end type tuple2

  integer, parameter :: sizeoflong = 8
  integer, parameter :: linewidth = 80
  integer, parameter :: wordsize = 25

contains

  function isalpha(c) result(zz)
    use iso_c_binding
    implicit none
    character(kind=c_char), intent(in) :: c
    logical :: zz
    zz = ((ichar('a') <= ichar(c) .and. ichar(c) <= ichar('z')) &
         .or. (ichar('A') <= ichar(c) .and. ichar(c) <= ichar('Z')))
  end function isalpha

  function upcase(s) result(zz)
    use iso_c_binding
    implicit none
    character(kind=c_char, len=*), intent(in) :: s
    character(kind=c_char, len=len(s)) :: zz
    integer :: n, i
    character(*), parameter :: UC = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    character(*), parameter :: LC = "abcdefghijklmnopqrstuvwxyz"
    zz = s
    do i = 1, len_trim(s)
       n = index(LC, s(i:i))
       if (n /= 0) zz(i:i) = UC(n:n)
    end do
  end function upcase

  integer(c_int) function read_words_from_a_file(kv, kvi, kvo, p, i) &
       bind(c) result(zz)
    use iso_c_binding
    use kmrf
    implicit none
    type(kmr_kv_box), value, intent(in) :: kv
    type(c_ptr), value, intent(in) :: kvi, kvo
    type(c_ptr), value, intent(in) :: p
    integer(c_long), value, intent(in) :: i

    type(kmr_kv_box) :: nkv
    integer :: j, r

    character(kind=c_char, len=wordsize) :: b
    character(len=linewidth) :: line
    character(kind=c_char) :: cc
    integer ios

    call kmr_assert(.not. c_associated(kvi) .and. kv%klen == 0 &
         .and. kv%vlen == 0 .and. c_associated(kvo), &
         "kvi == 0 && kv.klen == 0 && kv.vlen == 0 && kvo != 0")
    open(17, file='LICENSE', status='old')
    j = 0
    do while (.true.)
       call kmr_assert(j <= (wordsize - 1), "j <= (wordsize - 1)")
       read(17, '(a)', iostat=ios) line
       if (ios .lt. 0) exit
       call kmr_assert(len(trim(line)) < linewidth, &
            "len(trim(line)) < linewidth")
       !!(Look at one more trailing char for flushing word at line end)
       do r = 1, len(trim(line)) + 1
          cc = line(r:r)
          if ((ios .lt. 0 .or. .not. isalpha(cc) .or. (j == (wordsize - 1))) &
               .and. j /= 0) then
             b(j+1:) = C_NULL_CHAR
             nkv%klen = (j + 1)
             nkv%k = kmr_strint(b)
             nkv%vlen = sizeoflong
             nkv%v = 1
             zz = kmr_add_kv(kvo, nkv)
             j = 0
          end if
          if (ios .lt. 0) exit
          if (isalpha(cc)) then
             b(j+1:) = cc
             j = j + 1
          end if
       end do
    end do
    close(17)
    zz = 0
  end function read_words_from_a_file

  integer(c_int) function print_top_five(kv, kvi, kvo, p, i) &
       bind(c) result(zz)
    use iso_c_binding
    use kmrf
    implicit none
    type(kmr_kv_box), value, intent(in) :: kv
    type(c_ptr), value, intent(in) :: kvi, kvo
    type(c_ptr), value, intent(in) :: p
    integer(c_long), value, intent(in) :: i

    integer :: rank
    integer(c_int) :: n
    character(kind=c_char, len=kv%vlen), target :: v

    rank = kmr_get_rank(kvi);
    if (rank == 0 .and. i < 5) then
       n = kmr_intstr(kv%v, v, kv%vlen)
       print "(A,A,A,I0)", "#", v(1:kv%vlen-1), "=", (0 - kv%k)
    end if
    zz = 0
  end function print_top_five

  integer(c_int) function sum_counts_for_a_word(kv, n, kvi, kvo, p) &
       bind(c) result(zz)
    use iso_c_binding
    use kmrf
    implicit none
    type(kmr_kv_box), intent(in) :: kv(*)
    integer(c_long), value, intent(in) :: n
    type(c_ptr), value, intent(in) :: kvi, kvo
    type(c_ptr), value, intent(in) :: p

    integer(c_long) :: i
    integer(c_long) :: c
    type(kmr_kv_box) :: nkv

    c = 0
    do i = 1, n
       c = c + kv(i)%v
    end do

    nkv%klen = kv(1)%klen
    nkv%k = kv(1)%k
    nkv%vlen = sizeoflong
    nkv%v = -c
    zz = kmr_add_kv(kvo, nkv)
  end function sum_counts_for_a_word

end module wcfn

program main
  use iso_c_binding
  use kmrf
  use wcfn
  implicit none
  include "mpif.h"

  type(c_ptr) :: mr
  type(c_ptr) :: kvs0, kvs1, kvs2, kvs3, kvs4
  integer :: nprocs, rank, thlv
  integer :: ierr

  call mpi_init_thread(MPI_THREAD_SERIALIZED, thlv, ierr)
  call mpi_comm_size(MPI_COMM_WORLD, nprocs, ierr)
  call mpi_comm_rank(MPI_COMM_WORLD, rank, ierr)
  ierr = kmr_init()
  mr = kmr_create_context(MPI_COMM_WORLD, MPI_INFO_NULL)

  call mpi_barrier(MPI_COMM_WORLD, ierr)
  if (rank == 0) print "(A)", "Ranking words..."

  kvs0 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER)
  ierr = kmr_map_once(kvs0, c_null_ptr, 0, .false., read_words_from_a_file)

  kvs1 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER)
  ierr = kmr_shuffle(kvs0, kvs1, 0)

  kvs2 = kmr_create_kvs(mr, KMR_KV_OPAQUE, KMR_KV_INTEGER)
  ierr = kmr_reduce(kvs1, kvs2, c_null_ptr, 0, sum_counts_for_a_word)

  kvs3 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE)
  ierr = kmr_reverse(kvs2, kvs3, 0)

  kvs4 = kmr_create_kvs(mr, KMR_KV_INTEGER, KMR_KV_OPAQUE)
  ierr = kmr_sort(kvs3, kvs4, 0)

  ierr = kmr_map(kvs4, c_null_ptr, c_null_ptr, 0, print_top_five)

  ierr = kmr_free_context(mr)
  ierr = kmr_fin()
  call mpi_finalize()
end program main
