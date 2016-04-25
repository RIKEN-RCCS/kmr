# configure.ac (2014-02-04)

# KMR only needs compilers for MPI, that is, MPICC/MPIF90, and prefers
# Fujitsu cross-compilers on K and FX10.  Thus, it does not run
# "AC_REQUIRE(AC_PROG_CC)" with normal CC, and runs it only after
# finding MPICC.  "m4_define"/"m4_undefine" are used for this purpose
# to place test code after finding MPICC.  The same for C++ and
# Fortran.  So, it first fails to test on compiling with "mpi.h"
# because MPICC is not yet found.  On K and FX10, C/CXX/FC FLAGS are
# explicitly set to skip adding "-g", because Fujitsu compilers
# generate slow debugging code with "-g".

AC_INIT([KMR], [1], [],	[], [http://mt.aics.riken.jp/kmr])
AC_PREREQ([2.68])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_FILES([config.make])

# 'version' is the version of KMR release and 'release_date' is its
# release date.
# Update these two variables when new version of KMR is released.
version="1.8.1"
release_date="20160425"

# It sets {target_os}, which is used to add rt library for
# clock_gettime() in Linux.

AC_CANONICAL_TARGET

# Check platforms, K and FX10.

test ! -d /opt/FJSVtclang -a ! -d /opt/FJSVfxlang; mp10fx10=$?

# Record of software version.
AC_SUBST(KMRFJSV) #[K/FX10 software version]

if test "1" -eq "$mp10fx10"; then
  test -z "$host_alias" && host_alias=sparc-sun-linux
  test "$cross_compiling"!="yes" && cross_compiling=maybe
  test -z "$CC" && CC=fccpx
  test -z "$CXX" && CXX=FCCpx
  test -z "$F77" && F77=frtpx
  test -z "$F90" && F90=frtpx
  test -z "$LD" && LD=ldpx
  if command -v mpifccpx >/dev/null 2>&1; then
    fjsv=`command -v mpifccpx`
    KMRFJSV="Env_base=$Env_base;`dirname $fjsv`"
  fi
fi

AC_ARG_ENABLE(debug,
[  --enable-debug          turn on debugging [default=no]],, enable_debug=no)

AC_ARG_ENABLE(assert,
[  --enable-assert         turn on assert [default=yes]],, enable_assert=yes)

AC_ARG_ENABLE(fortran,
[  --enable-fortran        turn on fortran binding [default=yes]],, enable_fortran=yes)

AC_ARG_ENABLE(openmp,
[  --enable-openmp         turn on openmp [default=yes]],, enable_openmp=yes)

AC_ARG_ENABLE(tofu,
[  --enable-tofu           turn on tofu support [default=yes]],, enable_tofu=yes)

AC_ARG_ENABLE(embed,
[  --enable-embed          turn on embedding helper commands [default=yes]],, enable_embed=yes)

AC_ARG_WITH(netcdf,
[  --with-netcdf=PATH      tell prefix to netcdf libs (not effective)], [], [])

AC_ARG_WITH(hdf5,
[  --with-hdf5=PATH        tell prefix to hdf5 libs (not effective)], [], [])

# Set CFLAGS early to suppress setting "-O2".

if test "$enable_debug" = "yes"; then
  CFLAGS="-DDEBUG -g $CFLAGS"
  CXXFLAGS="-DDEBUG -g $CXXFLAGS"
  FCFLAGS="-DDEBUG -g $FCFLAGS"
fi

# Set CFLAGS to suppress setting "-g" on K and FX10.

if test "1" -eq "$mp10fx10"; then
  test -z "$CFLAGS" && CFLAGS=""
  test -z "$CXXFLAGS" && CXXFLAGS=""
  test -z "$FCFLAGS" && FCFLAGS=""
fi

AC_SUBST(OPENMP_CFLAGS)   #[C openmp flags]
AC_SUBST(OPENMP_CXXFLAGS) #[C++ openmp flags]
AC_SUBST(OPENMP_FFLAGS)   #[F openmp flags]

# Check compiler vendors for C, C++, F90.

# Delay checking gcc after mpicc is set, by m4_define/undefine.

m4_define([m4_provide(AC_PROG_CC)], [])
AC_LANG(C)
AX_MPI()
CC=$MPICC
m4_undefine([m4_provide(AC_PROG_CC)])
AX_MPI()
if test "$enable_openmp" != "no"; then
  AX_OPENMP([OPENMP_CFLAGS=$ax_cv_c_openmp])
fi
AC_MSG_CHECKING([for compiler vendor of mpicc])
CC="$MPICC" sh config.ccvendor.sh >conftest.out 2>&1
CC_VENDOR=`cat conftest.out`
AC_MSG_RESULT([$CC_VENDOR])

m4_define([m4_provide(AC_PROG_CXX)], [])
AC_LANG(C++)
AX_MPI()
CXX=$MPICXX
m4_undefine([m4_provide(AC_PROG_CXX)])
AX_MPI()
if test "$enable_openmp" != "no"; then
  AX_OPENMP([OPENMP_CXXFLAGS=$ax_cv_cxx_openmp])
fi
AC_MSG_CHECKING([for compiler vendor of mpic++])
CC="$MPICXX" sh config.ccvendor.sh >conftest.out 2>&1
CXX_VENDOR=`cat conftest.out`
AC_MSG_RESULT([$CXX_VENDOR])

m4_define([m4_provide(AC_PROG_FC)], [])
AC_LANG(Fortran)
AX_MPI()
FC=$MPIFC
m4_undefine([m4_provide(AC_PROG_FC)])
AX_MPI()
if test "$enable_openmp" != "no"; then
  AX_OPENMP([OPENMP_FFLAGS=$ax_cv_fc_openmp])
fi
AC_MSG_CHECKING([for compiler vendor of mpif90])
CC="$MPIFC" sh config.ccvendor.sh >conftest.out 2>&1
FC_VENDOR=`cat conftest.out`
AC_MSG_RESULT([$FC_VENDOR])

# Set compiler flags for vendors.

AC_SUBST(SO_CCFLAGS) #[C pic flags]
AC_SUBST(SO_LDFLAGS) #[build shared library flags]

AC_SUBST(WARN_CC)  #[C warning flags]
AC_SUBST(WARN_CXX) #[C++ warning flags]
AC_SUBST(WARN_FC)  #[F warning flags]
AC_SUBST(STD_CC)   #[C standard flags]
AC_SUBST(STD_CXX)  #[C++ standard flags]
AC_SUBST(STD_FC)   #[F standard flags]

AC_ARG_VAR(LIBS)   #[libraries]

case "$CC_VENDOR" in
unknown)
    ;;
gcc|clang|pathscale)
    CFLAGS="-m64 $CFLAGS"
    STD_CC="-std=gnu99"
    WARN_CC=${WARN_CC-"-Wall -Wextra -pedantic -Wno-long-long -Wno-unused-parameter -Wmissing-prototypes -Wshadow -Wconversion"}
    SO_CCFLAGS="-fPIC"
    SO_LDFLAGS="-shared"
    ;;
intel)
    CFLAGS="-m64 $CFLAGS"
    STD_CC="-std=gnu99"
    WARN_CC=${WARN_CC-"-Wall -w5 -Wremarks -Wcheck -Wmissing-prototypes -Wshadow"}
    SO_CCFLAGS="-fPIC"
    SO_LDFLAGS="-shared"
    ;;
pgi)
    CFLAGS="-m64 $CFLAGS"
    STD_CC="-c9x"
    SO_CCFLAGS="-fPIC"
    SO_LDFLAGS="-shared"
    ;;
solarisstudio)
    CFLAGS="-m64 -mt $CFLAGS"
    STD_CC="-xc99"
    SO_CCFLAGS="-KPIC"
    SO_LDFLAGS="-shared"
    ;;
fujitsu)
    CFLAGS="-Kfast,preex -Xg -std=gnu99 $CFLAGS"
    LIBS="-L/opt/FJSVpxtof/sparc64fx/lib64 -ltofutop -ltofucom $LIBS"
    SO_CCFLAGS="-KPIC"
    SO_LDFLAGS="-shared"
    ;;
ibmxlc)
    CFLAGS="-q64 $CFLAGS"
    STD_CC="-qlanglvl=stdc99"
    WARN_CC=${WARN_CC-"-qinfo=gen"}
    #???
    SO_CCFLAGS="-fPIC"
    SO_LDFLAGS="-shared"
    ;;
necsx)
    CFLAGS="-Cvopt $CFLAGS"
    STD_CC="-Kc99"
    WARN_CC=${WARN_CC-"-w all -pvctl nomsg"}
    #???
    SO_CCFLAGS="-fPIC"
    SO_LDFLAGS="-shared"
    ;;
digital|*)
    #???
    SO_CCFLAGS="-fPIC"
    SO_LDFLAGS="-shared"
    ;;
esac

case "$CXX_VENDOR" in
unknown)
    ;;
gcc|clang|pathscale)
    CXXFLAGS="-m64 $CXXFLAGS"
    STD_CXX="-std=c++98"
    WARN_CXX=${WARN_CXX-"-Wall -Wextra -pedantic -Wno-long-long -Wno-unused-parameter -Wshadow -Wconversion"}
    ;;
intel)
    CXXFLAGS="-m64 $CXXFLAGS"
    STD_CXX="-std=c++98"
    WARN_CXX=${WARN_CXX-"-Wall -w5 -Wremarks -Wcheck -Wmissing-prototypes -Wshadow"}
    ;;
pgi)
    CXXFLAGS="-m64 $CXXFLAGS"
    ;;
solarisstudio)
    CXXFLAGS="-m64 -mt $CXXFLAGS"
    ;;
fujitsu)
    CXXFLAGS="-Kfast,preex -Xg $CXXFLAGS"
    ;;
ibmxlc)
    CXXFLAGS="-q64 $CXXFLAGS"
    WARN_CXX=${WARN_CXX-"-qinfo=gen"}
    ;;
necsx)
    CXXFLAGS="-Cvopt $CXXFLAGS"
    WARN_CXX=${WARN_CXX-"-w all -pvctl nomsg"}
    ;;
digital|*)
    ;;
esac

case "$FC_VENDOR" in
unknown)
    enable_fortran=no
    ;;
gcc|clang|pathscale)
    FCFLAGS="-m64 $FCFLAGS"
    WARN_FC=${WARN_FC-"-Wall -Wextra -pedantic -Wno-unused-parameter -Wno-unused-dummy-argument -Wshadow"}
    ;;
intel)
    FCFLAGS="-m64 $FCFLAGS"
    WARN_FC=${WARN_FC-"-warn all"}
    ;;
pgi)
    FCFLAGS="-m64 $FCFLAGS"
    ;;
solarisstudio)
    FCFLAGS="-m64 -mt $FCFLAGS"
    ;;
fujitsu)
    FCFLAGS="-Kfast,preex $FCFLAGS"
    ;;
ibmxlc)
    FCFLAGS="-q64 $FCFLAGS"
    WARN_FC=${WARN_FC-"-qinfo=gen"}
    ;;
necsx)
    FCFLAGS="-Cvopt $FCFLAGS"
    WARN_FC=${WARN_FC-"-w all -pvctl nomsg"}
    ;;
digital|*)
    ;;
esac

rm -f conftest.out

# Add rt library for clock_gettime() in Linux and Intel-CC.
#
#case ${target_os} in
#linux*)
#    LIBS="${LIBS} -lrt"
#    ;;
#esac

# Mark if assertion is enabled.

if test "$enable_assert" != "yes"; then
  CFLAGS="-DNDEBUG $CFLAGS"
  CXXFLAGS="-DNDEBUG $CXXFLAGS"
fi

# Check if Fortran support is needed.

# KMRTARGET is a target of makefile.
AC_SUBST(KMRTARGET)
if test "$enable_fortran" = "yes"; then
  KMRTARGET=kmr_f
else
  KMRTARGET=kmr_c
fi

# Check if TOFU support files are available.

if test "$enable_tofu" = "yes" -a "1" -eq "$mp10fx10" -a -f "src/tofu.h"; then
  CFLAGS="-DKMRTOFU $CFLAGS"
fi

# Check if LD is GNU (for program embedding).
#
#AC_ARG_VAR(LD, [Linker command (gnu-ld)])
#AC_MSG_CHECKING([whether ld is gnu])
#if test "$LD" = ""; then LD=ld; fi
#case `"$LD" -v 2>&1 </dev/null` in
#*GNU* | *'with BFD'*)
#  gnu_ld=yes
#  ;;
#*)
#  gnu_ld=no
#  ;;
#esac
#AC_MSG_RESULT([$gnu_ld])

# KMRWATCH is a target of makefile.
AC_SUBST(KMRWATCH)
if test "$enable_embed" = "yes"; then
  CFLAGS="-DKMRBINEMBED $CFLAGS"
  KMRWATCH=kmrwatch0.bin.o
else
  KMRWATCH=""
fi

# TARGET OS
AC_SUBST(KMRSOLIB)
case ${target_os} in
darwin*)
    KMRSOLIB="kmr_c.dylib"
    ;;
*)
    KMRSOLIB="kmr_c.so"
    ;;
esac


#AC_ARG_VAR(LDEMULATION, [BDF binary target (used as ld -m option)])
#AC_SUBST(KMRLDBDF)
#if test "$LDEMULATION" != ""; then
#  KMRLDBDF="-m $LDEMULATION"
#else
#  KMRLDBDF=""
#fi

# Check NETCDF/HDF5 support.

if test "$with_netcdf" != ""; then
  CFLAGS="-DKMRNETCDF $CFLAGS"
  LIBS="$LIBS -L$with_netcdf -lnetcdf -lhdf5_hl -lhdf5 -lsz -lz -lm"
fi

if test "$with_hdf5" != ""; then
  CFLAGS="-DKMRHDF5 $CFLAGS"
  LIBS="$LIBS -L$with_hdf5 -lhdf5_hl -lhdf5 -lsz -lz -lm"
fi

# Let KMRLIBDIR show in config.h.

test "x$prefix" = xNONE && prefix=$ac_default_prefix
AC_DEFINE_UNQUOTED(KMRLIBDIR, ["$prefix"])

AC_SUBST(KMRVER, $version)
AC_SUBST(KMRVERDATE, $release_date)

AC_OUTPUT
