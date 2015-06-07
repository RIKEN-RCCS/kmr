#!/bin/sh
# config.ccvendor.sh - Tells a compiler vendor (2015-05-01)

# THIS CAN TELL: gcc, intel, pgi, clang, appleclang, solarisstudio,
# fujitsu, (AND MAYBE: digital, ibmxlc, pathscale, necsx).

if [ "${CC}" = "" ]; then
    echo 'Set CC for running config.ccvendor.sh';
    exit 1
fi

#AC_MSG_CHECKING([for compiler vendor])

CCVENDOR=unknown

$CC --version >conftestcc.out 2>&1

if grep GCC conftestcc.out >/dev/null 2>&1; then
    CCVENDOR=gcc
fi
if grep 'Free Software Foundation' conftestcc.out >/dev/null 2>&1; then
    CCVENDOR=gcc
fi
if grep 'PGI Compilers and Tools' conftestcc.out >/dev/null 2>&1; then
    CCVENDOR=pgi
fi
if grep 'AIX Compiler' conftestcc.out >/dev/null 2>&1; then
    CCVENDOR=ibmxlc
fi
if grep 'PathScale EKOPath(TM) Compiler Suite' conftestcc.out >/dev/null 2>&1; then
    CCVENDOR=pathscale
fi
if grep 'clang version' conftestcc.out >/dev/null 2>&1; then
    CCVENDOR=clang
fi
if grep 'Apple LLVM version' conftestcc.out >/dev/null 2>&1; then
# Give illegal name because Apple clang does not support OpenMP yet 2015-04
    CCVENDOR=appleclang
fi

if test "${CCVENDOR}" = "unknown"; then

$CC -V >conftestcc.out 2>&1

if grep 'Intel(R) [CF]' conftestcc.out >/dev/null 2>&1; then
    CCVENDOR=intel
fi
if grep 'STMicroelectronics' conftestcc.out >/dev/null 2>&1; then
    # (PGI till 2014)
    CCVENDOR=pgi
fi
if grep 'Sun [CF]' conftestcc.out >/dev/null 2>&1; then
    CCVENDOR=solarisstudio
fi
if grep 'Fujitsu [CF]' conftestcc.out >/dev/null 2>&1; then
    CCVENDOR=fujitsu
fi
if grep 'Compaq [CF]' conftestcc.out >/dev/null 2>&1; then
    CCVENDOR=digital
fi
if grep 'C++/SX' conftestcc.out >/dev/null 2>&1; then
    CCVENDOR=necsx
fi
if grep 'IBM XL [CF]' conftestcc.out >/dev/null 2>&1; then
    CCVENDOR=ibmxlc
fi
fi

rm -f conftestcc.out

echo $CCVENDOR
