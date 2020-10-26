#/bin/ksh
# ck.semicolons.sh (2014-02-04)

# It check if OMP directives are followed illegally by a semicolon,
# and returns 0 (OK/true) when followed.

grep KMR_OMP_ *.{c,h,cpp} | grep -v 'KMR_OMP_SET_LOCK' \
	| grep -v 'KMR_OMP_UNSET_LOCK' | grep ';'
