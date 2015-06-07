/* testmisc.c (2014-02-04) */

/* Check miscellaneous (sequential ones), kmr_isort, etc. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <limits.h>
#include <ctype.h>
#include <sys/time.h>
#include <assert.h>
#ifdef _OPENMP
#include <omp.h>
#endif

extern void kmr_isort(void *a, size_t n, size_t es, int depth);
extern int kmr_scan_argv_strings(void *mr, char *s, size_t len, int arglim,
				 int *argc, char **argv,
				 _Bool wssep, char *msg);

static double
wtime()
{
    static struct timeval tv0 = {.tv_sec = 0};
    struct timeval tv;
    int cc;
    cc = gettimeofday(&tv, 0);
    assert(cc == 0);
    if (tv0.tv_sec == 0) {
	tv0 = tv;
	assert(tv0.tv_sec != 0);
    }
    double dt = ((double)(tv.tv_sec - tv0.tv_sec)
		 + ((double)(tv.tv_usec - tv0.tv_usec) * 1e-6));
    return dt;
}

static char *
printablestring(char *s, int len)
{
    /* LEN includes terminating zero. */
    static char b[1024];
    assert(len < (int)sizeof(b));
    for (int i = 0; i < len; i++) {
	if (s[i] == 0) {
	    b[i] = '$';
	} else if (isblank(s[i])) {
	    b[i] = '_';
	} else if (isprint(s[i])) {
	    b[i] = s[i];
	} else {
	    b[i] = '?';
	}
    }
    b[len - 1] = 0;
    return b;
}

static int
check_isort(int argc, char *argv[])
{
#if 0
    {
	/* Check with length 20. */

	/*printf("sizeof(long)=%ld\n", sizeof(long));*/
	long a0[20] = {19, 18, 17, 16, 15, 14, 13, 12, 11, 10,
		       9, 8, 7, 6, 5, 4, 3, 2, 1, 0};
	size_t n0 = (sizeof(a0) / sizeof(long));
	for (int i = 0; i < (int)n0; i++) {
	    a0[i] += 1000000000000;
	}

	kmr_isort(a0, n0, sizeof(long), 5);

	for (int i = 0; i < (int)n0; i++) {
	    printf("%ld\n", a0[i]);
	}
	printf("\n");

	for (int i = 0; i < (int)n0; i++) {
	    assert(a0[i] == (i + 1000000000000));
	}

	for (int i = 0; i < (int)n0; i++) {
	    printf("%ld ", a0[i]);
	}
	printf("\n");
    }
#endif

#if 0
    {
	long n1 = 100000000; /*100M*/
	long *a1 = malloc(sizeof(long) * n1);
	assert(a1 != 0);

	printf("Generating random numbers...\n");
	for (long i = 0; i < n1; i++) {
	    a1[i] = ((((long)rand()) << 31) ^ ((long)rand()));
	}

	if (0) {
	    printf("Problem...\n");
	    for (long i = 0; i < 10; i++) {
		printf("%ld\n", a1[i]);
	    }
	    printf("\n");
	}

	printf("Sorting...\n");

	kmr_isort(a1, n1, sizeof(long), 5);

	if (0) {
	    printf("Result...\n");
	    for (long i = 0; i < 10; i++) {
		printf("%ld\n", a1[i]);
	    }
	    printf("\n");
	}

	printf("Checking...\n");
	long lb = LONG_MIN;
	for (long i = 0; i < n1; i++) {
	    assert(a1[i] >= lb);
	    if (a1[i] > lb) {
		lb = a1[i];
	    }
	}
	printf("OK\n");
    }
#endif

    {
#ifdef _OPENMP
	double t0, t1;

	long n5 = 100000000; /*100M*/
	long *a5 = malloc(sizeof(long) * (size_t)n5);
	assert(a5 != 0);

	printf("CHECK kmr_isort...\n");

	for (int loop = 0; loop <= 40; loop++) {
	    /*for *(int threads = 1; threads <= 8; threads *= 2) {*/
	    /*omp_set_num_threads(threads);*/
	    int threads;

#pragma omp parallel
	    {
		threads = omp_get_num_threads();
	    }

	    srand(20140204);
	    printf("Generating random numbers...\n");
	    for (long i = 0; i < n5; i++) {
		a5[i] = ((((long)rand()) << 31) ^ ((long)rand()));
	    }

	    printf("Sorting (threads=%d)...\n", threads);
	    t0 = wtime();
	    kmr_isort(a5, (size_t)n5, sizeof(long), 5);
	    t1 = wtime();
	    printf("dt=%f\n", (t1 - t0));

	    printf("Checking...\n");
	    long lb5 = LONG_MIN;
	    for (long i = 0; i < n5; i++) {
		assert(a5[i] >= lb5);
		if (a5[i] > lb5) {
		    lb5 = a5[i];
		}
	    }
	    printf("OK\n");
	}
#else
	printf("NOT OMP\n");
#endif
    }

    return 0;
}

static int
check_string_scan(int argc, char *argv[])
{
    {
	int argsc;
	char *argsv[128];

	printf("CHECK kmr_scan_argv_strings...\n");

	/* (space-separator) */

	char s0[] = "";
	printf("s=(%s)\n", s0);
	kmr_scan_argv_strings(0, s0, sizeof(s0), 128, &argsc, argsv, 1, "s0");
	assert(argsc == 0);

	char s1[] = "abc bad cab";
	printf("s=(%s)\n", s1);
	kmr_scan_argv_strings(0, s1, sizeof(s1), 128, &argsc, argsv, 1, "s1");
	assert(argsc == 3);
	assert(strcmp(argsv[0], "abc") == 0
	       && strcmp(argsv[1], "bad") == 0
	       && strcmp(argsv[2], "cab") == 0);

	char s2[] = "  abc   bad   cab  ";
	printf("s=(%s)\n", s2);
	kmr_scan_argv_strings(0, s2, sizeof(s2), 128, &argsc, argsv, 1, "s2");
	assert(argsc == 3);
	assert(strcmp(argsv[0], "abc") == 0
	       && strcmp(argsv[1], "bad") == 0
	       && strcmp(argsv[2], "cab") == 0);

	char s3[] = "abc";
	printf("s=(%s)\n", s3);
	kmr_scan_argv_strings(0, s3, sizeof(s3), 128, &argsc, argsv, 1, "s3");
	assert(argsc == 1);
	assert(strcmp(argsv[0], "abc") == 0);

	char s4[] = "   abc   ";
	printf("s=(%s)\n", s4);
	kmr_scan_argv_strings(0, s4, sizeof(s4), 128, &argsc, argsv, 1, "s4");
	assert(argsc == 1);
	assert(strcmp(argsv[0], "abc") == 0);

	/* (null-separator) */

	char s5[] = "";
	printf("s=(%s)\n", s5);
	kmr_scan_argv_strings(0, s5, sizeof(s5), 128, &argsc, argsv, 0, "s5");
	assert(argsc == 0);

	char s6[] = "abc\0bad\0cab";
	printf("s=(%s)\n", printablestring(s6, sizeof(s6)));
	kmr_scan_argv_strings(0, s6, sizeof(s6), 128, &argsc, argsv, 0, "s6");
	assert(argsc == 3);
	assert(strcmp(argsv[0], "abc") == 0
	       && strcmp(argsv[1], "bad") == 0
	       && strcmp(argsv[2], "cab") == 0);

	char s7[] = "  abc  bad  \0\0  cab  ";
	printf("s=(%s)\n", printablestring(s7, sizeof(s7)));
	kmr_scan_argv_strings(0, s7, sizeof(s7), 128, &argsc, argsv, 0, "s7");
	assert(argsc == 3);
	assert(strcmp(argsv[0], "  abc  bad  ") == 0
	       && strcmp(argsv[1], "") == 0
	       && strcmp(argsv[2], "  cab  ") == 0);

	char s8[] = "abc";
	printf("s=(%s)\n", s8);
	kmr_scan_argv_strings(0, s8, sizeof(s8), 128, &argsc, argsv, 0, "s8");
	assert(argsc == 1);
	assert(strcmp(argsv[0], "abc") == 0);

	char s9[] = "  abc  ";
	printf("s=(%s)\n", s9);
	kmr_scan_argv_strings(0, s9, sizeof(s9), 128, &argsc, argsv, 0, "s9");
	assert(argsc == 1);
	assert(strcmp(argsv[0], "  abc  ") == 0);
    }

    return 0;
}

/* ================================================================ */

int
main(int argc, char **argv)
{
    check_isort(argc, argv);
    check_string_scan(argc, argv);
}
