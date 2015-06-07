/** \file wc.reducer.c
    \brief Example for KMR shell command pipeline.  It is a reducer
    for word count. */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/** Maximum line length. */
#define LINELEN	32767

/** Maximum key-value string length. */
#define KEYLEN 32767

/** Reducer main routine.
    Read stdin and reduce. Write result to stdout.  */

int
main(int argc, char *argv[])
{
    char line[LINELEN], prevkey[KEYLEN];
    char *key = NULL;
    long sum = 0;

    memset(prevkey, 0, sizeof(prevkey));
    while (fgets(line, sizeof(line), stdin) != NULL) {
	char *cp = line;
	int len = (int)strlen(line);
	long count;

	// chomp
	if (cp[len-1] == '\n') {
	    cp[len-1] = '\0';
	}
	key = line;
	cp = strchr(line, ' ');
	if (cp == NULL) {
	    // No value field.
	    //printf("skip line\n");
	    continue;
	}
	*cp++ = '\0';

	count = 0;
	while (cp - line < len) {
	    long val;
	    char *endptr = NULL;
	    val = strtol(cp, &endptr, 10);
	    if (val == 0 && cp == endptr) {
		// no value field.
		break;
	    }
	    count += val;
	    cp = endptr;
	}

	if (strlen(prevkey) == 0) {
	    // No saved key.
	    memset(prevkey, sizeof(prevkey), 0);
	    // save key.
	    strncpy(prevkey, key, sizeof(prevkey)-1);
	    sum = count;
	    continue;
	}
	if (strcmp(prevkey, key) != 0) {
	    // key changed.
	    printf("%s %ld\n", prevkey, sum);
	    memset(prevkey, sizeof(prevkey), 0);
	    // save 'current' key.
	    strncpy(prevkey, key, sizeof(prevkey)-1);
	    sum = count;
	} else {
	    // continue with same key. Add count.
	    sum += count;
	}
    }
    if (strlen(prevkey) > 0) {
	printf("%s %ld\n", prevkey, sum);
    }
    fflush(stdout);
    return 0;
}
