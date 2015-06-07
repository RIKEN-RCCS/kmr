/** \file wc.mapper.c
    \brief Example for KMR shell command pipeline.  It is a mapper
    for word count. */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>

/** Maximum line length. */
#define LINELEN	32767

/** \brief Ignore characters.
    These characters are replaced to a space character. */

//static char ignoreChar[] = { ',', '.', '\t', '?', '"', '!', '$', '/', '<', '>', ':', ';', '[', ']', '(', ')', '-' };
static char ignoreChar[] = { '\t' };

/** \breif Replace Character.
    Replace ignore characters to a space character.  */

static inline void ReplaceChar(char *wp)
{
    while (*wp != 0) {
	/* uncomment if ignore case. */
	// if (isalpha(*wp) && isupper(*wp))
	// *wp = tolower(*wp);
	for (int i = 0; i < (int)sizeof(ignoreChar); i++) {
	    if (*wp == ignoreChar[i]) {
		*wp = ' ';
		break;
	    }
	}
	wp++;
    }
}

/** \brief Main function.
    Read line from stdin, replace character, separate word by space,
    and output "WORD 1" to stdout.  */

int
main(int argc, char *argv[])
{
    char line[LINELEN];

    while (fgets(line, sizeof(line), stdin) != NULL) {
	char *wtop = NULL, *cp = line;
	int len = (int)strlen(line);
	ReplaceChar(line);
	while ((cp - line) < len) {
	    while (*cp == ' ') {
		cp++;			// skip space
	    }
	    if (*cp == '\n' || *cp == '\0') {
		break;			// end of line or buffer end.
	    }
	    wtop = cp;
	    while (*cp != ' ' && *cp != '\n' && *cp != '\0') {
		cp++;
	    }
	    *cp = '\0';
	    printf("%s 1\n", wtop);
	    cp++;
	}
    }
    return 0;
}
