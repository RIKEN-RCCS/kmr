#!/bin/sh

# Make file contents data in C. (it increases the size by one byte).
# The use of gnu-ld (ld -b binary) is abandoned, because guessing the
# target is sometimes hard in (unusual) cross compiling environment.
# (It assumes the size of ELF binary is multiple of four).

if [ $# != 1 ]; then echo "USAGE: $0 file"; exit 1; fi

f=$1
n=`echo "$f" | sed 's/\./_/g'`
od -v -t x1 < $f \
  | awk 'BEGIN {print "unsigned char kmr_binary_NAME_start[] = {"} \
	 {if (NF == 1)  {} \
	  if (NF == 5)  {printf("0x%s, 0x%s, 0x%s, 0x%s,\n", \
				$2, $3, $4, $5)} \
	  if (NF == 9)  {printf("0x%s, 0x%s, 0x%s, 0x%s,", \
			        $2, $3, $4, $5); \
			 printf(" 0x%s, 0x%s, 0x%s, 0x%s,\n", \
				$6, $7, $8, $9)} \
	  if (NF == 13) {printf("0x%s, 0x%s, 0x%s, 0x%s,", \
			        $2, $3, $4, $5); \
			 printf(" 0x%s, 0x%s, 0x%s, 0x%s,\n", \
				$6, $7, $8, $9); \
			 printf("0x%s, 0x%s, 0x%s, 0x%s,\n", \
			       	$10, $11, $12, $13)} \
	  if (NF == 17) {printf("0x%s, 0x%s, 0x%s, 0x%s,", \
				$2, $3, $4, $5); \
			 printf(" 0x%s, 0x%s, 0x%s, 0x%s,\n", \
				$6, $7, $8, $9); \
			 printf("0x%s, 0x%s, 0x%s, 0x%s,", \
			       	$10, $11, $12, $13); \
			 printf(" 0x%s, 0x%s, 0x%s, 0x%s,\n", \
				$14, $15, $16, $17)}} \
	 END   {print "0x0};"; \
	        print "unsigned char *kmr_binary_NAME_end = &kmr_binary_NAME_start[sizeof(kmr_binary_NAME_start) - 1];"; \
		print "unsigned long kmr_binary_NAME_size = (sizeof(kmr_binary_NAME_start) - 1);"}' \
  | sed "s/_NAME_/_${n}_/g" > $f.bin.c
