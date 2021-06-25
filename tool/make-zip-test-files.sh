#!/bin/ksh

echo "IT WILL TAKE A FEW MINUTES, ON A USUAL HARD-DISK DRIVE."
echo "It takes about 1 minute for making 2GB random numbers,"
echo "and about 10 seconds for copying 2GB file each."
echo "In total, it takes roughly about 5 minutes."

## Make one 2GB file, and make many alias links for it.

dd if=/dev/urandom of=gomi iflag=fullblock bs=1G count=2

for i in `seq -f "%02g" 0 19`; do
    ln gomi gomi$i
done

## Make four zip-files, each contains one 2GB file.

for j in `seq -f "%g" 0 4`; do
    zip -0 a0$j.zip gomi0$j
done

## Make four zip-files, each contains five 2GB files (offsets > 4GB).

zip -0 a10.zip gomi0[0-4]
zip -0 a11.zip gomi0[5-9]
zip -0 a12.zip gomi1[0-4]
zip -0 a13.zip gomi1[5-9]
