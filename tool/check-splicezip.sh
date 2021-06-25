#!/bin/ksh

## Run splicezip on 2GB files.  A resulting file contains 4 files.

./splicezip a0x.zip a00.zip a01.zip a02.zip a03.zip a04.zip
zipinfo a0x.zip
unzip -t a0x.zip
zip -F a0x.zip --out a0y.zip

## Run splicezip on >10GB files.  A resulting file contains 20 files.

./splicezip a1x.zip a10.zip a11.zip a12.zip a13.zip
zipinfo a1x.zip
unzip -t a1x.zip
zip -F a1x.zip --out a1y.zip
