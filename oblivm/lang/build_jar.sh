#!/bin/bash -x

./compile.sh
cd bin;
jar cvfm ../lib/oblivm-lang.jar ../META-INF/MANIFEST.MF  com

cd ..
cp lib/oblivm-lang.jar ../../smcql/lib/