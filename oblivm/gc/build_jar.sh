#!/bin/bash -x

./compile.sh
cd bin;
jar cvfm ../lib/oblivm-flexsc-0.2.jar ../META-INF/MANIFEST.MF  com
cd ..
cp lib/oblivm-flexsc-0.2.jar ../lang/lib/
cp lib/oblivm-flexsc-0.2.jar ../../smcql/lib/
