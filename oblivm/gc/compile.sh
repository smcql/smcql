mkdir -p bin
find . -name "*.java" > source.txt;
javac -cp bin:lib/* -d bin @source.txt;
