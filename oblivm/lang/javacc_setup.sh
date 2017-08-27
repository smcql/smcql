echo 'java -cp /path/to/javacc.jar $(basename $0) "$@"' > javacc
chmod 755 javacc
ln -s javacc jjtree
ln -s javacc jjdoc