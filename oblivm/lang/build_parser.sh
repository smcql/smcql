#!/bin/bash -x
java -classpath lib/javacc.jar javacc  ./java/com/oblivm/compiler/parser/C.jj && mv TokenMgrError.java  ParseException.java Token.java SimpleCharStream.java CParser.java CParserConstants.java CParserTokenManager.java java/com/oblivm/compiler/parser/
