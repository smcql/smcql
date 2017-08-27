package com.oblivm.compiler.ir;


public class WriteArray extends IRCode {
	public Variable array;
	public ConstExp filename;
	
	public WriteArray(Variable v, ConstExp str) {
		array = v;
		filename = str;
	}
	
	@Override
	public String toString(int indent) {
		return indent(indent) + "SecureArray.serialize(" + array + ", " + filename + ")" ;
	}
	@Override
	public IRCode clone(boolean withTypeDef) {
		IRCode ret = new WriteArray(array, filename);
		ret.withTypeDef = withTypeDef;
		return ret;
	}
}
