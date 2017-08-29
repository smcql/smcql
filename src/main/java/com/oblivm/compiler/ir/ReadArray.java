package com.oblivm.compiler.ir;


public class ReadArray extends IRCode {
	public Variable array;
	public ConstExp filename;
	
	public ReadArray(Variable v, ConstExp str) {
		array = v;
		filename = str;
	}
	
	@Override
	public String toString(int indent) {
		return indent(indent) + "SecureArray.deserialize(" + array + ", " + filename + ")" ;
	}
	@Override
	public IRCode clone(boolean withTypeDef) {
		IRCode ret = new ReadArray(array, filename);
		ret.withTypeDef = withTypeDef;
		return ret;
	}
}
