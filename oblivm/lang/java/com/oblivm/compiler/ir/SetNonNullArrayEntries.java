package com.oblivm.compiler.ir;

// non-null entries in array
public class SetNonNullArrayEntries extends IRCode {
	public IRCode code;
	public Variable name;
	public Variable value;
	
	public SetNonNullArrayEntries(IRCode code, Variable arr, Variable value) {
		this.name = arr;
		this.value = value;
		this.code = code;
	}

	@Override
	public String toString(int indent) {
		StringBuffer sb = new StringBuffer(this.indent(indent));
		if(code != null) {
			sb.append(code.toString());
		}
		sb.append(name.name + ".setNonNullEntries(" + value.name + ");\n");
		return sb.toString();
	}

	@Override
	public IRCode clone(boolean withTypeDef) {
		IRCode ret = new SetNonNullArrayEntries( code, name, value);
		ret.withTypeDef = withTypeDef;
		return ret;
	}

}
