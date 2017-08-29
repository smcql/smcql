package com.oblivm.compiler.ir;

// non-null entries in array
public class GetNonNullArrayEntries extends IRCode {
	public Variable name;
	public Variable value;
	public IRCode code;
	
	public GetNonNullArrayEntries(IRCode code, Variable arr, Variable value) {
		this.code = code;
		this.name = arr;
		this.value = value;
	}

	@Override
	public String toString(int indent) {
		StringBuffer sb = new StringBuffer(this.indent(indent));
		if(code != null) {
			sb.append(code.toString());
		}
		sb.append(value.name + " = " + name.name + ".getNonNullEntries();\n");
		return sb.toString();
	}

	@Override
	public IRCode clone(boolean withTypeDef) {
		IRCode ret = new GetNonNullArrayEntries(code,  name, value);
		ret.withTypeDef = withTypeDef;
		return ret;
	}

}
