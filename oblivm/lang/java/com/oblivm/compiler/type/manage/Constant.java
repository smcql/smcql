/***
 * Copyright (C) 2015 by Chang Liu <liuchang@cs.umd.edu>
 */
package com.oblivm.compiler.type.manage;

public class Constant extends VariableConstant {
	public long value;
	
	public Constant(long value2) {
		this.value = value2;
	}
	
	public String toString() {
		return Long.toString(value);
	}
	
	public boolean equals(VariableConstant obj) {
		if(!(obj instanceof Constant))
			return false;
		return value == ((Constant)obj).value;
	}

	@Override
	public boolean isConstant(int value) {
		return value == this.value;
	}
}
