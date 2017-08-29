/***
 * Copyright (C) 2015 by Chang Liu <liuchang@cs.umd.edu>
 */
package com.oblivm.compiler.ir;

import com.oblivm.compiler.type.manage.Label;
import com.oblivm.compiler.type.manage.VariableConstant;

public class ConstExp extends Expression {
	public long n;
	public String s;
	public VariableConstant bits;
	public boolean isInt;
	public boolean isString;
	
	public ConstExp(long value, VariableConstant bits) {
		this.n = value;
		this.bits = bits;
		this.isInt = true;
		this.isString = false;

	}

	public ConstExp(String value, VariableConstant bits) {
		this.s = value;
		this.bits = bits;
		this.isInt = false;
		this.isString = true;
	}

	
	@Override
	public String toString() {
		if(isString) {
			return s;
		}
		
		return Long.toString(n);
	}

	@Override
	public Label getLabels() {
		return Label.Pub;
	}

}
