/***
 * Copyright (C) 2015 by Chang Liu <liuchang@cs.umd.edu>
 */
package com.oblivm.compiler.ast.expr;

/**
 * Extends ASTExpression. It defines a constant expression of
 * the form "n = a;" where "a" is a natural number and "n" is a 
 * variable.
 * <p>
 * <b> Member variables </b>: <p>
 * - public String value <br>
 * - public int bitSize
 */
public class ASTStringConstantExpression extends ASTExpression {
	public String value;
	public ASTExpression bitSize;

	public ASTStringConstantExpression cloneInternal() {
		return new ASTStringConstantExpression(value);
	}
	
	public ASTStringConstantExpression(String v) {
		value = new String(v);
		bitSize = null; // 8 * value.length()
	}
	
	
	public ASTStringConstantExpression(String v, ASTExpression bitsize) {
		this.value = v;
		this.bitSize = bitsize;
	}
	
	
	public String toString() {
		return value;
	}
	
	
	public boolean equals(Object obj) {
		if(!(obj instanceof ASTStringConstantExpression))
			return false;
		String oVal = ((ASTStringConstantExpression)obj).value;
		return value.equals(oVal);
	}

	@Override
	public int level() {
		
		return 100;
	}
	
	
}
