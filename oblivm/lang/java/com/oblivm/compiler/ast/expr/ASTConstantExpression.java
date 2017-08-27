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
 * - public int value <br>
 * - public int bitSize
 */
public class ASTConstantExpression extends ASTExpression {
	public long value;
	public ASTExpression bitSize;
	
	public ASTConstantExpression(long n) {
		this(n, null);
	}
	
	public ASTConstantExpression(long n, ASTExpression bitsize) {
		this.value = n;
		this.bitSize = bitsize;
	}

	public ASTConstantExpression(int v, int constant) {
		this(v, new ASTConstantExpression(constant, new ASTConstantExpression(32)));
	}
	
	public String toString() {
		return Long.toString(value);
	}
	
	public int level() {
		return 100;
	}
	
	public ASTExpression getBits() {
		return bitSize;
	}
	
	public boolean equals(Object obj) {
		if(!(obj instanceof ASTConstantExpression))
			return false;
		return value == ((ASTConstantExpression)obj).value; 
	}

	public ASTConstantExpression cloneInternal() {
		return new ASTConstantExpression(value, bitSize);
	}
}
