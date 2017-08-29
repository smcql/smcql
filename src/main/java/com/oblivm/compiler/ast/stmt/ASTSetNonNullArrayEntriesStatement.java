package com.oblivm.compiler.ast.stmt;

import com.oblivm.compiler.ast.expr.ASTExpression;

/* record the # of initialized values in a secure array */

public class ASTSetNonNullArrayEntriesStatement extends ASTStatement {
	public ASTExpression var;
	public ASTExpression expr;
	
	
	public ASTSetNonNullArrayEntriesStatement(ASTExpression var, ASTExpression exp) {
		this.var = var;
		this.expr = exp;
	}
	
	@Override
	public String toString(int indent) {
		return indent(indent) + toString();
	}
	
	public String toString() {
		return var+".setEntryCount("+expr.toString(0)+");";
	}
	
}
