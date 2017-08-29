package com.oblivm.compiler.ast.stmt;

import com.oblivm.compiler.ast.expr.ASTExpression;

public class ASTGetNonNullArrayEntriesStatement extends ASTStatement {
	public ASTExpression var;
	public ASTExpression expr;
	
	
	public ASTGetNonNullArrayEntriesStatement(ASTExpression var, ASTExpression exp) {
		this.var = var;
		this.expr = exp;
	}
	
	@Override
	public String toString(int indent) {
		return indent(indent) + toString();
	}
	
	public String toString() {
		return expr.toString(0) + " = " + var+".getEntryCount();";
	}

}
