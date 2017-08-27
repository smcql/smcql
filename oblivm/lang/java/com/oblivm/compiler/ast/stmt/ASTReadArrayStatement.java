package com.oblivm.compiler.ast.stmt;

import com.oblivm.compiler.ast.expr.ASTExpression;
import com.oblivm.compiler.ast.expr.ASTStringConstantExpression;

public class ASTReadArrayStatement extends ASTStatement {
	public ASTExpression array;
	public ASTStringConstantExpression srcFile;
	
	public ASTReadArrayStatement(ASTExpression var, ASTStringConstantExpression dst) {
		this.array = var;
		this.srcFile = dst;
	}
	
	@Override
	public String toString(int indent) {
		return indent(indent) + toString();
	}
	
	public String toString() {
		return "SecureArray.deserialize(" + array.toString() + ", " +srcFile + ");";
	}
	
}
