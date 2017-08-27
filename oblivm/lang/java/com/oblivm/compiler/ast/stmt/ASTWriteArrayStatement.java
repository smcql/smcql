package com.oblivm.compiler.ast.stmt;

import com.oblivm.compiler.ast.expr.ASTExpression;
import com.oblivm.compiler.ast.expr.ASTStringConstantExpression;

public class ASTWriteArrayStatement extends ASTStatement {
	public ASTExpression array;
	public ASTStringConstantExpression dstFile;
	
	public ASTWriteArrayStatement(ASTExpression var, ASTStringConstantExpression dst) {
		this.array = var;
		this.dstFile = dst;
	}
	
	@Override
	public String toString(int indent) {
		return indent(indent) + toString();
	}
	
	public String toString() {
		return "SecureArray.serialize(" + array.toString() + ", " + dstFile + ");";
	}
	
}
