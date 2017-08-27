package org.smcql.executor.step;

import java.util.List;

import org.smcql.codegen.CodeGenerator;
import org.smcql.type.SecureRelRecordType;
import org.smcql.executor.config.RunConfig;
import org.smcql.executor.smc.OperatorExecution;
import org.smcql.plan.operator.Operator;

public interface ExecutionStep {
	 
	
	// generates a main method for smc
	public String generate() throws Exception;
		
	public String getPackageName();
	
	public ExecutionStep getParent();

	public ExecutionStep getChild(int idx);
	
	public SecureRelRecordType getInSchema();
	
	public SecureRelRecordType getSchema();

	public SecureRelRecordType getSchema(boolean forSecureLeaf);

	
	public RunConfig getRunConfig();
	
	public void setParent(ExecutionStep e);
	
	public Operator getSourceOperator();
	
	public CodeGenerator getCodeGenerator();
	
	public List<ExecutionStep> getChildren();
	
	public boolean visited(); // check if visited
	
	public void visit(); // mark step as executed
	
	public void setHostname(String host); // where it will be run, Alice/generator if SecureStep

	public String printTree();
	
	public OperatorExecution getExec();
}
