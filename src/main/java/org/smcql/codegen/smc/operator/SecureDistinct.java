package org.smcql.codegen.smc.operator;

import java.util.Map;

import org.smcql.config.SystemConfiguration;
import org.smcql.executor.config.RunConfig.ExecutionMode;
import org.smcql.plan.operator.Operator;
import org.smcql.util.CodeGenUtils;


public class SecureDistinct extends SecureOperator{

	public SecureDistinct(Operator o) throws Exception {
		super(o);
		
	}
	
	@Override
	public String generate() throws Exception  {
		Map<String, String> variables = baseVariables();
		
		String generatedCode = null;

		if(planNode.getExecutionMode() == ExecutionMode.Slice && SystemConfiguration.getInstance().getProperty("sliced-execution").equals("true")) {
			generatedCode = CodeGenUtils.generateFromTemplate("distinct/sliced.txt", variables);
		}
		else {
			generatedCode = CodeGenUtils.generateFromTemplate("distinct/simple.txt", variables);
		}

		return generatedCode;
	}

}
