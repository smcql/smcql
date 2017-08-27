package org.smcql.codegen.smc.operator;

import java.util.List;
import java.util.Map;

import org.smcql.codegen.smc.operator.support.RexNodeUtilities;
import org.smcql.executor.config.RunConfig.ExecutionMode;
import org.smcql.plan.operator.Operator;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelRecordType;
import org.smcql.plan.operator.Aggregate;
import org.smcql.util.CodeGenUtils;

public class SecureAggregate extends SecureOperator {

	private SecureRelRecordType schema;
	public SecureAggregate(Operator o) throws Exception {
		super(o);
		schema = o.getSchema();
	}
	
	@Override
	public String generate() throws Exception  {
		Map<String, String> variables = baseVariables();	
		Aggregate a = (Aggregate) planNode;
		
		String compute = "";
		compute += "secure int$size deref = merged[mIdx];\n";
		compute += "secure int$size toAdd = a[aIdx];\n";
		compute += "deref$cntMask = deref$cntMask + toAdd$cntMask;\n";
		compute += "merged[mIdx] = deref;\n";
		variables.put("compute", compute);
		
		List<SecureRelDataTypeField> groupByAttributes = a.getGroupByAttributes();
		if (groupByAttributes.isEmpty()) {
			return CodeGenUtils.generateFromTemplate("aggregate/singular/full/count.txt", variables);
		} 
		
		String groupByMatch = generateGroupBy(groupByAttributes);
		variables.put("groupByMatch", groupByMatch);
			
		int aggregateIdx = a.getComputeAttributeIndex();
		String cntMask = planNode.getSchema().getBitmask(aggregateIdx);
		
		variables.put("cntMask", cntMask);
	
		generatedCode = CodeGenUtils.generateFromTemplate("aggregate/groupby/partial/count.txt", variables);
		return generatedCode;
	}
	
	
	private String generateGroupBy(List<SecureRelDataTypeField> attrs) throws Exception {
		int i = 1;
		String ret = "    int1 ret = 1;\n\n";
		
		for(SecureRelDataTypeField r : attrs) {
			int size = r.size();
			String bitmask = CodeGenUtils.getBitmask(schema.getAttributes(), r);
			String lVar = "l" + i;
			String rVar = "r" + i;
			
			ret += "    int" + size + " " + lVar + " = lhs" + bitmask + ";\n";
			ret += "    int" + size + " " + rVar + " = rhs" + bitmask + ";\n";
			ret += "    if(" + lVar + " != " + rVar + ")  {\n";
			ret += "            ret = 0;\n";
			ret += "     }\n\n";
			
			
			++i;
		}
		
		ret += "    return ret;\n";
		
		return ret;
		
	}
	

}
