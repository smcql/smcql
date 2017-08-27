package org.smcql.codegen.smc.operator;

import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.logical.LogicalFilter;
import org.smcql.codegen.smc.operator.support.RexNodeUtilities;
import org.smcql.config.SystemConfiguration;
import org.smcql.executor.config.RunConfig.ExecutionMode;
import org.smcql.plan.operator.Filter;
import org.smcql.plan.operator.Operator;
import org.smcql.plan.operator.WindowAggregate;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelRecordType;
import org.smcql.util.CodeGenUtils;

// only supports row number for now
public class SecureWindowAggregate extends SecureOperator  {

	public SecureWindowAggregate(Operator o) throws Exception {
		super(o);

	}

	private SecureRelDataTypeField fieldFromSchema(SecureRelRecordType schema, SecureRelDataTypeField field) {
		for (SecureRelDataTypeField f : schema.getSecureFieldList()) {
			if (field.getName().equals(f.getName()))
				return f;
		}
		return field;
	}
	
	@Override
	public String generate() throws Exception  {
		Map<String, String> variables = baseVariables();		
		
		assert(planNode instanceof WindowAggregate);
		
		WindowAggregate win = (WindowAggregate) planNode;
		SecureRelDataTypeField ref = win.getAggregateAttributes().get(0); // only one for row #
		SecureRelRecordType dstSchema = win.getSchema();
		SecureRelDataTypeField windowAttr = win.getSliceAttributes().get(0);  // partition by
		

		String rowNumMask = CodeGenUtils.getBitmask(dstSchema, ref);
		
		variables.put("rowNum", rowNumMask);
		variables.put("mSize", Integer.toString(windowAttr.size()));
		variables.put("winMask", dstSchema.getBitmask(windowAttr));

		
		// everything else is a 1:1 copy
		if (this.filters.isEmpty()) {
			variables.put("applyFilter", "ret = 1;");
		} else {
			String condition = "";
			String assigns = "";
			int index = 0;
			for (Filter f : this.filters) {
				SecureRelRecordType inSchema = f.getChild(0).getSchema(true);
				
				SecureRelDataTypeField field = f.computesOn(inSchema).get(0);
				field = fieldFromSchema(inSchema, field);
				
				String filterCond = RexNodeUtilities.flattenFilter(f, "tuple", Integer.parseInt(variables.get("sSize")));
				condition += (index == 0) ? filterCond : " && " + filterCond;
				
				index++;
			}
			condition = condition.replace("(", "").replace(")", "");
			String filterStr = assigns + "\n\tif (" + condition + ") {\n\t\tret = 1;\n\t}";
			
			variables.put("applyFilter", filterStr);
		}		
		
		String generatedCode = null;
		if(planNode.getExecutionMode() == ExecutionMode.Slice && SystemConfiguration.getInstance().getProperty("sliced-execution").equals("true")) {
			generatedCode = CodeGenUtils.generateFromTemplate("windowAggregate/sliced/row_num.txt", variables);
		}
		else {
			generatedCode = CodeGenUtils.generateFromTemplate("windowAggregate/singular/row_num.txt", variables);
		}
	
		return generatedCode;
		
	}
}
