package org.smcql.codegen.smc.operator.support;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.smcql.codegen.CodeGenerator;
import org.smcql.codegen.smc.DynamicCompiler;
import org.smcql.executor.config.RunConfig.ExecutionMode;
import org.smcql.executor.step.ExecutionStep;
import org.smcql.executor.step.PlaintextStep;
import org.smcql.plan.operator.Operator;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelRecordType;
import org.smcql.util.CodeGenUtils;
import org.smcql.util.Utilities;

// takes in the sorted input of Alice and Bob 
// and creates a single joint set of tuples for query execution
public class MergeMethod implements CodeGenerator, Serializable {

	// if smc leaf is splittable, operate on secure leaf
	// else operates on the *child* of the SMC op
	Operator src;
	String packageName;
	String generatedCode = null;
	private String srcSQL;
	private SecureRelRecordType schema;
	private List<SecureRelDataTypeField> orderKey;
	ExecutionStep childStep;
	
	public MergeMethod(Operator op, ExecutionStep child, List<SecureRelDataTypeField> orderBy) throws Exception{
		src = op;
		orderKey = orderBy;
		childStep = child;
		
		srcSQL = (child instanceof PlaintextStep) ? child.generate() : null;

		packageName = child.getPackageName() + ".merge";

		if (srcSQL != null) {
			schema = Utilities.getOutSchemaFromString(srcSQL);
		} else {
			schema = child.getSourceOperator().getSchema(true);
		}
	}
	// for use in testing
	public MergeMethod(String sql, String pack) throws Exception {
		packageName = pack;
		srcSQL = sql;
		orderKey = new ArrayList<SecureRelDataTypeField>();
		schema = Utilities.getOutSchemaFromString(sql);
		packageName = pack + ".merge";
	}
	
	public void addOrderByAttribute(String attrName) {
		SecureRelDataTypeField attr  = schema.getAttribute(attrName);
		orderKey.add(attr);
			
	}
	
	@Override
	public String generate() throws Exception {
		
		int size = schema.size();

		Map<String, String> variables = new HashMap<String, String>();
		variables.put("lessThan", generateLessThan());
		variables.put("size", Integer.toString(size));
		variables.put("packageName", packageName);
		
		generatedCode = CodeGenUtils.generateFromTemplate("util/merge_inputs.txt", variables);
	
		return generatedCode;
		
	}
	
	// if l.a1 < l.a2 then r = 1
	// else if(a1 ==, l.a2 < r.a2
	// else if ...
	// do less than on order by attributes - the column order of the output may not be the same as the order by cols
	private String generateLessThan() throws Exception {
		
		assert(orderKey != null);

		int size = schema.size();
		
		
		String ret = "secure int1 lessThan(int" + size + "  lhs, int" + size + " rhs) {\n";
		ret += "    int1 res = 0;\n";
		
		if(orderKey.isEmpty()) {
			ret += "    if(lhs < rhs) {\n";
			ret += "           res = 1;\n";
			ret += "     }\n\n";
		}
		else {
			
			String bitmask = CodeGenUtils.getBitmask(schema.getAttributes(), orderKey.get(0));
			ret += "    if(lhs" + bitmask + " < rhs" + bitmask + ") {\n";
			ret += "        res = 1;\n";
			ret += "    }\n";

			for(int i = 1; i < orderKey.size(); ++i) {
				String predecessors = equalities(orderKey, i);
				bitmask = CodeGenUtils.getBitmask(schema.getAttributes(), orderKey.get(i));
				ret += "    else if(" + predecessors + " && lhs" + bitmask + " < rhs" + bitmask + ") { \n";
				ret += "        res = 1;\n";
				ret += "    }\n";
	
			}
		}
		ret += "    return res;\n";
		ret += "}\n\n";
		

		return ret;
		
	}
	
	private String equalities(List<SecureRelDataTypeField> orderKey, int idxFor) throws Exception {
		int i = 0;
		List<String> eqs = new ArrayList<String>();
		
		while(i < idxFor) {
			String bitmask = schema.getBitmask(orderKey.get(i));
		   eqs.add("lhs" + bitmask + " == rhs" + bitmask);
		   ++i;
		}
		
		return StringUtils.join(eqs.toArray(), " && ");
	}

	@Override
	public String getPackageName() {
		return packageName;
	}

	@Override
	public SecureRelRecordType getInSchema() {
		return schema;
	}

	@Override
	public SecureRelRecordType getSchema() {
		return schema;
	}

	@Override
	public SecureRelRecordType getSchema(boolean generateForSMC) {
		return schema;
	}
	
	public String getSourceSQL() {
		return srcSQL;
	}

	public void setSourceSQL(String srcSQL) {
		this.srcSQL = srcSQL;
	}
	@Override
	public String destFilename(ExecutionMode e) {
		String base = childStep.getCodeGenerator().destFilename(e);
		base = base.substring(0, base.length() - ".lcc".length());
		return base + "_merge.lcc";
	}
	@Override
	public void compileIt() throws Exception {
		String code = generate();
		DynamicCompiler.compileOblivLang(code, packageName);
	}
	@Override
	public String generate(boolean asSecureLeaf) throws Exception {
		return null;
	}
	





	

}
