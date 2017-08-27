package org.smcql.codegen.smc.operator.support;

import java.util.Iterator;
import java.util.List;

import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.smcql.plan.operator.Filter;
import org.smcql.plan.operator.Project;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelRecordType;

public class RexNodeUtilities {
	
	public static String flattenProjection(Project aProject, String srcVariable, String dstVariable, int srcSize) {
		SecureRelRecordType srcSchema = aProject.getInSchema();
		SecureRelRecordType dstSchema = aProject.getSchema();
		String ret = new String();
		LogicalProject projection = (LogicalProject) aProject.getSecureRelNode().getRelNode();
		
		assert(projection.getChildExps().size() == dstSchema.getFieldCount());
		List<RexNode> fieldProject = projection.getChildExps();
		Iterator<RexNode> itr = fieldProject.iterator();

		for(SecureRelDataTypeField field : dstSchema.getSecureFieldList()) {
			if(itr.hasNext())
				ret += dstVariable + dstSchema.getBitmask(field) + " = " + flattenForSmc(itr.next(), srcSchema, srcVariable, srcSize) + ";\n";
		}
		
		return ret;
		
	}
	
	public static String flattenFilter(Filter aFilter, String srcVariable, int srcSize) {
		SecureRelRecordType schema = aFilter.getChild(0).getSchema(true);
		String ret = new String();
		LogicalFilter filter = (LogicalFilter) aFilter.getSecureRelNode().getRelNode();
		
		List<RexNode> fieldFilter = filter.getChildExps();
		Iterator<RexNode> itr = fieldFilter.iterator();
		
		while (itr.hasNext()) {
			ret += flattenForSmc(itr.next(), schema, srcVariable, srcSize);
		}
		
		return ret;
	}
	
	
	public static String flattenForSmc(RexNode expr, SecureRelRecordType schema, String variable, int srcSize) {
		RexFlattener flatten = new RexNodeToSmc(schema, variable, srcSize);
		String result = expr.accept(flatten);
		return result;
	}
	
	
	public static String flattenForSql(RexNode expr, SecureRelRecordType schema) {
		RexFlattener flatten = new RexNodeToSql(schema);
		return expr.accept(flatten);

	}
	
	
}
