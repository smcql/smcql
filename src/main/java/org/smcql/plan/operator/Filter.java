package org.smcql.plan.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.smcql.plan.SecureRelNode;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelRecordType;

public class Filter extends Operator {
	
	public Join sourceJoin;
	
	public Filter(String name, SecureRelNode src, Operator[] childOps) throws Exception {
		super(name, src, childOps);
		sliceAgnostic = true;
		sourceJoin = null;
	}
	
	public Filter(String name, SecureRelNode src, Operator[] childOps, Join source) throws Exception {
		super(name, src, childOps);
		sliceAgnostic = true;
		sourceJoin = source;
	}
	
	public List<SecureRelDataTypeField> getSliceAttributes() {
		return super.derivedSliceKey();
	}
	
	public List<SecureRelDataTypeField> computesOn() {
		LogicalFilter filter = (LogicalFilter) baseRelNode.getRelNode();
		RexNode predicate = filter.getCondition();
		SecureRelRecordType schema = getSchema();
		
		return AttributeResolver.getAttributes(predicate, schema);
	}

	public List<SecureRelDataTypeField> computesOn(SecureRelRecordType schema) {
		LogicalFilter filter = (LogicalFilter) baseRelNode.getRelNode();
		RexNode predicate = filter.getCondition();
		
		return AttributeResolver.getAttributes(predicate, schema);
	}

}
