package org.smcql.plan.operator;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.smcql.plan.SecureRelNode;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelRecordType;

public class Sort extends Operator {

	public Sort(String name, SecureRelNode src, Operator ... children ) throws Exception {
		super(name, src, children);
		splittable = true;
		blocking = true;
	}
	
	@Override
	public int getLimit() {
		// limit associated with sort in Calcite

		assert(baseRelNode.getRelNode() instanceof LogicalSort);  
		
		RexNode limit = ((LogicalSort)baseRelNode.getRelNode()).fetch;
		if(limit != null)
			return  RexLiteral.intValue(limit); // fetch = RexNode

		return -1;

	}
	
	@Override
	public List<SecureRelDataTypeField> computesOn() {
		LogicalSort sort = (LogicalSort) baseRelNode.getRelNode();
		RelCollation sortOn = sort.getCollation();
		
		List<RelFieldCollation> sortKey = sortOn.getFieldCollations();
		SecureRelRecordType schema = getSchema();
		List<SecureRelDataTypeField> accessed = new ArrayList<SecureRelDataTypeField>();
		
		for(RelFieldCollation attr : sortKey) {
			int idx = attr.getFieldIndex();
			accessed.add(schema.getSecureField(idx));
		}
		
		return accessed;
	}	
	
	public List<SecureRelDataTypeField> secureComputeOrder() {
		return computesOn();
	}

	

	public RelFieldCollation.Direction getSortDirection() {
		LogicalSort sort = (LogicalSort) baseRelNode.getRelNode();
		
		RelCollation collation = sort.getCollation();
		return collation.getFieldCollations().get(0).direction;
	}
	
};
