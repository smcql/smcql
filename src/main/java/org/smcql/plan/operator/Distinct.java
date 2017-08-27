package org.smcql.plan.operator;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.logical.LogicalAggregate;
import org.smcql.plan.SecureRelNode;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelRecordType;

public class Distinct extends Operator {

	public Distinct(String name, SecureRelNode src, Operator ...children ) throws Exception {
		super(name, src, children);
		splittable = true;
		blocking = true;
		sliceAgnostic = true;

		
	}
	
	public List<SecureRelDataTypeField> getSliceAttributes() {
		return super.derivedSliceKey();
	}
	
	
	public List<SecureRelDataTypeField> computesOn() {
		return getSchema().getSecureFieldList();
	}

	public List<SecureRelDataTypeField> secureComputeOrder() {
		return getSchema().getSecureFieldList();
	}

};
