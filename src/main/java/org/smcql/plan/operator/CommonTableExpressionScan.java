package org.smcql.plan.operator;

import java.util.List;

import org.smcql.plan.SecureRelNode;
import org.smcql.type.SecureRelDataTypeField;

public class CommonTableExpressionScan extends Operator {

	public CommonTableExpressionScan(String name, SecureRelNode src, Operator ...children ) throws Exception {
		super(name, src, children);
		sliceAgnostic = true;
	}
	
	public List<SecureRelDataTypeField> getSliceAttributes() {
		return super.derivedSliceKey();
	}

};
