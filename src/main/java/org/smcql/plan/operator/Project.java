package org.smcql.plan.operator;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.smcql.plan.SecureRelNode;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelRecordType;

public class Project extends Operator {
	
	public Project(String name, SecureRelNode src, Operator[] childOps) throws Exception {
		super(name, src, childOps);
		sliceAgnostic = true;
	}
	
	public List<SecureRelDataTypeField> getSliceAttributes() {
		LogicalProject project = (LogicalProject) baseRelNode.getRelNode();
		List<RexNode> projExpressions = project.getChildExps();
		
		List<SecureRelDataTypeField> baseKey = children.get(0).getSliceAttributes();
		List<SecureRelDataTypeField> derivedKey = new ArrayList<SecureRelDataTypeField>();
		SecureRelRecordType projSchema = getSchema();
		
		for(SecureRelDataTypeField f : baseKey) {
			int dstIdx = 0;
			
			for(RexNode rex : projExpressions) {
				if(rex.getKind() == SqlKind.INPUT_REF) {
					int ordinal = ((RexInputRef) rex).getIndex();
					
					if(ordinal == f.getBaseField().getIndex()) {
						derivedKey.add(projSchema.getSecureField(dstIdx));
						break;
					}
				}
				++dstIdx;
			}
		}
		return derivedKey;

	
	}
	
	// keys based on input schema rather than mapped-to schema
	public List<SecureRelDataTypeField> getMatchableSliceAttributes() {
		LogicalProject project = (LogicalProject) baseRelNode.getRelNode();
		List<RexNode> projExpressions = project.getChildExps();
		
		List<SecureRelDataTypeField> baseKey = children.get(0).getSliceAttributes();
		List<SecureRelDataTypeField> derivedKey = new ArrayList<SecureRelDataTypeField>();
		
		for(SecureRelDataTypeField f : baseKey) {
			
			for(RexNode rex : projExpressions) {
				if(rex.getKind() == SqlKind.INPUT_REF) {
					int ordinal = ((RexInputRef) rex).getIndex();
					
					if(ordinal == f.getBaseField().getIndex()) { // if it is a match
						derivedKey.add(f);
						break;
					}
				}
			}
		}
		return derivedKey;

	
	}

	
	


}
