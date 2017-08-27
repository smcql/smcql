package org.smcql.plan.slice;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.smcql.codegen.smc.operator.support.RexNodeUtilities;
import org.smcql.executor.config.RunConfig.ExecutionMode;
import org.smcql.plan.operator.Operator;
import org.smcql.plan.operator.Project;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelRecordType;

// only supports single equality predicates per Join for now

public class SliceKeyDefinition implements Serializable {
	List<SecureRelDataTypeField> keyset; // equality attributes
	List<String> filters;
	SecureRelRecordType filterSchema;
	
	public SliceKeyDefinition(List<SecureRelDataTypeField> srcAttrs) {
		keyset = new ArrayList<SecureRelDataTypeField>(srcAttrs);
		filters = new ArrayList<String>();
	}
	
	
	public SliceKeyDefinition()  {
		keyset = new ArrayList<SecureRelDataTypeField>();
		filters = new ArrayList<String>();
	}
	
	
	public SliceKeyDefinition(Operator operator) {
		keyset =  new ArrayList<SecureRelDataTypeField>(operator.getSliceAttributes());
		filters = new ArrayList<String>();
	}
	
	public void addFilters(List<LogicalFilter> logFilters, SecureRelRecordType schema) {
		for (LogicalFilter fil : logFilters) {
			filters.add(RexNodeUtilities.flattenForSql(fil.getChildExps().get(0), schema));
		}
	}
	
	public void mergeFilters(List<String> inFilters) {
		for (String f : inFilters) {
			if (!filters.contains(f))
				filters.add(f);
		}
	}
	
	public void setFilters(List<String> aFilter) {
		filters = aFilter;
	}
	
	public List<String> getFilters() {
		return filters;
	}

	// if lhs or rhs exists in keyset, add any missing keys and return true
	// else return false
	public boolean addEquality(SecureRelDataTypeField lhs, SecureRelDataTypeField rhs) {
		if(matches(lhs) || matches(rhs)) {
			addIfNew(lhs);
			addIfNew(rhs);
			return true;
		}
		
		return false;
	}

	public void addAttributes(List<SecureRelDataTypeField> fields) {
		for(SecureRelDataTypeField f : fields)
			addIfNew(f);
	}
	
	public boolean matches(SecureRelDataTypeField aField) {
		for(SecureRelDataTypeField f : keyset) {
			RelDataTypeField baseField = f.getBaseField();
			if(baseField.equals(aField))
				return true;
		}

		return false;
	}
	
	void addIfNew(SecureRelDataTypeField aField) {
		if(!matches(aField))
			keyset.add(aField);
	}
	
	public static boolean sliceCompatible(Operator child, Operator parent)  {
		if(child.getExecutionMode() == ExecutionMode.Plain
				|| child.getExecutionMode() == ExecutionMode.Plain) {
			return true;
		}
		
		List<SecureRelDataTypeField> parentKeys = parent.getSliceAttributes();
		List<SecureRelDataTypeField> childKeys = child.getSliceAttributes();
		
		
		// for projections compare to input schema
		if(parent instanceof  Project) 
			parentKeys = ((Project) parent).getMatchableSliceAttributes();
	
		return intersects(parentKeys, childKeys);
		
	}


	private static boolean intersects(List<SecureRelDataTypeField> parentKeys, List<SecureRelDataTypeField> childKeys) {
		boolean found = false;
		
		
		for(SecureRelDataTypeField pKey : parentKeys)
			for(SecureRelDataTypeField cKey : childKeys)
				if(pKey.getBaseField().equals(cKey.getBaseField())) 
					found = true;
		return found;
	}
	
	
	public List<SecureRelDataTypeField> getAttributes() {
		return keyset;
	}
	
	public String toString() {
		return keyset.toString();
	}
	
}
