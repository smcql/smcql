package org.smcql.plan.operator;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.smcql.executor.config.RunConfig.ExecutionMode;
import org.smcql.plan.SecureRelNode;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelDataTypeField.SecurityPolicy;
import org.smcql.type.SecureRelRecordType;

public class SeqScan extends Operator {
	
	SecureRelRecordType inSchema;
	

	public SeqScan(String name, SecureRelNode src, Operator ... children ) throws Exception {
		super(name, src, children);
		executionMode = ExecutionMode.Plain;
		sliceAgnostic = true;
	}
	
	public SecureRelRecordType getInSchema() {
		return baseRelNode.getSchema();
	}
	
	@Override
	public SecureRelRecordType getSchema(boolean isSecureLeaf) {
		SecureRelRecordType schema = baseRelNode.getSchema();
		return schema;
	}
	
	private List<String> getOrderableFields() {
		List<String> fieldNames = new ArrayList<String>();
		Operator parent = this.parent;
		while (parent != null) {
			if (parent instanceof Project) {
				for (SecureRelDataTypeField field : parent.getSchema().getAttributes()) {
					SqlTypeName type = field.getType().getSqlTypeName();
					
					if ((SqlTypeName.NUMERIC_TYPES.contains(type) || SqlTypeName.DATETIME_TYPES.contains(type)) && !fieldNames.contains(field.getName())) 
						fieldNames.add(field.getName());
				}
			}
			
			parent = parent.getParent();
		}
		return fieldNames;
	}
	
	@Override
	public List<SecureRelDataTypeField> secureComputeOrder() {
		List<SecureRelDataTypeField> result = new ArrayList<SecureRelDataTypeField>();
		for (String name: getOrderableFields()) {
			for (SecureRelDataTypeField field : this.getSchema().getAttributes()) {
				if (field.getName().equals(name))
					result.add(field);
			}
		}
		
		return result;
	}

	public void inferExecutionMode() {
		executionMode = ExecutionMode.Plain;
	}

	
};
