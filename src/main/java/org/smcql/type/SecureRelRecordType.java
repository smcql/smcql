package org.smcql.type;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.StringUtils;
import org.smcql.db.data.field.Field;
import org.smcql.db.schema.SecureSchemaLookup;
import org.smcql.plan.operator.Join;
import org.smcql.plan.operator.Operator;
import org.smcql.plan.operator.SeqScan;
import org.smcql.type.SecureRelDataTypeField.SecurityPolicy;
import org.smcql.util.CodeGenUtils;
import org.smcql.util.Utilities;


// decorator for RelRecordType, annotate with a security policy
public class SecureRelRecordType implements Serializable {	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5544214600670533850L;
	
	// maximum policy over all attributes
	SecurityPolicy policy = SecurityPolicy.Private;
    RelRecordType baseType;
    
	List<SecureRelDataTypeField> secureFields;
	public SecureRelRecordType() {
		secureFields = new ArrayList<SecureRelDataTypeField>();
	}
	
	public SecureRelRecordType(RelRecordType baseRow, List<SecureRelDataTypeField> fields) {
		baseType = baseRow;
		secureFields = new ArrayList<SecureRelDataTypeField>(fields);
		
	}
	
	private void writeObject(ObjectOutputStream out) throws IOException {		
		//handle policy
		out.writeObject(policy);
		
		//handle baseType
		out.writeObject(baseType.getSqlTypeName());
		List<RelDataTypeField> fields = baseType.getFieldList();
		List<String> serialized = new ArrayList<String>();
		for (RelDataTypeField f : fields) {
			serialized.add(Integer.toString(f.getIndex()));
			serialized.add(f.getName());
			serialized.add(Integer.toString(f.getType().getPrecision()));
			serialized.add(Integer.toString(f.getType().getScale()));
		}
		out.writeObject(serialized);
		
		//handle secureFields
		for (SecureRelDataTypeField s : secureFields) {
			
			out.writeObject(s);
		}
	}
	
	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
		//handle policy
		policy = (SecurityPolicy)ois.readObject();
		
		//handle baseType
		SqlTypeName stn = (SqlTypeName) ois.readObject();
		List<RelDataTypeField> fields = new ArrayList<RelDataTypeField>();
		List<String> serialized = (List<String>) ois.readObject();
		for (int i=0; i<serialized.size(); i+=4) {
			int index = Integer.parseInt(serialized.get(i));
			String name = serialized.get(i+1);
			int precision = Integer.parseInt(serialized.get(i+2));
			int scale = Integer.parseInt(serialized.get(i+3));

			BasicSqlType type = new BasicSqlType(RelDataTypeSystem.DEFAULT, stn, precision, scale);
			RelDataTypeField f = new RelDataTypeFieldImpl(name, index, type);
			fields.add(f);
		}
		baseType = new RelRecordType(fields);
		
		//handle secureFields
		List<SecureRelDataTypeField> secure = new ArrayList<SecureRelDataTypeField>();
		try {
			while (true) {
				SecureRelDataTypeField cur = (SecureRelDataTypeField) ois.readObject();
				secure.add(cur);
			}
		} catch (Exception e) {
		}
		secureFields = secure;
	}
	
		
	public SecureRelRecordType(SecureRelRecordType schema) {
		baseType = schema.baseType;
		secureFields = new ArrayList<SecureRelDataTypeField>(schema.getSecureFieldList());
		
	}

	public SecureRelDataTypeField getSecureField(int idx) {
		if (idx >= secureFields.size()) {
			for (SecureRelDataTypeField field : secureFields) {
				if (field.getIndex() == idx)
					return field;
			}
		}
		return secureFields.get(idx);
	}

	

	public List<SecureRelDataTypeField> getSecureFieldList() {
		return secureFields;
	}

	public int getFieldCount() {
		return secureFields.size();
	}

	public List<String> getFieldNames() {
		
		return baseType.getFieldNames();
	}
	
	@Override
	public String toString() {
		String ret = "(";
		ret += StringUtils.join(secureFields.toArray(new SecureRelDataTypeField[secureFields.size()]), ",");
		ret += ")";
		return ret;
	}
	
	SecurityPolicy maxSecurityPoliy() {
		SecurityPolicy policy = SecurityPolicy.Public;
		for(SecureRelDataTypeField secField : secureFields) {
			if(secField.getSecurityPolicy().compareTo(policy) > 0)
				policy = secField.getSecurityPolicy();
		}
		
		return policy;
	}
	
	public int size() {
		int sum = 0;
		for(SecureRelDataTypeField secField : secureFields) {
			sum += secField.size();
		}
		return sum;
	}
	
	public RelRecordType getBaseType() {
		return baseType;
	}
	
	public String getBitmask(int idx)  {
		SecureRelDataTypeField field = null;
		for (SecureRelDataTypeField cur : secureFields) {
			if (idx == cur.getIndex()) {
				field = cur;
				break;
			}
		}
		return CodeGenUtils.getBitmask(this, field);
	}

	public String getBitmask(SecureRelDataTypeField field) {
		return CodeGenUtils.getBitmask(this, field);
	}

	public List<SecureRelDataTypeField> getAttributes() {
		return secureFields;
	}

	public void addAttribute(SecureRelDataTypeField attribute) {
		secureFields.add(attribute);
		
	}
	
	public void setAttribute(SecureRelDataTypeField attribute, int index) {
		secureFields.set(index, attribute);
	}
	
	public void removeAttribute(int index) {
		if (secureFields.size() > index) {
			secureFields.remove(index);
		}
	}

	public SecureRelDataTypeField getAttribute(String attrName) {
		int idx = getFieldNames().indexOf(attrName);
		if(idx != -1)
			return secureFields.get(idx);
		
		return null;
	}


}
