package org.smcql.type;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.SerializableCharset;
import org.apache.commons.lang.CharSet;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.smcql.util.Utilities;

// thin wrapper on top of RelDataTypeField for attaching security policy to an attribute
// how do we call this at schema construction time?
// create a secure table 
// instantiate SecureTable where we do "Table table = schema.getTable(tableName);" now
public class SecureRelDataTypeField extends RelDataTypeFieldImpl implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public enum SecurityPolicy {
		Public, Protected, Private
	};

	SecurityPolicy policy = SecurityPolicy.Private;
	RelDataTypeField baseField;

	// both of below are null if field sourced from greater than one attribute
	// stored in original tables
	private String storedTable;
	private String storedAttribute;
	transient List<LogicalFilter> filters;

	public SecureRelDataTypeField(String name, int index, RelDataType type) {
		super(name, index, type);
		filters = new ArrayList<LogicalFilter>();
	}

	public SecureRelDataTypeField(RelDataTypeField baseField, SecurityPolicy secPolicy) {
		super(baseField.getName(), baseField.getIndex(), baseField.getType());
		this.baseField = baseField;
		policy = secPolicy;
		filters = new ArrayList<LogicalFilter>();
	}

	public SecureRelDataTypeField(RelDataTypeField baseField, SecurityPolicy secPolicy, String aStoredTable,
			String aStoredAttribute, LogicalFilter aFilter) {
		super(baseField.getName(), baseField.getIndex(), baseField.getType());
		this.baseField = baseField;
		policy = secPolicy;
		setStoredTable(aStoredTable);
		setStoredAttribute(aStoredAttribute);
		filters = new ArrayList<LogicalFilter>();
		addFilter(aFilter);
	}

	// quasi-copy constructor
	public SecureRelDataTypeField(RelDataTypeField aBaseField, SecureRelDataTypeField src) {
		super(aBaseField.getName(), src.getBaseField().getIndex(), src.getBaseField().getType());
		baseField = aBaseField;
		policy = src.getSecurityPolicy();
		storedTable = src.getStoredTable();
		storedAttribute = src.getStoredAttribute();
		filters = new ArrayList<LogicalFilter>(src.getFilters());
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		List<String> field = new ArrayList<String>();
		field.add(Integer.toString(baseField.getIndex()));
		field.add(baseField.getName());
		field.add(Integer.toString(baseField.getType().getPrecision()));
		field.add(Integer.toString(baseField.getType().getScale()));

		out.writeObject(field);
		out.writeObject(baseField.getType().getSqlTypeName());
		out.writeObject(storedTable);
		out.writeObject(storedAttribute);
		out.writeObject(policy);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
		List<String> field = (List<String>) ois.readObject();
		SqlTypeName stn = (SqlTypeName) ois.readObject();

		int index = Integer.parseInt(field.get(0));
		String name = field.get(1);
		int precision = Integer.parseInt(field.get(2));
		int scale = Integer.parseInt(field.get(3));

		BasicSqlType type = new BasicSqlType(RelDataTypeSystem.DEFAULT, stn, precision, scale);
		baseField = new RelDataTypeFieldImpl(name, index, type);
		storedTable = (String)ois.readObject();
		storedAttribute = (String)ois.readObject();
		policy = (SecurityPolicy)ois.readObject();
	}

	public int size() {
		TypeMap tMap = TypeMap.getInstance();
		return tMap.sizeof(this);
	}

	@Override
	public int getIndex() {
		if (baseField == null)
			return super.getIndex();
		return baseField.getIndex();
	}

	public SecurityPolicy getSecurityPolicy() {
		return policy;
	}

	public void setSecurityPolicy(SecurityPolicy policy) {
		this.policy = policy;
	}

	public RelDataTypeField getBaseField() {
		return baseField;
	}

	@Override
	public String toString() {
		return baseField + " " + policy;
	}

	public String getStoredTable() {
		return storedTable;
	}

	public void setStoredTable(String storedTable) {
		this.storedTable = storedTable;
	}

	public String getStoredAttribute() {
		return storedAttribute;
	}

	public void setStoredAttribute(String storedAttribute) {
		this.storedAttribute = storedAttribute;
	}

	// for testing equality of slice keys
	public void addFilter(LogicalFilter aFilter) {
		if (aFilter != null)
			filters.add(aFilter);
	}

	public List<LogicalFilter> getFilters() {
		return filters;
	}

	public boolean isSliceAble() {
		if (storedAttribute != null && storedTable != null && policy == SecurityPolicy.Public)
			return true;
		return false;
	}
	
}
