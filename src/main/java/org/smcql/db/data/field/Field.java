package org.smcql.db.data.field;

import java.io.Serializable;
import java.util.Comparator;

import org.smcql.type.SecureRelDataTypeField;

// individual attribute value inside a tuple
// for use in slice values and for passing around data between SMC and plaintext
// hashcode() and equals() are compare by value, not source attribute name / type

public class Field implements Comparator<Field>, Comparable<Field>, Serializable {

	transient SecureRelDataTypeField srcAttribute = null;
	
	// psql ResultSet r, int rsetIdx
	Field(SecureRelDataTypeField attr) {
		srcAttribute = attr;
	}
	
	// bytes
	// to be overriden by children
	public int size()  {
		return 0;
	}
	
	
	public String serializeToBinaryString() {
		return null;
	}

	// meant to be overriden by children
	public int childCompare(Field f) {
		return 0;
	}
	
	// implemented in child classes
	@Override
	public int compareTo(Field o) {
		assert this.getClass().equals(o.getClass());
		return this.childCompare(o);
	}

	
	@Override
	public int compare(Field o1, Field o2) {
		assert(o1.getClass().equals(o2.getClass()));
		return o1.compareTo(o2);
	}

	public SecureRelDataTypeField getAttribute() {
		return srcAttribute;
	}
	
	// for override
	// source is an ObliVM-style binary string
	public void setValue(String source, int startOffset) {
		
	}
	
	public void deserialize(boolean[] src) {
		
	}
	
}
