package org.smcql.db.data.field;

import java.io.Serializable;

import org.smcql.type.SecureRelDataTypeField;

public class IntField extends Field  implements Serializable   {

	public long value;
	
	public IntField(SecureRelDataTypeField attr, int v) {
		super(attr);
		value = v;
	}
	
	public IntField(SecureRelDataTypeField attr) {
		super(attr);		
		value = 0;
	}

	@Override
	public int size()  {
		return 64;
	}
	
	
	@Override
	public String serializeToBinaryString() {
		String binString = Long.toBinaryString(value);
		
		// pad it for length
		while(binString.length() < this.size()) {
			binString = '0' + binString;
		}

	
		return binString;
	}
	
	public long getValue() {
		return value;
	}
	
	@Override
	public String toString() {
		return Long.toString(value);
	}
	
	@Override
    public int hashCode() {
		int hash = Long.hashCode(value);
        
        return hash;
	}
	

	public boolean equals(Object o) {
		if(o instanceof IntField) {
			IntField intField = (IntField) o;
			if(intField.value == this.value) {
				return true;
			}
		}
		return false;
	}

	
	@Override
	public int childCompare(Field f) {
		if(f instanceof IntField) {
			Long lhs = new Long(value);
			Long rhs = new Long(((IntField) f).getValue());
			return lhs.compareTo(rhs);
		}
		return 0;
	}

	
	@Override
	public void setValue(String source, int sourceOffset) {
		String rawBits = source.substring(sourceOffset, sourceOffset + this.size());
		value = Integer.parseInt(rawBits, 2);
		
	}
	
	@Override
	public void deserialize(boolean[] src) {
		assert(src.length == this.size());
		value = 0;

		for (boolean b : src)
			value = (value << 1) | (b ? 1 : 0);
		
	}
	
}
