package org.smcql.db.data.field;

import java.io.Serializable;
import java.sql.Timestamp;

import org.smcql.type.SecureRelDataTypeField;



public class TimestampField extends Field implements Serializable  {

	public Timestamp time;
	
	
	TimestampField(SecureRelDataTypeField attr, Timestamp timestamp) {
		super(attr);
		time = timestamp;
		timestamp.getTime();

	}
	
	public TimestampField(SecureRelDataTypeField attr) {
		super(attr);
	}

	@Override
	public int size()  {
		return 64;
	}
	
	@Override
	public String serializeToBinaryString() {
		long epoch = time.getTime();
		String binString = Long.toBinaryString(epoch);
		
		while(binString.length() < this.size()) {
			binString = '0' + binString;
		}
		
		return binString;

	}
	
	
	public String toString() {
		return new String(Long.toString(time.getTime()));
	}

	
	@Override
    public int hashCode() {
        Long hash = time.getTime();
        return hash.hashCode();

	}
	
	public boolean equals(Object o) {
		if(o instanceof IntField) {
			TimestampField field = (TimestampField) o;
			if(field.time.getTime() == this.time.getTime() ) {
				return true;
			}
		}
		return false;
	}

	@Override
	public int childCompare(Field f) {
		if(f instanceof TimestampField) {
			Long lhs = new Long(time.getTime());
			Long rhs = new Long(((TimestampField) f).time.getTime());
			return lhs.compareTo(rhs);
		}
		return 0;
	}

	
	@Override
	public void setValue(String source, int sourceOffset) {
		String rawBits = source.substring(sourceOffset, sourceOffset + this.size());
		Long epoch = Long.parseLong(rawBits, 2);
		time = new Timestamp(epoch);
		
	}
	
	
	@Override
	public void deserialize(boolean[] src) {
		
		assert(src.length == this.size());
		long epoch = 0;

		for (boolean b : src)
			epoch = (epoch << 1) | (b ? 1 : 0);
		time = new Timestamp(epoch);
		
	}


	
	
}
