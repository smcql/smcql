package org.smcql.db.data.field;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import org.smcql.type.SecureRelDataTypeField;

public class CharField extends Field implements Serializable   {
	public String value;
	int size = 0; // stored bit size - char array may be smaller, but not larger

	
	CharField(SecureRelDataTypeField attr) throws IOException {
		super(attr);
		value = new String();
		size = attr.size();
	}

	public CharField(SecureRelDataTypeField attr, String value) throws Exception {
		super(attr);
		this.value = value;
		if(value != null && value.length() > (attr.size() / 8)) {
			throw new Exception("String exceeds specified size!" + value + " at " + value.length() + " for attribute " + attr);
		}
		size = attr.size();

	}
	
	@Override
	public int size()  {
		return size;
	}


	@Override
	public String serializeToBinaryString() {
		String ret = new String();
		
		if(value != null) {
			for(char c : value.toCharArray()) {
				ret += serializeChar(c);
			}
		}
		
		
		
		while(ret.length() < size()) {
			ret += '0';
		}
		

		return ret;
	}
	
	private String serializeChar(char c) {

		String ret = Integer.toBinaryString((int) c);
		while(ret.length() < 8) {
			ret = "0" + ret;
		}
		
		return ret;
	}
	
	public String toString() {
		return new String(value);
	}
	
	@Override
    public int hashCode() {
        return value.hashCode();
        
	}
	
	public boolean equals(Object o) {
		if(o instanceof CharField) {
			CharField charField = (CharField) o;
		
			while(charField.value.length() < value.length()) { // fix null padding
				charField.value += "\0";
			}
			
			if(charField.value.equals(this.value)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public int childCompare(Field f) {
		if(f instanceof CharField) {
			return value.compareTo(((CharField) f).value);
		}
		return 0;
	}

	
	@Override
	public void setValue(String source, int startOffset) {
		value = new String();
		 
		for(int i = 0; i < size/8; ++i) {
			value += getCharacter(source, startOffset + i * 8);
		}
		
		
	}
	
	private char getCharacter(String source, int startOffset) {
		String rawBits = source.substring(startOffset, startOffset + 8);
		while(rawBits.length() < this.size()) {
			rawBits += "0";
		}
		

		int value = Integer.parseInt(rawBits, 2);
		
		return (char) value;
		
	}
	
	@Override
	public void deserialize(boolean[] src) {
		assert(src.length == this.size); 
		int chars = this.size / 8;
		
		for(int i = 0; i < chars; ++i)
		{
			boolean[] bits = Arrays.copyOfRange(src, i*8, (i+1)*8);
			value += deserializeChar(bits);
			
		}
		
	}

	
	
	private char deserializeChar(boolean[] bits) {
		assert(bits.length == 8);

	    int n = 0;
	    for (boolean b : bits)
	        n = (n << 1) | (b ? 1 : 0);
	    return (char) n;
	}

}
