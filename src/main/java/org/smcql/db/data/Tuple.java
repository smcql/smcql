package org.smcql.db.data;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.smcql.db.data.field.Field;
import org.smcql.db.data.field.FieldFactory;
import org.smcql.type.SecureRelRecordType;
import org.smcql.type.SecureRelDataTypeField;
public class Tuple implements Comparator<Tuple>, Comparable<Tuple>, Serializable {
	List<Field> fields;
	transient SecureRelRecordType schema;
	
	public Tuple(SecureRelRecordType s, ResultSet values) throws Exception {
		ResultSetMetaData rsmd = values.getMetaData();
		schema = s;

		if(schema.getAttributes().size() != rsmd.getColumnCount()) {
			throw new Exception("Result set column count does not match schema's " + rsmd.getColumnCount() + " != " + schema.getAttributes().size());
		}
		
		fields = new ArrayList<Field>();
		
		int i = 1;
		for(SecureRelDataTypeField attr : schema.getAttributes()) {
			Field f = FieldFactory.get(attr, values, i);
			fields.add(f);
			++i;
		}
		
		if(this.toBinaryString().length() != s.size()) {
			throw new Exception("Badly formed tuple!");
		}
		
	}
	
	public Tuple(String boolStr, SecureRelRecordType schema) throws Exception {
		assert (schema.size() == boolStr.length());
		int offset = 0;
		fields = new ArrayList<Field>();
			
		for(SecureRelDataTypeField attr : schema.getAttributes()) {
			Field f = FieldFactory.get(attr);
			f.setValue(boolStr, offset);
			offset += f.size();
		}
		
	}
	
	public Tuple(boolean[] bits, SecureRelRecordType schema) throws Exception {
		fields = new ArrayList<Field>();
		
		int start = 0;
		int end;

		
		for(SecureRelDataTypeField r : schema.getAttributes()) {
			Field f = FieldFactory.get(r);
			end = start + f.size();
			boolean[] fieldBits = Arrays.copyOfRange(bits, start, end);
			// reverse field bits
			fieldBits = reverseBits(fieldBits);
			f.deserialize(fieldBits);
			fields.add(f);
			start = end;
		}
	}
	
	public static boolean[] reverseBits(boolean[] src) {
		int len = src.length;
		boolean[] dst = new boolean[len];
		
		for(int i = 0; i < len; ++i) 
			dst[i] = src[len-i-1];
		
		return dst;
	}
	
	public SecureRelRecordType getSchema() {
		return schema;
	}
	
	public static List<Tuple> extractTuples(String bools, SecureRelRecordType schema) throws Exception {
		Tuple t = new Tuple(schema);
		int tSize = t.size();
		List<Tuple> output = new ArrayList<Tuple>();
		int offset = 0;
		
		assert(bools.length() % tSize == 0); // sizes align
		
		for(int i = 0; i < bools.length() / tSize; ++i) {
			String rawBits = bools.substring(offset, offset + tSize);
			output.add(new Tuple(rawBits, schema));
			offset += tSize;
		}
		
		return output;
		
	}
	
	public Tuple() {
		fields = new ArrayList<Field>();
		schema = new SecureRelRecordType();
		
				
	}

	// create blank tuple that matches schema
	public Tuple(SecureRelRecordType schema) throws Exception {
		fields = new ArrayList<Field>();
		
		for(SecureRelDataTypeField attr : schema.getAttributes()) {
			Field f = FieldFactory.get(attr);
			fields.add(f);
		}
		

	}

	public void addField(Field f) {
		fields.add(f);
		schema.addAttribute(f.getAttribute());
	}
	
	
	public int size() {
		int sum = 0;
		for(Field f : fields) {
			sum += f.size();
		}
		return sum;
	}
	
	public Field getField(int idx) {
		return fields.get(idx);
	}
	
	public int getAttributeCount() {
		return fields.size();
	}
	
	public List<Boolean> serializeToBinary() {
		List<Boolean> output = new ArrayList<Boolean>();
		String source = this.toBinaryString();
		
		
		for(char c : source.toCharArray()) {
			Boolean v = (c == '0') ? false : true;
			output.add(v);
		}
		return output;
	}
	
	public String toBinaryString() {
		String ret = new String();
		
		for(Field f : fields) {
			ret += StringUtils.reverse(f.serializeToBinaryString());
		}
		
		return ret;
	}
	
	@Override
    public int hashCode() {
        int hash = 0;
        
        for(Field f : fields) {
        	hash += f.hashCode();
        }
        
        return hash;
	}
	
	
	@Override
	public boolean equals(Object o) {
		if(!(o instanceof Tuple)) {
			return false;
		}
		
		Tuple t = (Tuple) o;
		
		if(t.fields.size() != this.fields.size()) {
		
			return false;
		}
		
		int i = 0;
		for(Field f : fields) {
			if(!f.equals(t.getField(i))) {
		
				return false;
			}
			++i;
		}

		return true;
	}

	@Override
	public String toString() {
		return fields.toString();
	}
	
	public String printField(int field) {
		return fields.get(field).toString();
	}

	@Override
	public int compareTo(Tuple o) {


		assert(o.fields.size() == this.fields.size());
		
		for(int i = 0; i < fields.size(); ++i) {
			int cmp = fields.get(i).compareTo(o.getField(i));
			if(cmp != 0) {
				return cmp;
			}
		}
		return 0;
		
	}

	@Override
	public int compare(Tuple o1, Tuple o2) {
		return o1.compareTo(o2);
	}

	public List<Field> getFields() {
		return fields;
	}
	
	
	public List<SecureRelDataTypeField> getAttributes() {

		List<SecureRelDataTypeField> attrs = new ArrayList<SecureRelDataTypeField>();
		for(Field f : fields) {
			attrs.add(f.getAttribute());
		}
		return attrs;
	}
	
	
	
}
