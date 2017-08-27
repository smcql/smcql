package org.smcql.db.data;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.smcql.plan.slice.SliceKeyDefinition;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelRecordType;
import org.smcql.util.Utilities;



public class QueryTable implements Serializable {
	
	private List<Tuple> tuples = null; // null until explicitly decrypted if init'd to PortableSecArray
	transient private SecureRelRecordType schema;
	
	int tupleSize = 0;
	int tupleCount = 0;
	
	
	public QueryTable(List<Tuple> tuples) throws Exception {
		this.tuples = tuples;
		
		if(tuples != null && !tuples.isEmpty()) {
			schema = tuples.get(0).schema;
			tupleCount = tuples.size();
			tupleSize = schema.size();
			
		}
	}


	
	
	public QueryTable(boolean[] bits, SecureRelRecordType s) throws Exception {
		schema = s;
		tupleSize = schema.size();
		
		tupleCount = bits.length / tupleSize;
    
        
        assert(bits.length == (tupleCount * tupleSize));
           
           tuples = new ArrayList<Tuple>();

           for(int i = 0; i < tupleCount; ++i) {
                   boolean[] tupleBits = Arrays.copyOfRange(bits, i*tupleSize, (i+1)*tupleSize);
                   Tuple t = new Tuple(tupleBits, schema);

                   tuples.add(t);
           }

	}

	public QueryTable(SecureRelRecordType outSchema)  {
		schema = outSchema; 
		tupleSize = schema.size();
		tupleCount = 0;
		tuples = new ArrayList<Tuple>();
		
	}

	public void addTuples(QueryTable src) {
		if(src == null) 
			return;
		
		for(Tuple t : src.tuples) {
			tuples.add(t);
		}
 
	}
	
	// compare outputs by sorting them first
	public void sort() {
		Collections.sort(tuples);
	}
	
	
	public String printBitArray(boolean[] bits) {
		StringBuffer buf = new StringBuffer();
		for(boolean b : bits) {
			char o = b == true ? '1' : '0';
			buf.append(o);
		}
		return buf.toString();
	}
	
	
	public SecureRelRecordType getSchema() {
		return schema;
	}
	
	public void setSchema(SecureRelRecordType s) {
		schema = s;
	}
	
	// for use in testing
	public void truncateData(int limit) {
		if(tuples == null) 
			return;
		
		while(tuples.size() > limit) {
			tuples.remove(tuples.size() - 1);
		}
	}
	
	
	public List<Tuple> tuples() {
		return tuples;
	}
	
	public void toBinaryFile(String dstFile) throws Exception {
		String tuplesStr = this.toBinaryString();	
		Utilities.writeFile(dstFile, tuplesStr);

	}
	
	public String toBinaryString() throws Exception{
		String tupleStr = "";

		for(Tuple t : tuples) {
			tupleStr += t.toBinaryString();
		}
		return tupleStr;
		
	}
	
	public int tupleSize() throws IOException {
		if(schema != null) 
			return schema.size();

		if(tuples == null || tuples.isEmpty()) {
			return 0;
		}
		
		return tuples.get(0).size();

	}
	
	
	public boolean[] toBinary() throws Exception{
		if(tuples == null || tuples.isEmpty())
			return null;
		
		
		return toBinary(tuples);
				
	}
	
	
	public static boolean[] toBinary(List<Tuple> tuples) throws Exception{
		if(tuples == null || tuples.isEmpty())
			return null;
		int tCount = tuples.size();
		int tSize = tuples.get(0).schema.size();
		
		boolean[] tableBits = new boolean[tCount * tSize];
		int tIdx = 0;
		
		
		for(Tuple t : tuples) {
		
			List<Boolean> bitArray = t.serializeToBinary();
			if(bitArray.size() != tSize) {
				throw new Exception("Tuple size mismatched! (probably owing to varchars not matching specified length)");
			}
			
			for(Boolean b : bitArray) {
				tableBits[tIdx] = b;
				++tIdx;
			}
				
			
		}

		return tableBits;

		
	}
	
	
	public int tupleCount() {
		if(tuples == null) {
			return 0;
		}
		
		return tuples.size();
	}
	
	@Override
	public boolean equals(Object o) {
		if(o == this) { 
			return true;
		}

		if(o instanceof QueryTable) {
			QueryTable qt = (QueryTable) o;

			if(qt.tuples.size() != tuples.size()) {
				return false;
			}

			if(qt.tuples == this.tuples) {
				return true;
			}
			
			
			if(qt.tuples.equals(this.tuples)) {
				return true;
			}
			
		}
		
		return false;
	}

	public Tuple getTuple(int i) {
		return tuples.get(i);
	}
	
	
	
	public String toString() {
		String ret = new String();
		List<String> attrNames = new ArrayList<String>();
		
		if(schema == null || schema.getAttributes().isEmpty()) {
			return null;
		}
		
		for(SecureRelDataTypeField r : schema.getAttributes()) {
			attrNames.add(r.getName());
		}
		ret = "(" + StringUtils.join(attrNames.toArray(), ", ") + ")\n";
		if(tuples != null)
			for(Tuple t : tuples) {
				ret += t.toString() + "\n";
			}
		return ret;
	}
	
	
	Tuple extractSliceValue(Tuple src, List<Integer> sliceIdxs) {
		Tuple ret = new Tuple();
		
		for(int i : sliceIdxs) {
			ret.addField(src.getField(i));
		}
		
		return ret;
	}
	
	public Map<Tuple, List<Tuple>> asSlices(SliceKeyDefinition key)  throws Exception {
		// for each distinct value in slice key, list tuples associated with it
		Map<Tuple, List<Tuple>> slices = new LinkedHashMap<Tuple, List<Tuple>>();
		List<Integer> sliceIdxs = new ArrayList<Integer>();
		
		// what field(s) are associated with a slice?
		for(SecureRelDataTypeField slice : key.getAttributes()) {
			int i = 0;
			
			for(SecureRelDataTypeField r : schema.getAttributes()) {
				if(slice != null && r != null && slice.getName().equals(r.getName()) && slice.getIndex()==r.getIndex()) {
					sliceIdxs.add(i);
					break;
				}
				++i;
			}
		}
		
		if(sliceIdxs.isEmpty())
			throw new Exception("No slices matched with source data!");

		for(Tuple t : tuples) {
			Tuple value = extractSliceValue(t, sliceIdxs); 
			if(slices.containsKey(value)) {
				slices.get(value).add(t);
			}
			else {
				List<Tuple> tmp = new ArrayList<Tuple>();
				tmp.add(t);

				if(slices.containsKey(t)) 
					throw new Exception("Input not sorted by slice key!"); // no repeat keys
				
				slices.put(value, tmp);
			}
		}
		
		return slices;
	}

}
