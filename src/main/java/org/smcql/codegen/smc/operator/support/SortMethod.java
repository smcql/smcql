package org.smcql.codegen.smc.operator.support;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.smcql.plan.operator.Operator;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelRecordType;
import org.smcql.type.TypeMap;
import org.smcql.util.CodeGenUtils;


// if an operator and its child have different sort compute orders
// adjust this order when we transition from one op to the next
// several smc ops rely on a sorted order, e.g., merge join, distinct (comparing adjacent tuples)
public class SortMethod {

	Operator caller;
	List<SecureRelDataTypeField> sortKey;
	boolean plainSort = false;
	
	// called on the input to be sorted
	public SortMethod(Operator c, List<SecureRelDataTypeField> sk)  {
		
		caller = c;
		sortKey = sk;
		

		if(caller.getInSchema().getSecureFieldList().equals(sortKey)) {
			plainSort = true;
		}


	}
	
	public String sorter() throws Exception {
		
		String ret = new String();
		Map<String, String> variables = new HashMap<String, String>();
		String tupleSize = Integer.toString(caller.getSchema().size());
		variables.put("size", tupleSize);
		variables.put("signal", "1");

		// plain sort
		if(plainSort) {
			String srcFile = "util/sort_method_simple.txt";
			
			ret += "\n" + CodeGenUtils.generateFromTemplate(srcFile, variables);

			return ret;
		}


		ret = extractKeyFunction(caller);

		
				
		
		// add functionality for ORDER BY over part of tuple or different order of attributes
		TypeMap tmap = TypeMap.getInstance();
		String sortKeySize = Integer.toString(tmap.getSize(sortKey)); 
		variables.put("sortKeySize", sortKeySize);
		
		
		
		String srcFile = "util/sort_method.txt";
		
		ret += "\n" + CodeGenUtils.generateFromTemplate(srcFile, variables);
		
		return ret;
	}
	
	public String sortInvocation(String bitsVar, String arrName) {
		if(plainSort) {
			String sortCall = "    int1 sortSignal = 1;\n";
			sortCall += "    sortSimple@" + bitsVar + "(" + arrName+");\n";
			return sortCall;
		}
		
		return  "    sortKeyed@" + bitsVar + "(" + arrName + ");";
	}
	
	// pulls the sort keys out of the tuple
	// regardless of their order in the schema
	public static String extractKeyFunction(Operator s) throws Exception {

		
			TypeMap tmap = TypeMap.getInstance();
			List<SecureRelDataTypeField> sortKey = s.secureComputeOrder();

			int sortKeySize = tmap.getSize(sortKey);
			
			if(sortKey.size() < 1) {
				throw new Exception("No valid sort order for " + s.getOpName() + s.getOperatorId());
			}
			
			
			SecureRelRecordType srcSchema = s.getInSchema();

			int writeStartIdx = 0;
			int writeEndIdx;

			String ret = "int" + sortKeySize + " extractKey" + "(int" + srcSchema.size() + " src) {\n";
			ret += "    int" + sortKeySize + " dst;\n\n";

		
			for(int i = 0; i < sortKey.size(); ++i) {
				// sort key refers to dstSchema
				// find its idx in dst schema, use SchemaMap to get it in srcSchema
				SecureRelDataTypeField outAttr = sortKey.get(i);
				String bitmask = srcSchema.getBitmask(outAttr);
				writeEndIdx = writeStartIdx + outAttr.size();
				ret += "    dst$" + writeStartIdx + "~" + writeEndIdx + "$ = src" + bitmask + ";\n";
				writeStartIdx = writeEndIdx;
			}

			ret += "\n    return dst;\n }";
			
			
			return ret;

			
		}
}
