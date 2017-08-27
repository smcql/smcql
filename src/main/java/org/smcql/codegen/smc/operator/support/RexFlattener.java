package org.smcql.codegen.smc.operator.support;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.StringUtils;
import org.smcql.type.SecureRelRecordType;

// generic flattener for expressions, needs override for input references
public abstract class RexFlattener implements RexVisitor<String> {
	protected SecureRelRecordType schema;
	protected int inputSize;
	
	public RexFlattener(SecureRelRecordType aSchema, int srcSize) {
		schema = aSchema;
		inputSize = srcSize;
	}
	
	
	@Override
	public String visitLocalRef(RexLocalRef localRef) {
		System.out.println("Not yet implemented!");
		System.exit(-1);
		return null;
	}

	@Override
	public String visitLiteral(RexLiteral literal) {
		if (literal.getTypeName().toString().equals("CHAR")) {
			String result = "0";
			for (int i=literal.toString().length()-2; i >0; i--) {
				int val = (int) literal.toString().charAt(i) + 1024;
				result += "," + val;
			}
			return result;
		}
		
		return literal.toString();
	}

	private String getSmcString(RexCall call) {
		SqlKind kind = call.getKind();
		String delimiter;
		
		//handle boolean types
		if (call.getType().getSqlTypeName() == SqlTypeName.BOOLEAN 
				&& call.getOperands().get(1) instanceof RexLiteral
				&& !kind.equals(SqlKind.DIVIDE)
				&& !kind.equals(SqlKind.GREATER_THAN_OR_EQUAL)
				&& !kind.equals(SqlKind.LESS_THAN_OR_EQUAL)) {
			return ((RexNodeToSmc) this).variableName + "$" + (inputSize - 1) + "~" + inputSize + "$ == 1";				
		}
		
		if(kind.equals(SqlKind.AND)) {
			delimiter = "&&";
		} else if(kind.equals(SqlKind.OR)) {
			delimiter = "||";
		} else if(kind.equals(SqlKind.CAST)) {// skip these for now
			delimiter = "";
		} else if (kind.equals(SqlKind.LIKE)) {
			return ((RexNodeToSmc) this).variableName + "$" + (inputSize - 1) + "~" + inputSize + "$ == 1";
		} else if (kind.equals(SqlKind.EQUALS)) {
			delimiter = "==";
		} else if (kind.equals(SqlKind.DIVIDE)) {
			RexLiteral comp = (RexLiteral) call.getOperands().get(1);
			int value = Integer.parseInt(comp.toString());
			int startIndex = 0;
			int endIndex = 0;
			for (int i=0; i<schema.getFieldCount(); i++) {
				if (!call.getOperands().get(0).toString().contains("$" + i)) {
					startIndex += schema.getSecureField(i).size();
				} else {
					endIndex = startIndex + schema.getSecureField(i).size();
					break;
				}
			}
			return "(rTuple$" + startIndex + "~" + endIndex + "$ - lTuple$" + startIndex + "~" + endIndex + "$)/" + value;
		} else {
			delimiter = call.getOperator().getName();
		}
		List<String> children = new ArrayList<String>();
		
		for(RexNode op : call.operands) {
			if (op.toString().indexOf("%") > 0)
				continue;
			String entry = op.accept(this);
			if (entry.contains(",")) {
				children = new ArrayList<String>();
				String[] vals = entry.split(",");
				int startIndex = 0;
				for (String v : vals) {
					int endIndex = startIndex + 8;
					String line = "(" + ((RexNodeToSmc) this).variableName + "$" + startIndex + "~" + endIndex + "$ == " + v + ")";
					children.add(line);
					startIndex += 8;
				}
				delimiter = "&&";
			} else {
				children.add(entry);
				if (kind.equals(SqlKind.LIKE))
					children.add("one");
			}
			
		}
		
		String separater = (delimiter.equals("=") || delimiter.equals("LIKE")) ? " && " : " " + delimiter + " ";
		String result = StringUtils.join(children, separater);
		return result;
	}
	
	private String getSqlString(RexCall call) {
		String result = "";
		for (int i=0; i<call.operands.size(); i++) {
			RexNode op = call.operands.get(i);
			String entry = (op.toString().contains("$")) ? op.accept(this) : op.toString();
			if (i > 0)
				result += " " + call.getOperator() + " ";
			result += entry;
		}
		return result;
	}
	
	@Override
	public String visitCall(RexCall call) {
		if (this instanceof RexNodeToSmc) {
			return getSmcString(call);
		} else {
			return getSqlString(call);
		}
	}

	@Override
	public String visitOver(RexOver over) {
		return null;
	}

	@Override
	public String visitCorrelVariable(RexCorrelVariable correlVariable) {
		return null;
	}

	@Override
	public String visitDynamicParam(RexDynamicParam dynamicParam) {
		return null;
	}

	@Override
	public String visitRangeRef(RexRangeRef rangeRef) {
		return null;
	}

	@Override
	public String visitFieldAccess(RexFieldAccess fieldAccess) {
		return null;
	}

	@Override
	public String visitSubQuery(RexSubQuery subQuery) {
		return null;
	}

}
