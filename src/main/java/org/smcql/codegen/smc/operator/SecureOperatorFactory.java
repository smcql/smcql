package org.smcql.codegen.smc.operator;


import org.smcql.plan.operator.Operator;
import org.smcql.util.CodeGenUtils;

public class SecureOperatorFactory {
	
	public static SecureOperator get(Operator o) throws Exception {
		switch(o.getOpName()) {
			case "Aggregate":
				return new SecureAggregate(o);
			case "Sort":
				return new SecureSort(o);
			case "Distinct":
				return new SecureDistinct(o);
			case "WindowAggregate":
				return new SecureWindowAggregate(o);
			case "Join":
				return new SecureJoin(o);
			case "Filter":
				return new SecureFilter(o);
			case "Merge":
				return new SecureMerge(o);
			default:
				return null;
		
		}
	}
}
