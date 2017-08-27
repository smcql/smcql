package org.smcql.plan.operator;

import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.smcql.plan.SecureRelNode;

public class OperatorFactory {
	
	
	public static Operator get(String name, SecureRelNode secNode, Operator ...children ) throws Exception {
		RelNode node = secNode.getRelNode();
		
		if(node instanceof JdbcTableScan)
			return new SeqScan(name, secNode, children);
		
		if(node instanceof LogicalAggregate){
			LogicalAggregate agg = (LogicalAggregate) node;
			if(agg.getAggCallList().isEmpty()) {
				return new Distinct(name, secNode, children);
			}
			return new Aggregate(name, secNode, children);
			
		}
		
		if(node instanceof LogicalWindow) 
			return new WindowAggregate(name, secNode, children);
		
		
		if(node instanceof LogicalJoin)
			return new Join(name, secNode, children);
		
		if(node instanceof LogicalSort)
			return new Sort(name, secNode, children);
		
		if(node instanceof LogicalProject)
			return new Project(name, secNode, children);
		
		if(node instanceof LogicalFilter)
			return new Filter(name, secNode, children);
		
		return null;
	}

}
