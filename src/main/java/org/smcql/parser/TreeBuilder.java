package org.smcql.parser;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexSubQuery;
import org.smcql.plan.SecureRelNode;
import org.smcql.plan.operator.Operator;
import org.smcql.plan.operator.OperatorFactory;

//light version of query planner
// take in a tree of logical RelNodes
// produces a tree of physical Operators
public class TreeBuilder {
	
	// first pass - traverse tree, fill in Operator structure
	public static Operator create(String name, RelRoot relRoot) throws Exception {
		
		Operator root = operatorHelper(name, relRoot.project());
		root.inferExecutionMode();
		
		return root;
	}

	public static Operator operatorHelper(String name, RelNode rel) throws Exception {
		List<RelNode> inputs = rel.getInputs();
		if(inputs.isEmpty()) {
			SecureRelNode theNode = new SecureRelNode(rel, (SecureRelNode[]) null);
			return OperatorFactory.get(name, theNode);
		}
		
		List<Operator> children = new ArrayList<Operator>();
		for(RelNode r : inputs) {
			children.add(operatorHelper(name, r));
		}
		


		// join
		if(children.size() == 2) {
			SecureRelNode lhs = children.get(0).getSecureRelNode();
			SecureRelNode rhs = children.get(1).getSecureRelNode();
			SecureRelNode theNode = new SecureRelNode(rel, lhs, rhs);
			Operator op = OperatorFactory.get(name, theNode, children.toArray(new Operator[2]));
			return op;
		}
		

		// all single-input ops
		assert(children.size() == 1);
		SecureRelNode child = children.get(0).getSecureRelNode();
		SecureRelNode theNode = new SecureRelNode(rel, child);
		Operator op = OperatorFactory.get(name, theNode, children.get(0));
		return op;
		
		
	}
	

	
	
}
