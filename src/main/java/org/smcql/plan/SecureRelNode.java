package org.smcql.plan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.smcql.plan.operator.AttributeResolver;
import org.smcql.plan.operator.Operator;
import org.smcql.type.SecureRelRecordType;

// decorator for calcite's RelNode
// for attribute-level security policy inference 
public class SecureRelNode implements Serializable {
	transient RelNode baseNode;
	Operator physicalNode; 
	List<SecureRelNode> children;
	
	SecureRelNode parent;
	
	
	SecureRelRecordType schema; // out schema of this node
	
	
	public SecureRelNode(RelNode base, SecureRelNode ... childNodes) throws Exception {
		baseNode = base;
		children = new ArrayList<SecureRelNode>();
		
		if(childNodes != null)
			for(SecureRelNode child : childNodes) 
				addChild(child);
		
		schema = AttributeResolver.resolveNode(this);
	}
	
	
	public void addChild(SecureRelNode op) {
		children.add(op);
		op.setParent(this);
	}
	
	public void addChildren(List<SecureRelNode> ops) {
		children.addAll(ops);	
		for(SecureRelNode op : ops)
			op.setParent(this);
	}
	
	public void setParent(SecureRelNode op) {
		parent = op;
	}


	public RelNode getRelNode() {
		return baseNode;
	}


	public SecureRelNode getChild(int i) {
		return children.get(i);
	}

	public List<SecureRelNode> getChildren() {
		return children;
	}

	public SecureRelRecordType getSchema() {
		return schema;
	}

	public void setPhysicalNode(Operator op) {
		physicalNode = op;
	}
	
	
	public Operator getPhysicalNode() {
		return physicalNode;
	}


	
	

}
