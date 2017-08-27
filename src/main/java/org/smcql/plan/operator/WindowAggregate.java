package org.smcql.plan.operator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window.Group;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.smcql.util.Utilities;
import org.smcql.plan.SecureRelNode;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelDataTypeField.SecurityPolicy;
import org.smcql.type.SecureRelRecordType;

public class WindowAggregate extends Operator {

	List<SecureRelDataTypeField> aggFields;
	
	public WindowAggregate(String name, SecureRelNode src, Operator ... children ) throws Exception {
		super(name, src, children);
		aggFields = new ArrayList<SecureRelDataTypeField>();
		splittable = true;
		blocking = true;
	}
	
	
	public List<SecureRelDataTypeField> getSliceAttributes() {
		List<SecureRelDataTypeField> sliceKey = new ArrayList<SecureRelDataTypeField>();
		LogicalWindow win = (LogicalWindow) this.getSecureRelNode().getRelNode();
		SecureRelRecordType inSchema = getInSchema();
		
		assert(win.groups.size() <= 1); 
		
		Group aggregate = win.groups.get(0);
		List<Integer> partitionBy = new ArrayList<Integer>(aggregate.keys.asList());
		
		for(Integer i : partitionBy)
			if(inSchema.getSecureField(i).isSliceAble())
				sliceKey.add(inSchema.getSecureField(i));
		
		return sliceKey;
	}
	
	// computes on PARTITION BY and ORDER BY
	public List<SecureRelDataTypeField> computesOn() {
		List<SecureRelDataTypeField> accessed = new ArrayList<SecureRelDataTypeField>();
		LogicalWindow win = (LogicalWindow) this.getSecureRelNode().getRelNode();
		List<Group> groups = win.groups;
		SecureRelRecordType schema = getSchema();

		for(Group g : groups) {
			accessed =  resolveGroup(g, schema, accessed);
			
		}
		
		return accessed;
	}
	
	public static List<SecureRelDataTypeField> resolveGroup(Group group, SecureRelRecordType schema, List<SecureRelDataTypeField> accessed) {
		RelCollation keys = group.orderKeys;
		List<RelFieldCollation> sortKey = keys.getFieldCollations();
		List<Integer> partitionBy = new ArrayList<Integer>(group.keys.asList());
		
		
		for(Integer idx : partitionBy) {
			if(!accessed.contains(schema.getSecureField(idx)))
				accessed.add(schema.getSecureField(idx));
		}

		for(RelFieldCollation attr : sortKey) {
			int idx = attr.getFieldIndex();
			if(!accessed.contains(schema.getSecureField(idx)))
					accessed.add(schema.getSecureField(idx));
		}
		
		return accessed;
		
	}
	
	
	public List<SecureRelDataTypeField> secureComputeOrder() {
		return this.computesOn();
	}
	
	public List<SecureRelDataTypeField> getAggregateAttributes() {

		List<SecureRelDataTypeField> aggs = new ArrayList<SecureRelDataTypeField>();
		
		for(SecureRelDataTypeField field : getSchema().getSecureFieldList()) {
			if(field.getBaseField().getName().contains("$")) {
				aggs.add(field);
			}
		}
		return aggs;
	}
};
