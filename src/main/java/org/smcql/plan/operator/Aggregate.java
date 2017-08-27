package org.smcql.plan.operator;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.util.ImmutableBitSet;
import org.smcql.plan.SecureRelNode;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelRecordType;

public class Aggregate extends Operator {

	
	
	public Aggregate(String name, SecureRelNode src, Operator ...children) throws Exception {
		super(name, src, children);
		splittable = true;
		blocking = true;
	}

	public List<SecureRelDataTypeField> getSliceAttributes() {
		LogicalAggregate agg = (LogicalAggregate) this.getSecureRelNode().getRelNode();
		List<Integer> groupBy = agg.getGroupSet().asList();
		List<SecureRelDataTypeField> sliceKeys = new ArrayList<SecureRelDataTypeField>();
		SecureRelRecordType inSchema = this.getInSchema();
		
		for(Integer i : groupBy)
			if(inSchema.getSecureField(i).isSliceAble())
				sliceKeys.add(inSchema.getSecureField(i));
		
		for(AggregateCall aggCall : agg.getAggCallList()) {
			List<Integer> args  =  aggCall.getArgList();
			for(Integer i : args) {
				if(inSchema.getSecureField(i).isSliceAble())
					sliceKeys.add(inSchema.getSecureField(i));
			}
		}
		
		return sliceKeys;
	}
	
	public int getComputeAttributeIndex() {
		LogicalAggregate agg = (LogicalAggregate) this.getSecureRelNode().getRelNode();
		return agg.getGroupCount();
	}
	
	public List<SecureRelDataTypeField> getGroupByAttributes() {
		LogicalAggregate agg = (LogicalAggregate) this.getSecureRelNode().getRelNode();
		List<Integer> groupBy = agg.getGroupSet().asList();
		
		SecureRelRecordType schema = this.getSchema();
		List<SecureRelDataTypeField> result = new ArrayList<SecureRelDataTypeField>();
		for (Integer i : groupBy) {
			result.add(schema.getSecureField(i));
		}
		return result;
	}
	
	public List<SecureRelDataTypeField> computesOn() {
		List<SecureRelDataTypeField> attrs = getSliceAttributes(); // group by
		LogicalAggregate agg = (LogicalAggregate) this.getSecureRelNode().getRelNode();
		List<SecureRelDataTypeField> allFields = getSchema().getSecureFieldList();
		
		List<AggregateCall> aggregates = agg.getAggCallList();
		for(AggregateCall aggCall : aggregates) {
			List<Integer> args  =  aggCall.getArgList();
			if(args.isEmpty()) { // compute over entire row
				return baseRelNode.getSchema().getSecureFieldList();
			}

			for(Integer i : args) {
				SecureRelDataTypeField lookup = allFields.get(i);
				
				if(!attrs.contains(lookup))
					attrs.add(lookup);
			}
		}
		
		for(ImmutableBitSet bits : agg.groupSets.asList()) {
			for(Integer i : bits.asList()) {
				SecureRelDataTypeField lookup = allFields.get(i);
				
				if(!attrs.contains(lookup))
					attrs.add(lookup);
			}
		
		}
				
		
		
		return attrs;
		
	}

	public List<SecureRelDataTypeField> secureComputeOrder() {
		LogicalAggregate agg = (LogicalAggregate) this.getSecureRelNode().getRelNode();
		List<Integer> groupBy = agg.getGroupSet().asList();
		List<SecureRelDataTypeField> orderBy = new ArrayList<SecureRelDataTypeField>();
		SecureRelRecordType inSchema = this.getInSchema();
		
		for(Integer i : groupBy)
				orderBy.add(inSchema.getSecureField(i));
		
		return orderBy;
	}
	
};
