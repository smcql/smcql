package org.smcql.plan.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.core.Window.Group;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor.Builder;
import org.apache.calcite.rel.rel2sql.SqlImplementor.Clause;
import org.apache.calcite.rel.rel2sql.SqlImplementor.Result;
import org.apache.calcite.rel.logical.LogicalAggregate;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
import org.smcql.codegen.sql.ExtendedRelToSqlConverter;
import org.smcql.db.schema.SecureSchemaLookup;
import org.smcql.plan.SecureRelNode;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelDataTypeField.SecurityPolicy;
import org.smcql.type.SecureRelRecordType;
import org.smcql.util.Utilities;

public class AttributeResolver {
	
	
	public static SecureRelRecordType resolveNode(SecureRelNode aNode) throws Exception {
		RelNode baseNode = aNode.getRelNode();
		
		if(baseNode instanceof LogicalJoin)
			return resolveJoin(aNode);
		if(baseNode instanceof JdbcTableScan)
			return resolveScan(aNode);
		
		if(baseNode instanceof LogicalProject)
			return resolveProjection(aNode);
		
		if(baseNode instanceof LogicalFilter || baseNode instanceof LogicalSort
				|| (baseNode instanceof LogicalAggregate && ((LogicalAggregate) baseNode).getAggCallList().isEmpty())) // distinct
				return copySchema(aNode);
		
		if(baseNode instanceof LogicalAggregate)
			return resolveAggregate(aNode);
		
		
		if(baseNode instanceof LogicalWindow) 
			return resolveWinAggregate(aNode);
		
		// unknown type
		return null;
				
		
	}
	
	private static SecureRelRecordType resolveWinAggregate(SecureRelNode aNode) {
		SecureRelRecordType inSchema = aNode.getChild(0).getSchema();
		
		LogicalWindow win = (LogicalWindow) aNode.getRelNode();

		RelRecordType outRow = (RelRecordType) win.getRowType();
		
		List<SecureRelDataTypeField> secureFields = new ArrayList<SecureRelDataTypeField>();
		
		for(RelDataTypeField field : outRow.getFieldList()) {
			String name = field.getName();
			SecurityPolicy policy;
			if(name.contains("$")) { // w<winNo>$o<orderByNo> for window aggs
				String[] tokens = name.split("\\$");
				
				String winNoStr = tokens[0].substring(1, tokens[0].length());
				
				int winNo = Integer.parseInt(winNoStr);
				Group aggregate = win.groups.get(winNo);
				List<Integer> attrsUsed = new ArrayList<Integer>(aggregate.keys.asList());
				List<RelFieldCollation> orderBy = aggregate.orderKeys.getFieldCollations();
				for(RelFieldCollation ref : orderBy) {
					int idx = ref.getFieldIndex();
					if(!attrsUsed.contains(idx)) {
						attrsUsed.add(new Integer(idx));
					}				
				}
				policy = getFieldPolicy(inSchema, new HashSet<Integer>(attrsUsed));
				secureFields.add(new SecureRelDataTypeField(field, policy));
					
			}
			else { // 1:1 mapping
				List<String> fieldNames = inSchema.getFieldNames();
				int srcIdx = fieldNames.indexOf(name);
				SecureRelDataTypeField srcField = inSchema.getSecureField(srcIdx);
				secureFields.add(new SecureRelDataTypeField(field, srcField));

			}
		}
	
		 return new SecureRelRecordType(outRow, secureFields);

	}

	private static SecureRelRecordType resolveAggregate(SecureRelNode aNode) {
		SecureRelRecordType inSchema = aNode.getChild(0).getSchema(); 
		LogicalAggregate agg = (LogicalAggregate) aNode.getRelNode();
		RelRecordType record = (RelRecordType) aNode.getRelNode().getRowType();
		List<SecureRelDataTypeField> secFields = new ArrayList<SecureRelDataTypeField>();
		
		Map<String, AggregateCall> aggMap = new HashMap<String, AggregateCall>();
		Map<String,SecureRelDataTypeField> scalarMap = new HashMap<String, SecureRelDataTypeField>();
		
		
		Iterator<Pair<AggregateCall, String>> aggItr = agg.getNamedAggCalls().iterator();
		while(aggItr.hasNext()) {
			Pair<AggregateCall, String> entry = aggItr.next();
			aggMap.put(entry.right, entry.left);
		}
		
		for(SecureRelDataTypeField inField : inSchema.getSecureFieldList()) {
			String name = inField.getName();
			scalarMap.put(name, inField);
		}

		for(RelDataTypeField field : record.getFieldList()) {
			String name = field.getName();
			if(scalarMap.containsKey(name)) {
				SecureRelDataTypeField prev = scalarMap.get(name);
				SecureRelDataTypeField secField = new SecureRelDataTypeField(field, prev);
				secFields.add(secField);
			}
			else if(aggMap.containsKey(name)) {
				SecurityPolicy policy = AttributeResolver.getAggPolicy(aggMap.get(name), inSchema);
				SecureRelDataTypeField secField = new SecureRelDataTypeField(field, policy);
				secFields.add(secField);
			}
		}
			
			return new SecureRelRecordType(record, secFields);

			
	}

	private static SecureRelRecordType resolveProjection(SecureRelNode aProjection) {

		LogicalProject projection = (LogicalProject) aProjection.getRelNode();
		SecureRelRecordType srcSchema = aProjection.getChild(0).getSchema();
		
		List<RexNode> rexNodes = projection.getChildExps();
		RelRecordType outRow = (RelRecordType) projection.getRowType();
		
		Iterator<RelDataTypeField> baseItr = outRow.getFieldList().iterator();

		List<SecureRelDataTypeField> secureFields = new ArrayList<SecureRelDataTypeField>();
		
		for(RexNode r : rexNodes) {
			SecureRelDataTypeField secField = resolveField(r, baseItr.next(), srcSchema);
			secureFields.add(secField);
		}
		
		return new SecureRelRecordType((RelRecordType) projection.getRowType(), secureFields);
	}
	
	public static SecureRelDataTypeField resolveField(RexNode rex, RelDataTypeField baseField, SecureRelRecordType inSchema) {
		final RelOptUtil.InputReferencedVisitor shuttle = new RelOptUtil.InputReferencedVisitor();
		rex.accept(shuttle);
		SortedSet<Integer> ordinalsAccessed = shuttle.inputPosReferenced;
		if(ordinalsAccessed.size() ==  1) { // can preserve stored source info
			return new SecureRelDataTypeField(baseField, inSchema.getSecureField(ordinalsAccessed.first()));
		}
		else {
			SecurityPolicy policy  = getFieldPolicy(inSchema, ordinalsAccessed);
		
			return new SecureRelDataTypeField(baseField, policy);
		}
	}


	// straight copy of permissions, accept any new aliases
	public static SecureRelRecordType copySchema(SecureRelNode aNode) {
		SecureRelRecordType srcSchema = aNode.getChild(0).getSchema();
		RelRecordType dstRecord = (RelRecordType) aNode.getRelNode().getRowType();
		
		List<SecureRelDataTypeField> dstFields = new ArrayList<SecureRelDataTypeField>();
		
		assert(srcSchema.getFieldCount() == dstRecord.getFieldCount());
		
		Iterator<RelDataTypeField> baseItr = dstRecord.getFieldList().iterator();
		Iterator<SecureRelDataTypeField> srcItr = srcSchema.getSecureFieldList().iterator();
		
		while(baseItr.hasNext()) {
			SecureRelDataTypeField secureField = new SecureRelDataTypeField(baseItr.next(), srcItr.next());
			dstFields.add(secureField);
		}

		if(aNode.getRelNode() instanceof LogicalFilter) {
			LogicalFilter filter = (LogicalFilter) aNode.getRelNode();
			for(int i = 0; i  < dstFields.size(); ++i)
				dstFields.get(i).addFilter(filter);
		}
		return new SecureRelRecordType(dstRecord, dstFields);
	}
	
	public static SecurityPolicy getFieldPolicy(SecureRelRecordType srcSchema, Set<Integer> ordinalsAccessed) {
		SecurityPolicy maxPolicy = SecurityPolicy.Public;
		
		for(Integer i : ordinalsAccessed) {
			SecureRelDataTypeField field = srcSchema.getSecureField(i);
			SecurityPolicy attrPolicy = field.getSecurityPolicy();
			if(attrPolicy.compareTo(maxPolicy) > 0) {
				maxPolicy = attrPolicy;
			}
		}
		return maxPolicy;
	}
	
	public static SecurityPolicy getAggPolicy(AggregateCall agg, SecureRelRecordType inSchema) {
		List<Integer> ordinalsAccessed = agg.getArgList();
		if(ordinalsAccessed.isEmpty()) {
			// get max over all attributes
			SecurityPolicy maxPolicy = SecurityPolicy.Public;
			for(SecureRelDataTypeField field : inSchema.getSecureFieldList()) {
				SecurityPolicy fieldPolicy = field.getSecurityPolicy();
				if(fieldPolicy.compareTo(maxPolicy) > 0) 
					maxPolicy = fieldPolicy;
			}
		
			return maxPolicy;
			
		}
		else 
			return getFieldPolicy(inSchema, new HashSet<Integer>(agg.getArgList()));
		
	}
	
	private static SecureRelRecordType resolveJoin(SecureRelNode aJoin) {
		
		List<SecureRelDataTypeField> secureFields = new ArrayList<SecureRelDataTypeField>();
		LogicalJoin join = (LogicalJoin) aJoin.getRelNode();
		SecureRelNode lhsChild = aJoin.getChild(0);
		SecureRelNode rhsChild = aJoin.getChild(1);
		
		
		SecureRelRecordType lhs = lhsChild.getSchema();
		SecureRelRecordType rhs = rhsChild.getSchema();
		
		RelRecordType baseType = (RelRecordType) join.getRowType();
		Iterator<RelDataTypeField> baseItr = baseType.getFieldList().iterator();
		
		for(SecureRelDataTypeField field : lhs.getSecureFieldList()) {
			RelDataTypeField dstField = baseItr.next();
			secureFields.add(new SecureRelDataTypeField(dstField, field));
		}
		
		for(SecureRelDataTypeField field : rhs.getSecureFieldList()) {
			RelDataTypeField dstField = baseItr.next();
			secureFields.add(new SecureRelDataTypeField(dstField, field));
		}
		
		
		return new SecureRelRecordType(baseType, secureFields);
		
	}
	
	public static SecureRelRecordType resolveScan(SecureRelNode aScan) throws Exception {
		List<SecureRelDataTypeField> secureFields = new ArrayList<SecureRelDataTypeField>();
		
		JdbcTableScan rel = (JdbcTableScan) aScan.getRelNode();
		RelRecordType record = (RelRecordType) rel.getRowType();
		String table = rel.getTable().getQualifiedName().get(0);
		SecureSchemaLookup permissions = SecureSchemaLookup.getInstance();
		
		for(RelDataTypeField field : record.getFieldList()) {
			String attr = field.getName();
			SecurityPolicy policy = permissions.getPolicy(table, attr);

			SecureRelDataTypeField secField = new SecureRelDataTypeField(field, policy, table, attr, null);
			secureFields.add(secField);
		}
		
		return new SecureRelRecordType(record, secureFields);
	
	}
		
	static boolean hasCall(LogicalProject p) {
		
		
		for(RexNode r : p.getChildExps())
			// something that requires recursion
			if((r instanceof RexOver) || (r instanceof RexSubQuery) || (r instanceof Window.RexWinAggCall))
				return true;
		
		return false;
	}
	
	public static List<SecureRelDataTypeField> getAttributes(RexNode aNode, SecureRelRecordType schema) {
		List<SecureRelDataTypeField> accessed = new ArrayList<SecureRelDataTypeField>();
		
		final RelOptUtil.InputReferencedVisitor shuttle = new RelOptUtil.InputReferencedVisitor();
		aNode.accept(shuttle);
		SortedSet<Integer> ordinalsAccessed = shuttle.inputPosReferenced;
		
		for(Integer i : ordinalsAccessed) {
			accessed.add(schema.getSecureField(i));
		}
		
		return accessed;

	}
}
