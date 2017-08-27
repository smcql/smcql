package org.smcql.codegen.sql;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.smcql.plan.SecureRelNode;
import org.smcql.plan.operator.Operator;

public class SqlGenerator {
	public static String getSql(RelRoot root, SqlDialect dialect) {
		return getSql(root.rel, dialect);
	}
	
	public static String getSql(RelNode rel, SqlDialect dialect) {	
		RelToSqlConverter converter = new ExtendedRelToSqlConverter(dialect);
		return getStringFromNode(rel, converter, dialect);
	}
	
	public static String getSourceSql(Operator node, SqlDialect dialect) {
		SecureRelNode secNode = node.getSecureRelNode();
		RelNode rel = secNode.getRelNode();
		RelToSqlConverter converter = new SecureRelToSqlConverter(dialect, secNode.getPhysicalNode());
		return getStringFromNode(rel, converter, dialect);
	}
	
	private static String getStringFromNode(RelNode rel, RelToSqlConverter converter, SqlDialect dialect) {
		SqlNode node = converter.visitChild(0, rel).asQuery();
		
		if (node instanceof SqlSelect && ((SqlSelect) node).getWhere() != null) {
			SqlNodeList list = ((SqlSelect) node).getSelectList();
			list.add(((SqlSelect) node).getWhere());
			((SqlSelect) node).setWhere(null);
			((SqlSelect) node).setSelectList(list);
		}
		
		String sqlOut = node.toSqlString(dialect).getSql();
		
		sqlOut = sqlOut.replace("\"", "");
		return sqlOut;	
	}
}
