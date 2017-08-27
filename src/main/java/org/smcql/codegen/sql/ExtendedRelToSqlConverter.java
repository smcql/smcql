package org.smcql.codegen.sql;

import java.util.ArrayList;

import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;

public class ExtendedRelToSqlConverter extends RelToSqlConverter {

	public ExtendedRelToSqlConverter(SqlDialect dialect) {
		super(dialect);
	}

	// fix bug in calcite where it omits distinct from SQL generation
	@Override
	public Result visit(Aggregate e) {
	
		if(e.getAggCallList().isEmpty()) {
			// it is a distinct
			 final Result x = visitChild(0, e.getInput());
			 SqlSelect select = x.asSelect();
			 // caution: this may interfere with other keywords.  
			 // Seems like the only other use for keyword is stream vs. relational, and that is N/A here.
			 SqlNodeList list = new SqlNodeList(select.getParserPosition());
			 list.add(SqlSelectKeyword.DISTINCT.symbol(select.getParserPosition()));
			 select.setOperand(0, list);
			 
			 final Builder builder = new Builder(e, new ArrayList<Clause>(), select, x.qualifiedContext());
			Result res = builder.result();
			return res;
		}
		
		return super.visit(e);
	}
}
