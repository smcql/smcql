package org.smcql.parser;

import java.io.IOException;
import java.sql.SQLException;
import org.smcql.BaseTest;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

public class ParseTest  extends  BaseTest {

	
	public void testComorbidity() throws ClassNotFoundException, SQLException, SqlParseException, ValidationException, RelConversionException, IOException {
		runTest("comorbidity");
		
	}
	
	public void testCDiff() throws ClassNotFoundException, SQLException, SqlParseException, ValidationException, RelConversionException, IOException {
			runTest("cdiff");
			
	}
		
	
	public void testAsprinRate() throws ClassNotFoundException, SQLException, SqlParseException, ValidationException, RelConversionException, IOException {
		runTest("aspirin-count");
	}

	
	public void runTest(String testName) throws IOException, ClassNotFoundException, SQLException, SqlParseException, ValidationException, RelConversionException {
		String sql = super.readSQL(testName);
		System.out.println("Parsing " + sql);

		runParse(sql);
		System.out.println("***********************************");

	}
		
		
	public void runParse(String sql) throws ClassNotFoundException, SQLException, SqlParseException, ValidationException, RelConversionException {

		root = parser.parseSQL(sql);
		System.out.println("Parsed " + root);
		
		System.out.println("Root type " + root.getClass());
		
		
		
		relRoot = parser.compile(root);
		relRoot = parser.optimize(relRoot);
		
		System.out.println("Relational root " + relRoot);

		System.out.println("Out schema " + relRoot.project().getRowType());
		System.out.println("Relational tree " + RelOptUtil.toString(relRoot.project(), SqlExplainLevel.ALL_ATTRIBUTES));
		RelRecordType schema = (RelRecordType) relRoot.project().getRowType();
		System.out.println("Type dump " + RelOptUtil.dumpType(schema));
		
		
	}

	
	
	

	
		
}
