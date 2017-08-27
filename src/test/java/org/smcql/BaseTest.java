package org.smcql;

import java.io.IOException;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.smcql.config.SystemConfiguration;
import org.smcql.executor.config.WorkerConfiguration;
import org.smcql.parser.SqlStatementParser;
import org.smcql.util.FileUtils;
import org.smcql.util.Utilities;

import junit.framework.TestCase;

public class BaseTest extends TestCase {
	protected SqlStatementParser parser;
	protected SqlNode root;
	protected RelRoot relRoot;
	protected SqlDialect dialect = SqlDialect.DatabaseProduct.POSTGRESQL.getDialect();
	protected String codePath = Utilities.getSMCQLRoot() + "/conf/workload/sql";
	protected WorkerConfiguration honestBroker;
	
	protected void setUp() throws Exception {
		System.setProperty("smcql.setup", Utilities.getSMCQLRoot() + "/conf/setup.localhost");

		parser = new SqlStatementParser();
		honestBroker = SystemConfiguration.getInstance().getHonestBrokerConfig();
		
	}
	
	protected String readSQL(String testName) throws IOException {
		String fileName = codePath + "/" + testName + ".sql";
		String sql = FileUtils.readSQL(fileName);
		return sql;

	}
	
	protected void dummyTest() {
		// gets rid of warnings
	}


}
