package org.smcql.runner;

import org.apache.calcite.sql.SqlDialect;
import org.smcql.codegen.QueryCompiler;
import org.smcql.config.SystemConfiguration;
import org.smcql.db.data.QueryTable;
import org.smcql.executor.SMCQLQueryExecutor;
import org.smcql.executor.config.WorkerConfiguration;
import org.smcql.parser.SqlStatementParser;
import org.smcql.plan.SecureRelRoot;
import org.smcql.util.Utilities;

public class SMCQLRunner {
	protected SqlDialect dialect = SqlDialect.DatabaseProduct.POSTGRESQL.getDialect();
	protected String codePath = Utilities.getSMCQLRoot() + "/conf/workload/sql";
	protected static WorkerConfiguration honestBroker;
	protected static SqlStatementParser parser;
	
	private static void setUp() throws Exception {
		System.setProperty("smcql.setup", Utilities.getSMCQLRoot() + "/conf/setup.localhost");

		parser = new SqlStatementParser();
		honestBroker = SystemConfiguration.getInstance().getHonestBrokerConfig();
		SystemConfiguration.getInstance().resetCounters();
	}
	
	public static void main(String[] args) throws Exception {
		setUp();

		String sql = args[0];
		System.out.println("\nQuery:\n" + sql);
		
		String aWorkerId = args[1];
		String bWorkerId = args[2];
		
		String testName = "userQuery";
		SecureRelRoot secRoot = new SecureRelRoot(testName, sql);
		QueryCompiler qc = new QueryCompiler(secRoot, sql);
		
		SMCQLQueryExecutor exec = new SMCQLQueryExecutor(qc, aWorkerId, bWorkerId);
		exec.run();
		
	    QueryTable results = exec.getOutput();
	    System.out.println("\nOutput:\n" + results);
	    System.exit(0);
	}

}
