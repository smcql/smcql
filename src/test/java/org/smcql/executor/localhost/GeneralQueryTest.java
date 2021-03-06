package org.smcql.executor.localhost;

import org.smcql.BaseTest;
import org.smcql.codegen.QueryCompiler;
import org.smcql.config.SystemConfiguration;
import org.smcql.db.data.QueryTable;
import org.smcql.executor.SMCQLQueryExecutor;
import org.smcql.plan.SecureRelRoot;

public class GeneralQueryTest extends BaseTest {
	public String aWorkerId = "testDB1";
	public String bWorkerId = "testDB2";
	
	public void testCount() throws Exception {
		String testName = "count";
		String sql = "SELECT count(icd9) FROM cdiff_cohort_diagnoses";
		String expectedOutput = "(EXPR$0)\n[2]";
		executeTest(testName, sql, expectedOutput);		
	}
	
	public void testCountDistinct() throws Exception {
		String testName = "countDistinct";
		String sql = "SELECT count(DISTINCT icd9) FROM cdiff_cohort_diagnoses";
		String expectedOutput = "(EXPR$0)\n[2]";
		executeTest(testName, sql, expectedOutput);		
	}
	
	public void testSelectCount() throws Exception {
		String testName = "selectCount";
		String sql = "SELECT icd9, count(*) FROM cdiff_cohort_diagnoses GROUP BY icd9";
		String expectedOutput = "(icd9, EXPR$1)\n[008.45                          , 8]\n[414.01                          , 8]";
		executeTest(testName, sql, expectedOutput);		
	}
	
	public void testSelectPrivate() throws Exception {
		String testName = "selectPrivate";
		String sql = "SELECT site FROM cdiff_cohort_diagnoses";
		String expectedOutput = null;
		executeTest(testName, sql, expectedOutput);	
	}
	
	public void executeTest(String testName, String sql, String expectedOutput) throws Exception {
		SystemConfiguration.getInstance().resetCounters();
		
		SecureRelRoot secRoot = new SecureRelRoot(testName, sql);
		QueryCompiler qc = new QueryCompiler(secRoot, sql);
		
		SMCQLQueryExecutor exec = new SMCQLQueryExecutor(qc, aWorkerId, bWorkerId);
		exec.run();
		
	    QueryTable results = exec.getOutput();
	    System.out.println("output: " + results);
	    assertEquals(expectedOutput, (results == null) ? null : results.toString().trim());
	}
}
