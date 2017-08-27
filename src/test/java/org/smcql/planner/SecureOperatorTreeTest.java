package org.smcql.planner;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.calcite.plan.RelOptUtil;
import org.smcql.BaseTest;
import org.smcql.codegen.QueryCompiler;
import org.smcql.config.SystemConfiguration;
import org.smcql.executor.step.ExecutionStep;
import org.smcql.plan.SecureRelRoot;

public class SecureOperatorTreeTest extends BaseTest {
	
	public void testAspirinCount() throws Exception {
		String expectedTree = "LogicalAggregate-Slice, schema:(#0: rx_cnt BIGINT Public), slice key: [#0: patient_id INTEGER Public]\n"
				+ "    LogicalAggregate-Slice, schema:(#0: patient_id INTEGER Public), slice key: [#0: patient_id INTEGER Public]\n"
				+ "        LogicalJoin-Slice, schema:(#0: patient_id INTEGER Public,#1: timestamp_ TIMESTAMP(6) Private,#2: patient_id0 INTEGER Public,#3: timestamp_0 TIMESTAMP(6) Private), slice key: [#0: patient_id INTEGER Public, #2: patient_id0 INTEGER Public]\n"
				+ "            LogicalMerge-Secure\n"
				+ "                JdbcTableScan-Plain, schema:(#0: patient_id INTEGER Public,#1: site INTEGER Private,#2: year INTEGER Private,#3: month INTEGER Private,#4: visit_no INTEGER Public,#5: type_ INTEGER Private,#6: encounter_id INTEGER Private,#7: diag_src VARCHAR(2147483647) Protected,#8: icd9 VARCHAR(2147483647) Protected,#9: primary_ INTEGER Public,#10: timestamp_ TIMESTAMP(6) Private,#11: clean_icd9 VARCHAR(2147483647) Private,#12: major_icd9 VARCHAR(2147483647) Protected)\n"
				+ "            LogicalMerge-Secure\n"
				+ "                JdbcTableScan-Plain, schema:(#0: patient_id INTEGER Public,#1: site INTEGER Private,#2: year INTEGER Private,#3: month INTEGER Private,#4: medication VARCHAR(2147483647) Protected,#5: dosage VARCHAR(2147483647) Public,#6: route VARCHAR(2147483647) Public,#7: timestamp_ TIMESTAMP(6) Private)\n";

		String expectedSql = "SELECT patient_id, timestamp_, medication FROM mi_cohort_medications ORDER BY patient_id";
		
		runTest("aspirin-count", expectedTree, expectedSql);
	
	}
	
	public void testCDiff() throws Exception {
		String expectedTree = "LogicalAggregate-Slice, schema:(#0: patient_id INTEGER Public), slice key: [#0: patient_id INTEGER Public]\n"
				+ "    LogicalJoin-Slice, schema:(#0: patient_id INTEGER Public,#1: timestamp_ TIMESTAMP(6) Private,#2: w0$o0 INTEGER Private,#3: patient_id0 INTEGER Public,#4: timestamp_0 TIMESTAMP(6) Private,#5: w0$o00 INTEGER Private), slice key: [#0: patient_id INTEGER Public, #3: patient_id0 INTEGER Public]\n"
				+ "        LogicalWindow-Slice, schema:(#0: patient_id INTEGER Public,#1: timestamp_ TIMESTAMP(6) Private,#2: w0$o0 INTEGER Private), slice key: [#0: patient_id INTEGER Public]\n"
				+ "            LogicalMerge-Secure\n"
				+ "                JdbcTableScan-Plain, schema:(#0: patient_id INTEGER Public,#1: site INTEGER Private,#2: year INTEGER Private,#3: month INTEGER Private,#4: visit_no INTEGER Public,#5: type_ INTEGER Private,#6: encounter_id INTEGER Private,#7: diag_src VARCHAR(2147483647) Protected,#8: icd9 VARCHAR(2147483647) Protected,#9: primary_ INTEGER Public,#10: timestamp_ TIMESTAMP(6) Private,#11: clean_icd9 VARCHAR(2147483647) Private,#12: major_icd9 VARCHAR(2147483647) Protected)\n"
				+ "        LogicalWindow-Slice, schema:(#0: patient_id INTEGER Public,#1: timestamp_ TIMESTAMP(6) Private,#2: w0$o0 INTEGER Private), slice key: [#0: patient_id INTEGER Public]\n"
				+ "            LogicalMerge-Secure\n"
				+ "                JdbcTableScan-Plain, schema:(#0: patient_id INTEGER Public,#1: site INTEGER Private,#2: year INTEGER Private,#3: month INTEGER Private,#4: visit_no INTEGER Public,#5: type_ INTEGER Private,#6: encounter_id INTEGER Private,#7: diag_src VARCHAR(2147483647) Protected,#8: icd9 VARCHAR(2147483647) Protected,#9: primary_ INTEGER Public,#10: timestamp_ TIMESTAMP(6) Private,#11: clean_icd9 VARCHAR(2147483647) Private,#12: major_icd9 VARCHAR(2147483647) Protected)\n";
	
		String expectedSql = "SELECT patient_id, timestamp_, icd9 FROM cdiff_cohort_diagnoses ORDER BY patient_id, timestamp_";
		
		runTest("cdiff", expectedTree, expectedSql);
	}
	
	public void testComorbidity() throws Exception {
		String expectedTree = "LogicalSort-Secure, schema:(#0: major_icd9 VARCHAR(2147483647) Protected,#1: cnt BIGINT Protected)\n" 
				+ "    LogicalAggregate-Secure, schema:(#0: major_icd9 VARCHAR(2147483647) Protected,#1: cnt BIGINT Protected)\n"
				+ "        LogicalMerge-Secure\n"
				+ "            LogicalAggregate-Secure, schema:(#0: major_icd9 VARCHAR(2147483647) Protected,#1: cnt BIGINT Protected)\n";
		
		String expectedSql = "SELECT major_icd9, COUNT(*) FROM cdiff_cohort_diagnoses GROUP BY major_icd9 ORDER BY major_icd9";
		
		runTest("comorbidity", expectedTree, expectedSql);
	}
	

	void runTest(String testName, String expectedTree, String expectedSql) throws Exception {
		SystemConfiguration.getInstance().resetCounters();
		Logger logger = SystemConfiguration.getInstance().getLogger();

		String sql = super.readSQL(testName);
		logger.log(Level.INFO, "Parsing " + sql);
		SecureRelRoot secRoot = new SecureRelRoot(testName, sql);
	
		logger.log(Level.INFO, "Parsed " + RelOptUtil.toString(secRoot.getRelRoot().project()));
		
		QueryCompiler qc = new QueryCompiler(secRoot);
		ExecutionStep root = qc.getRoot();
		
		String testTree = root.printTree();
		logger.log(Level.INFO, "Resolved secure tree to:\n " + testTree);
		assertEquals(expectedTree, testTree);
	}
}
