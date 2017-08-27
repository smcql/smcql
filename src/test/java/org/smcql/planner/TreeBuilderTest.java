package org.smcql.planner;


import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.calcite.plan.RelOptUtil;
import org.smcql.BaseTest;
import org.smcql.config.SystemConfiguration;
import org.smcql.plan.SecureRelRoot;

public class TreeBuilderTest extends BaseTest {

	public void testAspirinCount() throws Exception {
		String expectedTree = "LogicalAggregate-Secure, schema:(#0: rx_cnt BIGINT Public)\n"
				 + "    LogicalAggregate-Slice, schema:(#0: patient_id INTEGER Public), slice key: [#0: patient_id INTEGER Public]\n"
				 + "        LogicalProject-Slice, schema:(#0: patient_id INTEGER Public), slice key: [#0: patient_id INTEGER Public]\n"
				 + "            LogicalJoin-Slice, schema:(#0: patient_id INTEGER Public,#1: timestamp_ TIMESTAMP(6) Private,#2: patient_id0 INTEGER Public,#3: timestamp_0 TIMESTAMP(6) Private), slice key: [#0: patient_id INTEGER Public, #2: patient_id0 INTEGER Public]\n"
				 + "                LogicalProject-Slice, schema:(#0: patient_id INTEGER Public,#1: timestamp_ TIMESTAMP(6) Private), slice key: [#0: patient_id INTEGER Public]\n"
				 + "                    LogicalFilter-Slice, schema:(#0: patient_id INTEGER Public,#1: icd9 VARCHAR(2147483647) Protected,#2: timestamp_ TIMESTAMP(6) Private), slice key: [#0: patient_id INTEGER Public]\n"
				 + "                        LogicalProject-Plain, schema:(#0: patient_id INTEGER Public,#1: icd9 VARCHAR(2147483647) Protected,#2: timestamp_ TIMESTAMP(6) Private)\n"
				 + "                            JdbcTableScan-Plain, schema:(#0: patient_id INTEGER Public,#1: site INTEGER Private,#2: year INTEGER Private,#3: month INTEGER Private,#4: visit_no INTEGER Public,#5: type_ INTEGER Private,#6: encounter_id INTEGER Private,#7: diag_src VARCHAR(2147483647) Protected,#8: icd9 VARCHAR(2147483647) Protected,#9: primary_ INTEGER Public,#10: timestamp_ TIMESTAMP(6) Private,#11: clean_icd9 VARCHAR(2147483647) Private,#12: major_icd9 VARCHAR(2147483647) Protected)\n"
				 + "                LogicalProject-Slice, schema:(#0: patient_id INTEGER Public,#1: timestamp_ TIMESTAMP(6) Private), slice key: [#0: patient_id INTEGER Public]\n"
				 + "                    LogicalFilter-Slice, schema:(#0: patient_id INTEGER Public,#1: medication VARCHAR(2147483647) Protected,#2: timestamp_ TIMESTAMP(6) Private), slice key: [#0: patient_id INTEGER Public]\n"
				 + "                        LogicalProject-Plain, schema:(#0: patient_id INTEGER Public,#1: medication VARCHAR(2147483647) Protected,#2: timestamp_ TIMESTAMP(6) Private)\n"
				 + "                            JdbcTableScan-Plain, schema:(#0: patient_id INTEGER Public,#1: site INTEGER Private,#2: year INTEGER Private,#3: month INTEGER Private,#4: medication VARCHAR(2147483647) Protected,#5: dosage VARCHAR(2147483647) Public,#6: route VARCHAR(2147483647) Public,#7: timestamp_ TIMESTAMP(6) Private)\n";

		runTest("aspirin-count", expectedTree);
	
	}
	
	public void testCDiff() throws Exception {
		String expectedTree = "LogicalAggregate-Slice, schema:(#0: patient_id INTEGER Public), slice key: [#0: patient_id INTEGER Public]\n"
				 + "    LogicalProject-Slice, schema:(#0: patient_id INTEGER Public), slice key: [#0: patient_id INTEGER Public]\n"
				 + "        LogicalJoin-Slice, schema:(#0: patient_id INTEGER Public,#1: timestamp_ TIMESTAMP(6) Private,#2: w0$o0 INTEGER Private,#3: patient_id0 INTEGER Public,#4: timestamp_0 TIMESTAMP(6) Private,#5: w0$o00 INTEGER Private), slice key: [#0: patient_id INTEGER Public, #3: patient_id0 INTEGER Public]\n"
				 + "            LogicalWindow-Slice, schema:(#0: patient_id INTEGER Public,#1: timestamp_ TIMESTAMP(6) Private,#2: w0$o0 INTEGER Private), slice key: [#0: patient_id INTEGER Public]\n"
				 + "                LogicalProject-Slice, schema:(#0: patient_id INTEGER Public,#1: timestamp_ TIMESTAMP(6) Private), slice key: [#0: patient_id INTEGER Public]\n"
				 + "                    LogicalFilter-Slice, schema:(#0: patient_id INTEGER Public,#1: site INTEGER Private,#2: year INTEGER Private,#3: month INTEGER Private,#4: visit_no INTEGER Public,#5: type_ INTEGER Private,#6: encounter_id INTEGER Private,#7: diag_src VARCHAR(2147483647) Protected,#8: icd9 VARCHAR(2147483647) Protected,#9: primary_ INTEGER Public,#10: timestamp_ TIMESTAMP(6) Private,#11: clean_icd9 VARCHAR(2147483647) Private,#12: major_icd9 VARCHAR(2147483647) Protected), slice key: [#0: patient_id INTEGER Public]\n"
				 + "                        JdbcTableScan-Plain, schema:(#0: patient_id INTEGER Public,#1: site INTEGER Private,#2: year INTEGER Private,#3: month INTEGER Private,#4: visit_no INTEGER Public,#5: type_ INTEGER Private,#6: encounter_id INTEGER Private,#7: diag_src VARCHAR(2147483647) Protected,#8: icd9 VARCHAR(2147483647) Protected,#9: primary_ INTEGER Public,#10: timestamp_ TIMESTAMP(6) Private,#11: clean_icd9 VARCHAR(2147483647) Private,#12: major_icd9 VARCHAR(2147483647) Protected)\n"
				 + "            LogicalWindow-Slice, schema:(#0: patient_id INTEGER Public,#1: timestamp_ TIMESTAMP(6) Private,#2: w0$o0 INTEGER Private), slice key: [#0: patient_id INTEGER Public]\n"
				 + "                LogicalProject-Slice, schema:(#0: patient_id INTEGER Public,#1: timestamp_ TIMESTAMP(6) Private), slice key: [#0: patient_id INTEGER Public]\n"
				 + "                    LogicalFilter-Slice, schema:(#0: patient_id INTEGER Public,#1: site INTEGER Private,#2: year INTEGER Private,#3: month INTEGER Private,#4: visit_no INTEGER Public,#5: type_ INTEGER Private,#6: encounter_id INTEGER Private,#7: diag_src VARCHAR(2147483647) Protected,#8: icd9 VARCHAR(2147483647) Protected,#9: primary_ INTEGER Public,#10: timestamp_ TIMESTAMP(6) Private,#11: clean_icd9 VARCHAR(2147483647) Private,#12: major_icd9 VARCHAR(2147483647) Protected), slice key: [#0: patient_id INTEGER Public]\n"
				 + "                        JdbcTableScan-Plain, schema:(#0: patient_id INTEGER Public,#1: site INTEGER Private,#2: year INTEGER Private,#3: month INTEGER Private,#4: visit_no INTEGER Public,#5: type_ INTEGER Private,#6: encounter_id INTEGER Private,#7: diag_src VARCHAR(2147483647) Protected,#8: icd9 VARCHAR(2147483647) Protected,#9: primary_ INTEGER Public,#10: timestamp_ TIMESTAMP(6) Private,#11: clean_icd9 VARCHAR(2147483647) Private,#12: major_icd9 VARCHAR(2147483647) Protected)\n";
		runTest("cdiff", expectedTree);
	}
	
	public void testComorbidity() throws Exception {
		
		String expectedTree = "LogicalSort-Secure, schema:(#0: major_icd9 VARCHAR(2147483647) Protected,#1: cnt BIGINT Protected)\n"
				 + "    LogicalAggregate-Secure, schema:(#0: major_icd9 VARCHAR(2147483647) Protected,#1: cnt BIGINT Protected)\n"
				 + "        LogicalProject-Plain, schema:(#0: major_icd9 VARCHAR(2147483647) Protected)\n"
				 + "            JdbcTableScan-Plain, schema:(#0: patient_id INTEGER Public,#1: site INTEGER Private,#2: year INTEGER Private,#3: month INTEGER Private,#4: visit_no INTEGER Public,#5: type_ INTEGER Private,#6: encounter_id INTEGER Private,#7: diag_src VARCHAR(2147483647) Protected,#8: icd9 VARCHAR(2147483647) Protected,#9: primary_ INTEGER Public,#10: timestamp_ TIMESTAMP(6) Private,#11: clean_icd9 VARCHAR(2147483647) Private,#12: major_icd9 VARCHAR(2147483647) Protected)\n";
		
		runTest("comorbidity", expectedTree);
	}
	

	void runTest(String testName, String expectedTree) throws Exception {
		Logger logger = SystemConfiguration.getInstance().getLogger();

		String sql = super.readSQL(testName);
		logger.log(Level.INFO, "Parsing " + sql);
		SecureRelRoot secRoot = new SecureRelRoot(testName, sql);
	
		logger.log(Level.INFO, "Parsed " + RelOptUtil.toString(secRoot.getRelRoot().project()));
		
		String testTree = secRoot.toString();
		logger.log(Level.INFO, "Resolved tree to:\n " + testTree);
		assertEquals(expectedTree, testTree);
	}
}
