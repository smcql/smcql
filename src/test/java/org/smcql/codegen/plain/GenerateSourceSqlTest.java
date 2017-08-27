package org.smcql.codegen.plain;

import java.util.HashMap;
import java.util.Map;

import org.smcql.BaseTest;
import org.smcql.codegen.QueryCompiler;
import org.smcql.config.SystemConfiguration;
import org.smcql.executor.step.ExecutionStep;
import org.smcql.plan.SecureRelRoot;

public class GenerateSourceSqlTest extends BaseTest {
	protected void setUp() throws Exception {
		super.setUp();
	}
	
	public void testAsprinCount() throws Exception {
		Map<String, String> expected = new HashMap<String, String>();
		expected.put("org.smcql.generated.aspirin_count.SeqScan4", "SELECT patient_id, timestamp_, LOWER(medication) LIKE '%aspirin%' FROM (SELECT patient_id, medication, timestamp_ FROM mi_cohort_medications) AS t ORDER BY patient_id, timestamp_");
		expected.put("org.smcql.generated.aspirin_count.SeqScan0", "SELECT patient_id, timestamp_, icd9 LIKE '414%' FROM (SELECT patient_id, icd9, timestamp_ FROM mi_cohort_diagnoses) AS t ORDER BY patient_id, timestamp_");
		runTest("aspirin-count", expected);		
	}
	
	public void testCDiff() throws Exception {		
		Map<String, String> expected = new HashMap<String, String>();
		expected.put("org.smcql.generated.cdiff.SeqScan0", "SELECT patient_id, timestamp_, icd9 = '008.45' FROM cdiff_cohort_diagnoses ORDER BY patient_id, timestamp_");
		runTest("cdiff", expected);
	}

	public void testComorbidity() throws Exception {		
		Map<String, String> expected = new HashMap<String, String>();
		expected.put("org.smcql.generated.comorbidity.Aggregate2", "SELECT major_icd9, COUNT(*) AS cnt FROM cdiff_cohort_diagnoses GROUP BY major_icd9 ORDER BY major_icd9");
		runTest("comorbidity", expected);
	}
	
	public void runTest(String testName, Map<String, String> expected) throws Exception {
		SystemConfiguration.getInstance().resetCounters();
		
		String sql = super.readSQL(testName);
		System.out.println("Parsing " + sql);
		
		SecureRelRoot secRoot = new SecureRelRoot(testName, sql);
		QueryCompiler qc = new QueryCompiler(secRoot, sql);
		
		Map<ExecutionStep, String> steps = qc.getSQLCode();
		assertEquals(expected.size(), steps.size());
		
		for (ExecutionStep step : steps.keySet()) {
			String packageName = step.getPackageName();
			String actualQuery = steps.get(step).replace("\n", " ").replace("\r", " ");
			assertEquals(expected.get(packageName), actualQuery);
		}
		
	}
}
