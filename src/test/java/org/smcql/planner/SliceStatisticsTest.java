package org.smcql.planner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.smcql.db.data.Tuple;
import org.smcql.db.data.field.IntField;
import org.smcql.plan.execution.slice.statistics.SliceStatistics;
import org.smcql.plan.execution.slice.statistics.StatisticsCollector;
import org.smcql.plan.slice.SliceKeyDefinition;
import org.smcql.type.SecureRelDataTypeField;

public class SliceStatisticsTest extends SliceKeyTest {
	private Map<String, Map<SecureRelDataTypeField, SliceStatistics> > expectedStatistics; // (test, (key, values))
	private Map<String, String> expectedValues;
				
	protected void setUp() throws Exception {
		super.setUp();
		expectedStatistics = new HashMap<String, Map<SecureRelDataTypeField, SliceStatistics> >();
		expectedValues = new HashMap<String, String>();
	
		setupAspirintCount();
		setupCDiff();
		setupComorbidity();		
	}
	
	private void setupAspirintCount() throws Exception {
		
		String testName = "aspirin-count";
		
		
		Map<SecureRelDataTypeField, SliceStatistics> statistics = new HashMap<SecureRelDataTypeField, SliceStatistics>();
		expectedStatistics.put(testName + "-diagnoses", statistics);
		
		
		statistics = new HashMap<SecureRelDataTypeField, SliceStatistics>();
		expectedStatistics.put(testName + "-medications", statistics);
		expectedValues.put(testName, "");
	}
	

	protected void setupCDiff() throws Exception {
		
		final String testName = "cdiff";
		
		
		List<SecureRelDataTypeField> slices = expectedSliceKeys.get(testName);
		SecureRelDataTypeField slice = slices.get(0);
		
		final Map<SecureRelDataTypeField, SliceStatistics> statistics = new HashMap<SecureRelDataTypeField, SliceStatistics>();
		SliceStatistics attrStatistics = new SliceStatistics(new SliceKeyDefinition(Arrays.asList(slice)));
		
		statistics.put(slice, attrStatistics);
		
		expectedStatistics.put(testName + "-diagnoses", statistics);		
		
		expectedValues.put(testName + "-diagnoses", "Key: [#0: patient_id INTEGER Public]\n" + 
				"Single site values: <[3], (4, 2)> <[4], (4, 2)> <[5], (7, 2)> <[6], (7, 2)> \n" + 
				"Distributed values: <[1], [(4, 2), (7, 2)]> <[2], [(4, 2), (7, 2)]> ");
	}

	
	
	private void setupComorbidity() {
		final String testName = "comorbidity";
		
		final Map<SecureRelDataTypeField, SliceStatistics> statistics = new HashMap<SecureRelDataTypeField, SliceStatistics>();
		
		expectedStatistics.put(testName, statistics);
		expectedValues.put(testName, "");
	}



	
	@Test
	public void testAspirinCount() throws Exception {
			testCase("aspirin-count");
	}
	
	
	@Test
	public void testCDiff() throws Exception {
			testCase("cdiff");
	}

	@Test
	public void testComorbidity() throws Exception {
			testCase("comorbidity");
	}
	
	private void testCase(String testName) throws Exception {
		String sql = super.readSQL(testName);

		System.out.println("Running query " + sql);
		
		List<SecureRelDataTypeField> sliceKeys = expectedSliceKeys.get(testName);
		for(SecureRelDataTypeField key : sliceKeys) {
			SliceStatistics attrStatistics = StatisticsCollector.collect(new SliceKeyDefinition(Arrays.asList(key))); 
			
			
			String expected = expectedValues.get(testName + "-" + key.getStoredTable());
			System.out.println("Expected: " + expected);
			System.out.println("Observed: " + attrStatistics);
			assertEquals(attrStatistics.toString().trim(), expected.trim());
		}
		
	}
}