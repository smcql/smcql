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
				
	protected void setUp() throws Exception {
		super.setUp();
		expectedStatistics = new HashMap<String, Map<SecureRelDataTypeField, SliceStatistics> >();
	
		setupAspirintCount();
		setupCDiff();
		setupComorbidity();		
	}
	
	private void setupAspirintCount() throws Exception {
		
		String testName = "aspirin-count";
		final List<SecureRelDataTypeField> expectedKeys = expectedSliceKeys.get(testName);
		SecureRelDataTypeField diagsSlice = expectedKeys.get(0);
		SecureRelDataTypeField medsSlice = expectedKeys.get(1);
		
		
		Map<SecureRelDataTypeField, SliceStatistics> statistics = new HashMap<SecureRelDataTypeField, SliceStatistics>();
		SliceStatistics diagnosisStatistics = new SliceStatistics(new SliceKeyDefinition(Arrays.asList(diagsSlice)));
		
		int[][] diagnosisOutcomes = {
				{ 3, 4, 2 },
				{ 4, 4, 2 },
				{ 5, 7, 2 },
				{ 6, 7, 2 },
				{ 1, 4, 2 },
				{ 1, 7, 2 },
				{ 2, 4, 2 },
				{ 2, 7, 2 }	
		};
		for(int i = 0; i < diagnosisOutcomes.length; ++i)  {
			Tuple t = createTuple(diagsSlice.getStoredAttribute(), diagnosisOutcomes[i]);
			diagnosisStatistics.addDataSource(t);
			
		}
		
		statistics.put(diagsSlice, diagnosisStatistics);
		expectedStatistics.put(testName + "-diagnoses", statistics);
		
		
		statistics = new HashMap<SecureRelDataTypeField, SliceStatistics>();
		SliceStatistics medicationStatistics = new SliceStatistics(new SliceKeyDefinition(Arrays.asList(medsSlice)));
		
		int[][] medicationOutcomes = {
				{ 1, 4, 1 },
				{ 3, 4, 1 },
				{ 5, 7, 1 },
				{ 6, 7, 1 }
		};
		
		for(int i = 0; i < medicationOutcomes.length; ++i)  {
			Tuple t = createTuple(medsSlice.getStoredAttribute(), medicationOutcomes[i]);
			medicationStatistics.addDataSource(t);
			
		}
		
		statistics.put(medsSlice, medicationStatistics);
		expectedStatistics.put(testName + "-medications", statistics);
	}
	

	protected void setupCDiff() throws Exception {
		
		final String testName = "cdiff";
		
		
		List<SecureRelDataTypeField> slices = expectedSliceKeys.get(testName);
		SecureRelDataTypeField slice = slices.get(0);
		
		final Map<SecureRelDataTypeField, SliceStatistics> statistics = new HashMap<SecureRelDataTypeField, SliceStatistics>();
		SliceStatistics attrStatistics = new SliceStatistics(new SliceKeyDefinition(Arrays.asList(slice)));

		int[][] outcomes = new int[][] {
				{ 1, 4, 2 },
				{ 2, 4, 2 },
				{ 2, 7, 2 },
				{ 1, 7, 2 },
				{ 3, 4, 2 },
				{ 4, 4, 2 },
				{ 5, 7, 2 },
				{ 6, 7, 2 }
				};

		for(int i = 0; i < outcomes.length; ++i)  {
			Tuple t = createTuple(slice.getStoredAttribute(), outcomes[i]);
			attrStatistics.addDataSource(t);
			
		}
		
		statistics.put(slice, attrStatistics);
		
		expectedStatistics.put(testName + "-diagnoses", statistics);		
	}

	
	
	private void setupComorbidity() {
		final String testName = "comorbidity";
		
		final Map<SecureRelDataTypeField, SliceStatistics> statistics = new HashMap<SecureRelDataTypeField, SliceStatistics>();
		
		expectedStatistics.put(testName, statistics);
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
			Map<SecureRelDataTypeField, SliceStatistics> expectedLookup = expectedStatistics.get(testName + "-" + key.getStoredTable());	
			SliceStatistics expected = expectedLookup.get(key);
			
			System.out.println("Expected: " + expected);
			System.out.println("Observed: " + attrStatistics);
			
			assertTrue(attrStatistics.toString().equals(expected.toString()));	
		}
		
	}
	
	// create a tuple with k int fields
	private Tuple createTuple(String src, int[] fields) {
		Tuple t = new Tuple();
		
		SecureRelDataTypeField sPrime = new SecureRelDataTypeField(src, 0, null);
		for(int i = 0; i < fields.length; ++i) {
			IntField intField = new IntField(sPrime, fields[i]);
			t.addField(intField);
		}
		
		return t;
	}
}