package org.smcql.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.smcql.BaseTest;
import org.smcql.plan.SecureRelRoot;
import org.smcql.plan.operator.Operator;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.util.Utilities;

public class SliceKeyTest extends BaseTest {
	protected Map<String, List<SecureRelDataTypeField> > expectedSliceKeys;
	
	
	protected void setUp() throws Exception {
		String setupFile = Utilities.getSMCQLRoot() + "/conf/setup.localhost";
		System.setProperty("smcql.setup", setupFile);

		expectedSliceKeys  = new HashMap<String, List<SecureRelDataTypeField> >();
				
		setupAspirinCount();
		setupCDiff();
		setupComorbidity();
	}
	
	


	private void setupAspirinCount() throws Exception {
		
		String testName = "aspirin-count";
		List<SecureRelDataTypeField> expectedSliceKey = new ArrayList<SecureRelDataTypeField>();

		SecureRelDataTypeField srcAttribute = Utilities.lookUpAttribute("diagnoses", "patient_id");
		expectedSliceKey.add(srcAttribute);
	
		expectedSliceKeys.put(testName, expectedSliceKey);
	}
	

	private void setupCDiff() throws Exception {
		
		String testName = "cdiff";
		
		List<SecureRelDataTypeField> expectedSliceKey = new ArrayList<SecureRelDataTypeField>();

		
		SecureRelDataTypeField diagsAttr = Utilities.lookUpAttribute("diagnoses", "patient_id");
		expectedSliceKey.add(diagsAttr);

		
		expectedSliceKeys.put(testName, expectedSliceKey);
		
	}

	private void setupComorbidity() {
		String testName = "comorbidity";
		List<SecureRelDataTypeField> expectedSliceKey = new ArrayList<SecureRelDataTypeField>(); // icd9 is protected, so it cannot be used for  piecewise distributed execution		
		expectedSliceKeys.put(testName, expectedSliceKey);
		
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
		SecureRelRoot secRoot = new SecureRelRoot(testName, sql);
		Operator root = secRoot.getPlanRoot();
		
		List<SecureRelDataTypeField> sliceKeys = root.getSliceAttributes();
		List<SecureRelDataTypeField> expectedSliceKeysList = expectedSliceKeys.get(testName);
		
		System.out.println("Expected: " + expectedSliceKeysList);
		System.out.println("Observed: " + sliceKeys);
		
		assertEquals(expectedSliceKeysList.size(), sliceKeys.size());

		for(int i = 0; i < expectedSliceKeysList.size(); ++i) {
			assertTrue(expectedSliceKeysList.get(i).toString().equals(sliceKeys.get(i).toString()));
		}
	}
}
