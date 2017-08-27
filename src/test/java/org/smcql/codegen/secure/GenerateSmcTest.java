package org.smcql.codegen.secure;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smcql.BaseTest;
import org.smcql.codegen.QueryCompiler;
import org.smcql.codegen.smc.DynamicCompiler;
import org.smcql.config.SystemConfiguration;
import org.smcql.executor.step.ExecutionStep;
import org.smcql.plan.SecureRelRoot;
import org.smcql.util.Utilities;

public class GenerateSmcTest extends BaseTest {
	protected void setUp() throws Exception {
		String setupFile = Utilities.getSMCQLRoot() + "/conf/setup.localhost";
		System.setProperty("smcql.setup", setupFile);

	}
	
	public void testComorbidity() throws Exception {
		testCase("comorbidity");
	}
	

	public void testCDiff() throws Exception {
		testCase("cdiff");
	}

	public void testAspirinCount() throws Exception {
		testCase("aspirin-count");
	}
	
	
	protected void testCase(String testName) throws Exception {
		SystemConfiguration.getInstance().resetCounters();
		Logger logger = SystemConfiguration.getInstance().getLogger();
		String sql = super.readSQL(testName);
		logger.log(Level.INFO, "Parsing " + sql);
		
		SecureRelRoot secRoot = new SecureRelRoot(testName, sql);
		QueryCompiler qc = new QueryCompiler(secRoot);
		qc.writeToDisk();
		
		ExecutionStep root = qc.getRoot();
		String testTree = root.printTree();
		logger.log(Level.INFO, "Resolved secure tree to:\n " + testTree);
		
		testName = testName.replace("-", "_");
		String expectedDir = Utilities.getSMCQLRoot() + "/src/test/java/org/smcql/plan/operators/codegen/expected/" + testName;
		String observedDir = Utilities.getCodeGenTarget() + "/" + testName;
		assertEquals(true, Utilities.dirsEqual(expectedDir, observedDir));
	}
	
}
