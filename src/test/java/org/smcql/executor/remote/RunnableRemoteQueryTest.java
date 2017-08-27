package org.smcql.executor.remote;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smcql.BaseTest;
import org.smcql.codegen.QueryCompiler;
import org.smcql.config.SystemConfiguration;
import org.smcql.db.data.QueryTable;
import org.smcql.db.data.Tuple;
import org.smcql.db.data.field.CharField;
import org.smcql.db.data.field.IntField;
import org.smcql.executor.SMCQLQueryExecutor;
import org.smcql.executor.SegmentExecutor;
import org.smcql.executor.config.ConnectionManager;
import org.smcql.executor.config.WorkerConfiguration;
import org.smcql.executor.smc.ExecutionSegment;
import org.smcql.plan.SecureRelRoot;
import org.smcql.type.SecureRelRecordType;
import org.smcql.util.Utilities;


public class RunnableRemoteQueryTest extends BaseTest {
	public String aWorkerId = null;
	public String bWorkerId = null;
	
	SegmentExecutor plaintextRunner = null;
	
	Map<String, QueryTable> expectedOutput;
	Map<String, Double> plaintextTimes;
	
	
	protected void setUp() throws Exception {
		expectedOutput = new HashMap<String, QueryTable>();
		plaintextTimes = new HashMap<String, Double>();
		
		String setupFile = Utilities.getSMCQLRoot() + "/conf/setup.codd";
		System.setProperty("smcql.setup", setupFile);

		ConnectionManager cm = ConnectionManager.getInstance();
		List<WorkerConfiguration> workers = cm.getWorkerConfigurations();
		
		if(workers.size() >= 2) {
			aWorkerId = workers.get(0).workerId;
			bWorkerId = workers.get(1).workerId;

		}
		
		plaintextRunner = SegmentExecutor.getInstance();
	}
	
	/*private void setUpComorbidity(String testName) throws Exception {
		String sql = super.readSQL(testName);
		SecureRelRoot secRoot = new SecureRelRoot(testName, sql);
		QueryCompiler qc = new QueryCompiler(secRoot);

		SecureRelRecordType outSchema = qc.getRoot().getSchema();
		List<Tuple> output = new ArrayList<Tuple>();
		
		Tuple t0 = new Tuple();
		t0.addField(new CharField(outSchema.getSecureField(0), "008"));
		t0.addField(new IntField(outSchema.getSecureField(1), 4));
		output.add(t0);
		
		
		Tuple t1 = new Tuple();
		t1.addField(new CharField(outSchema.getSecureField(0), "410"));
		t1.addField(new IntField(outSchema.getSecureField(1), 1));
		output.add(t1);

		QueryTable result = new QueryTable(output);
		expectedOutput.put(testName, result);
	}*/


	private void runTest(String testName) throws Exception {
		SystemConfiguration.getInstance().resetCounters();
		Logger logger = SystemConfiguration.getInstance().getLogger();
		
		String sql = super.readSQL(testName);
		logger.log(Level.INFO, "Parsing " + sql);
		
		SecureRelRoot secRoot = new SecureRelRoot(testName, sql);
		
		if (testName.equals("cdiff"))
			sql = super.readSQL("postgres_cdiff");
		QueryCompiler qc = new QueryCompiler(secRoot, sql);
		
		List<ExecutionSegment> segments = qc.getSegments();
		ExecutionSegment segment = segments.get(0);
		logger.log(Level.INFO, "Segment has slice complement " + segment.sliceComplementSQL);
		logger.log(Level.INFO, "Segment has out schema " + segment.outSchema);
		logger.log(Level.INFO, "Have segment count " + segments.size());
		
		SMCQLQueryExecutor exec = new SMCQLQueryExecutor(qc, aWorkerId, bWorkerId);
		exec.run();
		
	    QueryTable results = exec.getOutput();
	    logger.log(Level.INFO, "output: " + results);
	    //assertEquals(expectedOutput.get(testName), results);
	}
	
	public void testComorbidity() throws Exception {
		String testName = "comorbidity";
		//setUpComorbidity(testName);
		runTest(testName);
	}

	public void testCDiff() throws Exception {
		String testName = "cdiff";
		//setUpCDiff(testName);
		runTest(testName);
	}
	
	public void testAspirinCount() throws Exception {
		String testName = "aspirin-count";
		//setUpComorbidity(testName);
		runTest(testName);
	}
}
