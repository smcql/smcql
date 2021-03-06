package org.smcql.executor.localhost;

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
import org.smcql.executor.smc.ExecutionSegment;
import org.smcql.plan.SecureRelRoot;
import org.smcql.type.SecureRelRecordType;

public class RunnableQueryTest extends BaseTest {
	public String aWorkerId = "testDB1";
	public String bWorkerId = "testDB2";
	
	Map<String, QueryTable> expectedOutput = new HashMap<String, QueryTable>();
		
	/* expect:
 	[008 , 4]
	[410 , 1]
	 */	
	public void testComorbidity() throws Exception {
		String testName = "comorbidity";
		setUpComorbidity(testName);
		executeTest(testName);		
	}
	
	private void setUpComorbidity(String testName) throws Exception {
		String sql = super.readSQL(testName);
		SecureRelRoot secRoot = new SecureRelRoot(testName, sql);
		QueryCompiler qc = new QueryCompiler(secRoot);

		SecureRelRecordType outSchema = qc.getRoot().getSchema();
		List<Tuple> output = new ArrayList<Tuple>();
		
		Tuple t0 = new Tuple();
		t0.addField(new CharField(outSchema.getSecureField(0), "414"));
		t0.addField(new IntField(outSchema.getSecureField(1), 8));
		output.add(t0);
		
		
		Tuple t1 = new Tuple();
		t1.addField(new CharField(outSchema.getSecureField(0), "008"));
		t1.addField(new IntField(outSchema.getSecureField(1), 8));
		output.add(t1);

		QueryTable result = new QueryTable(output);
		expectedOutput.put(testName, result);
	}

	/* expect: 
	 [1]
	 * */
	public void testCDiff() throws Exception {
		String testName = "cdiff";
		setUpCDiff(testName);
		executeTest(testName);
	}
	
	private void setUpCDiff(String testName) throws Exception {
		String sql = super.readSQL(testName);
		SecureRelRoot secRoot = new SecureRelRoot(testName, sql);
		QueryCompiler qc = new QueryCompiler(secRoot);

		SecureRelRecordType outSchema = qc.getRoot().getSchema();
		List<Tuple> output = new ArrayList<Tuple>();
		
		Tuple t0 = new Tuple();
		t0.addField(new IntField(outSchema.getSecureField(0), 1));
		output.add(t0);

		QueryTable result = new QueryTable(output);
		expectedOutput.put(testName, result);
	}

	/* expect: 
	 [3]
	 * */
	public void testAspirinRate() throws Exception {
		String testName = "aspirin-count";
		setUpAspirinRate(testName);
		executeTest(testName);
	}
	
	private void setUpAspirinRate(String testName) throws Exception {
		String sql = super.readSQL(testName);
		SecureRelRoot secRoot = new SecureRelRoot(testName, sql);
		QueryCompiler qc = new QueryCompiler(secRoot);

		SecureRelRecordType outSchema = qc.getRoot().getSchema();
		List<Tuple> output = new ArrayList<Tuple>();
		
		Tuple t0 = new Tuple();
		t0.addField(new IntField(outSchema.getSecureField(0), 3));
		output.add(t0);

		QueryTable result = new QueryTable(output);
		expectedOutput.put(testName, result);
	}

	
	public void executeTest(String testName) throws Exception {
		SystemConfiguration.getInstance().resetCounters();
		Logger logger = SystemConfiguration.getInstance().getLogger();
		
		String sql = super.readSQL(testName);
		logger.log(Level.INFO, "Parsing " + sql);
		
		SecureRelRoot secRoot = new SecureRelRoot(testName, sql);
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
	    assertEquals(expectedOutput.get(testName), results);
	}
}
