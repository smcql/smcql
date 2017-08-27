package org.smcql.executor;

import org.smcql.codegen.QueryCompiler;
import org.smcql.db.data.QueryTable;
import org.smcql.executor.smc.ExecutionSegment;
import org.smcql.executor.smc.SecureQueryTable;
import org.smcql.executor.step.ExecutionStep;
import org.smcql.executor.step.PlaintextStep;
import org.smcql.executor.step.SecureStep;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelDataTypeField.SecurityPolicy;
import org.smcql.util.SMCUtils;
import org.smcql.type.SecureRelRecordType;

import java.util.List;
import java.util.ListIterator;


// handles only 2 nodes, must contain at least one SecureStep in plan

public class SMCQLQueryExecutor implements Runnable {
	
	SegmentExecutor runner = null;
	QueryCompiler compiledPlan = null;
	private SecureRelRecordType lastSchema;
	private List<SecureQueryTable> lastOutput;
	private QueryTable plainOutput;
	
	public SMCQLQueryExecutor(QueryCompiler compiled, String aWorker, String bWorker) throws Exception {
		runner = new SegmentExecutor(aWorker, bWorker);
		compiledPlan = compiled;
	}
	
	
	public void run() {
		List<ExecutionSegment> segments = compiledPlan.getSegments();
		
		ExecutionStep root = compiledPlan.getRoot();
		
		for (SecureRelDataTypeField field : root.getExec().outSchema.getAttributes()) {
			if (field.getSecurityPolicy().equals(SecurityPolicy.Private)) {
				System.out.println("Exception: Private attribute " + field.getName() + " in out schema!");
				return;
			}
		}
		
		if(root instanceof PlaintextStep) {			
			try {
				plainOutput = runner.runPlain((PlaintextStep) root);
			} catch (Exception e) {
				System.out.println("Exception: No runnable execution step!");
			}
			return;
		}
		

		// iterate in reverse order to go bottom up
		ListIterator<ExecutionSegment> li = segments.listIterator(segments.size());

		try {
		
			while(li.hasPrevious()) { 
				ExecutionSegment segment = li.previous();
				 lastOutput = runner.runSecureSegment(segment);
				 lastSchema = segment.outSchema;
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public QueryTable getOutput() throws Exception {
		if (lastOutput == null && plainOutput == null)
			return null;
		
		if(compiledPlan.getRoot() instanceof PlaintextStep) {
			return plainOutput;
		}
		
		SecureQueryTable lhs = lastOutput.get(0);
		SecureQueryTable rhs = lastOutput.get(1);
		
		return lhs.declassify(rhs, lastSchema);		
		
	}
	
	
}
