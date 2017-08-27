package org.smcql.executor.smc;

import java.io.Serializable;
import java.util.List;

import org.smcql.db.data.Tuple;
import org.smcql.type.SecureRelRecordType;
import org.smcql.executor.config.RunConfig;
import org.smcql.executor.config.RunConfig.ExecutionMode;
import org.smcql.plan.slice.SliceKeyDefinition;

import com.oblivm.backend.flexsc.Party;

// sequence of generated operators for execution (no sync points with other ops / hosts)
public class ExecutionSegment implements Serializable {

	public OperatorExecution rootNode;
	
	public SliceKeyDefinition sliceSpec;
	
	// replacing SMCConfig
	public RunConfig runConf;
	public String workerId;
	public Party party;
	
	// for sliced merge
	public SecureRelRecordType outSchema;

	public ExecutionMode executionMode;
	public boolean isPlanRoot = false;
	
	public String sliceComplementSQL = null;
	public List<Tuple> sliceValues;
	public List<Tuple> complementValues;
	
	
	public void checkInit() throws Exception {
		if(party == null || workerId == null) {
			throw new Exception("Parent segment uninitialized!");
		}
		checkInitHelper(rootNode);
	}

	public void checkInitHelper(OperatorExecution op) throws Exception {
		if(op == null || op.parentSegment != this) {
			return;
		}
		
		if(op.parentSegment == null) {
			throw new Exception("Segment uninitialized for " + op);
		}
		
		
		if(op.getWorkerId() == null || op.getParty() == null) {
			throw new Exception("Bad configuration for " + op);
		}
		
		checkInitHelper(op.lhsChild);
		checkInitHelper(op.rhsChild);
		
	}

	// for use after a single slice execution to prepare for next one
	public void resetOutput() {
		resetOutputHelper(rootNode);
	}
	
	private void resetOutputHelper(OperatorExecution op) {
		if(op == null || op.parentSegment != this)
			return;
		
		op.output = null;
		resetOutputHelper(op.lhsChild);
		resetOutputHelper(op.rhsChild);

		
	}
	
}
