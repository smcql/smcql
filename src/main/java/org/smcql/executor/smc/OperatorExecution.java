package org.smcql.executor.smc;

import java.io.Serializable;
import org.smcql.config.SystemConfiguration;
import org.smcql.type.SecureRelRecordType;
import org.smcql.executor.step.ExecutionStep;
import org.smcql.executor.step.PlaintextStep;
import org.smcql.executor.step.SecureStep;
import org.smcql.util.Utilities;

import com.oblivm.backend.flexsc.Party;
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.oram.SecureArray;


// basically a stripped down version of ExecutionStep for serialization
public class OperatorExecution implements Comparable<OperatorExecution>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6565213527478140219L;
	public String packageName;
	public SecureRelRecordType outSchema;
	public OperatorExecution parent = null;
	public OperatorExecution lhsChild, rhsChild;  // may be root of another segment
	public ExecutionSegment parentSegment = null; // pointer to segment for SMCConfig 
	public byte[] byteCode; // compiled .class for this step

	 // for merge case
	String sourceSQL = null;
	public transient SecureArray<GCSignal> output; // optional - for passing around data w/in segment
	
	
	public OperatorExecution() {
		
	}
	
	
	public OperatorExecution(SecureStep s) {
		packageName = s.getPackageName();
		outSchema = s.getSchema(); //change this
		
		
		lhsChild = getChild(s, 0);
		rhsChild = getChild(s, 1);
		
		if(lhsChild != null) 
			lhsChild.parent = this;
		
		if(rhsChild != null) 
			rhsChild.parent = this;
		
		try {
			byteCode =  Utilities.readGeneratedClassFile(packageName);
		} catch(Exception e) {
			// do nothing
		}
		
	}
	
	
	public OperatorExecution(PlaintextStep s) {
		packageName = s.getPackageName();
		outSchema = s.getSchema();
		
		
		lhsChild = getChild(s, 0);
		rhsChild = getChild(s, 1);
		
		if(lhsChild != null) 
			lhsChild.parent = this;
		
		if(rhsChild != null) 
			rhsChild.parent = this;
	}


	private OperatorExecution getChild(ExecutionStep src, int idx) {
		ExecutionStep e = src.getChild(idx);
		if(e instanceof SecureStep)
			return ((SecureStep) e).getExec();
		return null;
	}
	@Override
	public int compareTo(OperatorExecution o) {
		return this.packageName.compareTo(o.packageName);
	}
	
	
	public Party getParty() {
		if(parentSegment != null)
			return parentSegment.party;
		return null;
	}
	
	public String getWorkerId() {
		if(parentSegment != null) 
			return parentSegment.workerId;
		return null;
	}

	@Override
	public String toString() {
		String lhsChildStr = (lhsChild != null) ? lhsChild.packageName : "null";
		String rhsChildStr = (rhsChild != null) ? rhsChild.packageName : "null";

		String ret = packageName + "(" + lhsChildStr + "," + rhsChildStr + ") " + getParty() + ", " + getWorkerId();
		if(getSourceSQL() != null)
			ret += "source: " + getSourceSQL();
		
		return ret;
	}


	public String getSourceSQL() {
		return sourceSQL;
	}


	public void setSourceSQL(String sourceSql) throws Exception {
		this.sourceSQL = sourceSql;
	}
	
}
