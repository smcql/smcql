package org.smcql.executor.smc.runnable;

import org.smcql.db.data.Tuple;
import org.smcql.executor.smc.ExecutionSegment;
import org.smcql.executor.smc.OperatorExecution;
import org.smcql.executor.smc.SecureQueryTable;

public interface SMCRunnable {

	public void sendInt(int toSend);
	
	public void sendTuple(Tuple toSend);
		
	public int getInt();
	
	public Tuple getTuple();
	
	
	public ExecutionSegment getSegment();
	
	public SecureQueryTable getOutput();
	
	public OperatorExecution getRootOperator();
	
}
