package org.smcql.executor.smc.io;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Party;
import com.oblivm.backend.gc.GCGenComp;
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.lang.inter.Util;
import com.oblivm.backend.oram.SecureArray;
import java.io.Serializable;
import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.Pair;
import org.smcql.config.SystemConfiguration;
import org.smcql.db.data.QueryTable;
import org.smcql.db.data.Tuple;
import org.smcql.executor.config.ConnectionManager;
import org.smcql.executor.plaintext.SqlQueryExecutor;
import org.smcql.executor.smc.BasicSecureQueryTable;
import org.smcql.executor.smc.ExecutionSegment;
import org.smcql.executor.smc.OperatorExecution;
import org.smcql.executor.smc.SecureBufferPool;
import org.smcql.executor.smc.SecureQueryTable;
import org.smcql.executor.smc.SlicedSecureQueryTable;
import org.smcql.executor.smc.runnable.SMCRunnable;
import org.smcql.util.SMCUtils;
import org.smcql.util.Utilities;

// for use in SMCQLRunnable$Generator, SMCQLRunnable$Evaluator
public class ArrayManager<T> implements Serializable {
	Map<OperatorExecution, SecureArray<T> > inputArrays;
	SecureBufferPool bufferPool;

	
	Tuple executingKey;
	Logger logger;
	
	public ArrayManager() throws Exception {
		inputArrays = new LinkedHashMap<OperatorExecution, SecureArray<T> >();
		bufferPool  = SecureBufferPool.getInstance();
		logger = SystemConfiguration.getInstance().getLogger();
	}
	
	
	@SuppressWarnings("unchecked")
	public SecureArray<T> getInput(OperatorExecution op, boolean isLhs, CompEnv<T> env, SMCRunnable parent) throws Exception {
	
		if(op == null)
			return null;

		OperatorExecution src = (isLhs) ? op.lhsChild : op.rhsChild;
		Party party = op.getParty();
		String workerId = op.getWorkerId();
		
		if(op.parentSegment != parent.getSegment()) {
			//throw new Exception("Mismatched operator " + op);
		}
		
	
		// local intermediate result
		if(inputArrays.containsKey(src)){
			return inputArrays.get(src);
		}
		
		
		if(src != null) {
			if(Utilities.isCTE(src)) {
				OperatorExecution cteSrc = src.lhsChild;
				if(inputArrays.containsKey(cteSrc)) 
					return inputArrays.get(cteSrc);
			}

			// intermediate result from another execution step
			SecureQueryTable table = bufferPool.readRecord(src.packageName, workerId, party);
			if(table != null) {
				return (SecureArray<T>) table.getSecureArray((CompEnv<GCSignal>)env, parent);
			}
		}
		
		if(op.getSourceSQL() != null) {
			if((isLhs && party == Party.Alice) || (!isLhs && party == Party.Bob)) {
				return prepareLocalPlainData(op, env, parent);
			}
			else {
				// retrieve local half of shared secret for Bob's input
		
				return SMCUtils.prepareRemotePlainArray(env, parent);

			}

		}
		
		

	return null;

	}
	
	


	public SlicedSecureQueryTable getSliceInputs(OperatorExecution op,  CompEnv<T> env, SMCRunnable parent, boolean isLhs) throws Exception {
		if(op == null)
			return null;

		OperatorExecution src = (isLhs) ? op.lhsChild : op.rhsChild;
		Party party = op.getParty();
		String workerId = op.getWorkerId();
		
		if(op.parentSegment != parent.getSegment()) {
			throw new Exception("Mismatched operator " + op);
		}
		
		if(src != null) {

			// intermediate result from another execution step
			SecureQueryTable table = bufferPool.readRecord(src.packageName, workerId, party);
			if(table != null) {
				return (SlicedSecureQueryTable) table;
			}
		}
		
		if(op.getSourceSQL() != null) {
			if((isLhs && party == Party.Alice) || (!isLhs && party == Party.Bob)) {
				// encode local secure query table
				SlicedSecureQueryTable local = new SlicedSecureQueryTable(op.outSchema, (CompEnv<GCSignal>) env, parent, false);
				QueryTable plainInput = queryIt(op);

				Map<Tuple, List<Tuple> > plainSlices = plainInput.asSlices(op.parentSegment.sliceSpec);
				
				int sliceCount = plainSlices.keySet().size();	
				parent.sendInt(sliceCount);
				for(Tuple key : plainSlices.keySet()) {
					BasicSecureQueryTable table = SMCUtils.prepareLocalPlaintext(key, plainSlices.get(key), env, parent);
					table.R = GCGenComp.R;
					table.schema = op.outSchema;
					local.slices.put(key, table);
				}
				
				local.bufferPoolKey = SecureBufferPool.getKey(op);
				local.resetSliceIterator();
				return local;
			}
			else {
				// retrieve local half of shared secret for Bob's input - collect remote
				// 
				SlicedSecureQueryTable remote = new SlicedSecureQueryTable(op.outSchema, (CompEnv<GCSignal>) env, parent, true);
				int sliceCount = parent.getInt();
				
				for(int i = 0; i < sliceCount; ++i) {
					Pair<Tuple, BasicSecureQueryTable> slice = SMCUtils.prepareRemoteSlicedPlaintext(env, parent);
					BasicSecureQueryTable table = slice.getRight();
					table.schema = op.outSchema;
					table.R = GCGenComp.R;			
					remote.slices.put(slice.getLeft(), table);
				}
				remote.resetSliceIterator();
				return remote;

			}

		}
		
		

		return null;

	}

	
	public void registerArray(OperatorExecution srcOp, SecureArray<T> arr, CompEnv<T> env, SMCRunnable parent) throws Exception {
		inputArrays.put(srcOp, arr);
		
		T[] dstArray = (arr == null) ? null : Util.secToIntArray(env, arr);
		T[] length = (arr == null) ? null : arr.getNonNullEntries();
		
		bufferPool.addArray(srcOp, (GCSignal[]) dstArray, (GCSignal[]) length, (CompEnv<GCSignal>) env, parent);
	}
	
	public void registerSlicedArray(OperatorExecution srcOp, SecureArray<T> arr, CompEnv<T> env, SMCRunnable parent, Tuple t) throws Exception {
		inputArrays.put(srcOp, arr);
		
		T[] dstArray = (arr == null) ? null : Util.secToIntArray(env, arr);
		T[] length = (arr == null) ? null : arr.getNonNullEntries();
		
		bufferPool.addArray(srcOp, (GCSignal[]) dstArray, (GCSignal[]) length, (CompEnv<GCSignal>) env, parent, t);
	}
	
	public boolean hasArray(OperatorExecution op, SMCRunnable parent) {
		ExecutionSegment segment = parent.getSegment(); // reference parent segment because sometimes we draw from other segments
		String workerId = segment.workerId;
		Party p = segment.party;
		
		if(inputArrays.containsKey(op)) {
			return true;
		}
		if(bufferPool.readRecord(op.packageName, workerId, p) != null) {
			return true;
		}
		
		return false;
	}
	



	public SecureArray<T> prepareLocalPlainData(OperatorExecution o, CompEnv<T> env, SMCRunnable parent) throws Exception {
		QueryTable table = queryIt(o);
		return SMCUtils.prepareLocalPlainArray(table, env, parent);
	}
	
	private QueryTable queryIt(OperatorExecution op) throws Exception {
		ConnectionManager cm = ConnectionManager.getInstance();
        Connection c = cm.getConnection(op.getWorkerId());
        double start = System.nanoTime();
        logger.info("For operator " + op.packageName + ", running plaintext query: " + op.getSourceSQL());
        
        QueryTable tupleData = SqlQueryExecutor.query(op.outSchema, op.getSourceSQL(), c);	
        
        String limitStr = SystemConfiguration.getInstance().getProperty("truncate-input");
        if(limitStr != null) {
        	int limit = Integer.parseInt(limitStr);
        	logger.info("Truncating to " + limit + " tuples.");
        	//  for testing
        	tupleData.truncateData(limit);
        }
        double end = System.nanoTime();
		double elapsed = (end - start) / 1e9;
		logger.info("Finished running plaintext for operator " + op.packageName + " in " + elapsed + " seconds.");
        return tupleData;

	}
	

	public SecureArray<T> sendAlice(boolean[] srcData, int tupleSize, CompEnv<T> env) throws Exception {

		int len = srcData.length;
		
		T[] inputA = env.inputOfAlice(srcData);
		env.flush();

		SecureArray<T> input = Util.intToSecArray(env, inputA, tupleSize, len / tupleSize);

		env.flush();
		return input;
		
	}
	
}
