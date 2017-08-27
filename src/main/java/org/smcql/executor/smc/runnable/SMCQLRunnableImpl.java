package org.smcql.executor.smc.runnable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.smcql.codegen.smc.DynamicCompiler;
import org.smcql.config.SystemConfiguration;
import org.smcql.db.data.QueryTable;
import org.smcql.db.data.Tuple;
import org.smcql.executor.config.RunConfig.ExecutionMode;
import org.smcql.executor.plaintext.SqlQueryExecutor;
import org.smcql.executor.smc.BasicSecureQueryTable;
import org.smcql.executor.smc.ExecutionSegment;
import org.smcql.executor.smc.OperatorExecution;
import org.smcql.executor.smc.SecureBufferPool;
import org.smcql.executor.smc.SecureQueryTable;
import org.smcql.executor.smc.SlicedSecureQueryTable;
import org.smcql.executor.smc.io.ArrayManager;
import org.smcql.util.Utilities;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.lang.inter.ISecureRunnable;
import com.oblivm.backend.lang.inter.Util;
import com.oblivm.backend.oram.SecureArray;

// place for methods entirely duplicated by gen and eva
public class SMCQLRunnableImpl<T> implements Serializable {

	ExecutionSegment runSpec;
	ArrayManager<T> dataManager;
	boolean sliceEnabled = true;
	Map<String, SlicedSecureQueryTable> sliceInputs;
	Map<String, Double> perfReport;
	
	// single slice key / segment
	SlicedSecureQueryTable sliceOutput;
	boolean slicedExecution = true;
	boolean semijoinExecution = true;
	Tuple executingSliceValue = null; // all slices move in lockstep, so we need only one
	SMCRunnable parent;
	SecureQueryTable lastOutput = null;
	Logger logger; 
	
	
	public SMCQLRunnableImpl(ExecutionSegment segment, SMCRunnable runnable) throws Exception {
		runSpec = segment;
		dataManager = new ArrayManager<T>();
		perfReport = new HashMap<String, Double>();
		
		logger = SystemConfiguration.getInstance().getLogger();
		
		try {
			slicedExecution = SystemConfiguration.getInstance().getProperty("sliced-execution").equals("true");
			semijoinExecution = SystemConfiguration.getInstance().getProperty("semijoin-execution").equals("true");
		} catch (Exception e) {
		}
		
		slicedExecution = slicedExecution && runSpec.rootNode.parentSegment.executionMode == ExecutionMode.Slice;

		if(slicedExecution) {
			sliceInputs = new HashMap<String, SlicedSecureQueryTable>();
		}
		
		logger.info("Running in slicedExecution=" + slicedExecution + " mode.");

		parent = runnable;
	}
	
	public static String getKey(OperatorExecution opEx, boolean isLhs) {
		String ret = opEx.packageName;
		ret += isLhs ? "-lhs" : "-rhs";
		return ret;
	}

	
	@SuppressWarnings("unchecked")
	public void secureCompute(CompEnv<T> env) throws Exception {
		if(slicedExecution) {
			sliceOutput = new SlicedSecureQueryTable(runSpec.rootNode, (CompEnv<GCSignal>) env, parent);
			List<Tuple> sliceVals = runSpec.sliceValues;
			for(Tuple t : sliceVals) {
				runSpec.resetOutput();
				executingSliceValue = t;

				SecureArray<T> output = runOneSliced(runSpec.rootNode, env); 

				if(output != null) {
					GCSignal[] payload = (GCSignal[]) Util.secToIntArray(env, output);
					GCSignal[] nonNulls = (GCSignal[]) output.getNonNullEntries();
					sliceOutput.addSlice(executingSliceValue, payload, nonNulls);
				}
			}
		
			lastOutput = sliceOutput;
			SecureBufferPool.getInstance().addArray(runSpec.rootNode, sliceOutput); // bypass flattening it until we need to
		} else  {
			SecureArray<T> secResult = runOne(runSpec.rootNode, env);
			GCSignal[] payload = (GCSignal[]) Util.secToIntArray(env, secResult);
			GCSignal[] nonNulls = (GCSignal[]) secResult.getNonNullEntries();
			
			BasicSecureQueryTable output = new BasicSecureQueryTable(payload, nonNulls, runSpec.rootNode.outSchema, (CompEnv<GCSignal>) env, parent);
			lastOutput = output;
		}
	}
	
	boolean slicesRemain() {
		if(sliceInputs.isEmpty())  // no runs yet
			return true;
		
		SlicedSecureQueryTable firstOne = (SlicedSecureQueryTable) sliceInputs.values().toArray()[0];
		if(firstOne.hasNext())
			return true;
		
		return false;
	}

	private String addSlicePredicate(String query) {
		String key = "";
		try {
			key = runSpec.sliceSpec.getAttributes().get(0).getName();
		} catch (Exception e) {
			return query;
		}
		
		int orderByIndex = query.lastIndexOf("ORDER BY");
		String result = (orderByIndex == -1) ? query: query.substring(0, orderByIndex);
		String orderBy = (orderByIndex == -1) ? "": query.substring(orderByIndex);
	
		String predicate = "(";
		for (int i=0; i < runSpec.sliceValues.size(); i++) {
			Tuple t = runSpec.sliceValues.get(i);
			String val = t.getField(0).toString();
			
			if (i > 0) 
				predicate += ", "; 
			
			predicate += val;
		}
		predicate += ")";
		
		return (predicate.equals("()")) ? null : result + " WHERE " + key + " IN " + predicate + " " + orderBy;
	}
	
	// returns output of run		
	@SuppressWarnings("unchecked")
	SecureArray<T> runOne(OperatorExecution op, CompEnv<T> env) throws Exception {

		if(op == null || (op.parentSegment != runSpec)) // != runspec implies that child was computed in another segment
			return null;

		// already exec'd cte
		if(op.output != null) {
			return (SecureArray<T>) op.output;
		}
		
		if(Utilities.isCTE(op)) { // skip ctes
			return runOne(op.lhsChild, env);
		}

		if (semijoinExecution && op.getSourceSQL() != null) {
			op.setSourceSQL(addSlicePredicate(op.getSourceSQL()));
		}
		
		SecureArray<T> lhs = runOne(op.lhsChild, env);
		if(lhs == null) { // get input from outside execution segment
			long start = System.nanoTime();
			lhs = dataManager.getInput(op, true, env, parent);
			long end = System.nanoTime();
			logger.info("Loaded lhs data in " + (end - start) / 1e9  + " seconds.");
		}
		SecureArray<T> rhs = runOne(op.rhsChild, env);

		if(rhs == null)  {
			long start = System.nanoTime();
			rhs = dataManager.getInput(op, false, env, parent);
			long end = System.nanoTime();
			logger.info("Loaded rhs data in " + (end - start) / 1e9  + " seconds.");

		}
		
		double start = System.nanoTime();
		ISecureRunnable<T> runnable = DynamicCompiler.loadClass(op.packageName, op.byteCode, env);
		int rhsLength = (rhs != null) ? rhs.length : 0;
		int lhsLength = (lhs != null) ? lhs.length : 0;
		String msg =  "Operator " + op.packageName + " started at " + Utilities.getTime() + " on " + lhsLength + "," + rhsLength  + " tuples.";
		logger.info(msg);
		
		SecureArray<T> secResult = null;
		if(Utilities.isMerge(op) && (lhs == null || rhs == null)) { // applies for both null too
			secResult = (lhs == null) ? rhs : lhs;
		}
		else {			
			secResult = runnable.run(lhs, rhs);
			if(secResult == null) 
				throw new Exception("Null result for " + op.packageName);

			if(secResult.getNonNullEntries() == null) {
				T[] prevEntries = lhs.getNonNullEntries();
				secResult.setNonNullEntries(prevEntries);
			}
		}
		double end = System.nanoTime();
		double elapsed = (end - start) / 1e9;
		
		msg = "Operator ended at " + Utilities.getTime() + " it ran in " + op.packageName + " ran in " + elapsed + " seconds, output=" + secResult;
		logger.info(msg);

		// sum for slices
		if(perfReport.containsKey(op.packageName)) {
			double oldSum = perfReport.get(op.packageName);
			
			perfReport.put(op.packageName, oldSum + elapsed);
		}
		else 
			perfReport.put(op.packageName, elapsed);
		
		dataManager.registerArray(op, secResult, env, parent);
		op.output = (SecureArray<GCSignal>) secResult;
		return secResult;
		
	}
	
	
	
	@SuppressWarnings("unchecked")
	SecureArray<T> runOneSliced(OperatorExecution op, CompEnv<T> env) throws Exception {
		if(op == null || op.parentSegment != runSpec) // != runspec implies that child was computed in another segment, pull this in with parent
			return null;

		// already exec'd cte
		if(op.output != null) {
			return (SecureArray<T>) op.output;
		}
		
		if(Utilities.isCTE(op)) { // skip ctes
			return runOneSliced(op.lhsChild, env);
		}

		
		SecureArray<T> lhs = runOneSliced(op.lhsChild, env);
		if(lhs == null) { // get input from outside execution segment
				String key = getKey(op, true);
				if(!sliceInputs.containsKey(key))  {
					SlicedSecureQueryTable allSlices = dataManager.getSliceInputs(op, env, parent, true);					
					sliceInputs.put(key, allSlices);
				}
				SlicedSecureQueryTable srcTable = sliceInputs.get(key);
				if (srcTable != null)
					lhs = (SecureArray<T>) srcTable.getSlice(executingSliceValue, (CompEnv<GCSignal>) env);
				 				
		}
	
		
		SecureArray<T> rhs = runOneSliced(op.rhsChild, env);

		if(rhs == null)  {
				String key = getKey(op, false);
				if(!sliceInputs.containsKey(key))  {
				
					SlicedSecureQueryTable allSlices = dataManager.getSliceInputs(op, env, parent, false);
					if(allSlices != null)
						sliceInputs.put(key, allSlices);
				}
				SlicedSecureQueryTable srcTable = sliceInputs.get(key);
				if (srcTable != null) 
					rhs = (SecureArray<T>) srcTable.getSlice(executingSliceValue, (CompEnv<GCSignal>) env);
		}

		if (rhs == null && lhs == null)
			return null;

		double start = System.nanoTime();
		SecureArray<T> secResult = null;
		if(Utilities.isMerge(op)) {
			secResult = mergeRun(op, env, lhs, rhs);
		}
		else
			secResult = slicedRun(op, env, lhs, rhs);
		
		double end = System.nanoTime();
		double elapsed = (end - start) / 1e9;

		logger.info("Operator ended at " + Utilities.getTime() + " it ran in " + op.packageName + " ran in " + elapsed + " seconds.");

		if(perfReport.containsKey(op.packageName)) {
			double oldSum = perfReport.get(op.packageName);
			
			perfReport.put(op.packageName, oldSum + elapsed);
		}
		else 
			perfReport.put(op.packageName, elapsed);
		
		return secResult;
		

	}
	
	
	private SecureArray<T> mergeRun(OperatorExecution op, CompEnv<T> env, SecureArray<T> lhs, SecureArray<T> rhs) throws Exception {
		if(lhs != null && rhs != null) {
			return slicedRun(op, env, lhs, rhs);
		}
		if(lhs != null) {
			return lhs;
		}
		if(rhs != null) {

			return rhs;
		}
		throw new Exception("Operator " + op.packageName + " has no input!");
	}
	
	@SuppressWarnings("unchecked")
	private SecureArray<T> slicedRun(OperatorExecution op, CompEnv<T> env, SecureArray<T> lhs, SecureArray<T> rhs) throws Exception {
		ISecureRunnable<T> runnable = DynamicCompiler.loadClass(op.packageName, op.byteCode, env);
		
		int lhsLength = lhs != null ? lhs.length: 0;
		int rhsLength = rhs != null ? rhs.length : 0;
		String msg =  "Operator " + op.packageName + " started at " + Utilities.getTime() + " on " + lhsLength + "," + rhsLength  + " tuples.";
		logger.info(msg);
	
		SecureArray<T> secResult = null;
		if (op.packageName.contains("Join") && (lhsLength == 0 || rhsLength == 0)) {
			secResult = (lhsLength == 0) ? runnable.run(rhs, rhs) : runnable.run(lhs, lhs);
			secResult = null;
		} else {
			secResult = runnable.run(lhs, rhs);
		}
		
		if(secResult != null && secResult.getNonNullEntries() == null) {
			T[] prevEntries = lhs.getNonNullEntries();
			secResult.setNonNullEntries(prevEntries);
		}

		dataManager.registerSlicedArray(op, secResult, env, parent, this.executingSliceValue);
		op.output = (SecureArray<GCSignal>) secResult;
		return secResult;
	}
	
	public void prepareOutput(CompEnv<T> env) throws Exception {		
		if(runSpec.sliceComplementSQL != null && !runSpec.sliceComplementSQL.isEmpty() && semijoinExecution) {
			 QueryTable plainOut = SqlQueryExecutor.query(runSpec.sliceComplementSQL, runSpec.outSchema, runSpec.workerId);
			 lastOutput.setPlaintextOutput(plainOut);
			 SecureBufferPool.getInstance().addArray(runSpec.rootNode, lastOutput);
		}
		
		
		logger.info("Finished prepare output for  " + runSpec.rootNode.packageName + " at " + Utilities.getTime());
		logger.info("Perf times " + perfReport);

	}
	
	public SecureQueryTable getOutput() {
		return lastOutput;
	}
}
