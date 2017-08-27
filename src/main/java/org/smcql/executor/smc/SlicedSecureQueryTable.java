package org.smcql.executor.smc;

import java.io.Serializable;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.smcql.db.data.QueryTable;
import org.smcql.db.data.Tuple;
import org.smcql.type.SecureRelRecordType;
import org.smcql.executor.config.ConnectionManager;
import org.smcql.executor.plaintext.SqlQueryExecutor;
import org.smcql.executor.smc.io.SecureOutputReader;
import org.smcql.executor.smc.merge.SecureMerge;
import org.smcql.executor.smc.merge.SecureMergeFactory;
import org.smcql.executor.smc.runnable.SMCRunnable;
import org.smcql.plan.slice.SliceKeyDefinition;
import org.smcql.util.SMCUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.oblivm.backend.circuits.arithmetic.IntegerLib;
import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Party;
import com.oblivm.backend.gc.GCGenComp;
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.lang.inter.Util;
import com.oblivm.backend.oram.SecureArray;
import com.oblivm.backend.util.Utils;

public class SlicedSecureQueryTable implements SecureQueryTable, Serializable {

	public Map<Tuple, BasicSecureQueryTable> slices;
	public String bufferPoolKey;
	SliceKeyDefinition sliceKey;
	public Party party; 
	transient public SecureRelRecordType schema;
	public GCSignal R;
	transient CompEnv<GCSignal> env;
	transient SMCRunnable parent;
	transient Iterator<Entry<Tuple, BasicSecureQueryTable>> sliceItr;
	QueryTable plaintextOutput;
	protected BasicSecureQueryTable aPlaintext, bPlaintext; // this will be skipped by sliceItr, but included in declassify and getSecureArray
	boolean merged = false;
	transient SecureArray<GCSignal> output = null;
	boolean written = false; // for after output is initialized 
	SecureMerge merger;
	
	
	public SlicedSecureQueryTable(OperatorExecution op, CompEnv<GCSignal> e, SMCRunnable p, boolean isRemote) throws Exception {
		env = e;
		parent = p;
		schema = op.outSchema;
		R = GCGenComp.R;
		party = env.party;
		slices = new LinkedHashMap<Tuple, BasicSecureQueryTable>();
		merger = SecureMergeFactory.get(op);
		
		bufferPoolKey = SecureBufferPool.getKey(op);

		if(isRemote) {
			initializeRemotePlaintext();
		}
		else {
			initializeLocalPlaintext();
		}
		
		sliceItr = slices.entrySet().iterator(); 

		
		
	}
	
	public SlicedSecureQueryTable(SecureRelRecordType opSchema, CompEnv<GCSignal> e, SMCRunnable p, boolean isRemote) throws Exception {
		env = e;
		parent = p;
		schema = opSchema;
		R = GCGenComp.R;
		party = env.party;
		slices = new LinkedHashMap<Tuple, BasicSecureQueryTable>();
		OperatorExecution op = parent.getRootOperator();
		merger = SecureMergeFactory.get(op);
		
		bufferPoolKey = SecureBufferPool.getKey(op);
		
		sliceItr = slices.entrySet().iterator(); 
	}
	
	public SlicedSecureQueryTable(OperatorExecution op, CompEnv<GCSignal> e, SMCRunnable p, BasicSecureQueryTable slice, Tuple t) throws Exception {
		env = e;
		parent = p;
		schema = op.outSchema;
		R = GCGenComp.R;
		party = env.party;
		slices = new LinkedHashMap<Tuple, BasicSecureQueryTable>();
		slices.put(t, slice);
		merger = SecureMergeFactory.get(op);
		
		bufferPoolKey = SecureBufferPool.getKey(op);
		sliceItr = slices.entrySet().iterator(); 
	}

	
	// for collecting segment output
		public SlicedSecureQueryTable(OperatorExecution op, CompEnv<GCSignal> e, SMCRunnable p) {
			R = GCGenComp.R;
			party = e.party;
			slices = new LinkedHashMap<Tuple, BasicSecureQueryTable>();
			bufferPoolKey = SecureBufferPool.getKey(op);
			schema = op.outSchema;
			env = e;
			parent = p;
			merger = SecureMergeFactory.get(op);


			
			
		}

	private void initializeRemotePlaintext() {
		int sliceCount = parent.getInt();
		
		for(int i = 0; i < sliceCount; ++i) {

			Pair<Tuple, BasicSecureQueryTable> slice = SMCUtils.prepareRemoteSlicedPlaintext(env, parent);
			BasicSecureQueryTable table = slice.getRight();
			table.schema = schema;
			table.R = this.R;
			
			slices.put(slice.getLeft(), table);
			
		}
		
	}

	
	

	private void initializeLocalPlaintext() throws Exception {
		ConnectionManager cm = ConnectionManager.getInstance();
		OperatorExecution operator = parent.getRootOperator();
        Connection c = cm.getConnection(operator.getWorkerId());

        
        QueryTable plainInput =  SqlQueryExecutor.query(operator.outSchema, operator.getSourceSQL(), c);	

		Map<Tuple, List<Tuple> > plainSlices = plainInput.asSlices(operator.parentSegment.sliceSpec);
		
		int sliceCount = plainSlices.keySet().size();	
		parent.sendInt(sliceCount);
		
		for(Tuple key : plainSlices.keySet()) {
			BasicSecureQueryTable table = SMCUtils.prepareLocalPlaintext(key, plainSlices.get(key), env, parent);
			table.R = R;
			table.schema = schema;
			slices.put(key, table);
		}
		
		bufferPoolKey = SecureBufferPool.getKey(operator);
		sliceItr = slices.entrySet().iterator(); 

		
	}
	

	
	
	@Override
	public String getBufferPoolKey() {
		return bufferPoolKey;
	}

	@Override
	public Party getParty() {
		return party;
	}

	
	public void resetSliceIterator() {
		sliceItr = slices.entrySet().iterator(); 
	}
	
	public boolean hasNext() {
		return sliceItr.hasNext();
	}
	
	public Pair<Tuple, SecureArray<GCSignal>> getNextSlice(CompEnv<GCSignal> e) throws Exception {
		if(!sliceItr.hasNext())
			return null;
		
		Map.Entry<Tuple, BasicSecureQueryTable> entry = sliceItr.next();
		BasicSecureQueryTable table = entry.getValue();
		SecureArray<GCSignal> secArray = table.getSecureArray(env, parent);
		Tuple key = entry.getKey();
		
		return new ImmutablePair<Tuple, SecureArray<GCSignal>>(key, secArray);
	}
	
	@Override
	public QueryTable declassify(SecureQueryTable table) throws Exception {
		if(!(table instanceof SlicedSecureQueryTable)){
			throw new Exception("Cannot decode unmatched tables!");
		}

		SlicedSecureQueryTable aTable, bTable;
		if(party == Party.Bob) {
			aTable = (SlicedSecureQueryTable) table;
			bTable = this;
		}
		else {
			aTable = this;
			bTable = (SlicedSecureQueryTable) table;
			
		}
		
		
		QueryTable result = new QueryTable(schema);

		for(Tuple key : slices.keySet()) {
			BasicSecureQueryTable a = aTable.slices.get(key);
			BasicSecureQueryTable b = bTable.slices.get(key);
			QueryTable keyTable = SecureOutputReader.assembleOutput(a, b, schema);
			result.addTuples(keyTable);
		}

		
		result.addTuples(aTable.plaintextOutput);
		result.addTuples(bTable.plaintextOutput);
		
		
		
		return result;
	}

	public SecureArray<GCSignal> getSMCSecureArray(CompEnv<GCSignal> localEnv) throws Exception {
		int tupleCount = 0;
		int tupleSize = schema.size();
		for(BasicSecureQueryTable t : slices.values()) {
			tupleCount += t.payload.length / tupleSize;
		}
		
		output = new SecureArray<GCSignal>(localEnv, tupleCount, tupleSize);
		GCSignal[] arrPos = localEnv.inputOfAlice(Utils.fromInt(0, 32));
		
		
		for(Tuple t : slices.keySet()) {
			// for each tuple in t
			// if t's idx < nonNullElements
			// write t to result
			BasicSecureQueryTable value = slices.get(t);
			arrPos = appendToSecArray(value, arrPos, localEnv);

		}	
		output.setNonNullEntries(this.getSecureNonNullLength(localEnv));
		return output;
		
	}
	
	public SecureArray<GCSignal> getPlaintextSecureArray(CompEnv<GCSignal> localEnv) throws Exception {
		int tupleCount = 0;
		int tupleSize = schema.size();
		
		if(aPlaintext != null) {
			tupleCount += aPlaintext.payload.length / tupleSize;
		}
		

		
		if(bPlaintext != null) {
			tupleCount += bPlaintext.payload.length / tupleSize;
		}
		output = new SecureArray<GCSignal>(localEnv, tupleCount, tupleSize);
		output = new SecureArray<GCSignal>(localEnv, tupleCount, tupleSize);
		GCSignal[] arrPos = localEnv.inputOfAlice(Utils.fromInt(0, 32));
		
		arrPos = appendToSecArray(aPlaintext, arrPos, localEnv);
		arrPos = appendToSecArray(bPlaintext, arrPos, localEnv);

		return output;

	}
	
	// stitch it into one ORAM instance for input to a secure operator
	// is the plaintext output replicated?  No, because it is a parallel secure step
	// is it input of bob, alice or both? Basically need a merge step to cover the two
	@Override
	public SecureArray<GCSignal> getSecureArray(CompEnv<GCSignal> localEnv, SMCRunnable runnable) throws Exception {
		if(plaintextOutput == null) { // for use within a sliced operator
			return getSMCSecureArray(localEnv);
		}
		
		return merger.merge(this, localEnv, runnable);
	}


	// returns new writeIdx
	private GCSignal[] appendToSecArray(BasicSecureQueryTable toWrite, GCSignal[] arrPos, CompEnv<GCSignal> localEnv) throws Exception {
		int tupleSize = schema.size();
		int tTuples = toWrite.payload.length / tupleSize;
		GCSignal[] cutoff = toWrite.nonNullLength;
		IntegerLib<GCSignal> intLib = new IntegerLib<GCSignal>(localEnv);
		GCSignal[] incrementer = localEnv.inputOfAlice(Utils.fromInt(1, 32));
		
		for(int i = 0; i < tTuples; ++i) {
	
			GCSignal[] tIdx = localEnv.inputOfAlice(Utils.fromInt(i, 32));
	
			GCSignal lt = intLib.not(intLib.geq(tIdx, cutoff));
	
			GCSignal[] srcData = Arrays.copyOfRange(toWrite.payload, i*tupleSize, (i+1)*tupleSize);
	
			output.conditionalWrite(arrPos, srcData,lt);
			GCSignal[] arrPosPrime = intLib.add(arrPos, incrementer);
	
			// update write position in result
			arrPos = intLib.mux(arrPos, arrPosPrime, lt);
		}
		
		return arrPos;
	}

	
	@Override
	public void setPlaintextOutput(QueryTable pc) throws Exception {
		
		plaintextOutput = pc;		
	}



	
	
	public SecureArray<GCSignal> getSlice(Tuple value, CompEnv<GCSignal> localEnv) throws Exception {
		BasicSecureQueryTable basic = slices.get(value);
		if(basic != null)
			return basic.getSecureArray(localEnv, parent);
		return null;
	}


	public void addSlice(Tuple key, GCSignal[] payload, GCSignal[] nonNulls) {
		BasicSecureQueryTable table = new BasicSecureQueryTable(payload, schema.size(), env, parent);
		table.nonNullLength = nonNulls;
		table.R = GCGenComp.R;
		table.schema = schema;
		slices.put(key, table);
		written = true;
		
	}



	@Override
	public GCSignal[] getSecurePayload(CompEnv<GCSignal> localEnv) throws Exception {
		output = getSMCSecureArray(localEnv);

		return Util.secToIntArray(localEnv, output);
	
	}



	@Override
	public GCSignal[] getSecureNonNullLength(CompEnv<GCSignal> localEnv) {
		IntegerLib<GCSignal> intLib = new IntegerLib<GCSignal>(localEnv);

		   GCSignal[] length = localEnv.inputOfAlice(Utils.fromInt(0, 32));
		   for(BasicSecureQueryTable t : slices.values()) {
			   length = intLib.add(length, t.nonNullLength);
		   }


		return length;
	}



	@Override
	public QueryTable getPlaintextOutput() {
		return plaintextOutput;
	}

	


	@Override
	public QueryTable declassify(SecureQueryTable other, SecureRelRecordType schema) throws Exception {
		if(!(other instanceof SlicedSecureQueryTable)){
			throw new Exception("Cannot decode unmatched tables!");
		}
		QueryTable output = null;
		
		if(party == Party.Bob) {
			SlicedSecureQueryTable aTable = (SlicedSecureQueryTable) other;
			output = SecureOutputReader.assembleOutput(aTable, this, schema);
		}
		else {
			SlicedSecureQueryTable bTable = (SlicedSecureQueryTable) other;
			output = SecureOutputReader.assembleOutput(this, bTable, schema);
		}
		
		output.addTuples(plaintextOutput);
		output.addTuples(((SlicedSecureQueryTable) other).plaintextOutput); 
		 
		return output;
	}



}
