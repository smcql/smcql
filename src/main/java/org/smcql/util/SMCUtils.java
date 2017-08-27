package org.smcql.util;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.smcql.db.data.QueryTable;
import org.smcql.db.data.Tuple;
import org.smcql.executor.smc.BasicSecureQueryTable;
import org.smcql.executor.smc.runnable.SMCRunnable;
import org.smcql.type.SecureRelRecordType;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Party;
import com.oblivm.backend.gc.GCGenComp;
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.lang.inter.Util;
import com.oblivm.backend.oram.SecureArray;
import com.oblivm.backend.util.Utils;

public class SMCUtils {

	
	public static String toString(GCSignal[] tArray) {
		String ret = tArray[0].toHexStr();
		for(int i = 1; i < tArray.length; ++i) {
			ret += " " + tArray[1].toHexStr();
		}
		
		return ret;
	}
	
	public static String toString(GCSignal[][] tArray) {
		String ret = new String();
		for(int i = 0; i < tArray.length; ++i) {
			ret += i + ": " + toString(tArray[i]) + "\n";
		}
		
		return ret;
	}
	
	
	// decoupled version of CircuitLib::toSignals()
	public static GCSignal[] toSignals(long v, int width) {
		GCSignal[] result = new GCSignal[width];
		for (int i = 0; i < width; ++i) {
			if ((v & 1) == 1)
				result[i] = new GCSignal(true);
			else
				result[i] = new GCSignal(false);
			v >>= 1;
		}
		return result;
		
	}
	
	// standalone version of Util::secArrayToInt
	@SuppressWarnings("unchecked")
	public static<T> T[] flattenSecArray(CompEnv<T> env, SecureArray<T> secInput) throws Exception {
		T[] intArray = null;
		int arraySize = secInput.length;
		
		for(long i=0; i<arraySize; ++i) {
			if(i==0) {
				T[] idx = (T[]) SMCUtils.toSignals(i, 64);
				intArray = secInput.read(idx);
			} else {
				intArray = concat(intArray, secInput.read((T[]) SMCUtils.toSignals(i, 64)));
			}
		}
		
		return intArray;
	}

	public static<T> byte[] secArrayToBytes(CompEnv<T> env, SecureArray<T> secInput) throws Exception {
		byte[] output = new byte[secInput.dataSize * secInput.length * 10];
		int arraySize = secInput.length;
		T[] intArray = null;
		int dstIdx = 0;
		
		for(long i=0; i<arraySize; ++i) {
				@SuppressWarnings("unchecked")
				T[] idx = (T[]) SMCUtils.toSignals(i, 64);
				intArray = secInput.read(idx);
				for(int j = 0; j < intArray.length; ++j) {
					GCSignal signal = (GCSignal) intArray[j];
					
					System.arraycopy(signal.bytes, 0, output, dstIdx, 10);
					dstIdx += 10;
				}
		}
		
		return output;
	}

	
	private static <T> T[] concat(T[] first, T[] second) {
		  T[] result = Arrays.copyOf(first, first.length + second.length);
		  System.arraycopy(second, 0, result, first.length, second.length);
		  return result;
	}
	
	
	
	public static <T> SecureArray<T> prepareLocalPlainArray(QueryTable table, CompEnv<T> env, SMCRunnable parent) throws Exception {
		boolean[] srcData = table.toBinary();
		
		int len = (srcData != null) ? srcData.length : 0;
		Party party = env.party;
		int tupleSize = table.getSchema().size(); 

		assert(len % tupleSize == 0);
		parent.sendInt(len);

		if(len > 0) {
			 parent.sendInt(tupleSize);
			 boolean[] tupleCount = Utils.fromInt(table.tupleCount(), 32);
			T[] nonNullLength = (party == Party.Bob) ?  env.inputOfBob(tupleCount) : env.inputOfAlice(tupleCount);
			T[] tArray = (party == Party.Alice)  ? env.inputOfAlice(srcData) : env.inputOfBob(srcData);
			env.flush();

			SecureArray<T> input = Util.intToSecArray(env, tArray, tupleSize, len / tupleSize);
			input.setNonNullEntries(nonNullLength);
			env.flush();
		
			return input;
		}
		return null;

	}


	public static<T> SecureArray<T> prepareRemotePlainArray(CompEnv<T> env, SMCRunnable parent) throws Exception {
		Party party = env.party;

		int len = parent.getInt();
		
	
		if(len > 0) {
			int tupleSize = parent.getInt();
			T[] nonNullLength = (party == Party.Bob) ? env.inputOfAlice(new boolean[32]) : env.inputOfBob(new boolean[32]);

			T[] tArray = (party == Party.Bob)  ? env.inputOfAlice(new boolean[len]) : env.inputOfBob(new boolean[len]);
	
		
			env.flush();
		
			int tupleCount = len / tupleSize;
			SecureArray<T> input =  Util.intToSecArray(env, tArray, tupleSize, tupleCount);
			input.setNonNullEntries(nonNullLength);
			env.flush();
			return input;
		}
		return null;
		

	}
	
	public static<T> BasicSecureQueryTable prepareLocalPlaintext(QueryTable table, CompEnv<T> env, SMCRunnable parent) throws Exception {
		return prepareLocalPlaintext(null, table.tuples(), env, parent);
	}
	
	public static<T> BasicSecureQueryTable prepareLocalPlaintext(Tuple key, List<Tuple> tuples, CompEnv<T> env, SMCRunnable parent) throws Exception {
		boolean[] srcData = QueryTable.toBinary(tuples); 
		int tupleSize = (tuples.size() == 0) ? 0 : tuples.get(0).size(); // for merge ops input schema == output schema
		SecureRelRecordType schema = (tuples.size() == 0) ? null : tuples.get(0).getSchema();
		return prepareLocalPlaintext(key, srcData, tupleSize, schema, env, parent);
		
	}
	
	public static<T> BasicSecureQueryTable prepareLocalPlaintext(Tuple key, boolean[] srcData, int tupleSize, SecureRelRecordType schema, CompEnv<T> env, SMCRunnable parent) throws Exception {

		Party party = env.party;

		int len = (srcData == null) ? 0 : srcData.length;
		srcData = (srcData == null) ? new boolean[0] : srcData; 
		int tupleCount = (tupleSize == 0) ? 0 : len / tupleSize;

		assert(len % tupleSize == 0);

		parent.sendInt(len);
		if(len > 0) {
			parent.sendInt(tupleSize);
			if(key != null) {// slice mode
				parent.sendTuple(key);
			}
			
			T[] tArray;
			T[] nonNullLength;
			
			if(party == Party.Alice) {
				nonNullLength = env.inputOfAlice(Utils.fromInt(tupleCount, 32));
				env.flush();
				tArray = env.inputOfAlice(srcData);
			}
			else {
				nonNullLength = env.inputOfBob(Utils.fromInt(tupleCount, 32));
				env.flush();
				tArray = env.inputOfBob(srcData);
			}
			env.flush();
			
			@SuppressWarnings("unchecked")
			BasicSecureQueryTable output = new BasicSecureQueryTable((GCSignal[]) tArray, tupleSize, (CompEnv<GCSignal>) env, parent);
			
			output.nonNullLength = (GCSignal[]) nonNullLength;
			output.R = GCGenComp.R;
			output.schema = schema;
			
			return output;
			
		}
		
		
		return null;
	}
	
	

	@SuppressWarnings("unchecked")
	public static<T> BasicSecureQueryTable prepareRemotePlaintext(CompEnv<T> env, SMCRunnable parent) {
		Party party = env.party;

		int len = parent.getInt();
		if(len > 0) {
			int tupleSize = parent.getInt();
			T[] nonNullLength = (party == Party.Bob) ? env.inputOfAlice(new boolean[32]) : env.inputOfBob(new boolean[32]);
			env.flush();
			T[] tArray = (party == Party.Bob)  ? env.inputOfAlice(new boolean[len]) : env.inputOfBob(new boolean[len]);		
			env.flush();
			
			BasicSecureQueryTable output = new BasicSecureQueryTable((GCSignal[]) tArray, tupleSize, (CompEnv<GCSignal>) env, parent);
			output.nonNullLength = (GCSignal[]) nonNullLength;
			
			return output;
		}		
		
		return null;
	}

	@SuppressWarnings("unchecked")
	public static<T> Pair<Tuple, BasicSecureQueryTable> prepareRemoteSlicedPlaintext(CompEnv<T> env, SMCRunnable parent) {
		Party party = env.party;

		int len = parent.getInt();		

		if(len > 0) {
			int tupleSize = parent.getInt();
			Tuple slice = parent.getTuple();
			T[] nonNullLength = (party == Party.Bob) ? env.inputOfAlice(new boolean[32]) : env.inputOfBob(new boolean[32]);	
			T[] tArray = (party == Party.Bob)  ? env.inputOfAlice(new boolean[len]) : env.inputOfBob(new boolean[len]);		

			env.flush();
			BasicSecureQueryTable output = new BasicSecureQueryTable((GCSignal[]) tArray, tupleSize, (CompEnv<GCSignal>) env, parent);
			output.nonNullLength = (GCSignal[]) nonNullLength;

			return new ImmutablePair<Tuple, BasicSecureQueryTable>(slice, output);
		}			
		return null;
	}

	
}
