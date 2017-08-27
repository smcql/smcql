package org.smcql.executor.smc.merge;

import java.io.Serializable;
import java.util.Arrays;

import org.smcql.db.data.QueryTable;
import org.smcql.executor.smc.BasicSecureQueryTable;
import org.smcql.executor.smc.SecureQueryTable;
import org.smcql.executor.smc.runnable.SMCRunnable;
import org.smcql.util.SMCUtils;
import com.oblivm.backend.circuits.arithmetic.IntegerLib;
import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Party;
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.lang.inter.Util;
import com.oblivm.backend.oram.SecureArray;
import com.oblivm.backend.util.Utils;

// default case - just concatenates the secure and plaintext output
public class MergeProject implements SecureMerge, Serializable {

	SecureArray<GCSignal> output = null;
	CompEnv<GCSignal> env;
	
	@Override
	public SecureArray<GCSignal> merge(SecureQueryTable src, CompEnv<GCSignal> localEnv, SMCRunnable parent) throws Exception {
		env = localEnv;
		// this needs to be done simultaneously b/c it is a single object spanning Alice & Bob
		// is it hitting the keys in the right order?
		GCSignal[] payload = src.getSecurePayload(env);
		GCSignal[] aPayload = null, bPayload = null;
		QueryTable plaintext = src.getPlaintextOutput();
		Party party = env.party;
		int tupleSize = plaintext.tupleSize();
		int tupleCount = payload.length /tupleSize;
		
		BasicSecureQueryTable a, b;
		if(party == Party.Alice) {			
			a = SMCUtils.prepareLocalPlaintext(plaintext, env, parent);
			b = SMCUtils.prepareRemotePlaintext(env, parent);
		}
		else {
			a = SMCUtils.prepareRemotePlaintext(env, parent);
			b = SMCUtils.prepareLocalPlaintext(plaintext, env, parent);
		}	

		
		if (a != null) {
			aPayload = a.getSecurePayload(env);
			tupleCount += aPayload.length / tupleSize;
		}
		
		if (b != null) {
			bPayload = b.getSecurePayload(env);
			tupleCount += bPayload.length / tupleSize;
		}

		output = new SecureArray<GCSignal>(env, tupleCount, tupleSize);
		GCSignal[] arrPos = env.inputOfAlice(Utils.fromInt(0, 32));

		arrPos = appendToSecArray(payload, src.getSecureNonNullLength(env), arrPos, tupleSize);
		
		if (aPayload != null)
			arrPos = appendToSecArray(aPayload, arrPos, tupleSize);
		
		if (bPayload != null)
			arrPos = appendToSecArray(bPayload, arrPos, tupleSize);
		
		output.setNonNullEntries(arrPos);
		return output;
		
	}

	private GCSignal[] appendToSecArray(GCSignal[] toWrite, GCSignal[] arrPos, int tupleSize) throws Exception {
		int aTuples = toWrite.length / tupleSize;
		GCSignal[] nonNulls = env.inputOfAlice(Utils.fromInt(aTuples, 32));
		return appendToSecArray(toWrite, nonNulls, arrPos, tupleSize);
	}
	
	// returns new writeIdx
	private GCSignal[] appendToSecArray(GCSignal[] toWrite, GCSignal[] nonNullLength, GCSignal[] arrPos, int tupleSize) throws Exception {
		int tTuples = toWrite.length / tupleSize;
		IntegerLib<GCSignal> intLib = new IntegerLib<GCSignal>(env);
		GCSignal[] incrementer = env.inputOfAlice(Utils.fromInt(1, 32));
		
		for(int i = 0; i < tTuples; ++i) {
	
			GCSignal[] tIdx = env.inputOfAlice(Utils.fromInt(i, 32));
	
			GCSignal lt = intLib.not(intLib.geq(tIdx, nonNullLength));
	
			GCSignal[] srcData = Arrays.copyOfRange(toWrite, i*tupleSize, (i+1)*tupleSize);
	
			output.conditionalWrite(arrPos, srcData,lt);
			GCSignal[] arrPosPrime = intLib.add(arrPos, incrementer);
	
			// update write position in result
			arrPos = intLib.mux(arrPos, arrPosPrime, lt);

		}
		
		return arrPos;
	}


}
