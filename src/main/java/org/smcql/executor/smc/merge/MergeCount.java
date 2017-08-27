package org.smcql.executor.smc.merge;

import java.io.Serializable;

import org.smcql.db.data.QueryTable;
import org.smcql.db.data.field.IntField;
import org.smcql.executor.smc.SecureQueryTable;
import org.smcql.executor.smc.runnable.SMCRunnable;
import com.oblivm.backend.circuits.arithmetic.IntegerLib;
import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Party;
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.oram.SecureArray;
import com.oblivm.backend.util.Utils;

// implements counts
public class MergeCount implements SecureMerge, Serializable {

	@Override
	// 
	// for count distinct, distinct has already been applied
	public SecureArray<GCSignal> merge(SecureQueryTable src, CompEnv<GCSignal> env, SMCRunnable parent)
			throws Exception {
		
		IntegerLib<GCSignal> intLib = new IntegerLib<GCSignal>(env);
	
		IntField i = new IntField(null, 0);
		int intSize = i.size();
		SecureArray<GCSignal> output = new SecureArray<GCSignal>(env, 3, intSize);
		QueryTable plaintext = src.getPlaintextOutput();
		// returns single int field with count || distinct count
		long plainCount = ((IntField) plaintext.getTuple(0).getField(0)).getValue();
	
		GCSignal[] smcCount = src.getSecureNonNullLength(env);
		
		smcCount = intLib.padSignal(smcCount, intSize);

		GCSignal[] zero = env.inputOfAlice(Utils.fromInt(0, 32));
		GCSignal[] one = env.inputOfAlice(Utils.fromInt(1, 32));
		GCSignal[] two = env.inputOfAlice(Utils.fromInt(2, 32));

		
		GCSignal[] aliceCount, bobCount;
		
		if(env.party == Party.Alice) {
			aliceCount = env.inputOfAlice(Utils.fromLong(plainCount, intSize));
			bobCount = env.inputOfBob(new boolean[intSize]);
		}
		else {
			aliceCount = env.inputOfAlice(new boolean[intSize]);
			bobCount = env.inputOfBob(Utils.fromLong(plainCount, intSize));
		}
		

		output.write(zero, smcCount);
		output.write(one, aliceCount);
		output.write(two, bobCount);
		output.setNonNullEntries(env.inputOfAlice(Utils.fromInt(3, 32)));


		return output;
	}

}
