package org.smcql.executor.smc.merge;

import org.smcql.executor.smc.SecureQueryTable;
import org.smcql.executor.smc.runnable.SMCRunnable;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.oram.SecureArray;

// operator specific for combining plaintext and smc outputs
public interface SecureMerge {
	
	public SecureArray<GCSignal> merge(SecureQueryTable src, CompEnv<GCSignal> env, SMCRunnable parent) throws Exception;
	
}
