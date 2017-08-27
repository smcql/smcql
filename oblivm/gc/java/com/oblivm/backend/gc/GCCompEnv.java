package com.oblivm.backend.gc;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Mode;
import com.oblivm.backend.flexsc.Party;
import com.oblivm.backend.network.Network;

public abstract class GCCompEnv extends CompEnv<GCSignal> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1959218203163444929L;

	public GCCompEnv(Network channel, Party p, Mode mode) {
		super(channel, p, mode);
	}

	public GCSignal ONE() {
		return new GCSignal(true);
	}
	
	public GCSignal ZERO() {
		return new GCSignal(false);
	}
	
	public GCSignal[] newTArray(int len) {
		return new GCSignal[len];
	}
	
	public GCSignal[][] newTArray(int d1, int d2) {
		return new GCSignal[d1][d2];
	}
	
	public GCSignal[][][] newTArray(int d1, int d2, int d3) {
		return new GCSignal[d1][d2][d3];
	}
	
	public GCSignal newT(boolean v) {
		return new GCSignal(v);
	}
}
