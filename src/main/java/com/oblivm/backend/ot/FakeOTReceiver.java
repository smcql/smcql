// Copyright (C) 2013 by Yan Huang <yhuang@cs.umd.edu>
// Improved by Xiao Shaun Wang <wangxiao@cs.umd.edu>

package com.oblivm.backend.ot;

import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.network.Network;

public class FakeOTReceiver extends OTReceiver {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3285866891649130783L;

	public FakeOTReceiver(Network channel) {
		super(channel);
	}

	@Override
	public GCSignal receive(boolean c) {
		GCSignal[] m = new GCSignal[2];
		m[0] = GCSignal.receive(channel);
		m[1] = GCSignal.receive(channel);
		return m[c ? 1 : 0];
	}

	@Override
	public GCSignal[] receive(boolean[] c) {
		GCSignal[] res = new GCSignal[c.length];
		for (int i = 0; i < c.length; i++) {
			GCSignal[] m = new GCSignal[2];
			m[0] = GCSignal.receive(channel);
			m[1] = GCSignal.receive(channel);
			res[i] = m[c[i] ? 1 : 0];
		}
		return res;
	}
}
