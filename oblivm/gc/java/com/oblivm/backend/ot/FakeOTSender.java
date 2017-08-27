// Copyright (C) 2013 by Yan Huang <yhuang@cs.umd.edu>
// Improved by Xiao Shaun Wang <wangxiao@cs.umd.edu>

package com.oblivm.backend.ot;

import java.io.IOException;

import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.network.Network;

public class FakeOTSender extends OTSender {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6426366006980927253L;

	public FakeOTSender(int bitLen, Network channel) {
		super(bitLen, channel);
	}

	@Override
	public void send(GCSignal[] m) {
		m[0].send(channel);
		m[1].send(channel);
	}

	@Override
	public void send(GCSignal[][] m) throws IOException {
		for (int i = 0; i < m.length; i++) {
			m[i][0].send(channel);
			m[i][1].send(channel);
		}
		channel.flush();
	}
}