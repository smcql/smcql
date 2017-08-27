// Copyright (C) 2013 by Yan Huang <yhuang@cs.umd.edu>

package com.oblivm.backend.ot;

import java.io.IOException;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.Security;

import com.oblivm.backend.flexsc.Flag;
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.network.Network;
import com.oblivm.backend.rand.ISAACProvider;

public class NPOTReceiver extends OTReceiver {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8387380166505849048L;
	static SecureRandom rnd;
	static {
		Security.addProvider(new ISAACProvider());
		try {
			rnd = SecureRandom.getInstance("ISAACRandom");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}

	private BigInteger p, q, g, C;
	private BigInteger gr;

	private BigInteger[][] pk;

	private BigInteger[] keys;

	Cipher cipher;

	public NPOTReceiver(Network channel) throws Exception {
		super(channel);

		cipher = new Cipher();

		initialize();
	}

	@Override
	public GCSignal receive(boolean c) throws IOException {
		return receive(new boolean[] { c })[0];
	}

	public GCSignal[] receive(boolean[] choices) throws IOException {
		step1(choices);
		return step2(choices);
	}

	private void initialize() throws Exception {
		Flag.sw.startOTIO();
		C = channel.readBI();
		p = channel.readBI();
		q = channel.readBI();
		g = channel.readBI();
		gr = channel.readBI();
		msgBitLength = channel.readInt();
		Flag.sw.stopOTIO();
	}

	private void step1(boolean[] choices) throws IOException {
		keys = new BigInteger[choices.length];
		pk = new BigInteger[choices.length][2];
		BigInteger[] pk0 = new BigInteger[choices.length];
		for (int i = 0; i < choices.length; i++) {
			BigInteger k = (new BigInteger(q.bitLength(), rnd)).mod(q);
			BigInteger gk = g.modPow(k, p);
			BigInteger C_over_gk = C.multiply(gk.modInverse(p)).mod(p);
			keys[i] = gr.modPow(k, p);

			int sigma = choices[i] ? 1 : 0;
			pk[i][sigma] = gk;
			pk[i][1 - sigma] = C_over_gk;

			pk0[i] = pk[i][0];
		}

		Flag.sw.startOTIO();
		for (int i = 0; i < choices.length; i++)
			channel.writeBI(pk0[i]);
		channel.flush();
		Flag.sw.stopOTIO();

	}

	private GCSignal[] step2(boolean[] choices) {
		BigInteger[][] msg = new BigInteger[choices.length][2];
		Flag.sw.startOTIO();
		for (int i = 0; i < choices.length; i++) {
			msg[i][0] = channel.readBI();
			msg[i][1] = channel.readBI();
		}
		Flag.sw.stopOTIO();

		GCSignal[] data = new GCSignal[choices.length];
		for (int i = 0; i < choices.length; i++) {
			int sigma = choices[i] ? 1 : 0;
			data[i] = GCSignal.newInstance(cipher.decrypt(
					keys[i].toByteArray(), msg[i][sigma], msgBitLength)
					.toByteArray());
		}
		return data;
	}
}