// Copyright (C) 2013 by Yan Huang <yhuang@cs.umd.edu>
// Improved by Xiao Shaun Wang <wangxiao@cs.umd.edu>

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

public class OTExtSender extends OTSender {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5604566226320048225L;

	static class SecurityParameter {
		public static final int k1 = 80; // number of columns in T
	}

	private static SecureRandom rnd;
	static {
		Security.addProvider(new ISAACProvider());
		try {
			rnd = SecureRandom.getInstance("ISAACRandom");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	private OTReceiver rcver;
	private boolean[] s;
	private GCSignal[] keys;

	Cipher cipher;

	public OTExtSender(int msgBitLength, Network channel) {
		super(msgBitLength, channel);

		cipher = new Cipher();

		try {
			initialize();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	int poolIndex = 0;

	@Override
	public void send(GCSignal[] m) {
		try {
			throw new Exception(
					"It doesn't make sense to do single OT with OT extension!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * Everything in msgPairs are effective Sender's messages.
	 */
	GCSignal[][] keyPairs = new GCSignal[SecurityParameter.k1][2];

	public void send(GCSignal[][] msgPairs) throws IOException {
		GCSignal[][] pairs = new GCSignal[SecurityParameter.k1
				+ msgPairs.length][2];
		for (int i = 0; i < SecurityParameter.k1; i++) {
			pairs[i][0] = GCSignal.freshLabel(rnd);
			pairs[i][1] = GCSignal.freshLabel(rnd);
		}

		for (int i = SecurityParameter.k1; i < pairs.length; i++) {
			pairs[i][0] = msgPairs[i - SecurityParameter.k1][0];
			pairs[i][1] = msgPairs[i - SecurityParameter.k1][1];
		}

		reverseAndExtend(s, keys, msgBitLength, pairs, channel, cipher);

		for (int i = 0; i < SecurityParameter.k1; i++) {
			keyPairs[i][0] = pairs[i][0];
			keyPairs[i][1] = pairs[i][1];
		}
		for (int i = 0; i < s.length; i++)
			s[i] = rnd.nextBoolean();
		keys = OTExtReceiver.reverseAndExtend(keyPairs, s,
				SecurityParameter.k1, channel, cipher);
	}

	// Given s and keys, obliviously sends msgPairs which contains 'numOfPairs'
	// pairs of strings, each of length 'msgBitLength' bits.

	static void reverseAndExtend(boolean[] s, GCSignal[] keys,
			int msgBitLength, GCSignal[][] msgPairs, Network channel,
			Cipher cipher) throws IOException {
		BigInteger[][] cphPairs = new BigInteger[SecurityParameter.k1][2];

		Flag.sw.startOTIO();
		for (int i = 0; i < SecurityParameter.k1; i++) {
			cphPairs[i][0] = channel.readBI();
			cphPairs[i][1] = channel.readBI();
		}
		Flag.sw.stopOTIO();

		int numOfPairs = msgPairs.length;

		BitMatrix Q = new BitMatrix(numOfPairs, SecurityParameter.k1);

		for (int i = 0; i < SecurityParameter.k1; i++) {
			if (s[i])
				Q.data[i] = cipher.decrypt(keys[i].bytes, cphPairs[i][1],
						numOfPairs);
			else
				Q.data[i] = cipher.decrypt(keys[i].bytes, cphPairs[i][0],
						numOfPairs);
		}

		BitMatrix tQ = Q.transpose();

		BigInteger biS = fromBoolArray(s);

		GCSignal[][] y = new GCSignal[numOfPairs][2];
		for (int i = 0; i < numOfPairs; i++) {
			y[i][0] = cipher.enc(
					GCSignal.newInstance(tQ.data[i].toByteArray()),
					msgPairs[i][0], i);
			y[i][1] = cipher.enc(
					GCSignal.newInstance(tQ.data[i].xor(biS).toByteArray()),
					msgPairs[i][1], i);
			y[i][0].send(channel);
			y[i][1].send(channel);
		}

		Flag.sw.startOTIO();
		channel.flush();
		Flag.sw.stopOTIO();

	}

	private void initialize() throws Exception {
		Flag.sw.startOTIO();
		channel.writeInt(msgBitLength);
		channel.flush();
		Flag.sw.stopOTIO();

		rcver = new NPOTReceiver(channel);

		s = new boolean[SecurityParameter.k1];
		for (int i = 0; i < s.length; i++)
			s[i] = rnd.nextBoolean();

		keys = rcver.receive(s);
	}

	public static BigInteger fromBoolArray(boolean[] a) {
		BigInteger res = BigInteger.ZERO;
		for (int i = 0; i < a.length; i++)
			if (a[i])
				res = res.setBit(i);
		return res;
	}
}