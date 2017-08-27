// Copyright (C) 2013 by Yan Huang <yhuang@cs.umd.edu>
// Improved by Xiao Shaun Wang <wangxiao@cs.umd.edu>

package com.oblivm.backend.ot;

import java.io.IOException;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.Security;
import java.util.Arrays;

import com.oblivm.backend.flexsc.Flag;
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.network.Network;
import com.oblivm.backend.ot.OTExtSender.SecurityParameter;
import com.oblivm.backend.rand.ISAACProvider;

public class OTExtReceiver extends OTReceiver {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2921886449119501715L;

	static SecureRandom rnd;
	static {
		Security.addProvider(new ISAACProvider());
		try {
			rnd = SecureRandom.getInstance("ISAACRandom");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}

	private int msgBitLength;

	private OTSender snder;
	private GCSignal[][] keyPairs;

	Cipher cipher;

	public OTExtReceiver(Network channel) {
		super(channel);

		cipher = new Cipher();

		try {
			initialize();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	boolean[] s = new boolean[SecurityParameter.k1];

	public GCSignal[] receive(boolean[] choices) throws IOException {
		GCSignal[] keys = new GCSignal[SecurityParameter.k1];

		boolean[] c = new boolean[SecurityParameter.k1 + choices.length];
		for (int i = 0; i < SecurityParameter.k1; i++)
			c[i] = rnd.nextBoolean();
		for (int i = SecurityParameter.k1; i < c.length; i++)
			c[i] = choices[i - SecurityParameter.k1];

		GCSignal[] received = reverseAndExtend(keyPairs, c, msgBitLength, channel, cipher);

		for (int i = 0; i < OTExtSender.SecurityParameter.k1; i++) {
			keys[i] = received[i];
			s[i] = c[i];
		}
		for (int i = 0; i < OTExtSender.SecurityParameter.k1; i++) {
			keyPairs[i][0] = GCSignal.freshLabel(rnd);
			keyPairs[i][1] = GCSignal.freshLabel(rnd);
		}
		OTExtSender.reverseAndExtend(s, keys, msgBitLength, keyPairs, channel,
				cipher);

		return Arrays.copyOfRange(received, SecurityParameter.k1,
				received.length);
	}

	static GCSignal[] reverseAndExtend(GCSignal[][] keyPairs,
			boolean[] choices, int msgBitLength, Network channel, Cipher cipher) throws IOException {
		BigInteger[][] msgPairs = new BigInteger[SecurityParameter.k1][2];
		BigInteger[][] cphPairs = new BigInteger[SecurityParameter.k1][2];

		BitMatrix T = new BitMatrix(choices.length, SecurityParameter.k1);
		T.initialize(rnd);

		BigInteger biChoices = OTExtSender.fromBoolArray(choices);
		for (int i = 0; i < SecurityParameter.k1; i++) {
			msgPairs[i][0] = T.data[i];
			msgPairs[i][1] = T.data[i].xor(biChoices);

			cphPairs[i][0] = cipher.encrypt(keyPairs[i][0].bytes,
					msgPairs[i][0], choices.length);
			cphPairs[i][1] = cipher.encrypt(keyPairs[i][1].bytes,
					msgPairs[i][1], choices.length);
			channel.writeBI(cphPairs[i][0]);
			channel.writeBI(cphPairs[i][1]);
		}

		Flag.sw.startOTIO();
		channel.flush();
		Flag.sw.stopOTIO();

		BitMatrix tT = T.transpose();
		GCSignal[] res = new GCSignal[choices.length];

		GCSignal[][] y = new GCSignal[choices.length][2];

		for (int i = 0; i < choices.length; i++) {
			y[i][0] = GCSignal.receive(channel);
			y[i][1] = GCSignal.receive(channel);
			int sigma = choices[i] ? 1 : 0;
			res[i] = cipher.dec(GCSignal.newInstance(tT.data[i].toByteArray()),
					y[i][sigma], i);
		}

		return res;
	}

	private void initialize() throws Exception {
		Flag.sw.startOTIO();
		msgBitLength = channel.readInt();
		Flag.sw.stopOTIO();

		snder = new NPOTSender(OTExtSender.SecurityParameter.k1, channel);

		keyPairs = new GCSignal[OTExtSender.SecurityParameter.k1][2];
		for (int i = 0; i < OTExtSender.SecurityParameter.k1; i++) {
			keyPairs[i][0] = GCSignal.freshLabel(rnd);
			keyPairs[i][1] = GCSignal.freshLabel(rnd);
		}

		snder.send(keyPairs);
		channel.flush();
	}

	GCSignal[] pool;
	int poolIndex = 0;

	@Override
	public GCSignal receive(boolean c) {
		try {
			throw new Exception(
					"It doesn't make sense to do single OT with OT extension!");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}