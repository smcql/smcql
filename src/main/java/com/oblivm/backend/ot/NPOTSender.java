// Copyright (C) 2013 by Yan Huang <yhuang@cs.umd.edu>

package com.oblivm.backend.ot;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.Security;

import com.oblivm.backend.flexsc.Flag;
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.network.Network;
import com.oblivm.backend.rand.ISAACProvider;

public class NPOTSender extends OTSender {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8829431881457672703L;

	static SecureRandom rnd;
	static {
		Security.addProvider(new ISAACProvider());
		try {
			rnd = SecureRandom.getInstance("ISAACRandom");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	private static final int certainty = 80;

	private final static int qLength = 160; // 512;
	private final static int pLength = 1024; // 15360;

	private BigInteger p, q, g, C, r;
	private BigInteger Cr, gr;

	Cipher cipher;

	public NPOTSender(int msgBitLength, Network channel)
			throws Exception {
		super(msgBitLength, channel);
		cipher = new Cipher();

		initialize();
	}

	public void send(GCSignal[][] msgPairs) throws IOException {
		step1(msgPairs);
	}

	private void initialize() throws Exception {
		File keyfile = new File("NPOTKey");
		if (keyfile.exists()) {
			FileInputStream fin = new FileInputStream(keyfile);
			ObjectInputStream fois = new ObjectInputStream(fin);

			C = (BigInteger) fois.readObject();
			p = (BigInteger) fois.readObject();
			q = (BigInteger) fois.readObject();
			g = (BigInteger) fois.readObject();
			gr = (BigInteger) fois.readObject();
			r = (BigInteger) fois.readObject();
			fois.close();

			Flag.sw.startOTIO();
			channel.writeBI(C);
			channel.writeBI(p);
			channel.writeBI(q);
			channel.writeBI(g);
			channel.writeBI(gr);
			channel.writeInt(msgBitLength);
			channel.flush();
			Flag.sw.stopOTIO();

			Cr = C.modPow(r, p);
		} else {
			BigInteger pdq;
			q = new BigInteger(qLength, certainty, rnd);

			do {
				pdq = new BigInteger(pLength - qLength, rnd);
				pdq = pdq.clearBit(0);
				p = q.multiply(pdq).add(BigInteger.ONE);
			} while (!p.isProbablePrime(certainty));

			do {
				g = new BigInteger(pLength - 1, rnd);
			} while ((g.modPow(pdq, p)).equals(BigInteger.ONE)
					|| (g.modPow(q, p)).equals(BigInteger.ONE));

			r = (new BigInteger(qLength, rnd)).mod(q);
			gr = g.modPow(r, p);
			C = (new BigInteger(qLength, rnd)).mod(q);

			Flag.sw.startOTIO();
			channel.writeBI(C);
			channel.writeBI(p);
			channel.writeBI(q);
			channel.writeBI(g);
			channel.writeBI(gr);
			channel.writeInt(msgBitLength);
			channel.flush();
			Flag.sw.stopOTIO();

			Cr = C.modPow(r, p);

			FileOutputStream fout = new FileOutputStream(keyfile);
			ObjectOutputStream foos = new ObjectOutputStream(fout);

			foos.writeObject(C);
			foos.writeObject(p);
			foos.writeObject(q);
			foos.writeObject(g);
			foos.writeObject(gr);
			foos.writeObject(r);

			foos.flush();
			foos.close();
		}
	}

	GCSignal[][] m = new GCSignal[1][2];

	@Override
	public void send(GCSignal[] msgPair) throws IOException {
		m[0][0] = msgPair[0];
		m[0][1] = msgPair[1];
		send(m);
	}

	private void step1(GCSignal[][] msgPairs) throws IOException {
		BigInteger[] pk0 = new BigInteger[msgPairs.length];
		Flag.sw.startOTIO();
		for (int i = 0; i < pk0.length; i++)
			pk0[i] = channel.readBI();
		Flag.sw.stopOTIO();

		BigInteger[] pk1 = new BigInteger[msgPairs.length];
		BigInteger[][] msg = new BigInteger[msgPairs.length][2];

		for (int i = 0; i < msgPairs.length; i++) {
			pk0[i] = pk0[i].modPow(r, p);
			pk1[i] = Cr.multiply(pk0[i].modInverse(p)).mod(p);

			msg[i][0] = cipher.encrypt(pk0[i].toByteArray(), new BigInteger(
					msgPairs[i][0].bytes), msgBitLength);
			msg[i][1] = cipher.encrypt(pk1[i].toByteArray(), new BigInteger(
					msgPairs[i][1].bytes), msgBitLength);
		}
		Flag.sw.startOTIO();
		for (int i = 0; i < msg.length; i++) {
			channel.writeBI(msg[i][0]);
			channel.writeBI(msg[i][1]);
		}
		channel.flush();
		Flag.sw.stopOTIO();

	}
}