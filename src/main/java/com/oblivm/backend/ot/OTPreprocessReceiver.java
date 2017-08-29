// Copyright (C) by Xiao Shaun Wang <wangxiao@cs.umd.edu>

package com.oblivm.backend.ot;

import java.io.IOException;
import java.util.Arrays;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Flag;
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.network.Network;

public class OTPreprocessReceiver  extends OTReceiver {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6856259865198469546L;
	GCSignal[] buffer = new GCSignal[OTPreprocessSender.bufferSize];
	boolean[] choose = new boolean[OTPreprocessSender.bufferSize];
	int bufferusage = 0;

	public void fillup() {
			channel.flush();

		while(bufferusage < OTPreprocessSender.bufferSize) {
			int l = Math.min(OTPreprocessSender.fillLength, OTPreprocessSender.bufferSize-bufferusage);
			
			for(int i = bufferusage; i < bufferusage+l; ++i)
				choose[i] = CompEnv.rnd.nextBoolean();
			GCSignal[] kc = null;
			try {
				kc = reciever.receive(Arrays.copyOfRange(choose, bufferusage, bufferusage+l));
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.arraycopy(kc, 0, buffer, bufferusage, kc.length);
			bufferusage += l;
		}
		
			channel.flush();
		
	}

	OTExtReceiver reciever;
	public OTPreprocessReceiver(Network channel) {
		super(channel);
		reciever = new OTExtReceiver(channel);
		fillup();
	}


	public GCSignal receive(boolean b) throws IOException {
		bufferusage--;
		byte z = (b^choose[bufferusage]) ? (byte)1 : (byte)0;
		Flag.sw.startOTIO();
		channel.writeByte(z);
		channel.flush();
		GCSignal[] y = new GCSignal[]{GCSignal.receive(channel),  GCSignal.receive(channel)};
		Flag.sw.stopOTIO();
		if(bufferusage == 0)
			fillup();
		return y[b?1:0].xor(buffer[bufferusage]);
	}

	public GCSignal[] receive(boolean[] b) throws IOException {
		if(bufferusage < b.length)
			fillup();
		byte[] z = new byte[b.length];
		int tmp = bufferusage;
		for(int i = 0; i < b.length; ++i) {
			--tmp;
			z[i] = (b[i]^choose[tmp]) ? (byte)1 : (byte)0;
		}
		Flag.sw.startOTIO();
		channel.writeByte(z, z.length);
		channel.flush();
		Flag.sw.stopOTIO();
		GCSignal[] ret = new GCSignal[b.length];
		for(int i = 0; i < b.length; ++i) {
			bufferusage--;
			Flag.sw.startOTIO();
			GCSignal[] y = new GCSignal[]{GCSignal.receive(channel),  GCSignal.receive(channel)};
			Flag.sw.stopOTIO();
			ret[i] = y[b[i]?1:0].xor(buffer[bufferusage]);
		}
		return ret;
	}
}
