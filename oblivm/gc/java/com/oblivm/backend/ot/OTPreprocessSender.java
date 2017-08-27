// Copyright (C) by Xiao Shaun Wang <wangxiao@cs.umd.edu>

package com.oblivm.backend.ot;

import java.io.IOException;
import java.util.Arrays;

import com.oblivm.backend.flexsc.Flag;
import com.oblivm.backend.gc.GCGenComp;
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.network.Network;

public class OTPreprocessSender  extends OTSender {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1699714410989997158L;
	OTExtSender sender;
	public OTPreprocessSender(int msgBitLength, Network channel) {
		super(msgBitLength, channel);
		sender = new OTExtSender(msgBitLength, channel);
		fillup();
	}

	final static public int bufferSize = 1024*1024*1;
	final static public int fillLength = 300000;
	GCSignal[][] buffer = new GCSignal[bufferSize][2];
	int bufferusage = 0;

	public void fillup () {

		channel.flush();

		while(bufferusage < bufferSize) {
			int l = Math.min(fillLength, bufferSize-bufferusage);
			for(int i = bufferusage; i < bufferusage+l; ++i)
				buffer[i] = GCGenComp.genPair();
			try {
				sender.send(Arrays.copyOfRange(buffer, bufferusage, bufferusage+l));
			} catch (IOException e) {
				e.printStackTrace();
			}
			bufferusage +=l;
			System.out.println("preprocessing OT"+bufferusage/(double)bufferSize);
		}
		channel.flush();
	}


	public  void send(GCSignal[] m) throws IOException {
		Flag.sw.startOTIO();
		byte z = channel.readBytes(1)[0];
		Flag.sw.stopOTIO();
		bufferusage--;
		if(z == 0) {
			m[0].xor(buffer[bufferusage][0]).send(channel);
			m[1].xor(buffer[bufferusage][1]).send(channel);
		}
		else {
			m[0].xor(buffer[bufferusage][1]).send(channel);
			m[1].xor(buffer[bufferusage][0]).send(channel);
		}
		if(bufferusage == 0)
			fillup();
	}

	public void send(GCSignal[][] m) throws IOException {
		if(bufferusage < m.length)
			fillup();
		Flag.sw.startOTIO();
		byte[] z = channel.readBytes(m.length);
		Flag.sw.stopOTIO();
		for(int i = 0; i < m.length; ++i) {
			bufferusage--;
			if(z[i] == 0) {
				m[i][0].xor(buffer[bufferusage][0]).send(channel);
				m[i][1].xor(buffer[bufferusage][1]).send(channel);
			}
			else {
				m[i][0].xor(buffer[bufferusage][1]).send(channel);
				m[i][1].xor(buffer[bufferusage][0]).send(channel);
			}
		}
	}
}
