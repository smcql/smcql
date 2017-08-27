// Copyright (C) 2013 by Yan Huang <yhuang@cs.umd.edu>
// Improved by Xiao Shaun Wang <wangxiao@cs.umd.edu> and Kartik Nayak <kartik@cs.umd.edu>

package com.oblivm.backend.gc;

import java.io.IOException;
import java.io.OutputStream;
import java.security.SecureRandom;
import java.util.Arrays;

import com.oblivm.backend.network.Network;

public class GCSignal implements java.io.Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 6445809307174564457L;
	/**
	 * 
	 */

	public static final int len = 10;
	public byte[] bytes;
	public boolean v;

	public static final GCSignal ZERO = new GCSignal(new byte[len]);

	public GCSignal(byte[] b) {
		bytes = b;
	}

	public GCSignal(boolean b) {
		v = b;
	}

	public static GCSignal freshLabel(SecureRandom rnd) {
		byte[] b = new byte[len];
		rnd.nextBytes(b);
		return new GCSignal(b);
	}

	public static GCSignal newInstance(byte[] bs) {
		assert (bs.length <= len) : "Losing entropy when constructing signals.";
		byte[] b = new byte[len];
		Arrays.fill(b, (byte) ((bs[0] < 0) ? 0xff : 0));
		int newlen = len < bs.length ? len : bs.length;
		System.arraycopy(bs, 0, b, len - newlen, newlen);
		return new GCSignal(b);
	}

	public GCSignal(GCSignal lb) {
		v = lb.v;
		bytes = (lb.bytes == null) ? null : Arrays.copyOf(lb.bytes, len);
	}

	public boolean isPublic() {
		return bytes == null;
	}

	public GCSignal xor(GCSignal lb) {
		byte[] nb = new byte[len];
		for (int i = 0; i < len; i++)
			nb[i] = (byte) (bytes[i] ^ lb.bytes[i]);
		return new GCSignal(nb);
	}
	
	public static void xor(GCSignal l, GCSignal r, GCSignal ret) {
		for (int i = 0; i < len; i++)
			ret.bytes[i] = (byte) (l.bytes[i] ^ r.bytes[i]);
	}	
	

	public void setLSB() {
		bytes[0] |= 1;
	}

	public int getLSB() {
		return (bytes[0] & 1);
	}

	// 'send' and 'receive' are supposed to be used only for secret signals
	public void send(Network channel) {
		channel.writeByte(bytes, len);
	}

	// 'send' and 'receive' are supposed to be used only for secret signals
	public void send(OutputStream os) {
		try {
			os.write(bytes);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// 'send' and 'receive' are supposed to be used only for secret signals
	public static GCSignal receive(Network channel) {
		byte[] b = channel.readBytes(len);
		return new GCSignal(b);
	}

	public static void receive(Network channel, GCSignal s) {
		if(s.bytes == null)
			s.bytes = new byte[len];
		channel.readBytes(s.bytes);
	}

	
	@Override
	public boolean equals(Object lb) {
		if (this == lb)
			return true;
		else if (lb instanceof GCSignal)
			return Arrays.equals(bytes, ((GCSignal) lb).bytes);
		else
			return false;
	}

	public String toHexStr() {
		StringBuilder str = new StringBuilder();
		for (byte b : bytes)
			str.append(Integer.toHexString(b & 0xff));
		return str.toString();
	}
}
