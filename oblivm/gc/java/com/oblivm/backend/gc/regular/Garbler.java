package com.oblivm.backend.gc.regular;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import com.oblivm.backend.gc.GCSignal;

final class Garbler implements java.io.Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7688870622764154448L;
	/**
	 * 
	 */

	private MessageDigest sha1 = null;
	Garbler() {}

	public void enc(GCSignal lb0, GCSignal lb1, long k, GCSignal m, GCSignal ret) {
		getPadding(lb0, lb1, k, ret);
		GCSignal.xor(ret, m, ret);
	}

	public void dec(GCSignal lb0, GCSignal lb1, long k, GCSignal c, GCSignal ret) {
		getPadding(lb0, lb1, k, ret);
		GCSignal.xor(ret, c, ret);
	}
	
	byte[] bArray = new byte[GCSignal.len*2+8];
	private void getPadding(GCSignal lb0, GCSignal lb1, long k, GCSignal ret) {
		ByteBuffer buffer = ByteBuffer.allocate(GCSignal.len*2+8); 
		buffer.clear();
		try {
			sha1 = MessageDigest.getInstance("SHA-1");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		sha1.update((buffer.put(lb0.bytes).put(lb1.bytes).putLong(k)));
		bArray = buffer.array();
		System.arraycopy(sha1.digest(), 0, ret.bytes, 0, GCSignal.len);
	}
}
