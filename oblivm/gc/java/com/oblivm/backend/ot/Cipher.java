//Copyright (C) 2014 by Yan Huang <yhuang@virginia.edu>

package com.oblivm.backend.ot;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

import com.oblivm.backend.gc.GCSignal;

public final class Cipher {
	private static final int unitLength = 160; // SHA-1 has 160-bit output.
	private static final int bytesPerUnit = (unitLength-1)/8+1; // SHA-1 has 20 bytes.

	private MessageDigest sha1;

	public Cipher() {
		try {
			sha1 = MessageDigest.getInstance("SHA-1");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	BigInteger a = BigInteger.ONE;
	public BigInteger encrypt(byte[] key, BigInteger msg, int msgLength) {
		assert (msgLength <= unitLength) : "Message longer than hash block width.";
		return msg.xor(getPaddingOfLength(key, msgLength));
	}

	public BigInteger decrypt(byte[] key, BigInteger cph, int cphLength) {
		assert (cphLength > unitLength) : "Ciphertext longer than hash block width.";
		return cph.xor(getPaddingOfLength(key, cphLength));
	}

	private BigInteger getPaddingOfLength(byte[] key, int padLength) {
		
		byte[] pad = new byte[(padLength-1)/8+1];
		byte[] tmp;
		int i;
		for (i = 0; i < (pad.length-1)/bytesPerUnit; i++) {
			assert (i < 128) : "Padding is unexpectedly long.";
			sha1.update(key);
			sha1.update((byte)i);
			tmp = sha1.digest();
			System.arraycopy(tmp, 0, pad, i*bytesPerUnit, bytesPerUnit);
		}
		
		return new BigInteger(1, pad);
	}

	public BigInteger encrypt(int j, byte[] key, BigInteger msg,
			int msgLength) {
		return msg.xor(getPaddingOfLength(j, key, msgLength));
	}

	public BigInteger decrypt(int j, byte[] key, BigInteger cph,
			int cphLength) {
		return cph.xor(getPaddingOfLength(j, key, cphLength));
	}

	private BigInteger getPaddingOfLength(int j, byte[] key, int padLength) {
		sha1.update(ByteBuffer.allocate(4).putInt(j).array());
		sha1.update(key);

		byte[] pad = new byte[(padLength-1)/8+1];
		byte[] tmp;
		tmp = sha1.digest();
		int i;
		for (i = 0; i < (pad.length-1)/bytesPerUnit; i++) {
			System.arraycopy(tmp, 0, pad, i*bytesPerUnit, bytesPerUnit);
			sha1.update(tmp);
			tmp = sha1.digest();
		}
		System.arraycopy(tmp, 0, pad, i*bytesPerUnit, pad.length-i*bytesPerUnit);
		
		return new BigInteger(1, pad);
	}
	
	public GCSignal enc(GCSignal key, GCSignal m, int k) {
		return getPadding(key, k).xor(m);
	}

	public GCSignal dec(GCSignal key, GCSignal c, int k) {
		return getPadding(key, k).xor(c);
	}
	
	private GCSignal getPadding(GCSignal key, int k) {
        sha1.update(key.bytes);
        sha1.update(ByteBuffer.allocate(4).putInt(k).array());
        GCSignal ret = GCSignal.newInstance(sha1.digest());
        return ret;
    }
}