package com.oblivm.backend.gc.halfANDs;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import com.oblivm.backend.gc.GCSignal;

final class Garbler implements java.io.Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -2914004105088472805L;
	/**
	 * 
	 */

	private MessageDigest sha1 = null;
	Garbler() {
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
	
	byte[] bArray = new byte[GCSignal.len+9];
	
	public GCSignal hash(GCSignal lb, long k, boolean b) {
		ByteBuffer buffer = ByteBuffer.allocate(GCSignal.len+9); 
		buffer.clear();
		sha1.update(buffer.put(lb.bytes).putLong(k).put(b?(byte)1:(byte)0));
		bArray = buffer.array();
		return GCSignal.newInstance(sha1.digest());
	}
}