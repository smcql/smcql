package com.oblivm.backend.rand;

import java.security.SecureRandom;
import java.security.SecureRandomSpi;

/**
 * This class defines the Service Provider for the ISAAC algorithm.
 * 
 * @author Daniel Berlin
 */
public class ISAACEngine extends SecureRandomSpi {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4291616083094498338L;
	/**
	 * 
	 */

	private ISAACAlgorithm isaac;
	private byte[]         remainder;
	private int            remCount = 0;
	
	/**
	 * Creates a new instance and seeds it with random data obtained from
	 * <code>java.security.SecureRandom</code>'s <code>getSeed()</code> method.
	 */
	public ISAACEngine () {
		byte[] temp = new byte[1024];//SecureRandom.getSeed (1024);
		new SecureRandom().nextBytes(temp);
		this.isaac = new ISAACAlgorithm (ISAACEngine.packToIntArray (temp));
	}
	
	/**
	 * Returns the given number of seed bytes computed using the ISAAC algorithm.<br>
	 * It just calls <code>engineNextBytes()</code> internally.<br>
	 * This call may be used to seed other random number generators.
	 * 
	 * @param numBytes The number of seed bytes to generate.
	 * @return The seed bytes.
	 */
	public byte[] engineGenerateSeed (int numBytes) {
		byte[] seed = new byte[numBytes];
		this.engineNextBytes (seed);
		
		return (seed);
	}
	
	/**
	 * Generates a user-specified number of random bytes.
	 * 
	 * @param bytes The array to fill with random bytes.
	 */
	public void engineNextBytes (byte[] bytes) {
		int    index  = 0;
		int    todo;
		byte[] output = this.remainder;
		
		// First use remainder from last time
		int rC = this.remCount;
		if (rC > 0) {
			todo = (bytes.length - index) < (4 - rC) ? (bytes.length - index) : (4 - rC);
			for (int i = 0; i < todo; i++, rC++)
				bytes[i] = output[rC];
			
			this.remCount += todo;
			index         += todo;
		}
		
		// If we need more bytes, get them
		while (index < bytes.length) {
			output = ISAACEngine.toByteArray (this.isaac.nextInt ());
			
			todo = (bytes.length - index) > 4 ? 4 : (bytes.length - index);
			for (int i = 0; i < todo; i++, index++)
				bytes[index] = output[i];
			
			this.remCount += todo;
		}
		
		// Store remainder for next time
		this.remainder = output;
		this.remCount %= 4;
	}
	
	/**
	 * Reseeds this random object.
	 * The given seed supplements, rather than replaces, the existing seed.
	 * 
	 * @param seed The seed.
	 */
	public void engineSetSeed (byte[] seed) {
		this.isaac.supplementSeed (ISAACEngine.packToIntArray (seed));
	}
	
	// ====================================
	// ===== private "helper" methods =====
	// ====================================
	
	private static final int[] mask = new int[] { 0xFF000000, 0x00FF0000, 0x0000FF00, 0x000000FF };
	
	/**
	 * Returns a byte array containing the two's-complement representation of the integer.<br>
	 * The byte array will be in big-endian byte-order with a fixes length of 4
	 * (the least significant byte is in the 4th element).<br>
	 * <br>
	 * <u>Example:</u><br>
	 * <code>toByteArray(258)</code> returns { 0, 0, 1, 2 },<br>
	 * <code>BigInteger.valueOf(258).toByteArray()</code> returns { 1, 2 }.
	 *
	 * @param intValue The integer to be converted.
	 * @return The byte array of length 4.
	 */
	private static byte[] toByteArray (final int intValue) {
		int    byteNum   = (40 - numberOfLeadingZeros (intValue < 0 ? ~intValue : intValue)) / 8;
		byte[] byteArray = new byte[4];
		
		for (int i = 0; i < byteNum; i++)
			byteArray[3 - i] = (byte) (intValue >>> (i * 8));
		
		return (byteArray);
	}
	
	/**
	 * Returns the number of zero bits preceding the highest-order ("leftmost") one-bit in the two's
	 * complement binary representation of the specified integer value.
	 * 
	 * @return The number of zero bits preceding the highest-order one-bit in the two's complement
	 *         binary representation of the specified integer value, or 32 if the value is equal to zero.
	 */
	private static int numberOfLeadingZeros (int intValue) {
		if (intValue == 0)
			return (32);
		
		int num = 1;
		for (int i = 16, j = 16; j > 1; i += j) {
			if (intValue >>> i == 0) { num += j; intValue <<= j; }
			j /= 2;
		}
		num -= intValue >>> 31;
		
		return (num);
	}
	
	/**
	 * Creates an array of integers and "packs" the bytes from the byte array into it.<br>
	 * This method returns an integer array of length zero if <code>ba</code> is <code>null</code>.<br>
	 * <br>
	 * <u>Example:</u><br>
	 * <code>packToIntArray(new byte[] { 0x1, 0x2, 0x3, 0x4, 0x5 })</code> returns an integer array<br>
	 * containing the values <code>0x01020304</code> and <code>0x00000005</code>.
	 * 
	 * @param ba The byte array, may be <code>null</code>.
	 * @return An array of integers containing the "packed" byte(s).
	 */
	private static int[] packToIntArray (final byte[] ba) {
		if (ba == null)
			return (new int[0]);
		
		int[] ia = new int[(ba.length + 3) / 4];
		
		for (int i = 0; i < ia.length; i++) {
			int restLen = Math.min (4, ba.length - i * 4);
			
			for (int b = 0; b < restLen; b++)
				ia[i] |= (ba[b + i * 4] << ((restLen - b - 1) * 8)) & mask[b + 4 - restLen];
		}
		
		return (ia);
	}
}
