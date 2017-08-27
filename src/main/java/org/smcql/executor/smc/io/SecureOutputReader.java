package org.smcql.executor.smc.io;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

import org.smcql.db.data.QueryTable;
import org.smcql.db.data.Tuple;
import org.smcql.type.SecureRelRecordType;
import org.smcql.executor.smc.BasicSecureQueryTable;
import org.smcql.executor.smc.SecureBufferPool;
import org.smcql.executor.smc.SlicedSecureQueryTable;

import com.oblivm.backend.gc.GCSignal;

public class SecureOutputReader {

	
	
	public static QueryTable assembleOutput(BasicSecureQueryTable alice, BasicSecureQueryTable bob, SecureRelRecordType schema) throws Exception {
		GCSignal R = alice.R;
		
		if (schema == null)
			schema = alice.schema;
		
		assert(R != null);
		
		int length = decodeInt(alice.nonNullLength, bob.nonNullLength, R);
		
		//get schema from the honest broker (embedded in the smcqlqueryexecutor
		return decodeSignals(alice.payload, bob.payload, length * schema.size(), R, schema);	
	}
	
	public static QueryTable assembleOutput(SlicedSecureQueryTable alice, SlicedSecureQueryTable bob, SecureRelRecordType schema) throws Exception {
		GCSignal R = alice.R;
		
		if (schema == null)
			schema = alice.schema;
		
		assert(R != null);
		
		QueryTable result = new QueryTable(schema);
		for (Tuple t : alice.slices.keySet()) {
			BasicSecureQueryTable a = alice.slices.get(t);
			BasicSecureQueryTable b = bob.slices.get(t);
			int length = decodeInt(a.nonNullLength, b.nonNullLength, R);
			QueryTable sliceResult = decodeSignals(a.payload, b.payload, length * schema.size(), R, schema);
			result.addTuples(sliceResult);
		}
		
		return result;	
	}
	
	
	private static GCSignal[] readSignals(String filename) throws Exception {
		byte[] data = readFile(filename);
		int signalCount = data.length / 10;
		GCSignal[] output = new GCSignal[signalCount];

		
		assert(data.length % 10 == 0); 
		
		for(int i = 0; i < signalCount; ++i) {
			byte[] signalBytes = Arrays.copyOfRange(data, i*10, (i+1)*10);
			output[i] = new GCSignal(signalBytes);
		}
		
		return output;

	}
	
	static byte[] readFile(String filename) throws IOException {
		Path path = Paths.get(filename);
	    return Files.readAllBytes(path);
	}
	
	// Alice data: R|# of elements|data array
	// Bob data: #of elements|data array
	public static QueryTable assembleOutput(String alice, String bob, SecureRelRecordType schema) throws Exception{
		GCSignal[] aData = readSignals(alice);
		GCSignal[] bData = readSignals(bob);

		GCSignal R = aData[0];
		int lengthBits = SecureBufferPool.lengthBits;
		
		int bitsToCopy = aData.length - lengthBits - 1;

		GCSignal[] aLength = new GCSignal[lengthBits];
		GCSignal[] bLength = new GCSignal[lengthBits];
		GCSignal[] aBits = new GCSignal[bitsToCopy];
		GCSignal[] bBits = new GCSignal[bitsToCopy];
		
		System.arraycopy(aData, 1, aLength, 0, lengthBits);
		System.arraycopy(bData, 0, bLength, 0, lengthBits);

		System.arraycopy(aData, lengthBits + 1, aBits, 0, bitsToCopy);
		System.arraycopy(bData, lengthBits, bBits, 0, bitsToCopy);

		int length = decodeInt(aLength, bLength, R);
		
		return decodeSignals(aBits, bBits, length * schema.size(), R, schema);
	}

	

	public static int decodeInt(GCSignal[] aData, GCSignal[] bData, GCSignal R) throws Exception {
		boolean[] bits = new boolean[32];
		int value = 0;
		assert(aData.length == bData.length && aData.length == 32);
		
		
		for(int i = 0; i < 32; ++i) {
			GCSignal aBit = aData[i];
			GCSignal bBit = bData[i];

			if(aBit.equals(bBit)) {
				bits[i] = false;
			}
			else if((R.xor(aBit)).equals(bBit)) {
				bits[i] = true;
			}
			else {
				throw new Exception("Bad label in output!");
			}
		}

		bits = Tuple.reverseBits(bits);
		for (boolean b : bits)
			value = (value << 1) | (b ? 1 : 0);
		
		return value;
	}
	
	public static long decodeLong(GCSignal[] aData, GCSignal[] bData, GCSignal R) throws Exception {
		boolean[] bits = new boolean[64];
		long value = 0L;
		assert(aData.length == bData.length && aData.length == 64);
		
		for(int i = 0; i < aData.length; ++i) {
			GCSignal aBit = aData[i];
			GCSignal bBit = bData[i];
			if(aBit.equals(bBit)) {
				bits[i] = false;
			}
			else if((R.xor(aBit)).equals(bBit)) {
				bits[i] = true;
			}
			else {
				throw new Exception("Bad label in output!");
			}
		}

		bits = Tuple.reverseBits(bits);
		for (boolean b : bits)
			value = (value << 1) | (b ? 1 : 0);
		
		return value;
	}
	
	
	// elements = # of bits to read
	public static QueryTable decodeSignals(GCSignal[] aData, GCSignal[] bData, int elements, GCSignal R, SecureRelRecordType schema) throws Exception {		
		assert(aData.length >= elements);
		assert(bData.length >= elements);
		
		
		boolean[] plaintext = new boolean[elements];
		
		for(int i = 0; i < elements; ++i) {
			GCSignal aBit = aData[i];
			GCSignal bBit = bData[i];
			if(aBit.equals(bBit)) {
				plaintext[i] = false;
			}
			else if((R.xor(aBit)).equals(bBit)) {
				plaintext[i] = true;
			}
			else {
				throw new Exception("Bad label in output!");
			}
		}
		
		return new QueryTable(plaintext, schema);

	}
}
