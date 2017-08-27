package oram;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Party;
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.oram.SecureArray;

import harness.TestHarness;;

public class SecureArrayTest extends TestHarness {
	@Test
	public void testSerialization() throws Exception {
		String filename = "tmp/test_secure_output.ser";
		@SuppressWarnings("unchecked")
		CompEnv<GCSignal> env = CompEnv.getEnv(m, Party.Alice, null);
		SecureArray<GCSignal> arr = new SecureArray<GCSignal>(env, 10, 10);
		arr.serializeToDisk(filename);
		
		@SuppressWarnings("unchecked")
		SecureArray<GCSignal> result = (SecureArray<GCSignal>) SecureArray.deserializeFromDisk(filename);
		
		assertEquals(arr.serializeToString(), result.serializeToString());
	}
}
