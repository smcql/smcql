package testlibs;

import harness.TestHarness;
import harness.TestSortHarness;
import harness.TestSortHarness.Helper;

import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import com.oblivm.backend.circuits.BitonicSortLib;
import com.oblivm.backend.flexsc.CompEnv;

public class TestBitonicSortLib extends TestHarness {

	@Test
	public void testAllCases() throws Exception {
		Random rnd = new Random();
		for (int i = 0; i < 10; i++) {
			int[] a = new int[1000];
			for (int j = 0; j < a.length; ++j)
				a[j] = rnd.nextInt() % (1 << 30);

			TestSortHarness.runThreads(new Helper(a) {
				public <T>T[][] secureCompute(T[][] Signala, CompEnv<T> e) {
					BitonicSortLib<T> lib = new BitonicSortLib<T>(e);
					lib.sort(Signala, lib.SIGNAL_ONE);
					return Signala;
				}

				@Override
				public int[] plainCompute(int[] intA) {
					Arrays.sort(intA);
					return intA;
				}
			});
		}
	}

}