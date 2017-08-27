package testlibs;

import harness.TestBigInteger;
import harness.TestHarness;
import harness.Test_2Input1Output;

import java.math.BigInteger;
import java.util.Random;

import org.junit.Test;

import com.oblivm.backend.circuits.CircuitLib;
import com.oblivm.backend.circuits.arithmetic.IntegerLib;
import com.oblivm.backend.flexsc.CompEnv;


public class TestCircuitLib extends TestHarness {
	Random rnd = new Random();
	
	@Test
	public void testHammingDistance() throws Exception {
		
		for (int i = 0; i < testCases; i++) {
			BigInteger a = new BigInteger(TestBigInteger.LENGTH, rnd);
			BigInteger b = new BigInteger(TestBigInteger.LENGTH, rnd);
			TestBigInteger.runThreads(new TestBigInteger.Helper(a, b) {
				public <T>T[] secureCompute(T[] Signala, T[] Signalb, CompEnv<T> e) {
					return new IntegerLib<T>(e).hammingDistance(Signala, Signalb);}

				public BigInteger plainCompute(BigInteger x, BigInteger y) {
					BigInteger rb = x.xor(y);
					BigInteger res = new BigInteger("0");
					for(int i = 0; i < rb.bitLength(); ++i) {
							if( rb.testBit(i) )
								res = res.add(new BigInteger("1"));
					}
					return res;
					}
			});
		}
	}
	
	@Test
	public void testRightPublicShift() throws Exception {
		

		for (int i = 0; i < testCases; i++) {
			final int shift = Math.abs(rnd.nextInt() % 32);
			Test_2Input1Output.runThreads(new Test_2Input1Output.Helper(rnd.nextInt(1 << 30), 0) {
				public <T>T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e) {
					return new IntegerLib<T>(e).rightPublicShift(Signala,
							shift);
				}

				public int plainCompute(int x, int y) {
					return x >> shift;
				}
			});
		}
	}

	@Test
	public void testConditionalLeftPublicShift() throws Exception {
		

		for (int i = 0; i < testCases; i++) {
			final int shift = Math.abs(rnd.nextInt() % 32);
			Test_2Input1Output.runThreads(new Test_2Input1Output.Helper(rnd.nextInt(1 << 30), 0) {
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e) {
					IntegerLib<T> lib = new IntegerLib<T>(e);
					return lib.conditionalLeftPublicShift(Signala, shift,
							lib.SIGNAL_ONE);
				}

				public int plainCompute(int x, int y) {
					return x << shift;
				}
			});

			Test_2Input1Output.runThreads(new Test_2Input1Output.Helper(rnd.nextInt(1 << 30), 0) {
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e) {
					IntegerLib<T> lib = new IntegerLib<T>(e);
					return lib.conditionalLeftPublicShift(Signala, shift,
							lib.SIGNAL_ZERO);
				}

				public int plainCompute(int x, int y) {
					return x;
				}
			});

		}
	}

	@Test
	public void testConditionalRightPublicShift() throws Exception {
		

		for (int i = 0; i < testCases; i++) {
			final int shift = Math.abs(rnd.nextInt() % 32);
			Test_2Input1Output.runThreads(new Test_2Input1Output.Helper(rnd.nextInt(1 << 30), 0) {
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e) {
					IntegerLib<T> lib = new IntegerLib<T>(e);
					return lib.conditionalRightPublicShift(Signala, shift,
							lib.SIGNAL_ONE);
				}

				public int plainCompute(int x, int y) {
					return x >> shift;
				}
			});

			Test_2Input1Output.runThreads(new Test_2Input1Output.Helper(rnd.nextInt(1 << 30), 0) {
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e) {
					IntegerLib<T> lib = new IntegerLib<T>(e);
					return lib.conditionalRightPublicShift(Signala, shift,
							lib.SIGNAL_ZERO);
				}

				public int plainCompute(int x, int y) {
					return x;
				}
			});

		}
	}

	@Test
	public void testLeftPrivateShift() throws Exception {
		

		for (int i = 0; i < testCases; i++) {
			int shift = rnd.nextInt(1 << 5);
			Test_2Input1Output.runThreads(new Test_2Input1Output.Helper(rnd.nextInt(1 << 30), shift) {
				public <T>T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e) {
					return new IntegerLib<T>(e).leftPrivateShift(Signala,
							Signalb);
				}

				public int plainCompute(int x, int y) {
					return x << y;
				}
			});
		}
	}

	@Test
	public void testLeftPublicShift() throws Exception {
		

		for (int i = 0; i < testCases; i++) {
			final int shift = Math.abs(rnd.nextInt() % 32);
			Test_2Input1Output.runThreads(new Test_2Input1Output.Helper(rnd.nextInt(1 << 30), 0) {
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e) {
					return new IntegerLib<T>(e).leftPublicShift(Signala,
							shift);
				}

				public int plainCompute(int x, int y) {
					return x << shift;
				}
			});
		}
	}

	public int commonPrefix(int l1, int l2) {
		int res = 0;
		int diff = l1 ^ l2;
		if ((diff & 0xFFFF0000) == 0) {
			res += 16;
			diff <<= 16;
		}
		if ((diff & 0xFF000000) == 0) {
			res += 8;
			diff <<= 8;
		}
		if ((diff & 0xF0000000) == 0) {
			res += 4;
			diff <<= 4;
		}
		if ((diff & 0xC0000000) == 0) {
			res += 2;
			diff <<= 2;
		}
		if ((diff & 0x80000000) == 0) {
			res += 1;
			diff <<= 1;
		}
		if (diff == 0) {
			res += 1;
		}
		return res;
	}

	@Test
	public void testLengthOfCommenPrefix() throws Exception {
		

		for (int i = 0; i < testCases; i++) {
			Test_2Input1Output.runThreads(new Test_2Input1Output.Helper(rnd.nextInt(1 << 30), rnd.nextInt(1 << 30)) {
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e) {
					return new IntegerLib<T>(e).lengthOfCommenPrefix(
							Signala, Signalb);
				}

				public int plainCompute(int x, int y) {
					return commonPrefix(x, y);
				}
			});
		}
	}

	@Test
	public void testRightPrivateShift() throws Exception {
		

		for (int i = 0; i < testCases; i++) {
			int shift = rnd.nextInt(1 << 5);
			Test_2Input1Output.runThreads(new Test_2Input1Output.Helper(rnd.nextInt(1 << 30), shift) {
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e) {
					return new IntegerLib<T>(e).rightPrivateShift(
							Signala, Signalb);
				}

				public int plainCompute(int x, int y) {
					return x >> y;
				}
			});
		}
	}

	@Test
	public void testAllCases() throws Exception {

		for (int i = 0; i < 1; i++) {
			Test_2Input1Output.runThreads(new Test_2Input1Output.Helper(0b1100, 0b1010) { // This particular pair of
				// inputs exhausts 4
				// possible inputs,
				// excluding selection
				// signal
				public <T>T[] secureCompute(T[] a, T[] b, CompEnv<T> e) {
					CircuitLib<T> lib = new CircuitLib<T>(e);
					return lib.mux(a, b, lib.SIGNAL_ONE);
				}

				public int plainCompute(int x, int y) {
					return y;
				}
			});
		}

		for (int i = 0; i < testCases; i++) {
			Test_2Input1Output.runThreads(new Test_2Input1Output.Helper(0b1100, 0b1010) { // This particular pair of
				// inputs exhausts 4
				// possible inputs,
				// excluding selection
				// signal
				public<T> T[] secureCompute(T[] a, T[] b, CompEnv<T> e) {
					CircuitLib<T> lib = new CircuitLib<T>(e);
					return lib.mux(a, b, lib.SIGNAL_ZERO);
				}

				public int plainCompute(int x, int y) {
					return x;
				}
			});
		}
	}
}