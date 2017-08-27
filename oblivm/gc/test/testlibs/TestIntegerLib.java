package testlibs;

import harness.TestHarness;
import harness.Test_2Input1Output;
import harness.Test_2Input1Output.Helper;

import java.util.Random;

import org.junit.Test;

import com.oblivm.backend.circuits.arithmetic.IntegerLib;
import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.util.Utils;


public class TestIntegerLib extends TestHarness {

	Random rnd = new Random();

	@Test
	public void testIntAdd() throws Exception {
		for (int i = 0; i < testCases; i++) {
			Test_2Input1Output.runThreads(new Helper(rnd.nextInt() % (1 << 31), rnd.nextInt()
					% (1 << 31)) {
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e)  {
					return new IntegerLib<T>(e).add(Signala, Signalb);
				}

				public int plainCompute(int x, int y) {
					return x + y;
				}
			});
		}
	}

	@Test
	public void testIntSub() throws Exception {

		for (int i = 0; i < testCases; i++) {
			Test_2Input1Output.runThreads(new Helper(rnd.nextInt() % (1 << 30), rnd.nextInt()
					% (1 << 30)) {
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e)  {
					return new IntegerLib<T>(e).sub(Signala, Signalb);
				}

				public int plainCompute(int x, int y) {
					return x - y;
				}
			});
		}
	}

	@Test
	public void testIntDiv() throws Exception {

		for (int i = 0; i < testCases; i++) {
			int b = rnd.nextInt() % (1 << 15);
			int a = 0;//rnd.nextInt() % (1 << 15);
			b = (b == 0) ? 1 : b;
			Test_2Input1Output.runThreads(new Helper(a, b) {
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e)  {
					return new IntegerLib<T>(e).div(Signala, Signalb);
				}

				public int plainCompute(int x, int y) {
					return x / y;
				}
			});
		}
	}

	@Test
	public void testIntMultiplication() throws Exception {

		for (int i = 0; i < testCases; i++) {
			int b = rnd.nextInt() % (1 << 15);
			int a = 0;//rnd.nextInt() % (1 << 15);
			b = (b == 0) ? 1 : b;
			Test_2Input1Output.runThreads(new Helper(a, b) {
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e)  {
					return new IntegerLib<T>(e).multiply(Signala, Signalb);
				}

				public int plainCompute(int x, int y) {
					return x * y;
				}
			});
		}
	}

	@Test
	public void testIntMod() throws Exception {

		for (int i = 0; i < testCases; i++) {
			int b = rnd.nextInt() % (1 << 15);
			b = (b == 0) ? 1 : b;
			Test_2Input1Output.runThreads(new Helper(rnd.nextInt() % (1 << 15), b) {
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e)  {
					return new IntegerLib<T>(e).mod(Signala, Signalb);
				}

				public int plainCompute(int x, int y) {
					return x % y;
				}
			});
		}
	}

	@Test
	public void testIntEq() throws Exception {

		for (int i = 0; i < testCases; i++) {
			Test_2Input1Output.runThreads(new Helper(rnd.nextInt() % (1 << 30), rnd.nextInt()
					% (1 << 30)) {
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e)  {
					T[] res = e.newTArray(1);
					res[0] = new IntegerLib<T>(e).eq(
							Signala, Signalb);
					return res;
				}

				public int plainCompute(int x, int y) {
					return (x == y) ? 1 : 0;
				}
			});
		}

		for (int i = 0; i < testCases; i++) {
			int a = rnd.nextInt(1 << 30);
			Test_2Input1Output.runThreads(new Helper(a, a) {
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e)  {
					T[] res = e.newTArray(1);
					res[0] = new IntegerLib<T>(e).eq(
							Signala, Signalb);
					return res;
				}

				public int plainCompute(int x, int y) {
					return (int) Utils.toSignedInt(new boolean[] { (x == y) });
				}
			});
		}
	}

	@Test
	public void testIntGeq() throws Exception {

		for (int i = 0; i < 1; i++) {
			Test_2Input1Output.runThreads(new Helper(rnd.nextInt() % (1 << 30), rnd.nextInt()
					% (1 << 30)) {
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e)  {
					T[] res = e.newTArray(1);
					res[0] = new IntegerLib<T>(e).geq(
							Signala, Signalb);
					return res;
				}

				public int plainCompute(int x, int y) {
					return (int) Utils.toSignedInt(new boolean[] { (x >= y) });
				}
			});
		}

		for (int i = 0; i < testCases; i++) {
			int a = rnd.nextInt() % (1 << 30);
			Test_2Input1Output.runThreads(new Helper(a, a) {
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e)  {
					T[] res = e.newTArray(1);
					res[0] = new IntegerLib<T>(e).geq(
							Signala, Signalb);
					return res;
				}

				public int plainCompute(int x, int y) {
					return (int) Utils.toSignedInt(new boolean[] { (x >= y) });
				}
			});
		}
	}

	@Test
	public void testIntLeq() throws Exception {

		for (int i = 0; i < testCases; i++) {
			Test_2Input1Output.runThreads(new Helper(rnd.nextInt() % (1 << 30), rnd.nextInt()
					% (1 << 30)) {
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e)  {
					T[] res = e.newTArray(1);
					res[0] = new IntegerLib<T>(e).leq(
							Signala, Signalb);
					return res;
				}

				public int plainCompute(int x, int y) {
					return (int) Utils.toSignedInt(new boolean[] { (x <= y) });
				}
			});
		}

		for (int i = 0; i < testCases; i++) {
			int a = rnd.nextInt(1 << 30);
			Test_2Input1Output.runThreads(new Helper(a, a) {
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e)  {
					T[] res = e.newTArray(1);
					res[0] = new IntegerLib<T>(e).leq(
							Signala, Signalb);
					return res;
				}

				public int plainCompute(int x, int y) {
					return (int) Utils.toSignedInt(new boolean[] { (x <= y) });
				}
			});
		}
	}

	@Test
	public void testIntSqrt() throws Exception {
		for (int i = 0; i < testCases; i++) {
			Test_2Input1Output.runThreads(new Helper(rnd.nextInt(1 << 30), 0) {
				@Override
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e)  {
					return new IntegerLib<T>(e).sqrt(Signala);
				}

				@Override
				public int plainCompute(int x, int y) {
					return (int) Math.sqrt(x);
				}
			});
		}
	}

	@Test
	public void testIntAbs() throws Exception {
		Random rnd = new Random();

		for (int i = 0; i < testCases; i++) {
			Test_2Input1Output.runThreads(new Helper(rnd.nextInt(1 << 30), 0) {
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e)  {
					return new IntegerLib<T>(e).absolute(Signala);
				}

				public int plainCompute(int x, int y) {
					return (int) (Math.abs(x));
				}
			});
		}
	}

	@Test
	public void testIncrementByOne() throws Exception {
		Random rnd = new Random();

		for (int i = 0; i < testCases; i++) {
			Test_2Input1Output.runThreads(new Helper(rnd.nextInt(1 << 30), 0) {
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e)  {
					return new IntegerLib<T>(e).incrementByOne(Signala);
				}

				public int plainCompute(int x, int y) {
					return x + 1;
				}
			});
		}
	}

	@Test
	public void testDecrementByOne() throws Exception {
		Random rnd = new Random();

		for (int i = 0; i < testCases; i++) {
			Test_2Input1Output.runThreads(new Helper(rnd.nextInt(1 << 30), 0) {
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e)  {
					return new IntegerLib<T>(e).decrementByOne(Signala);
				}

				public int plainCompute(int x, int y) {
					return x - 1;
				}
			});
		}
	}
	
	@Test
	public void testLeftPrivateShift() throws Exception {
		Random rnd = new Random();

		for (int i = 0; i < testCases; i++) {
			Test_2Input1Output.runThreads(new Helper(rnd.nextInt(1 << 30), 4) {
				public<T> T[] secureCompute(T[] Signala,
						T[] Signalb, CompEnv<T> e)  {
					return new IntegerLib<T>(e).leftPrivateShift(Signala, Signalb);
				}

				public int plainCompute(int x, int y) {
					return x << y;
				}
			});
		}
	}
}