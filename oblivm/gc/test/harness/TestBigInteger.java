package harness;

import java.math.BigInteger;

import org.junit.Assert;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Mode;
import com.oblivm.backend.flexsc.PMCompEnv;
import com.oblivm.backend.gc.BadLabelException;
import com.oblivm.backend.util.EvaRunnable;
import com.oblivm.backend.util.GenRunnable;
import com.oblivm.backend.util.Utils;



public class TestBigInteger extends TestHarness{
	public static final int LENGTH = 799;
	final static int RANGE = LENGTH;
	public static abstract class Helper {
		BigInteger intA, intB;
		boolean[] a;
		boolean[] b;
		public Helper(BigInteger aa, BigInteger bb) {
			intA = aa;
			intB = bb;

			a = Utils.fromBigInteger(aa, RANGE);
			b = Utils.fromBigInteger(bb, RANGE);
		}
		public abstract <T>T[] secureCompute(T[] Signala, T[] Signalb, CompEnv<T> e);
		public abstract BigInteger plainCompute(BigInteger x, BigInteger y);
	}
	
	public static class GenRunnableTestBigInteger<T> extends GenRunnable<T> {
		public double andgates;
		public double encs;
		
		boolean[] z;
		Helper h;
		public GenRunnableTestBigInteger(Helper h) {
			this.h = h;
			verbose = false;
		}
		
		T[] a;
		T[] b;
		T[] d;
		@Override
		public void prepareInput(CompEnv<T> gen) {
			a = gen.inputOfAlice(h.a);
			b = gen.inputOfBob(new boolean[LENGTH]);
		}

		@Override
		public void secureCompute(CompEnv<T> gen) {
			d = h.secureCompute(a, b, gen);
		}

		@Override
		public int[] prepareOutput(CompEnv<T> gen) throws BadLabelException {
			z = gen.outputToAlice(d);
			if (m == Mode.COUNT) {
					((PMCompEnv) gen).statistic.finalize();
					andgates = ((PMCompEnv) gen).statistic.andGate;
					encs = ((PMCompEnv) gen).statistic.NumEncAlice;
				System.out.println(andgates + " " + encs);
			} else {
				
//				System.out.println(Arrays.toString( Utils.fromBigInteger(h.plainCompute(h.intA, h.intB), z.length)));
//				System.out.println(Arrays.toString(Utils.fromBigInteger(Utils.toBigInteger(z), z.length)));
				
				Assert.assertEquals(h.plainCompute(h.intA, h.intB), Utils.toBigInteger(z));
			}
			return new int[0];
		}
	}

	public static class EvaRunnableTestBigInteger<T> extends EvaRunnable<T> {		
		EvaRunnableTestBigInteger(Helper h) {
			this.h = h;
			verbose = false;
		}

		Helper h;
		T[] a;
		T[] b;
		T[] d;

		@Override
		public void prepareInput(CompEnv<T> env) {
			a = env.inputOfAlice(new boolean[LENGTH]);
			b = env.inputOfBob(h.b);
		}

		@Override
		public void secureCompute(CompEnv<T> env) {
			d = h.secureCompute(a, b, env);
		}

		@Override
		public int[] prepareOutput(CompEnv<T> env) throws BadLabelException {
			env.outputToAlice(d);
			return new int[0];
		}
	}
	static public <T>void runThreads(Helper h) throws Exception {
		GenRunnable<T> gen = new GenRunnableTestBigInteger<T>(h);
		gen.setParameter(m, 54321);
		EvaRunnable<T> env = new EvaRunnableTestBigInteger<T>(h);
		env.setParameter(m, "localhost", 54321);
		Thread tGen = new Thread(gen);
		Thread tEva = new Thread(env);
		tGen.start();
		Thread.sleep(5);
		tEva.start();
		tGen.join();
		tEva.join();
	}
}