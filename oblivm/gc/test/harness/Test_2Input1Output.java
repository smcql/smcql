package harness;

import org.junit.Assert;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Mode;
import com.oblivm.backend.flexsc.PMCompEnv;
import com.oblivm.backend.gc.BadLabelException;
import com.oblivm.backend.util.EvaRunnable;
import com.oblivm.backend.util.GenRunnable;
import com.oblivm.backend.util.Utils;

public class Test_2Input1Output extends TestHarness {

	public static abstract class Helper {
		int intA, intB;
		boolean[] a;
		boolean[] b;

		public Helper(int aa, int bb) {
			intA = aa;
			intB = bb;

			a = Utils.fromInt(aa, 32);
			b = Utils.fromInt(bb, 32);
		}

		public abstract<T> T[] secureCompute(T[] Signala, T[] Signalb, CompEnv<T> e);

		public abstract int plainCompute(int x, int y);
	}

	public static class GenRunnableTest2O1I<T> extends GenRunnable<T> {
		public double andgates;
		public double encs;
		
		boolean[] z;
		Helper h;
		public GenRunnableTest2O1I(Helper h) {
			verbose = false;
			this.h = h;
		}
		
		T[] a;
		T[] b;
		T[] d;
		@Override
		public void prepareInput(CompEnv<T> gen) {
			a = gen.inputOfAlice(h.a);
			b = gen.inputOfBob(new boolean[32]);
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
				Assert.assertEquals(h.plainCompute(h.intA, h.intB),
						Utils.toSignedInt(z));
			}
			return new int[0];
		}
	}

	public static class EvaRunnableTest2O1I<T> extends EvaRunnable<T> {		
		EvaRunnableTest2O1I(Helper h) {
			this.h = h;
			verbose = false;
		}

		Helper h;
		T[] a;
		T[] b;
		T[] d;

		@Override
		public void prepareInput(CompEnv<T> env) {
			a = env.inputOfAlice(new boolean[32]);
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
		GenRunnable<T> gen = new GenRunnableTest2O1I<T>(h);
		gen.setParameter(m, 54321);
		EvaRunnable<T> env = new EvaRunnableTest2O1I<T>(h);
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