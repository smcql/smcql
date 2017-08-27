package harness;

import org.junit.Assert;

import com.oblivm.backend.circuits.arithmetic.FloatLib;
import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Mode;
import com.oblivm.backend.flexsc.PMCompEnv;
import com.oblivm.backend.gc.BadLabelException;
import com.oblivm.backend.util.EvaRunnable;
import com.oblivm.backend.util.GenRunnable;
import com.oblivm.backend.util.Utils;

public class TestFloat extends TestHarness {
	public static int widthV = 24, widthP = 8;

	public static abstract class Helper {
		double a, b;

		public Helper(double a, double b) {
			this.b = b;
			this.a = a;
		}

		public abstract<T> T[] secureCompute(T[] a, T[] b, FloatLib<T> env);
		public abstract double plainCompute(double a, double b);
	}

	
	public static class GenRunnableTestFloat<T> extends GenRunnable<T> {
		public double andgates;
		public double encs;
		FloatLib<T> lib;
		double z;
		Helper h;
		public GenRunnableTestFloat(Helper h) {
			this.h = h;
			verbose = false;
		}
		
		T[] a;
		T[] b;
		T[] d;
		@Override
		public void prepareInput(CompEnv<T> gen) {
			lib = new FloatLib<T>(gen, widthV, widthP);
			a = lib.inputOfAlice(h.a);
			b = lib.inputOfBob(0);
		}

		@Override
		public void secureCompute(CompEnv<T> gen) {
			d = h.secureCompute(a, b, lib);
		}

		@Override
		public int[] prepareOutput(CompEnv<T> gen) throws BadLabelException {
			z = Utils.toFloat(gen.outputToAlice(d), widthV, widthP);
			if (m == Mode.COUNT) {
					((PMCompEnv) gen).statistic.finalize();
					andgates = ((PMCompEnv) gen).statistic.andGate;
					encs = ((PMCompEnv) gen).statistic.NumEncAlice;
				System.out.println(andgates + " " + encs);
			} else {
				double error = 0;
				if (z != 0)
					error = Math.abs((h.plainCompute(h.a, h.b) - z) / z);
				else
					error = Math.abs((h.plainCompute(h.a, h.b) - z));

				if (Math.abs((h.plainCompute(h.a, h.b) - z) / z) > 1E-6)
					System.out.print(error + " " + z + " "
							+ h.plainCompute(h.a, h.b) + " " + h.a + " " + h.b
							+ "\n");
				Assert.assertTrue(error <= 1E-3);
			}
			return new int[0];
		}
	}

	public static class EvaRunnableTestFloat<T> extends EvaRunnable<T> {		
		EvaRunnableTestFloat(Helper h) {
			this.h = h;
			verbose = false;
		}

		Helper h;
		T[] a;
		T[] b;
		T[] d;
		FloatLib<T> lib;
		@Override
		public void prepareInput(CompEnv<T> env) {
			lib = new FloatLib<T>(env, widthV, widthP);
			a = lib.inputOfAlice(0);
			b = lib.inputOfBob(h.b);
		}

		@Override
		public void secureCompute(CompEnv<T> env) {
			d = h.secureCompute(a, b, lib);
		}

		@Override
		public int[] prepareOutput(CompEnv<T> env) throws BadLabelException {
			env.outputToAlice(d);
			return new int[0];
		}
	}
	static public <T>void runThreads(Helper h) throws Exception {
		GenRunnable<T> gen = new GenRunnableTestFloat<T>(h);
		gen.setParameter(m, 54321);
		EvaRunnable<T> env = new EvaRunnableTestFloat<T>(h);
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