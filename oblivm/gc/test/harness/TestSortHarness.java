package harness;

import org.junit.Assert;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Mode;
import com.oblivm.backend.flexsc.PMCompEnv;
import com.oblivm.backend.gc.BadLabelException;
import com.oblivm.backend.util.EvaRunnable;
import com.oblivm.backend.util.GenRunnable;
import com.oblivm.backend.util.Utils;


public class TestSortHarness extends TestHarness {
	public static abstract class Helper {
		int[] intA;
		boolean[][] a;
		Mode m;

		public Helper(int[] aa) {
			intA = aa;
			a = new boolean[aa.length][32];
			for (int i = 0; i < intA.length; ++i)
				a[i] = Utils.fromInt(aa[i], 32);
		}

		public abstract<T> T[][] secureCompute(T[][] Signala, CompEnv<T> e);

		public abstract int[] plainCompute(int[] intA2);
	}

	public static class GenRunnableTestSort<T> extends GenRunnable<T> {
		public double andgates;
		public double encs;

		boolean[][] z;
		Helper h;
		public GenRunnableTestSort(Helper h) {
			this.h = h;
		}

		T[][] a;
		T[][] d;
		@Override
		public void prepareInput(CompEnv<T> gen) {
			a = gen.newTArray(h.a.length, h.a[0].length);// new
			// T[h.a.length][h.a[0].length];
			for (int i = 0; i < a.length; ++i)
				a[i] = gen.inputOfBob(new boolean[32]);
		}

		@Override
		public void secureCompute(CompEnv<T> gen) {
			d = h.secureCompute(a, gen);		
		}

		@Override
		public int[] prepareOutput(CompEnv<T> gen) throws BadLabelException {
			z = new boolean[d.length][d[0].length];
			for (int i = 0; i < d.length; i++)
				z[i] = gen.outputToAlice(d[i]);
			if (m == Mode.COUNT) {
				((PMCompEnv) gen).statistic.finalize();
				andgates = ((PMCompEnv) gen).statistic.andGate;
				encs = ((PMCompEnv) gen).statistic.NumEncAlice;
				System.out.println(andgates + " " + encs);
			} else {
				boolean asc = true;
				boolean dec = true;
				for (int i = 0; i < z.length - 1; ++i) {
//					System.out.println(Utils.toInt(z[i]));
					asc = asc && Utils.toInt(z[i]) <= Utils.toInt(z[i + 1]);
					dec = dec && Utils.toInt(z[i]) >= Utils.toInt(z[i + 1]);
				}
				Assert.assertTrue(asc || dec);
				
				
			}
			return new int[0];
		}
	}

	public static class EvaRunnableTestSort<T> extends EvaRunnable<T> {		
		EvaRunnableTestSort(Helper h) {
			this.h = h;
		}

		Helper h;
		T[][] a;
		T[][] d;

		@Override
		public void prepareInput(CompEnv<T> env) {
			a = env.newTArray(h.a.length, h.a[0].length);
			for (int i = 0; i < a.length; ++i)
				a[i] = env.inputOfBob(h.a[i]);
		}

		@Override
		public void secureCompute(CompEnv<T> env) {
			d = h.secureCompute(a, env);
		}

		@Override
		public int[] prepareOutput(CompEnv<T> env) throws BadLabelException {
			for (int i = 0; i < d.length; i++)
				env.outputToAlice(d[i]);
			return new int[0];
		}
	}

	static public <T>void runThreads(Helper h) throws Exception {
		GenRunnable<T> gen = new GenRunnableTestSort<T>(h);
		gen.setParameter(m, 54321);
		EvaRunnable<T> env = new EvaRunnableTestSort<T>(h);
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