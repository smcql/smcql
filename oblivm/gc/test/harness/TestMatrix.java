package harness;

import java.util.Random;

import org.junit.Assert;

import com.oblivm.backend.circuits.arithmetic.DenseMatrixLib;
import com.oblivm.backend.circuits.arithmetic.FixedPointLib;
import com.oblivm.backend.circuits.arithmetic.FloatLib;
import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Mode;
import com.oblivm.backend.flexsc.PMCompEnv;
import com.oblivm.backend.gc.BadLabelException;
import com.oblivm.backend.util.EvaRunnable;
import com.oblivm.backend.util.GenRunnable;


public class TestMatrix extends TestHarness {
	public static final int len = 40;
	public static final int offset = 20;
	public static final int VLength = 24;
	public static final int PLength = 8;
	public static final boolean testFixedPoint = true;

	public static abstract class Helper {
		double[][] a, b;

		public Helper(double[][] a, double[][] b) {
			this.b = b;
			this.a = a;
		}

		public abstract<T> T[][][] secureCompute(T[][][] a, T[][][] b,
				DenseMatrixLib<T> lib) ;

		public abstract double[][] plainCompute(double[][] a, double[][] b);
	}

	static Random rng = new Random();

	public static double[][] randomMatrix(int n, int m, double s) {
		double[][] d1 = new double[n][m];
		for (int k = 0; k < d1.length; ++k)
			for (int j = 0; j < d1[0].length; ++j)
				d1[k][j] = rng.nextInt(1000) % 100.0;
		return d1;
	}
	
	public static double[][] randomMatrix(int n, int m) {
		return randomMatrix( n,  m, 1);
	}

	public static void PrintMatrix(double[][] result) {
		System.out.print("[\n");
		for (int i = 0; i < result.length; ++i) {
			for (int j = 0; j < result[0].length; ++j)
				System.out.print(result[i][j] + " ");
			System.out.print(";\n");
		}
		System.out.print("]\n");
	}

	
	public static class GenRunnableTestMatrix<T> extends GenRunnable<T> {
		public double andgates;
		public double encs;
		
		double[][] z;
		DenseMatrixLib<T> lib;
		Helper h;
		public double error;
		public GenRunnableTestMatrix(Helper h) {
			this.h = h;
		}
		
		T[][][] fgc1;
		T[][][] fgc2;
		T[][][] re;
		@Override
		public void prepareInput(CompEnv<T> gen) {
			if (testFixedPoint)
				lib = new DenseMatrixLib<T>(gen, new FixedPointLib<T>(gen, len, offset));
			else
				lib = new DenseMatrixLib<T>(gen, new FloatLib<T>(gen, VLength,PLength));

			fgc1 = gen.newTArray(h.a.length, h.a[0].length, 1);
			fgc2 = gen.newTArray(h.b.length, h.b[0].length, 1);
			for (int i = 0; i < h.a.length; ++i)
				for (int j = 0; j < h.a[0].length; ++j) 
						fgc1[i][j] = lib.lib.inputOfAlice(h.a[i][j]);
				
			for (int i = 0; i < h.b.length; ++i)
				for (int j = 0; j < h.b[0].length; ++j)
						fgc2[i][j] = lib.lib.inputOfAlice(h.b[i][j]);
		}

		@Override
		public void secureCompute(CompEnv<T> gen) {
			re = h.secureCompute(fgc1, fgc2, lib);
		}

		@Override
		public int[] prepareOutput(CompEnv<T> gen) throws BadLabelException {
			z = new double[re.length][re[0].length];
			for (int i = 0; i < re.length; ++i)
				for (int j = 0; j < re[0].length; ++j)
						z[i][j] = lib.lib.outputToAlice(re[i][j]);

			if (m == Mode.COUNT) {
					((PMCompEnv) gen).statistic.finalize();
					andgates = ((PMCompEnv) gen).statistic.andGate;
					encs = ((PMCompEnv) gen).statistic.NumEncAlice;
				System.out.println(andgates + " " + encs);
			} else {
				double[][] result = h.plainCompute(h.a, h.b);

				for (int i = 0; i < result.length; ++i)
					for (int j = 0; j < result[0].length; ++j) {
						double error = 0;
						if (Math.abs(result[i][j]) > 0.1)
							error = Math.abs((result[i][j] - z[i][j])
									/ z[i][j]);
						else
							error = Math.abs((result[i][j] - z[i][j]));

						if (error > error)
							System.out.print(error + " " + z[i][j] + " "
									+ result[i][j] + "(" + i + "," + j + ")\n");
						Assert.assertTrue(error < error);
					}
			}
			return new int[0];
		}
	}

	public static class EvaRunnableTestMatrix<T> extends EvaRunnable<T> {		
		EvaRunnableTestMatrix(Helper h) {
			this.h = h;
		}

		Helper h;
		double[][] z;
		DenseMatrixLib<T> lib;
		T[][][] fgc1;
		T[][][] fgc2;
		T[][][] re;
		@Override
		public void prepareInput(CompEnv<T> env) {
			if (testFixedPoint)
				lib = new DenseMatrixLib<T>(env, new FixedPointLib<T>(env, len, offset));
			else
				lib = new DenseMatrixLib<T>(env, new FloatLib<T>(env, VLength,PLength));
						


			fgc1 = env.newTArray(h.a.length, h.a[0].length, 1);
			fgc2 = env.newTArray(h.b.length, h.b[0].length, 1);
			for (int i = 0; i < h.a.length; ++i)
				for (int j = 0; j < h.a[0].length; ++j)
						fgc1[i][j] = lib.lib.inputOfAlice(h.a[i][j]);
				
			for (int i = 0; i < h.b.length; ++i)
				for (int j = 0; j < h.b[0].length; ++j)
					fgc2[i][j] = lib.lib.inputOfAlice(h.b[i][j]);
			}

		@Override
		public void secureCompute(CompEnv<T> env) {
			re = h.secureCompute(fgc1, fgc2, lib);
		}

		@Override
		public int[] prepareOutput(CompEnv<T> env) throws BadLabelException {
			for (int i = 0; i < re.length; ++i)
				for (int j = 0; j < re[0].length; ++j)
					env.outputToAlice(re[i][j]);
			return new int[0];
		}
	}

	static public <T>void runThreads(Helper h, double error) throws Exception {
		GenRunnableTestMatrix<T> gen = new GenRunnableTestMatrix<T>(h);
		gen.error = error;
		gen.setParameter(m, 54321);
		EvaRunnable<T> env = new EvaRunnableTestMatrix<T>(h);
		env.setParameter(m, "localhost", 54321);
		Thread tGen = new Thread(gen);
		Thread tEva = new Thread(env);
		tGen.start();
		Thread.sleep(5);
		tEva.start();
		tGen.join();
		tEva.join();
	}
	
	static public <T>void runThreads(Helper h) throws Exception {
		runThreads(h, 1e-3);
	}

}