package testlibs;

import harness.TestFloat;
import harness.TestFloat.Helper;
import harness.TestHarness;

import java.util.Random;

import org.junit.Test;

import com.oblivm.backend.circuits.arithmetic.FloatLib;


public class TestFloatLib extends TestHarness {

	Random rng = new Random();

	@Test
	public void testFloat() throws Exception {
		for (int i = 0; i < testCases; i++) {
			double a = rng.nextDouble() * (1 << 20) - (1 << 19);
			double b = rng.nextDouble() * (1 << 20) - (1 << 19);
			if(i==0)a=0;else if (i==1) b = 0;
			TestFloat.runThreads(new Helper(a, b) {
				@Override
				public double plainCompute(double a, double b) {
					return a;
				}
				@Override
				public <T> T[] secureCompute(T[] a, T[] b, FloatLib<T> env) {
					return a;
				}
			});
		}
	}

	@Test
	public void testFloatAdd() throws Exception {
		for (int i = 0; i < testCases; i++) {
			double a = rng.nextDouble() * (1 << 20) - (1 << 19);
			double b = rng.nextDouble() * (1 << 20) - (1 << 19);
			if(i==0)a=0;else if (i==1) b = 0;
			TestFloat.runThreads(new Helper(a, b) {
				@Override
				public double plainCompute(double a, double b) {
					return a+b;
				}

				@Override
				public <T> T[] secureCompute(T[] a, T[] b, FloatLib<T> env) {
					return env.add(a, b);
				}
			});
		}

		double a = rng.nextDouble() * (1 << 20) - (1 << 19);
		TestFloat.runThreads(new Helper(a, -a) {
			@Override
			public double plainCompute(double a, double b) {
				return a+b;
			}

			@Override
			public <T> T[] secureCompute(T[] a, T[] b, FloatLib<T> env) {
				return env.add(a, b);
			}
		});
	}

	@Test
	public void testFloatSub() throws Exception {
		Random rng = new Random();

		for (int i = 0; i < testCases; i++) {
			double a = rng.nextDouble() * (1 << 20) - (1 << 19);
			double b = rng.nextDouble() * (1 << 20) - (1 << 19);
			if(i==0)a=0;else if (i==1) b = 0;
			TestFloat.runThreads(new Helper(a, b) {
				@Override
				public double plainCompute(double a, double b) {
					return a-b;
				}

				@Override
				public <T> T[] secureCompute(T[] a, T[] b, FloatLib<T> env) {
					return env.sub(a, b);
				}
			});
		}
		double a = rng.nextDouble() * (1 << 20) - (1 << 19);
		TestFloat.runThreads(new Helper(a, -a) {
			@Override
			public double plainCompute(double a, double b) {
				return a-b;
			}

			@Override
			public <T> T[] secureCompute(T[] a, T[] b, FloatLib<T> env) {
				return env.sub(a, b);
			}
		});

	}

	@Test
	public void testFloatDiv() throws Exception {
		Random rng = new Random();

		for (int i = 0; i < testCases; i++) {
			double a = rng.nextDouble() * (1 << 20) - (1 << 19);
			double b = rng.nextDouble() * (1 << 20) - (1 << 19);
			TestFloat.runThreads(new Helper(a, b) {
				@Override
				public double plainCompute(double a, double b) {
					return a/b;
				}

				@Override
				public <T> T[] secureCompute(T[] a, T[] b, FloatLib<T> env) {
					return env.div(a, b);
				}
			});
		}
	}

	@Test
	public void testFloatMultiply() throws Exception {
		Random rng = new Random();

		for (int i = 0; i < testCases; i++) {
			double a = rng.nextDouble() * (1 << 20) - (1 << 19);
			double b = rng.nextDouble() * (1 << 20) - (1 << 19);
			if(i==0)a=0;else if (i==1) b = 0;
			TestFloat.runThreads(new Helper(a, b) {
				@Override
				public double plainCompute(double a, double b) {
					return a*b;
				}

				@Override
				public <T> T[] secureCompute(T[] a, T[] b, FloatLib<T> env) {
					return env.multiply(a, b);
				}
			});
		}
	}

	@Test
	public void testFloatSq() throws Exception {
		Random rng = new Random();

		for (int i = 0; i < testCases; i++) {
			double a = rng.nextDouble() * (1 << 10);
			TestFloat.runThreads(new Helper(a, a) {
				@Override
				public double plainCompute(double a, double b) {
					return Math.sqrt(a);
				}

				@Override
				public <T> T[] secureCompute(T[] a, T[] b, FloatLib<T> env) {
					return env.sqrt(a);
				}
			});
		}
	}
}