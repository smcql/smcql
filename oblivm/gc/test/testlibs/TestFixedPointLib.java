package testlibs;

import harness.TestFixedPoint;

import java.util.Random;

import org.junit.Test;

import com.oblivm.backend.circuits.arithmetic.FixedPointLib;
import com.oblivm.backend.flexsc.CompEnv;

//import gc.T;

public class TestFixedPointLib extends TestFixedPoint {

	@Test
	public void testFixPointAdd() throws Exception {
		Random rng = new Random();

		for (int i = 0; i < testCases; i++) {
			double d1 = rng.nextDouble() * 200.0-100.0;
			double d2 = rng.nextDouble() * 200.0-100.0;
			TestFixedPoint.runThreads(new Helper(d1, d2) {
				@Override
				public double plainCompute(double a, double b) {
					return a + b;
				}

				@Override
				public <T> T[] secureCompute(T[] a, T[] b, int offset,
						CompEnv<T> env)  {
					return new FixedPointLib<T>(env, width, offset).add(a, b);
				}
			});
		}
	}

	@Test
	public void testFixPointSub() throws Exception {
		Random rng = new Random();

		for (int i = 0; i < testCases; i++) {
			double d1 = rng.nextDouble() * 200.0-100.0;
			double d2 = rng.nextDouble() * 200.0-100.0;
			TestFixedPoint.runThreads(new Helper(d1, d2) {
				@Override
				public <T>T[] secureCompute(T[] a, T[] b,
						int offset, CompEnv<T> env) {
					return new FixedPointLib<T>(env, width, offset).sub(a, b);
				}
				@Override
				public double plainCompute(double a, double b) {
					return a - b;
				}
			});
		}
	}

	@Test
	public void testFixPointMultiply() throws Exception {
		Random rng = new Random();

		for (int i = 0; i < testCases; i++) {
			double d1 = rng.nextDouble() * 200.0-100.0;
			double d2 = rng.nextDouble() * 200.0-100.0;
			TestFixedPoint.runThreads(new Helper(d1, d2) {

				@Override
				public <T>T[] secureCompute(T[] a, T[] b,
						int offset, CompEnv<T> env) {
					return new FixedPointLib<T>(env, width, offset)
							.multiply(a, b);
				}

				@Override
				public double plainCompute(double a, double b) {
					return a*b;//Math.abs( a * b);
				}
			});
		}
	}

	@Test
	public void testFixPointDiv() throws Exception {
		Random rng = new Random();

		for (int i = 0; i < testCases; i++) {
			double d1 = rng.nextDouble() * 200.0-100.0;
			double d2 = rng.nextDouble() * 200.0-100.0;
			if (d2 == 0)
				++d2;
			TestFixedPoint.runThreads(new Helper(d1, d2) {

				@Override
				public<T> T[] secureCompute(T[] a, T[] b,
						int offset, CompEnv<T> env) {
					return new FixedPointLib<T>(env, width, offset).div(
							a, b);
				}

				@Override
				public double plainCompute(double a, double b) {
					return a / b;
				}
			}, 0.01);
		}
	}

	@Test
	public void testFixPointSqaureRoot() throws Exception {
		Random rng = new Random();

		for (int i = 0; i < testCases; i++) {
			double d1 = rng.nextDouble() * 100.0;
			double d2 = rng.nextDouble() * 100.0;
			if (d2 == 0)
				++d2;
			TestFixedPoint.runThreads(new Helper(d1, d2) {

				@Override
				public <T>T[] secureCompute(T[] a, T[] b,
						int offset, CompEnv<T> env) {
					return new FixedPointLib<T>(env, width, offset).sqrt(a);
				}

				@Override
				public double plainCompute(double a, double b) {
					return Math.sqrt(a);
				}
			}, 0.01);
		}
	}
}