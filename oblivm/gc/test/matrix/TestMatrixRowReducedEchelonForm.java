package matrix;

import harness.TestHarness;
import harness.TestMatrix;
import harness.TestMatrix.Helper;

import org.junit.Test;

import com.oblivm.backend.circuits.arithmetic.DenseMatrixLib;

public class TestMatrixRowReducedEchelonForm extends TestHarness {

	@Test
	public void testAllCases() throws Exception {
		for (int i = 0; i < 1; i++) {
			double[][] d1 = TestMatrix.randomMatrix(5, 5);
			double[][] d2 = TestMatrix.randomMatrix(5, 5);

			TestMatrix.runThreads(new Helper(d1, d2) {

				@Override
				public <T>T[][][] secureCompute(T[][][] a,
						T[][][] b, DenseMatrixLib<T> lib) {
					return lib.rref(a);
				}

				@Override
				public double[][] plainCompute(double[][] a, double[][] b) {
					return DenseMatrixLib.rref(a);
				}
			});
		}
	}
}