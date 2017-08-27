package matrix;

import harness.TestHarness;
import harness.TestMatrix;
import harness.TestMatrix.Helper;

import org.junit.Test;

import com.oblivm.backend.circuits.arithmetic.DenseMatrixLib;

public class TestMatrixEigenValue extends TestHarness{

	@Test
	public void testAllCases() throws Exception {
		for (int i = 0; i < 1; i++) {
			double[][] d1 = TestMatrix.randomMatrix(10, 10);
			for (int k = 0; k < 10; k++)
				for (int l = 0; l < 10; ++l)
					d1[k][l] = d1[l][k];

			double[][] d2 = TestMatrix.randomMatrix(10, 10);

			TestMatrix.runThreads(new Helper(d1, d2) {

				@Override
				public <T>T[][][] secureCompute(T[][][] a,
						T[][][] b, DenseMatrixLib<T> lib) {
					return lib.eigenValues(a, 10);
				}

				@Override
				public double[][] plainCompute(double[][] a, double[][] b) {
					return DenseMatrixLib.rref(a);
				}
			});
		}
	}
}