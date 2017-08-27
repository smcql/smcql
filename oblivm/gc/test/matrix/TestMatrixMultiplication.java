package matrix;

import harness.TestHarness;
import harness.TestMatrix;
import harness.TestMatrix.Helper;

import org.junit.Test;

import com.oblivm.backend.circuits.arithmetic.DenseMatrixLib;

public class TestMatrixMultiplication extends TestHarness {

	@Test
	public void testAllCases() throws Exception {

		for (int i = 0; i < 1; i++) {
			double[][] d1 = TestMatrix.randomMatrix(8, 8);
			double[][] d2 = TestMatrix.randomMatrix(8, 8);

			TestMatrix.runThreads(new Helper(d1, d2) {

				@Override
				public <T>T[][][] secureCompute(T[][][] a,
						T[][][] b, DenseMatrixLib<T> lib) {
					return lib.multiply(a, b);
				}

				@Override
				public double[][] plainCompute(double[][] a, double[][] b) {
					double[][] res = new double[a.length][b[0].length];
					for (int i = 0; i < a.length; ++i)
						for (int j = 0; j < b[0].length; ++j) {
							res[i][j] = 0;
							for (int k = 0; k < a[0].length; ++k)
								res[i][j] += (a[i][k] * b[k][j]);
						}
					return res;
				}
			});
		}
	}
}