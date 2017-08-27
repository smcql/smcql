package matrix;

import harness.TestHarness;
import harness.TestMatrix;
import harness.TestMatrix.Helper;

import org.junit.Test;

import com.oblivm.backend.circuits.arithmetic.DenseMatrixLib;

public class TestMatrixQRDecomposition extends TestHarness {

	@Test
	public void testAllCases() throws Exception {
		for (int i = 0; i < 1; i++) {
			double[][] d1 = TestMatrix.randomMatrix(100, 100);
					/*new double[][] { new double[] { 52, 30, 49, 28 },
					new double[] { 30, 50, 8, 44 },
					new double[] { 49, 8, 46, 16 },
					new double[] { 28, 44, 16, 22 } };// randomMatrix(10, 10);*/
			double[][] d2 = TestMatrix.randomMatrix(100, 100);

			TestMatrix.runThreads(new Helper(d1, d2) {

				@Override
				public <T>T[][][] secureCompute(T[][][] a,
						T[][][] b, DenseMatrixLib<T> lib) {
					 T[][][] e1=null,e2=null;
					 lib.QRDecomposition(a, e1, e2);
					 return e1;
				}

				@Override
				public double[][] plainCompute(double[][] a, double[][] b) {
					return DenseMatrixLib.rref(a);
				}
			});
		}
	}
}