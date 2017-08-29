package com.oblivm.backend.circuits.arithmetic;


import com.oblivm.backend.flexsc.CompEnv;

public class DenseMatrixLib<T> {
	public ArithmeticLib<T> lib;
	VectorLib<T> veclib;
	CompEnv<T> env;
	IntegerLib<T> integerlib;

	public DenseMatrixLib(CompEnv<T> e, ArithmeticLib<T> lib) {
		env = e;
		this.lib = lib;
		integerlib = new IntegerLib<T>(e);
		veclib = new VectorLib<T>(e, lib);
	}

	public T[][][] xor(T[][][] a, T[][][] b) {
		T[][][] res = env.newTArray(a.length, a[0].length, 1);
		for (int i = 0; i < a.length; ++i)
			res[i] = veclib.xor(a[i], b[i]);
		return res;
	}

	
	public T[][][] add(T[][][] a, T[][][] b) {
		T[][][] res = env.newTArray(a.length, a[0].length, 1);
		for (int i = 0; i < a.length; ++i)
			res[i] = veclib.add(a[i], b[i]);
		return res;
	}

	public T[][][] sub(T[][][] a, T[][][] b) {
		T[][][] res = env.newTArray(a.length, a[0].length, 1);
		for (int i = 0; i < a.length; ++i)
			res[i] = veclib.sub(a[i], b[i]);
		return res;
	}

	public T[][][] multiply(T[][][] a, T[][][] b) {
		T[][][] res = env.newTArray(a.length, b[0].length, 1);
		for (int i = 0; i < a.length; ++i)
			for (int j = 0; j < b[0].length; ++j) {
				res[i][j] = lib.multiply(a[i][0], b[0][j]);
				for (int k = 1; k < a[0].length; ++k)
					res[i][j] = lib.add(res[i][j],	
							lib.multiply(a[i][k], b[k][j]));
			}
		return res;
	}

	public T[][][] transpose(T[][][] a) {
		T[][][] res = env.newTArray(a[0].length, a.length, 1);
		for (int i = 0; i < a.length; ++i)
			for (int j = 0; j < a[0].length; ++j)
				res[j][i] = a[i][j];
		return res;
	}

	public T[][][] rref(T[][][] m) {
		T[][][] result = env.newTArray(m.length, m[0].length, 1);
		for (int r = 0; r < m.length; ++r)
			for (int c = 0; c < m[r].length; ++c)
				result[r][c] = m[r][c];

		for (int p = 0; p < result.length; ++p) {
			/* Make this pivot 1 */
			T[] pv = result[p][p];
			T pvZero = lib.eq(pv, lib.publicValue(0));
			T[] pvInv = lib.div(lib.publicValue(1), pv);
			for (int i = 0; i < result[p].length; ++i)
				result[p][i] = integerlib
						.mux(lib.multiply(result[p][i], pvInv), result[p][i],
								pvZero);

			/* Make other rows zero */
			for (int r = 0; r < result.length; ++r) {
				if (r != p) {
					T[] f = result[r][p];
					for (int i = 0; i < result[r].length; ++i)
						result[r][i] = lib.sub(result[r][i],
								lib.multiply(f, result[p][i]));
				}
			}
		}

		return result;
	}

	public T[][][] fastInverse(T[][][] m) {
		int dimension = m.length;
		T[][][] extended = env.newTArray(dimension, dimension * 2, 1);
		T[] zeroFloat = lib.publicValue(0);
		T[] oneFloat = lib.publicValue(1);
		for (int i = 0; i < dimension; ++i) {
			for (int j = 0; j < dimension; ++j)
				extended[i][j] = m[i][j];
			for (int j = 0; j < dimension; ++j)
				extended[i][dimension + j] = zeroFloat;
			extended[i][dimension + i] = oneFloat;
		}
		extended = rref(extended);
		T[][][] result = env.newTArray(dimension, dimension, 1);

		for (int i = 0; i < dimension; ++i) {
			for (int j = 0; j < dimension; ++j)
				result[i][j] = extended[i][dimension + j];
		}
		return result;
	}

	public T[][] solve(T[][][] A, T[][] b) {
		int dimension = A.length;
		T[][][] extended = env.newTArray(dimension, dimension + 1, 1);
		for (int i = 0; i < dimension; ++i) {
			for (int j = 0; j < dimension; ++j)
				extended[i][j] = A[i][j];
			extended[i][dimension] = b[i];
		}
		extended = rref(extended);
		T[][] result = env.newTArray(dimension, 1);
		for (int i = 0; i < dimension; ++i) {
			result[i] = extended[i][dimension];
		}
		return result;
	}

	public T[][] getColumn(T[][][] matrix, int col) {
		T[][] res = env.newTArray(matrix[0].length, 1);
		for (int i = 0; i < matrix[0].length; ++i)
			res[i] = matrix[i][col];
		return res;
	}

	public T[][] getRow(T[][][] matrix, int row) {
		T[][] res = env.newTArray(matrix.length, 1);
		for (int i = 0; i < matrix.length; ++i)
			res[i] = matrix[row][i];
		return res;
	}

	// using Gram-Schmidt process
	public void QRDecomposition(T[][][] matrix, T[][][] res1, T[][][] res2) {
		T[][][] u = env.newTArray(matrix[0].length, matrix.length, 1);
		T[][][] e = env.newTArray(matrix[0].length, matrix.length, 1);
		T[][][] a = transpose(matrix);
		for (int i = 0; i < matrix[0].length; ++i) {
			u[i] = a[i];
			for (int j = 0; j < i; ++j) {
				u[i] = veclib.sub(u[i], veclib.projection(a[i], e[j]));
			}
			e[i] = veclib.normalize(u[i]);
		}
		T[][][] r = env.newTArray(matrix.length, matrix[0].length, 1);
		T[] zero = lib.publicValue(0);
		for (int i = 0; i < r.length; ++i) {
			for (int j = 0; j < r[0].length; ++j) {
				if (i <= j)
					r[i][j] = veclib.innerProduct(e[i], a[j]);
				else
					r[i][j] = zero;
			}
		}
		res1 = transpose(e);
		res2 = r;
	}

	public T[][][] eigenValues(T[][][] matrix, int numberOfIterations) {
		T[][][] e1=null, e2=null;
		QRDecomposition(matrix, e1, e2);
		T[][][] newMatrix = null;
		for (int i = 0; i < numberOfIterations; ++i) {
			newMatrix = multiply(e1, e2);
			QRDecomposition(newMatrix, e1, e2);
		}
		return newMatrix;
	}

	public static double[][] rref(double[][] mat) {
		double[][] rref = new double[mat.length][mat[0].length];

		/* Copy matrix */
		for (int r = 0; r < rref.length; ++r) {
			for (int c = 0; c < rref[r].length; ++c) {
				rref[r][c] = mat[r][c];
			}
		}

		for (int p = 0; p < rref.length; ++p) {
			/* Make this pivot 1 */
			double pv = rref[p][p];
			if (pv != 0) {
				double pvInv = 1.0 / pv;
				for (int i = 0; i < rref[p].length; ++i) {
					rref[p][i] *= pvInv;
				}
			}

			/* Make other rows zero */
			for (int r = 0; r < rref.length; ++r) {
				if (r != p) {
					double f = rref[r][p];
					for (int i = 0; i < rref[0].length; ++i) {
						rref[r][i] -= (f * rref[p][i]);
					}
				}
			}
		}

		return rref;
	}
}
