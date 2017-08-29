package com.oblivm.backend.circuits.arithmetic;

import com.oblivm.backend.flexsc.CompEnv;

public class VectorLib<T> {
	ArithmeticLib<T> lib;
	CompEnv<T> env;

	public VectorLib(CompEnv<T> e, ArithmeticLib<T> lib) {
		env = e;
		this.lib = lib;
	}

	public T[][] xor(T[][] a, T[][] b) {
		IntegerLib<T> ilib = new IntegerLib<T>(lib.getEnv());
		T[][] res = env.newTArray(a.length, 1);
		for (int i = 0; i < a.length; ++i)
			res[i] = ilib.xor(a[i], b[i]);
		return res;
	}
	
	public T[][] add(T[][] a, T[][] b) {
		T[][] res = env.newTArray(a.length, 1);
		for (int i = 0; i < a.length; ++i)
			res[i] = lib.add(a[i], b[i]);
		return res;
	}

	public T[][] sub(T[][] a, T[][] b) {
		T[][] res = env.newTArray(a.length, 1);
		for (int i = 0; i < a.length; ++i)
			res[i] = lib.sub(a[i], b[i]);
		return res;
	}

	public T[] innerProduct(T[][] a, T[][] b) {
		T[] res = lib.publicValue(0);
		for (int i = 0; i < a.length; ++i)
			res = lib.add(res, lib.multiply(a[i], b[i]));
		return res;
	}

	public T[][] scalarProduct(T[] scalar, T[][] v) {
		T[][] res = env.newTArray(v.length, 1);
		for (int i = 0; i < v.length; ++i)
			res[i] = lib.multiply(scalar, v[i]);
		return res;
	}

	public T[][] projection(T[][] a, T[][] e) {
		T[] ea = innerProduct(e, a);
		T[] ee = innerProduct(e, e);
		T[] scalar = lib.div(ea, ee);
		return scalarProduct(scalar, e);
	}

	public T[][] normalize(T[][] vec) {
		T[] scalar = innerProduct(vec, vec);
		scalar = lib.sqrt(scalar);
		scalar = lib.div(lib.publicValue(1), scalar);
		return scalarProduct(scalar, vec);
	}
}
