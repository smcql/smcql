package com.oblivm.backend.circuits.arithmetic;

import com.oblivm.backend.flexsc.CompEnv;

public interface ArithmeticLib<T> {
	CompEnv<T> getEnv();
	T[] inputOfAlice(double d);

	T[] inputOfBob(double d);
	
	double outputToAlice(T[] a);

	T[] add(T[] x, T[] y);

	T[] multiply(T[] x, T[] y);

	T[] div(T[] x, T[] y);

	T[] sub(T[] x, T[] y);

	T[] publicValue(double v);

	T leq(T[] a, T[] b);

	T eq(T[] a, T[] b);

	T[] sqrt(T[] a);
	
	T[] toSecureInt(T[] a, IntegerLib<T> lib);
	T[] toSecureFloat(T[] a, FloatLib<T> lib);
	T[] toSecureFixPoint(T[] a, FixedPointLib<T> lib);
	int numBits();
}