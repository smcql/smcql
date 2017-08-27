package com.oblivm.backend.circuits.arithmetic;

import java.util.Arrays;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.util.Utils;

//http://x86asm.net/articles/fixed-point-arithmetic-and-tricks/
public class FixedPointLib<T> implements ArithmeticLib<T> {

	CompEnv<T> env;
	IntegerLib<T> lib;
	int offset;
	int width;

	public FixedPointLib(CompEnv<T> e, int width, int offset) {
		assert(offset%2 == 0);
		this.env = e;
		lib = new IntegerLib<>(e);
		this.offset = offset;
		this.width = width;
	}

	public T[] inputOfAlice(double d) {
		return env.inputOfAlice(Utils.fromFixPoint(d, width, offset));
	}

	public T[] inputOfBob(double d) {
		return env.inputOfBob(Utils.fromFixPoint(d, width, offset));
	}

	public T[] add(T[] x, T[] y) {
		return lib.add(x, y);
	}

	public T[] sub(T[] x, T[] y) {
		return lib.sub(x, y);
	}

	//http://dsp.stackexchange.com/questions/7906/fixed-point-multiplication-with-negative-numbers
	public T[] multiply(T[] x, T[] y) {
		T[] res = lib.karatsubaMultiply(lib.absolute(x), lib.absolute(y));
		res = Arrays.copyOfRange(res, offset,offset+ width);
		return lib.addSign(res, lib.xor(x[x.length-1], y[y.length-1]));
	}

	public T[] div(T[] x, T[] y) {
		T[] padX = lib.padSignedSignal(x, x.length + offset);
		return Arrays.copyOf(lib.div(lib.leftPublicShift(padX, offset), y), width);
	}

	public T[] publicValue(double d) {
		boolean[] a = Utils.fromFixPoint(d, width, offset);
		T[] res = env.newTArray(width);
		for (int i = 0; i < width; ++i)
			res[i] = a[i] ? lib.SIGNAL_ONE : lib.SIGNAL_ZERO;
		return res;
	}

	@Override
	public T leq(T[] a, T[] b) {
		return lib.leq(a, b);
	}

	@Override
	public T eq(T[] a, T[] b) {
		return lib.eq(a, b);
	}

	@Override
	public T[] sqrt(T[] a) {
		T[] res = lib.sqrt(a);
		return lib.leftPublicShift(res, offset/2);
	}

	@Override
	public CompEnv<T> getEnv() {
		return env;
	}

	@Override
	public T[] toSecureInt(T[] a, IntegerLib<T> lib) {
		T[] res = lib.env.newTArray(lib.width);
		T[] intPart = Arrays.copyOfRange(a, offset, a.length);
		if(res.length >= intPart.length)
			System.arraycopy(intPart, 0, res, 0, intPart.length);
		else 
			res = Arrays.copyOf(intPart, res.length);
		return res;
	}

	@Override
	public T[] toSecureFloat(T[] a, FloatLib<T> lib) {
		return null;
	}

	@Override
	public T[] toSecureFixPoint(T[] a, FixedPointLib<T> lib) {
		//later may support case between different libs;
		return a;
	}

	@Override
	public double outputToAlice(T[] a) {
		return Utils.toFixPoint(env.outputToAlice(a), offset);
	}

	@Override
	public int numBits() {
		return width;
	}
}
