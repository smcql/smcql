// Copyright (C) 2014 by Xiao Shaun Wang <wangxiao@cs.umd.edu>, Yan Huang <yhuang@cs.umd.edu> and Kartik Nayak <kartik@cs.umd.edu>
package com.oblivm.backend.circuits.arithmetic;

import java.util.Arrays;

import com.oblivm.backend.circuits.CircuitLib;
import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.util.Utils;

public class IntegerLib<T> extends CircuitLib<T> implements ArithmeticLib<T>, java.io.Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1834255827528673538L;
	/**
	 * 
	 */

	public int width;
	public IntegerLib(CompEnv<T> e) {
		super(e);
		width = 32;
	}

	public IntegerLib(CompEnv<T> e, int width) {
		super(e);
		this.width = width;
	}

	static final int S = 0;
	static final int COUT = 1;

	public T[] publicValue(double v) {
		int intv = (int) v;
		return toSignals(intv, width);
	}

	// full 1-bit adder
	protected T[] add(T x, T y, T cin) {
		T[] res = env.newTArray(2);

		T t1 = xor(x, cin);
		T t2 = xor(y, cin);
		res[S] = xor(x, t2);
		t1 = and(t1, t2);
		res[COUT] = xor(cin, t1);

		return res;
	}

	// full n-bit adder
	public T[] addFull(T[] x, T[] y, boolean cin) {
		assert (x != null && y != null && x.length == y.length) : "add: bad inputs.";
		
		T[] res = env.newTArray(x.length + 1);
		T[] t = add(x[0], y[0], env.newT(cin));
		res[0] = t[S];
		for (int i = 0; i < x.length - 1; i++) {
			t = add(x[i + 1], y[i + 1], t[COUT]);
			res[i + 1] = t[S];
		}
		res[res.length - 1] = t[COUT];
		return res;
	}

	public T[] add(T[] x, T[] y, boolean cin) {
		return Arrays.copyOf(addFull(x, y, cin), x.length);
	}

	public T[] add(T[] x, T[] y) {

		return add(x, y, false);
	}

	public T[] sub(T x, T y) throws Exception {
		T[] ax = env.newTArray(2);
		ax[1] = SIGNAL_ZERO;
		ax[0] = x;
		T[] ay = env.newTArray(2);
		ay[1] = SIGNAL_ZERO;
		ay[0] = y;
		return sub(x, y);
	}

	public T[] sub(T[] x, T[] y) {
		assert (x != null && y != null && x.length == y.length) : "sub: bad inputs.";

		return add(x, not(y), true);
	}

	public T[] incrementByOne(T[] x) {
		T[] one = zeros(x.length);
		one[0] = SIGNAL_ONE;
		return add(x, one);
	}

	public T[] decrementByOne(T[] x) {
		T[] one = zeros(x.length);
		one[0] = SIGNAL_ONE;
		return sub(x, one);
	}

	public T[] conditionalIncreament(T[] x, T flag) {
		T[] one = zeros(x.length);
		one[0] = mux(SIGNAL_ZERO, SIGNAL_ONE, flag);
		return add(x, one);
	}

	public T[] conditionalDecrement(T[] x, T flag) {
		T[] one = zeros(x.length);
		one[0] = mux(SIGNAL_ZERO, SIGNAL_ONE, flag);
		return sub(x, one);
	}

	public T geq(T[] x, T[] y) {
		assert (x.length == y.length) : "bad input";

		T[] result = sub(x, y);
		return not(result[result.length - 1]);
	}

	public T leq(T[] x, T[] y) {
		return geq(y, x);
	}

	public T[] multiply(T[] x, T[] y) {
		return Arrays.copyOf(multiplyInternal(x, y), x.length);// res;
	}

	// This multiplication does not truncate the length of x and y
	public T[] multiplyFull(T[] x, T[] y) {
		return multiplyInternal(x, y);
	}

	private T[] multiplyInternal(T[] x, T[] y) {
//		return karatsubaMultiply(x,y);
		assert (x != null && y != null) : "multiply: bad inputs";
		T[] res = zeros(x.length + y.length);
		T[] zero = zeros(x.length);

		T[] toAdd = mux(zero, x, y[0]);
		System.arraycopy(toAdd, 0, res, 0, toAdd.length);

		for (int i = 1; i < y.length; ++i) {
			toAdd = Arrays.copyOfRange(res, i, i + x.length);
			toAdd = add(toAdd, mux(zero, x, y[i]), false);
			System.arraycopy(toAdd, 0, res, i, toAdd.length);
		}
		return res;
	}

	public T[] absolute(T[] x) {
		T reachedOneSignal = SIGNAL_ZERO;
		T[] result = zeros(x.length);
		for (int i = 0; i < x.length; ++i) {
			T comp = eq(SIGNAL_ONE, x[i]);
			result[i] = xor(x[i], reachedOneSignal);
			reachedOneSignal = or(reachedOneSignal, comp);
		}
		return mux(x, result, x[x.length - 1]);
	}

	public T[] div(T[] x, T[] y) {
		T[] absoluteX = absolute(x);
		T[] absoluteY = absolute(y);
		T[] PA = divInternal(absoluteX, absoluteY);
		return addSign(Arrays.copyOf(PA, x.length),
				xor(x[x.length - 1], y[y.length - 1]));

	}

	// Restoring Division Algorithm
	public T[] divInternal(T[] x, T[] y) {
		T[] PA = zeros(x.length + y.length);
		T[] B = y;
		System.arraycopy(x, 0, PA, 0, x.length);
		for (int i = 0; i < x.length; ++i) {
			PA = leftShift(PA);
			T[] tempP = sub(Arrays.copyOfRange(PA, x.length, PA.length), B);
			PA[0] = not(tempP[tempP.length - 1]);
			System.arraycopy(
					mux(tempP, Arrays.copyOfRange(PA, x.length, PA.length),
							tempP[tempP.length - 1]), 0, PA, x.length, y.length);
		}
		return PA;
	}

	public T[] mod(T[] x, T[] y) {
		T Xneg = x[x.length - 1];
		T[] absoluteX = absolute(x);
		T[] absoluteY = absolute(y);
		T[] PA = divInternal(absoluteX, absoluteY);
		T[] res = Arrays.copyOfRange(PA, y.length, PA.length);
		return mux(res, sub(toSignals(0, res.length), res), Xneg);
	}

	public T[] addSign(T[] x, T sign) {
		T[] reachedOneSignal = zeros(x.length);
		T[] result = env.newTArray(x.length);
		for (int i = 0; i < x.length - 1; ++i) {
			reachedOneSignal[i + 1] = or(reachedOneSignal[i], x[i]);
			result[i] = xor(x[i], reachedOneSignal[i]);
		}
		result[x.length - 1] = xor(x[x.length - 1],
				reachedOneSignal[x.length - 1]);
		return mux(x, result, sign);
	}

	public T[] commonPrefix(T[] x, T[] y) {
		assert (x != null && y != null) : "multiply: bad inputs";
		T[] result = xor(x, y);

		for (int i = x.length - 2; i >= 0; --i) {
			result[i] = or(result[i], result[i + 1]);
		}
		return result;
	}

	public T[] leadingZeros(T[] x) {
		assert (x != null) : "leading zeros: bad inputs";

		T[] result = Arrays.copyOf(x, x.length);
		for (int i = result.length - 2; i >= 0; --i) {
			result[i] = or(result[i], result[i + 1]);
		}

		return numberOfOnes(not(result));
	}

	public T[] lengthOfCommenPrefix(T[] x, T[] y) {
		assert (x != null) : "lengthOfCommenPrefix : bad inputs";

		return leadingZeros(xor(x, y));
	}

	/*
	 * Integer manipulation
	 */
	public T[] leftShift(T[] x) {
		assert (x != null) : "leftShift: bad inputs";
		return leftPublicShift(x, 1);
	}

	public T[] rightShift(T[] x) {
		assert (x != null) : "rightShift: bad inputs";
		return rightPublicShift(x, 1);
	}

	public T[] leftPublicShift(T[] x, int s) {
		assert (x != null && s < x.length) : "leftshift: bad inputs";

		T res[] = env.newTArray(x.length);
		System.arraycopy(zeros(s), 0, res, 0, s);
		System.arraycopy(x, 0, res, s, x.length - s);

		return res;
	}

	public T[] rightPublicShift(T[] x, int s) {
		assert (x != null && s < x.length) : "rightshift: bad inputs";

		T[] res = env.newTArray(x.length);
		System.arraycopy(x, s, res, 0, x.length - s);
		System.arraycopy(zeros(s), 0, res, x.length - s, s);

		return res;
	}

	public T[] conditionalLeftPublicShift(T[] x, int s, T sign) {
		assert (x != null && s < x.length) : "leftshift: bad inputs";

		T[] res = env.newTArray(x.length);
		System.arraycopy(mux(Arrays.copyOfRange(x, 0, s), zeros(s), sign), 0,
				res, 0, s);
		System.arraycopy(
				mux(Arrays.copyOfRange(x, s, x.length),
						Arrays.copyOfRange(x, 0, x.length), sign), 0, res, s,
						x.length - s);
		return res;
	}

	public T[] conditionalRightPublicShift(T[] x, int s, T sign) {
		assert (x != null && s < x.length) : "rightshift: bad inputs";

		T res[] = env.newTArray(x.length);
		System.arraycopy(
				mux(Arrays.copyOfRange(x, 0, x.length - s),
						Arrays.copyOfRange(x, s, x.length), sign), 0, res, 0,
						x.length - s);
		System.arraycopy(
				mux(Arrays.copyOfRange(x, x.length - s, x.length), zeros(s),
						sign), 0, res, x.length - s, s);
		return res;
	}

	public T[] leftPrivateShift(T[] x, T[] lengthToShift) {
		T[] res = Arrays.copyOf(x, x.length);

		for (int i = 0; ((1 << i) < x.length) && i < lengthToShift.length; ++i)
			res = conditionalLeftPublicShift(res, (1 << i), lengthToShift[i]);
		T clear = SIGNAL_ZERO;
		for (int i = 0; i < lengthToShift.length; ++i) {
			if ((1 << i) >= x.length)
				clear = or(clear, lengthToShift[i]);
		}

		return mux(res, zeros(x.length), clear);
	}

	public T[] rightPrivateShift(T[] x, T[] lengthToShift) {
		T[] res = Arrays.copyOf(x, x.length);

		for (int i = 0; ((1 << i) < x.length) && i < lengthToShift.length; ++i)
			res = conditionalRightPublicShift(res, (1 << i), lengthToShift[i]);
		T clear = SIGNAL_ZERO;
		for (int i = 0; i < lengthToShift.length; ++i) {
			if ((1 << i) >= x.length)
				clear = or(clear, lengthToShift[i]);
		}

		return mux(res, zeros(x.length), clear);
	}

	T compare(T x, T y, T cin) {
		T t1 = xor(x, cin);
		T t2 = xor(y, cin);
		t1 = and(t1, t2);
		return xor(x, t1);
	}

	public T compare(T[] x, T[] y) {
		assert (x != null && y != null && x.length == y.length) : "compare: bad inputs.";

		T t = env.newT(false);
		for (int i = 0; i < x.length; i++) {
			t = compare(x[i], y[i], t);
		}

		return t;
	}

	public T eq(T x, T y) {
		assert (x != null && y != null) : "CircuitLib.eq: bad inputs";

		return not(xor(x, y));
	}

	public T eq(T[] x, T[] y) {
		assert (x != null && y != null && x.length == y.length) : "CircuitLib.eq[]: bad inputs.";

		T res = env.newT(true);
		for (int i = 0; i < x.length; i++) {
			T t = eq(x[i], y[i]);
			res = env.and(res, t);
		}

		return res;
	}

	public T[] twosComplement(T[] x) {
		T reachOne = SIGNAL_ZERO;
		T[] result = env.newTArray(x.length);
		for (int i = 0; i < x.length; ++i) {
			result[i] = xor(x[i], reachOne);
			reachOne = or(reachOne, x[i]);
		}
		return result;
	}

	public T[] hammingDistance(T[] x, T[] y) {
		T[] a = xor(x, y);
		return numberOfOnes(a);
	}

	public T[] numberOfOnes(T[] t) {
		if (t.length == 0) {
			T[] res = env.newTArray(1);
			res[0] = SIGNAL_ZERO;
			return res;
		}
		if (t.length == 1) {
			return t;
		} else {
			int length = 1;
			int w = 1;
			while (length <= t.length) {
				length <<= 1;
				w++;
			}
			length >>= 1;

			T[] res1 = numberOfOnesN(Arrays.copyOfRange(t, 0, length));
			T[] res2 = numberOfOnes(Arrays.copyOfRange(t, length, t.length));
			return add(padSignal(res1, w), padSignal(res2, w));
		}
	}
	
	public T[] numberOfOnesN(T[] res) {
		if(res.length == 1)
			return res;
		T[] left = numberOfOnesN(Arrays.copyOfRange(res, 0, res.length/2));
		T[] right = numberOfOnesN(Arrays.copyOfRange(res, res.length/2, res.length));
		return unSignedAdd(left, right);
	}

	public T[] unSignedAdd(T[] x, T[] y) {
		assert (x != null && y != null && x.length == y.length) : "add: bad inputs.";
		T[] res = env.newTArray(x.length + 1);

		T[] t = add(x[0], y[0], env.newT(false));
		res[0] = t[S];
		for (int i = 0; i < x.length - 1; i++) {
			t = add(x[i + 1], y[i + 1], t[COUT]);
			res[i + 1] = t[S];
		}
		res[res.length - 1] = t[COUT];
		return res;
	}

	public T[] unSignedMultiply(T[] x, T[] y) {
		assert (x != null && y != null) : "multiply: bad inputs";

		T[] res = zeros(x.length + y.length);
		T[] zero = zeros(x.length);

		T[] toAdd = mux(zero, x, y[0]);
		System.arraycopy(toAdd, 0, res, 0, toAdd.length);

		for (int i = 1; i < y.length; ++i) {
			toAdd = Arrays.copyOfRange(res, i, i + x.length);
			toAdd = unSignedAdd(toAdd, mux(zero, x, y[i]));
			System.arraycopy(toAdd, 0, res, i, toAdd.length);

		}
		return res;
	}

	public T[] karatsubaMultiply(T[] x, T[] y) {
		if (x.length <= 18)
			return unSignedMultiply(x, y);

		int length = (x.length + y.length);

		T[] xlo = Arrays.copyOfRange(x, 0, x.length / 2);
		T[] xhi = Arrays.copyOfRange(x, x.length / 2, x.length);
		T[] ylo = Arrays.copyOfRange(y, 0, y.length / 2);
		T[] yhi = Arrays.copyOfRange(y, y.length / 2, y.length);

		int nextlength = Math.max(x.length / 2, x.length - x.length / 2);
		xlo = padSignal(xlo, nextlength);
		xhi = padSignal(xhi, nextlength);
		ylo = padSignal(ylo, nextlength);
		yhi = padSignal(yhi, nextlength);

		T[] z0 = karatsubaMultiply(xlo, ylo);
		T[] z2 = karatsubaMultiply(xhi, yhi);

		T[] z1 = sub(
				padSignal(
						karatsubaMultiply(unSignedAdd(xlo, xhi),
								unSignedAdd(ylo, yhi)), 2 * nextlength + 2),
								padSignal(
										unSignedAdd(padSignal(z2, 2 * nextlength),
												padSignal(z0, 2 * nextlength)),
												2 * nextlength + 2));
		z1 = padSignal(z1, length);
		z1 = leftPublicShift(z1, x.length / 2);

		T[] z0Pad = padSignal(z0, length);
		T[] z2Pad = padSignal(z2, length);
		z2Pad = leftPublicShift(z2Pad, 2 * (x.length / 2));
		return add(add(z0Pad, z1), z2Pad);
	}

	public T[] min(T[] x, T[] y) {
		T leq = leq(x, y);
		return mux(y, x, leq);
	}

	public T[] sqrt(T[] a) {
		int newLength = a.length;
		if (newLength % 2 == 1)
			newLength++;
		T[] x = padSignal(a, newLength);

		T[] rem = zeros(x.length);
		T[] root = zeros(x.length);
		for (int i = 0; i < x.length / 2; i++) {
			root = leftShift(root);
			rem = add(leftPublicShift(rem, 2),
					rightPublicShift(x, x.length - 2));
			x = leftPublicShift(x, 2);
			T[] oldRoot = root;
			root = copy(root);
			root[0] = SIGNAL_ONE;
			T[] remMinusRoot = sub(rem, root);
			T isRootSmaller = not(remMinusRoot[remMinusRoot.length - 1]);
			rem = mux(rem, remMinusRoot, isRootSmaller);
			root = mux(oldRoot, incrementByOne(root), isRootSmaller);
		}
		return padSignal(rightShift(root), a.length);
	}

	public T[] inputOfAlice(double d) {
		return env.inputOfAlice(Utils.fromLong((long) d, width));
	}

	public T[] inputOfBob(double d) {
		return env.inputOfBob(Utils.fromLong((long) d, width));
	}

	@Override
	public CompEnv<T> getEnv() {
		return env;
	}

	@Override
	public T[] toSecureInt(T[] a, IntegerLib<T> lib) {
		return a;
	}

	// not fully implemented, more cases to consider
	@Override
	public T[] toSecureFloat(T[] a, FloatLib<T> lib) {
		T[] v = padSignal(a, lib.VLength);
		T[] p = leadingZeros(v);
		v = leftPrivateShift(v, p);
		p = padSignal(p, lib.PLength);
		p = sub(zeros(p.length), p);
		return lib.pack(new FloatLib.Representation<T>(SIGNAL_ZERO, v, p));
	}

	@Override
	public T[] toSecureFixPoint(T[] a, FixedPointLib<T> lib) {
		return leftPublicShift(padSignal(a, lib.width), lib.offset);
	}

	@Override
	public double outputToAlice(T[] a) {
		return Utils.toInt(env.outputToAlice(a));
	}

	@Override
	public int numBits() {
		return width;
	}
}