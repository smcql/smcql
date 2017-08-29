package com.oblivm.backend.circuits.arithmetic;

import java.util.Arrays;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.util.Utils;

public class FloatLib<T> implements ArithmeticLib<T>, java.io.Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1195246549920672218L;
	/**
	 * 
	 */

	CompEnv<T> env;
	IntegerLib<T> lib;
	public int VLength;
	public int PLength;

	public FloatLib(CompEnv<T> e, int VLength, int PLength) {
		this.env = e;
		lib = new IntegerLib<>(e);
		this.VLength = VLength;
		this.PLength = PLength;
	}

	public T[] inputOfAlice(double d) {
		return env.inputOfAlice(Utils.fromFloat(d, VLength, PLength));
	}

	public T[] inputOfBob(double d) {
		return env.inputOfBob(Utils.fromFloat(d, VLength, PLength));
	}

	public  T[] pack(Representation<T> f) {
		assert (f.v.length == VLength && f.p.length == PLength) : "pack: not compatiable";
		T[] res = env.newTArray(1 + f.v.length + f.p.length);
		res[0] = f.s;
		System.arraycopy(f.v, 0, res, 1, f.v.length);
		System.arraycopy(f.p, 0, res, 1 + f.v.length, f.p.length);
		return res;
	}

	public Representation<T> unpack(T[] data) {
		assert (data.length == VLength + PLength + 1) : "unpack: not compatiable";

		T[] v = Arrays.copyOfRange(data, 1, 1 + VLength);
		T[] p = Arrays.copyOfRange(data, 1 + VLength, data.length);
		return new Representation<T>(data[0], v, p);
	}

	public static class Representation<T> {
		public T s;
		public T[] v;
		public T[] p;

		public Representation(T sign, T[] v, T[] p) {
			this.s = sign;
			this.p = p;
			this.v = v;
		}
	}

	public T[] multiply(T[] fa, T[] fb) {
		Representation<T> a = unpack(fa);
		Representation<T> b = unpack(fb);

		T new_s = lib.xor(a.s, b.s);
		T[] a_multi_b = lib.karatsubaMultiply(a.v, b.v);// length 2*v.length
		T[] a_add_b = lib.add(a.p, b.p);

		T toShift = lib.not(a_multi_b[a_multi_b.length - 1]);
		T[] Shifted = lib.conditionalLeftPublicShift(a_multi_b, 1, toShift);

		T[] new_v = Arrays.copyOfRange(Shifted, a.v.length, a.v.length * 2);
		T[] new_p = lib.add(a_add_b, lib.toSignals(a.v.length, a_add_b.length));

		T[] decrement = lib.zeros(new_p.length);
		decrement[0] = toShift;
		new_p = lib.sub(new_p, decrement);
		Representation<T> res = new Representation<T>(new_s, new_v, new_p);
		return pack(res);
	}

	public T[] div(T[] fa, T[] fb) {
		Representation<T> a = unpack(fa);
		Representation<T> b = unpack(fb);

		T new_s = lib.xor(a.s, b.s);
		int length = a.v.length;
		int newLength = a.v.length * 2;
		T[] padded_av = lib.padSignal(a.v, newLength);
		T[] padded_bv = lib.padSignal(b.v, b.v.length + 1);
		T[] shifted_av = lib.leftPublicShift(padded_av, newLength - length - 1);
		// must be postive number div. so avoid div(shifted_av, padded_bv);
		T[] a_div_b = Arrays.copyOf(lib.divInternal(shifted_av, padded_bv),
				shifted_av.length);
		T[] leadingzero = lib.leadingZeros(a_div_b);

		T[] sh = lib.leftPrivateShift(a_div_b, leadingzero);
		sh = lib.rightPublicShift(sh, newLength - length);
		T[] new_v = Arrays.copyOf(sh, length);
		T[] new_p = lib.add(lib.sub(a.p, b.p), lib.toSignals(1, a.p.length));
		new_p = lib.sub(lib.padSignal(new_p, leadingzero.length), leadingzero);
		new_p = lib.padSignedSignal(new_p, a.p.length);

		Representation<T> res = new Representation<T>(new_s, new_v, new_p);
		return pack(res);
	}

	public T[] publicValue(double d) {
		boolean[] b = Utils.fromFloat(d, VLength, PLength);
		T[] res = env.newTArray(PLength + VLength + 1);
		for (int i = 0; i < b.length; ++i)
			res[i] = b[i] ? lib.SIGNAL_ONE : lib.SIGNAL_ZERO;
		return res;
	}

	// assuming na = va*2^p, nb = vb*2^(p+pDiff)
	private T[] addInternal(T sa, T sb, T[] va, T[] vb, T[] p, T[] pDiff) {
		int temp_length = 2 * VLength + 1;
		T[] signedVa = lib.padSignal(va, temp_length);
		T[] signedVb = lib.padSignal(vb, temp_length);
		signedVb = lib.leftPrivateShift(signedVb, pDiff);

		signedVa = lib.addSign(signedVa, sa);
		signedVb = lib.addSign(signedVb, sb);

		T[] new_v = lib.add(signedVa, signedVb);

		T new_s = new_v[new_v.length - 1];
		new_v = lib.absolute(new_v);

		T[] leadingzero = lib.leadingZeros(new_v);

		T[] sh = lib.leftPrivateShift(new_v, leadingzero);
		sh = lib.rightPublicShift(sh, temp_length - VLength);
		new_v = Arrays.copyOf(sh, VLength);

		T[] new_p = lib.sub(lib.padSignal(p, leadingzero.length), leadingzero);
		new_p = lib.add(new_p,
				lib.toSignals(temp_length - VLength, new_p.length));
		new_p = lib.padSignedSignal(new_p, PLength);

		Representation<T> res = new Representation<T>(new_s, new_v, new_p);

		return pack(res);
	}

	public T[] add(T[] fa, T[] fb) {
		T[] va = Arrays.copyOfRange(fa, 1, 1 + VLength);
		T[] vb = Arrays.copyOfRange(fb, 1, 1 + VLength);
		T[] pa = Arrays.copyOfRange(fa, 1 + VLength, fa.length);
		T[] pb = Arrays.copyOfRange(fb, 1 + VLength, fb.length);

		T[] pDifference = lib.sub(pa, pb);
		T[] pDiffAbs = lib.absolute(pDifference);

		T paGreater = lib.not(pDifference[pDifference.length - 1]);
		T[] pToUse = lib.mux(pa, pb, paGreater);
		T[] normalCase = addInternal(lib.mux(fa[0], fb[0], paGreater),
				lib.mux(fb[0], fa[0], paGreater), lib.mux(va, vb, paGreater),
				lib.mux(vb, va, paGreater), pToUse, pDiffAbs);
		T underFlowHappen = lib.not(lib.leq(pDiffAbs,
				lib.toSignals(VLength, pDiffAbs.length)));
		T[] underFlowResult = lib.mux(fb, fa, paGreater);
		return lib.mux(normalCase, underFlowResult, underFlowHappen);
	}

	// (v*s^p)^(1/2) =
	public T[] sqrt(T[] fa) {
		int newLength = VLength + 2 + 1;
		T[] va = Arrays.copyOfRange(fa, 1, 1 + VLength);
		T[] pa = Arrays.copyOfRange(fa, 1 + VLength, fa.length);

		va = lib.padSignal(va, newLength);
		va = lib.leftPublicShift(va, 1);
		pa = lib.sub(pa, lib.toSignals(1, PLength));

		va = lib.conditionalLeftPublicShift(va, 1, pa[0]);

		va = lib.sqrt(va);
		pa = lib.rightPublicShift(pa, 1);
		pa[pa.length - 1] = pa[pa.length - 2];

		T[] leadingzero = lib.leadingZeros(va);
		T[] sh = lib.leftPrivateShift(va, leadingzero);
		sh = lib.rightPublicShift(sh, newLength - VLength);
		T[] new_v = Arrays.copyOf(sh, VLength);

		pa = lib.sub(pa, lib.padSignal(leadingzero, pa.length));
		pa = lib.add(pa, lib.toSignals(newLength - VLength, PLength));

		return pack(new Representation<T>(lib.SIGNAL_ZERO, new_v, pa));
	}

	public T[] sub(T[] a, T[] b) {
		T[] negB = Arrays.copyOf(b, b.length);
		negB[0] = lib.not(negB[0]);
		return add(a, negB);
	}

	public T leq(T[] a, T[] b) {
		T[] res = sub(a, b);
		return lib.not(res[0]);
	}

	public T eq(T[] a, T[] b) {
		return lib.eq(a, b);
	}
	
	@Override
	public CompEnv<T> getEnv() {
		return env;
	}

	@Override
	public T[] toSecureInt(T[] a, IntegerLib<T> lib) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public T[] toSecureFloat(T[] a, FloatLib<T> lib) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public T[] toSecureFixPoint(T[] a, FixedPointLib<T> lib) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double outputToAlice(T[] a) {
		return Utils.toFloat(env.outputToAlice(a), VLength, PLength);
	}

	@Override
	public int numBits() {
		return VLength+PLength+1;
	}
}
