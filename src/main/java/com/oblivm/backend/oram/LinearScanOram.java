// Copyright (C) 2014 by Xiao Shaun Wang <wangxiao@cs.umd.edu>
package com.oblivm.backend.oram;

import com.oblivm.backend.circuits.arithmetic.IntegerLib;
import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.util.Utils;

public class LinearScanOram<T> implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -9185207538871984597L;
	/**
	 * 
	 */

	public T[][] content;
	public CompEnv<T> env;
	public int lengthOfIden;
	public IntegerLib<T> lib;
	public int dataSize;
	
	
	public LinearScanOram(CompEnv<T> env, int N, int dataSize) {
		this.env = env;
		this.dataSize = dataSize;
		lib = new IntegerLib<T>(env);
		content = env.newTArray(N, 0);
		lengthOfIden = Utils.log2Ceil(N);
		
		for(int i = 0; i < N; ++i)
			content[i] = lib.zeros(dataSize);
	}

	public void add(T[] iden, T[] data, T dummy) {
		T[] iden1 = lib.padSignal(iden, lengthOfIden);
		for(int i = 0; i < content.length; ++i) {
			T eq = lib.eq(iden1, lib.toSignals(i, lengthOfIden));
			eq = lib.and(eq, dummy);
			content[i] = lib.mux(content[i], data, eq);
		}
	}
	
	public void add(T[] iden, T[] data) {
		T[] iden1 = lib.padSignal(iden, lengthOfIden);
		for(int i = 0; i < content.length; ++i) {
			T eq = lib.eq(iden1, lib.toSignals(i, lengthOfIden));
			assert(eq != null);
			assert(content[i] != null);
			content[i] = lib.mux(content[i], data, eq);
		}
	}

	public T[] readAndRemove(T[] iden) {
		return readAndRemove(iden, false);
	}

	public T[] readAndRemove(T[] iden, boolean randomWhennotFound) {
		T[] iden1 = lib.padSignal(iden, lengthOfIden);
		T[] res = lib.zeros(content[0].length);
		
		for(int i = 0; i < content.length; ++i) {
			T eq = lib.eq(iden1, lib.toSignals(i, lengthOfIden));
			res = lib.mux(res, content[i],  eq);
		}
		return res;
	}

	public T[] read(T[] iden) {
		return readAndRemove(iden, false);
	}

	public void write(T[] iden, T[] data) {
		add(iden, data);
	}

	public void write(T[] iden, T[] data, T dummy) {
		add(iden, data, dummy);
	}
	
	public void putBack(T[] scIden, T[] scData) {
		add(scIden, scData);
	}
}
