// Copyright (C) 2014 by Xiao Shaun Wang <wangxiao@cs.umd.edu>
package com.oblivm.backend.circuits;

import com.oblivm.backend.circuits.arithmetic.IntegerLib;
import com.oblivm.backend.flexsc.CompEnv;

public class BitonicSortLib<T> extends IntegerLib<T> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 548013917964346826L;

	public BitonicSortLib(CompEnv<T> e) {
		super(e);
	}

	public void sortWithPayload(T[][] a, T[][] data, T isAscending) {
		bitonicSortWithPayload(a, data, 0, a.length, isAscending);
	}

	private void bitonicSortWithPayload(T[][] key, T[][] data, int lo, int n,
			T dir) {
		if (n > 1) {
			int m = n / 2;
			bitonicSortWithPayload(key, data, lo, m, not(dir));
			bitonicSortWithPayload(key, data, lo + m, n - m, dir);
			bitonicMergeWithPayload(key, data, lo, n, dir);
		}
	}

	protected void bitonicMergeWithPayload(T[][] key, T[][] data, int lo, int n,
			T dir) {
		if (n > 1) {
			int m = greatestPowerOfTwoLessThan(n);
			for (int i = lo; i < lo + n - m; i++)
				compareWithPayload(key, data, i, i + m, dir);
			bitonicMergeWithPayload(key, data, lo, m, dir);
			bitonicMergeWithPayload(key, data, lo + m, n - m, dir);
		}
	}

	private void compareWithPayload(T[][] key, T[][] data, int i, int j, T dir) {
		T greater = not(leq(key[i], key[j]));
		T swap = eq(greater, dir);
		T[] s = mux(key[j], key[i], swap);
		s = xor(s, key[i]);
		T[] ki = xor(key[j], s);
		T[] kj = xor(key[i], s);
		key[i] = ki;
		key[j] = kj;

		T[] s2 = mux(data[j], data[i], swap);
		s2 = xor(s2, data[i]);
		T[] di = xor(data[j], s2);
		T[] dj = xor(data[i], s2);
		data[i] = di;
		data[j] = dj;
	}

	public void sort(T[][] a, T isAscending) {
		bitonicSort(a, 0, a.length, isAscending);
	}
	
	public void merge(T[][] a, T isAscending) {
		bitonicMerge(a, 0, a.length, isAscending);
	}

	private void bitonicSort(T[][] key, int lo, int n, T dir) {
		if (n > 1) {
			int m = n / 2;
			bitonicSort(key, lo, m, not(dir));
			bitonicSort(key, lo + m, n - m, dir);
			bitonicMerge(key, lo, n, dir);
		}
	}

	protected void bitonicMerge(T[][] key, int lo, int n, T dir) {
		if (n > 1) {
			int m = greatestPowerOfTwoLessThan(n);
			for (int i = lo; i < lo + n - m; i++)
				compare(key, i, i + m, dir);
			bitonicMerge(key, lo, m, dir);
			bitonicMerge(key, lo + m, n - m, dir);
		}
	}

	private void compare(T[][] key, int i, int j, T dir) {
		T swap = eq(not(leq(key[i], key[j])), dir);
		T[] s = mux(key[j], key[i], swap);
		s = xor(s, key[i]);
		T[] ki = xor(key[j], s);
		T[] kj = xor(key[i], s);
		key[i] = ki;
		key[j] = kj;
	}

	private int greatestPowerOfTwoLessThan(int n) {
		int k = 1;
		while (k < n)
			k = k << 1;
		return k >> 1;
	}
	
	public void sortWithPayload(T[] a, T[][] data, T isAscending) {
		bitonicSortWithPayload(a, data, 0, a.length, isAscending);
	}

	private void bitonicSortWithPayload(T[] key, T[][] data, int lo, int n,
			T dir) {
		if (n > 1) {
			int m = n / 2;
			bitonicSortWithPayload(key, data, lo, m, not(dir));
			bitonicSortWithPayload(key, data, lo + m, n - m, dir);
			bitonicMergeWithPayload(key, data, lo, n, dir);
		}
	}

	private void bitonicMergeWithPayload(T[] key, T[][] data, int lo, int n,
			T dir) {
		if (n > 1) {
			int m = greatestPowerOfTwoLessThan(n);
			for (int i = lo; i < lo + n - m; i++)
				compareWithPayload(key, data, i, i + m, dir);
			bitonicMergeWithPayload(key, data, lo, m, dir);
			bitonicMergeWithPayload(key, data, lo + m, n - m, dir);
		}
	}

	private void compareWithPayload(T[] key, T[][] data, int i, int j, T dir) {
		T greater = and(key[i], not(key[j]));
		T swap = eq(greater, dir);
		T s = mux(key[j], key[i], swap);
		s = xor(s, key[i]);
		T ki = xor(key[j], s);
		T kj = xor(key[i], s);
		key[i] = ki;
		key[j] = kj;

		T[] s2 = mux(data[j], data[i], swap);
		s2 = xor(s2, data[i]);
		T[] di = xor(data[j], s2);
		T[] dj = xor(data[i], s2);
		data[i] = di;
		data[j] = dj;
	}
	
	public void sortWithPayload(T[][] a, T[] data, T isAscending) {
		bitonicSortWithPayload(a, data, 0, a.length, isAscending);
	}

	private void bitonicSortWithPayload(T[][] key, T[] data, int lo, int n,
			T dir) {
		if (n > 1) {
			int m = n / 2;
			bitonicSortWithPayload(key, data, lo, m, not(dir));
			bitonicSortWithPayload(key, data, lo + m, n - m, dir);
			bitonicMergeWithPayload(key, data, lo, n, dir);
		}
	}

	private void bitonicMergeWithPayload(T[][] key, T[] data, int lo, int n,
			T dir) {
		if (n > 1) {
			int m = greatestPowerOfTwoLessThan(n);
			for (int i = lo; i < lo + n - m; i++)
				compareWithPayload(key, data, i, i + m, dir);
			bitonicMergeWithPayload(key, data, lo, m, dir);
			bitonicMergeWithPayload(key, data, lo + m, n - m, dir);
		}
	}

	private void compareWithPayload(T[][] key, T[] data, int i, int j, T dir) {
		T greater = not(leq(key[i], key[j]));
		T swap = eq(greater, dir);
		T[] s = mux(key[j], key[i], swap);
		s = xor(s, key[i]);
		T[] ki = xor(key[j], s);
		T[] kj = xor(key[i], s);
		key[i] = ki;
		key[j] = kj;

		T s2 = mux(data[j], data[i], swap);
		s2 = xor(s2, data[i]);
		T di = xor(data[j], s2);
		T dj = xor(data[i], s2);
		data[i] = di;
		data[j] = dj;
	}
	

}