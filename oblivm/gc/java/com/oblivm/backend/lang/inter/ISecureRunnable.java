/***
 * Copyright (C) 2015 by Chang Liu <liuchang@cs.umd.edu>
 */
package com.oblivm.backend.lang.inter;

import com.oblivm.backend.oram.SecureArray;

public interface ISecureRunnable<T> {
	public T[] main(int lenA, int lenB, T[] x, T[] y) throws Exception;
		
	// implement for smcql
	 public SecureArray<T> run(SecureArray<T> a, SecureArray<T> b) throws Exception;
	 

}
