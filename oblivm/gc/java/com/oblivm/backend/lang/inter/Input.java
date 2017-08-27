/***
 * Copyright (C) 2015 by Chang Liu <liuchang@cs.umd.edu>
 */
package com.oblivm.backend.lang.inter;

public interface Input {
	public boolean nextBoolean();
	public boolean[] readAll();
	public boolean isEnd();
	public boolean closed();
}
