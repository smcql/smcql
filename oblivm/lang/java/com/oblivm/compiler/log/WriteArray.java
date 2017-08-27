/**
 * Copyright (C) 2015 by Chang Liu <liuchang@cs.umd.edu>
 */
package com.oblivm.compiler.log;

/**
 * @author Chang Liu
 *
 */
public class WriteArray extends Log {
	public static WriteArray LOG = new WriteArray();
	
	private WriteArray() { 
		super(System.err); 
	}

	@Override
	public String tag() {
		return "WRITE_ARRAY";
	}

}
