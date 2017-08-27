/**
 * Copyright (C) 2015 by Chang Liu <liuchang@cs.umd.edu>
 */
package com.oblivm.compiler.log;

/**
 * @author Chang Liu
 *
 */
public class ReadArray extends Log {
	public static ReadArray LOG = new ReadArray();
	
	private ReadArray() { 
		super(System.err); 
	}

	@Override
	public String tag() {
		return "READ_ARRAY";
	}

}
