/**
 * Copyright (C) 2015 by Chang Liu <liuchang@cs.umd.edu>
 */
package com.oblivm.compiler.log;

/**
 * @author Chang Liu
 *
 */
public class SetNonNullArrayEntries extends Log {
	public static SetNonNullArrayEntries LOG = new SetNonNullArrayEntries();
	
	private SetNonNullArrayEntries() { 
		super(System.err); 
	}

	@Override
	public String tag() {
		return "SET_NONNULL_ARRAY_ENTRIES";
	}

}
