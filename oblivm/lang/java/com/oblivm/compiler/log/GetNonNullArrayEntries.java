/**
 * Copyright (C) 2015 by Chang Liu <liuchang@cs.umd.edu>
 */
package com.oblivm.compiler.log;

/**
 * @author Chang Liu
 *
 */
public class GetNonNullArrayEntries extends Log {
	public static GetNonNullArrayEntries LOG = new GetNonNullArrayEntries();
	
	private GetNonNullArrayEntries() { 
		super(System.err); 
	}

	@Override
	public String tag() {
		return "GET_NONNULL_ARRAY_ENTRIES";
	}

}
