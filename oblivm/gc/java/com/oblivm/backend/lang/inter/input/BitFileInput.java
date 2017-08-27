/***
 * Copyright (C) 2015 by Chang Liu <liuchang@cs.umd.edu>
 */
package com.oblivm.backend.lang.inter.input;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.stream.IntStream;

import com.oblivm.backend.lang.inter.Input;

/**
 * @author Chang Liu
 *
 */
public class BitFileInput implements Input, java.io.Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6370910709031971077L;
	/**
	 * 
	 */

	private boolean[] array;
	private int current = 0;
	
	public static BitFileInput open(File file) throws IOException {
		BitFileInput input = new BitFileInput();
		BufferedReader binput = new BufferedReader(new FileReader(file));
		String str;
		StringBuffer sb = new StringBuffer();
		while((str = binput.readLine()) != null) {
			sb.append(str);
		}
		binput.close();
		IntStream ins = sb.toString().chars();
		int[] a = ins.toArray();
		input.array = new boolean[a.length];
		for(int i=0; i<a.length; ++i) {
			input.array[i] = a[i] == '1';
		}
		input.current = 0;
		return input;
	}
	
	private BitFileInput() {
		
	}
	
	@Override
	public boolean nextBoolean() {
		if(current >= array.length)
			return false;
		return array[current ++];
	}

	@Override
	public boolean[] readAll() {
		return array;
	}

	@Override
	public boolean isEnd() {
		return current >= array.length;
	}

	@Override
	public boolean closed() {
		return current >= array.length;
	}

	public static BitFileInput open(String path) throws IOException {
		return open(new File(path));
	}

}
