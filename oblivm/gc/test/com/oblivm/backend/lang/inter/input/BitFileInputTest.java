/***
 * Copyright (C) 2015 by Chang Liu <liuchang@cs.umd.edu>
 */
package com.oblivm.backend.lang.inter.input;

/**
 * @author Chang Liu
 *
 */
public class BitFileInputTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		BitFileInput input = BitFileInput.open("./testcase/bitinput.txt");
		boolean[] array = input.readAll();
		for(boolean b : array)
			if(b) System.out.print("1");
			else  System.out.print("0");
	}

}
