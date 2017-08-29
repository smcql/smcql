// Copyright (C) 2013 by Yan Huang <yhuang@cs.umd.edu>
// 					 and Xiao Shaun Wang <wangxiao@cs.umd.edu>

package com.oblivm.backend.flexsc;

public enum Mode {
	// verify the correctness of the circuit without running the protocol
	VERIFY, 
	//GRR3 + Free XOR
	REAL,
	//Simulating the protocol and count number of gates/encs
	COUNT,
	//Half Gates
	OPT,
	//Offline
	OFFLINE;

	public static Mode getMode(String optionValue) {
		if(optionValue.equals("VERIFY")) {
			return VERIFY;
		} else if(optionValue.equals("REAL")) {
			return REAL;
		} else if(optionValue.equals("COUNT")) {
			return COUNT;
		} else if(optionValue.equals("OPT")) {
			return OPT;
		} else if(optionValue.equals("OFFLINE")) {
			return OPT;
		} else return null;
	}
}
