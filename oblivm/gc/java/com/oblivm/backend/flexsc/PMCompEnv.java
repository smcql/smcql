// Copyright (C) 2014 by Xiao Shaun Wang <wangxiao@cs.umd.edu>
package com.oblivm.backend.flexsc;

import com.oblivm.backend.network.Network;
import com.oblivm.backend.util.Utils;

/*
 * The computational environment for performance measurement. 
 */
public class PMCompEnv extends BooleanCompEnv {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8968339036794936447L;

	public static class Statistics {
		public long andGate = 0;
		public long xorGate = 0;
		public long notGate = 0;
		public long OTs = 0;
		public long NumEncAlice = 0;
		public long NumEncBob = 0;
		public long bandwidth = 0;

		public void flush() {
			bandwidth = 0;
			andGate = 0;
			xorGate = 0;
			notGate = 0;
			OTs = 0;
			NumEncAlice = 0;
			NumEncBob = 0;
		}

		public void add(Statistics s2) {
			andGate += s2.andGate;
			xorGate += s2.xorGate;
			notGate += s2.notGate;
			OTs += s2.OTs;
			NumEncAlice += s2.NumEncAlice;
			NumEncBob += s2.NumEncBob;
			bandwidth += s2.bandwidth;
		}

		public void finalize() {
			NumEncAlice = andGate * 4 + OTs * 2;
			NumEncBob = andGate * 1 + OTs * 1;
		}

		public Statistics newInstance() {
			Statistics s = new Statistics();
			s.andGate = andGate;
			s.xorGate = xorGate;
			s.notGate = notGate;
			s.OTs = OTs;
			s.NumEncAlice = NumEncAlice;
			s.NumEncBob = NumEncBob;
			s.bandwidth = bandwidth;
			return s;
		}
	}

	public Statistics statistic;


	public PMCompEnv(Network channel, Party p) {
		super(channel, p, Mode.COUNT);
		this.party = p;
		t = true;
		f = false;
		statistic = new Statistics();
	}

	@Override
	public Boolean inputOfAlice(boolean in) {
		return f;
	}

	@Override
	public Boolean inputOfBob(boolean in) {
		++statistic.OTs;
		statistic.bandwidth += 10;
		return f;
	}

	@Override
	public boolean outputToAlice(Boolean out) {
		statistic.bandwidth += 10;
		return false;
	}

	@Override
	public boolean outputToBob(Boolean out) {
		statistic.bandwidth += 10;
		return false;
	}

	@Override
	public Boolean and(Boolean a, Boolean b) {
		++statistic.andGate;
		++this.numOfAnds;
		statistic.bandwidth += 3 * 10;
		return f;
	}

	@Override
	public Boolean xor(Boolean a, Boolean b) {
		++statistic.xorGate;
		return f;
	}

	@Override
	public Boolean not(Boolean a) {
		++statistic.notGate;
		return f;
	}

	@Override
	public boolean[] outputToAlice(Boolean[] out) {
		statistic.bandwidth += 10 * out.length;
		return Utils.tobooleanArray(out);
	}

	@Override
	public boolean[] outputToBob(Boolean[] out) {
		statistic.bandwidth += 10 * out.length;
		return Utils.tobooleanArray(out);
	}

	@Override
	public Boolean[] inputOfAlice(boolean[] in) {
		statistic.bandwidth += 10*in.length;
		return Utils.toBooleanArray(in);
	}

	@Override
	public Boolean[] inputOfBob(boolean[] in) {
		statistic.OTs += in.length;
		statistic.bandwidth += 10 * 2 * (80 + in.length);
		return Utils.toBooleanArray(in);
	}
}