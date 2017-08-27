// Copyright (C) 2014 by Xiao Shaun Wang <wangxiao@cs.umd.edu>
package com.oblivm.backend.flexsc;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.Security;

import com.oblivm.backend.network.Network;
import com.oblivm.backend.rand.ISAACProvider;
import com.oblivm.backend.util.Utils;

public abstract class CompEnv<T> implements java.io.Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 4236106863602585722L;
	/**
	 * 
	 */
	public long numOfAnds = 0;
	public static SecureRandom rnd;
	static{
		Security.addProvider(new ISAACProvider());
		try {
			rnd = SecureRandom.getInstance("ISAACRandom");

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("rawtypes")
	public static CompEnv getEnv(Mode mode, Party p, Network w) {
		if (mode == Mode.REAL)
			if (p == Party.Bob)
				return new com.oblivm.backend.gc.regular.GCEva(w);
			else
				return new com.oblivm.backend.gc.regular.GCGen(w);		
		else if (mode == Mode.OPT)
			if (p == Party.Bob)
				return new com.oblivm.backend.gc.halfANDs.GCEva(w);
			else
				return new com.oblivm.backend.gc.halfANDs.GCGen(w);
		else if (mode == Mode.VERIFY)
			return new CVCompEnv(w, p);
		else if (mode == Mode.COUNT)
			return new PMCompEnv(w, p);
		else if (mode == Mode.OFFLINE) {
			if (p == Party.Bob)
				return new com.oblivm.backend.gc.offline.GCEva(w);
			else
				return new com.oblivm.backend.gc.offline.GCGen(w);
		} else {
			try {
				throw new Exception("not a supported Mode!");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}
	}

	public Network channel;
	public Party party;
	public Mode mode;

	public CompEnv(Network w, Party p, Mode m) {
		this.channel = w;
		this.mode = m;
		this.party = p;
	}

	public abstract T inputOfAlice(boolean in);

	public abstract T inputOfBob(boolean in);

	public abstract boolean outputToAlice(T out);

	public abstract boolean outputToBob(T out);
	
	public abstract T[] inputOfAlice(boolean[] in);

	public abstract T[] inputOfBob(boolean[] in);
	
	public T[][] inputOfAlice(boolean[][] in) {
		boolean[] flattened = Utils.flatten(in);
		T[] res = inputOfAlice(flattened);
		T[][] unflattened = newTArray(in.length, in[0].length);
		Utils.unflatten(res, unflattened);
		return unflattened;
	}
	
	public T[][] inputOfBob(boolean[][] in) {
		boolean[] flattened = Utils.flatten(in);
		T[] res = inputOfBob(flattened);
		T[][] unflattened = newTArray(in.length, in[0].length);
		Utils.unflatten(res, unflattened);
		return unflattened;
	}

	public T[][][] inputOfAlice(boolean[][][] in) {
		boolean[] flattened = Utils.flatten(in);
		T[] res = inputOfAlice(flattened);
		T[][][] unflattened = newTArray(in.length, in[0].length, in[0][0].length);
		Utils.unflatten(res, unflattened);
		return unflattened;
	}
	
	public T[][][] inputOfBob(boolean[][][] in) {
		boolean[] flattened = Utils.flatten(in);
		T[] res = inputOfBob(flattened);
		T[][][] unflattened = newTArray(in.length, in[0].length, in[0][0].length);
		Utils.unflatten(res, unflattened);
		return unflattened;
	}

	
	public abstract boolean[] outputToAlice(T[] out);

	public abstract boolean[] outputToBob(T[] out);
	
	public abstract T and(T a, T b);

	public abstract T xor(T a, T b);

	public abstract T not(T a);

	public abstract T ONE();

	public abstract T ZERO();

	public abstract T[] newTArray(int len);

	public abstract T[][] newTArray(int d1, int d2);

	public abstract T[][][] newTArray(int d1, int d2, int d3);

	public abstract T newT(boolean v);

	public Party getParty() {
		return party;
	}

	public void flush() {
		channel.flush();
	}

	public Mode getMode() {
		return mode;
	}
}