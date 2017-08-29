// Copyright (C) 2014 by Xiao Shaun Wang <wangxiao@cs.umd.edu>
package com.oblivm.backend.oram;

import java.util.Arrays;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Party;

public class TrivialPrivateOram<T> extends OramParty<T> {
	public PlainBlock[] bucket;
	Block<T>[] result;
	int capacity;

	public TrivialPrivateOram(CompEnv<T> env, int N, int dataSize) {
		super(env, N, dataSize, 1);
		this.capacity = N;
		bucket = new PlainBlock[capacity];

		for (int i = 0; i < bucket.length; ++i) {
			bucket[i] = getDummyBlock(p == Party.Alice);
		}
		result = prepareBlocks(bucket, bucket);
	}

	int InitialValue = 0;
	public void setInitialValue(int initial) {
		InitialValue = initial;
	}
	public void add(T[] iden, T[] data) {
		T[] pos = env.newTArray(1);
		pos[0] = lib.SIGNAL_ONE;
		Block<T> scNewBlock = new Block<T>(iden, pos, data, lib.SIGNAL_ZERO);
		lib.add(result, scNewBlock);
	}

	public T[] readAndRemove(T[] scIden) {
		return readAndRemove(scIden, true);
	}

	public T[] readAndRemove(T[] scIden, boolean randomWhennotFound) {
		scIden = lib.padSignal(scIden, lengthOfIden);
		Block<T> res = lib.readAndRemove(result, scIden);
		T[] finalRes;
		if (randomWhennotFound) {
			PlainBlock b1 = randomBlock();
			Block<T> scb1 = inputBlockOfClient(b1);
			finalRes = lib.mux(res.data, scb1.data, res.isDummy);
		} else {
			finalRes = lib.mux(res.data, lib.toSignals(InitialValue, res.data.length),
					res.isDummy);
		}

		return finalRes;
	}
	
	public T[] read(int index) {
		return result[index].data;
	}
	
	public void write(int index, T[] d) {
		result[index].data = d;
	}

	public T[] read(T[] scIden) {
		scIden = Arrays.copyOf(scIden, lengthOfIden);
		T[] r = readAndRemove(scIden, false);
		putBack(scIden, r);

		return r;
	}

	public void write(T[] scIden, T[] b) {
		scIden = Arrays.copyOf(scIden, lengthOfIden);
		readAndRemove(scIden);
		putBack(scIden, b);
	}

	public void putBack(T[] scIden, T[] scData) {
		scIden = Arrays.copyOf(scIden, lengthOfIden);
		add(scIden, scData);
	}
	
	public T[] conditionalReadAndRemove(T[] iden, T condition) {
		return lib.conditionalReadAndRemove(result, iden, condition).data;
	}
	public void conditionalPutBack(T[] iden, T[] data, T condition) {
		T[] pos = env.newTArray(1);
		pos[0] = lib.SIGNAL_ONE;
		Block<T> scNewBlock = new Block<T>(iden, pos, data, lib.SIGNAL_ZERO);
		lib.conditionalAdd(result, scNewBlock, condition);
	}

}
