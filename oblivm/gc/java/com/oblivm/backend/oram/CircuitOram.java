// Copyright (C) 2014 by Xiao Shaun Wang <wangxiao@cs.umd.edu>
package com.oblivm.backend.oram;

import java.util.Arrays;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Party;

public class CircuitOram<T> extends TreeBasedOramParty<T> implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6371390195674056520L;
	/**
	 * 
	 */

	public CircuitOramLib<T> lib;
	Block<T>[] scQueue;
	int cnt = 0;
	public PlainBlock[] queue;
	public int queueCapacity;

	boolean[] nextPath() {
		boolean[] res = new boolean[logN];
		int temp = cnt;
		for (int i = res.length - 1; i >= 0; --i) {
			res[i] = (temp & 1) == 1;
			temp >>= 1;
		}
		cnt = (cnt + 1) % N;
		return res;
	}

	public CircuitOram(CompEnv<T> env, int N, int dataSize, int cap, int sp) {
		super(env, N, dataSize, cap);
		lib = new CircuitOramLib<T>(lengthOfIden, lengthOfPos, lengthOfData,
				logN, capacity, env);
		queueCapacity = 30;
		queue = new PlainBlock[queueCapacity];

		for (int i = 0; i < queue.length; ++i)
			queue[i] = getDummyBlock(p == Party.Alice);

		scQueue = prepareBlocks(queue, queue);
	}

	public CircuitOram(CompEnv<T> env, int N, int dataSize) {
		super(env, N, dataSize, 3);
		lib = new CircuitOramLib<T>(lengthOfIden, lengthOfPos, lengthOfData,
				logN, capacity, env);
		queueCapacity = 30;
		queue = new PlainBlock[queueCapacity];

		for (int i = 0; i < queue.length; ++i)
			queue[i] = getDummyBlock(p == Party.Alice);

		scQueue = prepareBlocks(queue, queue);

	}

	protected void ControlEviction() {
		flushOneTime(nextPath());
		flushOneTime(nextPath());
	}

	public void flushOneTime(boolean[] pos) {
		PlainBlock[][] blocks = getPath(pos);
		Block<T>[][] scPath = preparePath(blocks, blocks);

		lib.flush(scPath, pos, scQueue);

		blocks = preparePlainPath(scPath);
		putPath(blocks, pos);
	}

	int initalValue = 0;
	public void setInitialValue(int intial) {
		initalValue = intial;
	}
	public T[] readAndRemove(T[] scIden, boolean[] pos,
			boolean RandomWhenNotFound) {
		PlainBlock[][] blocks = getPath(pos);
		Block<T>[][] scPath = preparePath(blocks, blocks);

		Block<T> res = lib.readAndRemove(scPath, scIden);
		Block<T> res2 = lib.readAndRemove(scQueue, scIden);
		res = lib.mux(res, res2, res.isDummy);

		blocks = preparePlainPath(scPath);
		putPath(blocks, pos);

		if (RandomWhenNotFound) {
			PlainBlock b = randomBlock();
			Block<T> scb = inputBlockOfClient(b);
			Block<T> finalRes = lib.mux(res, scb, res.isDummy);

			return finalRes.data;
		} else {
			return lib.mux(res.data, lib.toSignals(initalValue, res.data.length), res.isDummy);
		}
	}

	public void putBack(T[] scIden, T[] scNewPos, T[] scData) {
		Block<T> b = new Block<T>(scIden, scNewPos, scData, lib.SIGNAL_ZERO);
		lib.add(scQueue, b);

		env.flush();
		ControlEviction();
	}

	public T[] read(T[] scIden, boolean[] pos, T[] scNewPos) {
		scIden = Arrays.copyOf(scIden, lengthOfIden);
		T[] r = readAndRemove(scIden, pos, false);
		putBack(scIden, scNewPos, r);
		return r;
	}

	public void write(T[] scIden, boolean[] pos, T[] scNewPos, T[] scData)  {
		scIden = Arrays.copyOf(scIden, lengthOfIden);
		readAndRemove(scIden, pos, false);
		putBack(scIden, scNewPos, scData);
	}
	
	public void write(T[] scIden, T[] pos, T[] scNewPos, T[] scData, T dummy)  {
		scIden = Arrays.copyOf(scIden, lengthOfIden);
		conditionalReadAndRemove(scIden, pos, dummy);
		conditionalPutBack(scIden, scNewPos, scData, dummy);
	}

	public T[] access(T[] scIden, boolean[] pos, T[] scNewPos, T[] scData, T op) {
		scIden = Arrays.copyOf(scIden, lengthOfIden);
		T[] r = readAndRemove(scIden, pos, false);
		T[] toWrite = lib.mux(r, scData, op);
		putBack(scIden, scNewPos, toWrite);
		return toWrite;
	}

	public T[] conditionalReadAndRemove(T[] scIden, T[] pos, T condition) {
		// Utils.print(env, "rar: iden:", scIden, pos, condition);
		scIden = Arrays.copyOf(scIden, lengthOfIden);
		T[] scPos = Arrays.copyOf(pos, lengthOfPos);
		T[] randbools = lib.randBools(scPos.length);
		T[] posToUse = lib.mux(randbools, scPos, condition);

		boolean[] path = lib.declassifyToBoth(posToUse);
		
		PlainBlock[][] blocks = getPath(path);
		Block<T>[][] scPath = preparePath(blocks, blocks);

		Block<T> res = lib.conditionalReadAndRemove(scPath, scIden, condition);
		Block<T> res2 = lib
				.conditionalReadAndRemove(scQueue, scIden, condition);
		res = lib.mux(res, res2, res.isDummy);

		blocks = preparePlainPath(scPath);
		putPath(blocks, path);
		env.flush();
		return lib.mux(res.data, lib.toSignals(initalValue, res.data.length), res.isDummy);
	}

	public void conditionalPutBack(T[] scIden, T[] scNewPos, T[] scData,
			T condition) {
		env.flush();
		scIden = Arrays.copyOf(scIden, lengthOfIden);

		Block<T> b = new Block<T>(scIden, scNewPos, scData, lib.SIGNAL_ZERO);
		lib.conditionalAdd(scQueue, b, condition);
		env.flush();
		ControlEviction();
	}
}
