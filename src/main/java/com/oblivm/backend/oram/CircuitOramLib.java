// Copyright (C) 2014 by Xiao Shaun Wang <wangxiao@cs.umd.edu>
package com.oblivm.backend.oram;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Party;
import com.oblivm.backend.util.Utils;

public class CircuitOramLib<T> extends BucketLib<T> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4272825938906352838L;
	int logN;
	int loglogN;
	int capacity;

	public CircuitOramLib(int lengthOfIden, int lengthOfPos, int lengthOfData,
			int logN, int capacity, CompEnv<T> e) {
		super(lengthOfIden, lengthOfPos, lengthOfData, e);
		this.logN = logN;
		this.capacity = capacity;
	}

	public T[] deepestLevellocal(T[] pos, T[] path) {
		T[] xored = xor(pos, path);
		return leadingZeros(xored);
	}

	public T[][] deepestBlockShort(Block<T>[] bucket, T[] pathSignal) {
		T[][] deepest = env.newTArray(bucket.length, 0);// ;new
														// T[nodeCapacity][];
		for (int j = 0; j < bucket.length; ++j) {
			deepest[j] = deepestLevellocal(bucket[j].pos, pathSignal);
		}

		T[] maxIden = bucket[0].iden;
		T[] maxdepth = deepest[0];
		T isDummy = bucket[0].isDummy;
		for (int j = 1; j < bucket.length; ++j) {
			T greater = geq(deepest[j], maxdepth);
			greater = and(greater, not(bucket[j].isDummy));

			maxIden = mux(maxIden, bucket[j].iden, greater);
			maxdepth = mux(maxdepth, deepest[j], greater);
			isDummy = mux(isDummy, bucket[j].isDummy, greater);
		}
		T[][] result = env.newTArray(3, 0);
		result[0] = maxIden;
		result[1] = maxdepth;
		result[2] = env.newTArray(1);
		result[2][0] = isDummy;
		return result;
	}

	public void flush(Block<T>[][] scPath, boolean[] path, Block<T>[] scQueue) {
		// make path to signal
		T[] pathSignal = env.newTArray(path.length);
		for (int i = 0; i < path.length; ++i)
			pathSignal[i] = path[i] ? SIGNAL_ONE : SIGNAL_ZERO;

		// PrepareDeepest(path)
		T[][] stashDeepest = deepestBlockShort(scQueue, pathSignal);
		loglogN = stashDeepest[1].length;
		T[][] deepest = env.newTArray(scPath.length + 1, 0);
		T[][] deepestIden = env.newTArray(scPath.length + 1, 0);
		for (int i = 0; i < deepest.length; ++i) {
			deepest[i] = zeros(loglogN);
			deepestIden[i] = zeros(lengthOfIden);
		}
		T[] deepestBot = ones(scPath.length + 1);

		T[] cur = zeros(loglogN);
		T curBot = SIGNAL_ONE;
		// T emptyStash = isEmpty(scQueue);
		T[] curv = stashDeepest[1];// mux(, zeros(loglogN), emptyStash);
		deepestIden[0] = stashDeepest[0];
		curBot = stashDeepest[2][0];
		for (int i = 0; i < logN; ++i) {
			T[] iSignal = toSignals(i + 1, loglogN);
			T curvGEQI = geq(curv, iSignal);
			deepest[i + 1] = mux(deepest[i + 1], cur, curvGEQI);
			deepestBot[i + 1] = mux(deepestBot[i + 1], curBot, curvGEQI);

			T[][] pathiDeepest = deepestBlockShort(scPath[i], pathSignal);
			deepestIden[i + 1] = pathiDeepest[0];

			T lGcurv = not(leq(pathiDeepest[1], curv));
			lGcurv = and(lGcurv, not(pathiDeepest[2][0]));
			lGcurv = or(lGcurv, curBot);
			curv = mux(curv, pathiDeepest[1], lGcurv);
			cur = mux(cur, iSignal, lGcurv);
			curBot = mux(curBot, SIGNAL_ZERO, lGcurv);
		}

		// prepareTarget(path)
		T[] c = toSignals(0, loglogN);
		T cBot = SIGNAL_ONE;
		T[] l = toSignals(0, loglogN);
		T lBot = SIGNAL_ONE;

		T[][] target = env.newTArray(scPath.length + 1, 0);
		for (int i = 0; i < target.length; ++i)
			target[i] = zeros(loglogN);
		T[] targetBot = ones(scPath.length + 1);

		for (int i = logN; i >= 0; --i) {
			// prepare conditions
			T[] iSignal = toSignals(i, curv.length);
			T iEQl;
			iEQl = and(not(lBot), eq(iSignal, l));
			T isFull;
			if (i > 0) {
				isFull = isFull(scPath[i - 1]);
			} else {
				isFull = isFull(scQueue);
			}
			T hasSlot = or(and(cBot, not(isFull)), not(targetBot[i]));
			T canPush = not(deepestBot[i]);

			// begin assignment
			target[i] = mux(target[i], c, iEQl);
			targetBot[i] = mux(targetBot[i], cBot, iEQl);
			if (i > 0) {// the last one can be skipped
				cBot = mux(cBot, SIGNAL_ONE, iEQl);
				lBot = mux(lBot, SIGNAL_ONE, iEQl);

				T secondIf = and(hasSlot, canPush);
				l = mux(l, deepest[i], secondIf);
				lBot = mux(lBot, deepestBot[i], secondIf);
				c = mux(c, iSignal, secondIf);
				cBot = mux(cBot, SIGNAL_ZERO, secondIf);
			}
		}

		// evictionFast(path)
		Block<T> hold = dummyBlock;
		lBot = SIGNAL_ONE;
		// do it for stash first
		{
			T toRemove = not(targetBot[0]);
			hold = conditionalReadAndRemove(scQueue, deepestIden[0], toRemove);
			l = mux(l, target[0], toRemove);
			lBot = mux(lBot, targetBot[0], toRemove);
		}

		for (int i = 0; i < logN; ++i) {
			T[] iSignal = toSignals(i + 1, curv.length);
			T iEQl = eq(iSignal, l);
			iEQl = and(iEQl, not(lBot));
			T firstIf = and(not(hold.isDummy), iEQl);
			Block<T> holdTmp = copy(hold);

			hold.isDummy = mux(hold.isDummy, SIGNAL_ONE, firstIf);
			lBot = mux(lBot, SIGNAL_ONE, firstIf);

			T notBot = not(targetBot[i + 1]);

			if (i != logN - 1) {
				Block<T> tmp = conditionalReadAndRemove(scPath[i],
						deepestIden[i + 1], notBot);
				hold = mux(hold, tmp, notBot);
			}

			l = mux(l, target[i + 1], notBot);
			lBot = mux(lBot, targetBot[i + 1], notBot);
			conditionalAdd(scPath[i], holdTmp, firstIf);
		}
	}
	
	public void print(T[][] data, T[] bot) {
		if (env.getParty() == Party.Bob || env instanceof com.oblivm.backend.gc.GCCompEnv)
			return;
		for (int i = 0; i < data.length; ++i) {
			if ((Boolean) bot[i]) {
				System.out.print("d ");
			} else
				System.out.print(Utils.toInt(Utils
						.tobooleanArray((Boolean[]) data[i])) + " ");
		}
		System.out.print("\n");
	}

	public void print(T[] data, T bot) {
		if (env.getParty() == Party.Bob || env instanceof com.oblivm.backend.gc.GCCompEnv)
			return;

		if ((Boolean) bot) {
			System.out.print("d ");
		} else
			System.out
					.print(Utils.toInt(Utils.tobooleanArray((Boolean[]) data))
							+ " ");
		System.out.print("\n");
	}

	public void print(String s, T[] data, T bot) {

		if (env.getParty() == Party.Bob)
			return;

		System.out.print(s);
		if ((Boolean) bot) {
			System.out.print("d ");
		} else
			System.out
					.print(Utils.toInt(Utils.tobooleanArray((Boolean[]) data))
							+ " ");
		System.out.print("\n");
	}
}