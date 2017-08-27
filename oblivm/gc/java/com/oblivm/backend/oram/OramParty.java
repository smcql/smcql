// Copyright (C) 2014 by Xiao Shaun Wang <wangxiao@cs.umd.edu>
package com.oblivm.backend.oram;

import java.util.Arrays;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Mode;
import com.oblivm.backend.flexsc.Party;
import com.oblivm.backend.util.Utils;

public abstract class OramParty<T> {
	public int N;
	int dataSize;

	public int logN;
	public int lengthOfIden;
	public int lengthOfPos;
	public int lengthOfData;

	
	public CompEnv<T> env;
	public Party p;
	public Mode mode;

	public BucketLib<T> lib;
	boolean[] dummyArray;

	void setParameters(CompEnv<T> env, int N, int dataSize) {
		this.env = env;

		this.dataSize = dataSize;
		long a = 1;
		logN = 1;
		while (a < N) {
			a *= 2;
			++logN;
		}

		--logN;

		this.N = 1 << logN;
		lengthOfData = dataSize;
		lengthOfIden = logN;
	}
	public OramParty(CompEnv<T> env, int N, int dataSize) {
		setParameters(env, N, dataSize);
		lengthOfPos = logN - 1;
		p = env.party;
		mode = env.mode;
		init();

	}

	public OramParty(CompEnv<T> env, int N, int dataSize, int lengthOfPos) {
		setParameters(env, N, dataSize);
		this.lengthOfPos = lengthOfPos;
		p = env.party;
		mode = env.mode;
		init();

	}

	public void init() {
		dummyArray = new boolean[lengthOfIden + lengthOfPos + lengthOfData + 1];
		for (int i = 0; i < dummyArray.length; ++i)
			dummyArray[i] = false;
		lib = new BucketLib<T>(lengthOfIden, lengthOfPos, lengthOfData, env);

		boolean[] data = new boolean[lengthOfData];
		for (int i = 0; i < lengthOfData; ++i)
			data[i] = true;
		pb_for_count_mode = new PlainBlock(0, 0, data, false);

	}

	public Block<T>[] prepareBlocks(PlainBlock[] clientBlock,
			PlainBlock[] serverBlock) {
		Block<T>[] s = inputBucketOfServer(serverBlock);
		Block<T>[] c = inputBucketOfClient(clientBlock);
		return lib.xor(s, c);
	}

	public Block<T> prepareBlock(PlainBlock clientBlock, PlainBlock serverBlock) {
		Block<T> s = inputBlockOfServer(serverBlock);
		Block<T> c = inputBlockOfClient(clientBlock);
		return lib.xor(s, c);
	}

	public PlainBlock preparePlainBlock(Block<T> blocks, Block<T> randomBlock) {
		PlainBlock result = outputBlock(lib.xor(blocks, randomBlock));
		return result;
	}

	public PlainBlock[] preparePlainBlocks(Block<T>[] blocks,
			Block<T>[] randomBlock) {
		PlainBlock[] result = outputBucket(lib.xor(blocks, randomBlock));
		return result;
	}

	public Block<T> inputBlockOfServer(PlainBlock b) {
		T[] TArray = env.inputOfBob(b.toBooleanArray(lengthOfIden, lengthOfPos));
		return new Block<T>(TArray, lengthOfIden, lengthOfPos, lengthOfData);

	}

	public Block<T> inputBlockOfClient(PlainBlock b) {
		T[] TArray = env.inputOfAlice(b.toBooleanArray(lengthOfIden, lengthOfPos));
		return new Block<T>(TArray, lengthOfIden, lengthOfPos, lengthOfData);
	}

	public Block<T>[] toBlocks(T[] Tarray, int lengthOfIden, int lengthOfPos,
			int lengthOfData, int capacity) {
		int blockSize = lengthOfIden + lengthOfPos + lengthOfData + 1;
		Block<T>[] result = lib.newBlockArray(capacity);
		for (int i = 0; i < capacity; ++i) {
			result[i] = new Block<T>(Arrays.copyOfRange(Tarray, i * blockSize,
					(i + 1) * blockSize), lengthOfIden, lengthOfPos,
					lengthOfData);
		}
		return result;
	}

	public Block<T>[] inputBucketOfServer(PlainBlock[] b) {
		T[] TArray = env.inputOfBob(PlainBlock.toBooleanArray(b,lengthOfIden, lengthOfPos));
		return toBlocks(TArray, lengthOfIden, lengthOfPos, lengthOfData,
				b.length);// new Block<T>(TArray,
							// lengthOfIden,lengthOfPos,lengthOfData);
	}

	public Block<T>[] inputBucketOfClient(PlainBlock[] b) {
		T[] TArray = env.inputOfAlice(PlainBlock.toBooleanArray(b,lengthOfIden, lengthOfPos));
		env.flush();
		return toBlocks(TArray, lengthOfIden, lengthOfPos, lengthOfData,
				b.length);
	}

	public PlainBlock outputBlock(Block<T> b) {
		boolean[] iden = env.outputToAlice(b.iden);
		boolean[] pos = env.outputToAlice(b.pos);
		boolean[] data = env.outputToAlice(b.data);
		boolean isDummy = env.outputToAlice(b.isDummy);

		return new PlainBlock(Utils.toLong(iden), Utils.toLong(pos), data, isDummy);
	}

	public PlainBlock[] outputBucket(Block<T>[] b) {
		PlainBlock[] result = new PlainBlock[b.length];
		for (int i = 0; i < b.length; ++i)
			result[i] = outputBlock(b[i]);
		return result;
	}

	public PlainBlock[][] outputBuckets(Block<T>[][] b) {
		PlainBlock[][] result = new PlainBlock[b.length][];
		for (int i = 0; i < b.length; ++i)
			result[i] = outputBucket(b[i]);
		env.flush();
		return result;
	}

	PlainBlock pb_for_count_mode;

	public PlainBlock getDummyBlock(boolean b) {
		if (mode == Mode.COUNT)
			return pb_for_count_mode;
		boolean[] data = new boolean[lengthOfData];

		for (int i = 0; i < lengthOfData; ++i)
			data[i] = true;
		return new PlainBlock(0, 0, data, b);
	}

	PlainBlock r = getDummyBlock(true);

	public PlainBlock randomBlock() {
		if (mode == Mode.COUNT)
			return pb_for_count_mode;

//		PlainBlock result = getDummyBlock(true);
//		for (int i = 0; i < lengthOfIden; ++i)
//			result.iden[i] = CompEnv.rnd.nextBoolean();
//		for (int i = 0; i < lengthOfPos; ++i)
//			result.pos[i] = CompEnv.rnd.nextBoolean();
		boolean[] data = new boolean[lengthOfData];
		for (int i = 0; i < lengthOfData; ++i)
			data[i] = CompEnv.rnd.nextBoolean();
		boolean isDummy = CompEnv.rnd.nextBoolean();
		return new PlainBlock(CompEnv.rnd.nextLong(), CompEnv.rnd.nextLong(), data, isDummy);
	}

	public PlainBlock[] randomBucket(int length) {
		PlainBlock[] result = new PlainBlock[length];
		for (int i = 0; i < length; ++i)
			result[i] = randomBlock();
		return result;
	}
}
