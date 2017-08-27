// Copyright (C) 2014 by Xiao Shaun Wang <wangxiao@cs.umd.edu>
package com.oblivm.backend.oram;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import com.oblivm.backend.util.Utils;

public class PlainBlock implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7471327090839756307L;
	public long iden;
	public long pos;
	public boolean[] data;
	public boolean isDummy;

	public PlainBlock(long iden, long pos,  boolean[] data, boolean isDummy) {
		this.iden = iden;
		this.pos = pos;
		this.data = data;
		this.isDummy = isDummy;
	}

	public boolean[] toBooleanArray(int lengthOfIden, int lengthOfPos) {
		boolean[] result = new boolean[lengthOfIden + lengthOfPos + data.length
				+ 1];
		System.arraycopy(Utils.fromLong(iden, lengthOfIden), 0, result, 0, lengthOfIden);
		System.arraycopy(Utils.fromLong(pos, lengthOfIden), 0, result, lengthOfIden, lengthOfPos);
		System.arraycopy(data, 0, result, lengthOfPos + lengthOfIden, data.length);
		result[result.length - 1] = isDummy;
		return result;
	}

	static public boolean[] toBooleanArray(PlainBlock[] blocks, int lengthOfIden, int lengthOfPos) {
		int blockSize = (lengthOfIden + lengthOfPos
				+ blocks[0].data.length + 1);
		boolean[] result = new boolean[blockSize * blocks.length];
		for (int i = 0; i < blocks.length; ++i) {
			boolean[] tmp = blocks[i].toBooleanArray(lengthOfIden, lengthOfPos);
			System.arraycopy(tmp, 0, result, i * blockSize, blockSize);
		}
		return result;
	}
	

}
