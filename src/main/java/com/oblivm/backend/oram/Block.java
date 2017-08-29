// Copyright (C) 2014 by Xiao Shaun Wang <wangxiao@cs.umd.edu>
package com.oblivm.backend.oram;

import java.util.Arrays;

public class Block<T> {

	public T[] iden;
	public T[] pos;
	public T[] data;
	public T isDummy;

	public Block(T[] iden, T[] pos, T[] data, T isDummy) {
		this.iden = iden;
		this.pos = pos;
		this.data = data;
		this.isDummy = isDummy;
	}

	public Block(T[] Tarray, int lengthOfIden, int lengthOfPos, int lengthOfData) {
		iden = Arrays.copyOfRange(Tarray, 0, lengthOfIden);
		pos = Arrays.copyOfRange(Tarray, lengthOfIden, lengthOfIden
				+ lengthOfPos);
		data = Arrays.copyOfRange(Tarray, lengthOfIden + lengthOfPos,
				lengthOfIden + lengthOfPos + lengthOfData);
		isDummy = Tarray[Tarray.length - 1];
	}

}
