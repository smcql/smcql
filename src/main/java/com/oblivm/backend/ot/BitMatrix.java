// Copyright (C) 2013 by Yan Huang <yhuang@cs.umd.edu>
// Improved by Xiao Shaun Wang <wangxiao@cs.umd.edu>

package com.oblivm.backend.ot;

import java.math.BigInteger;
import java.security.SecureRandom;

class BitMatrix {
	private int nRows;
	private int nCols;
	BigInteger[] data; // column vectors of the matrix

	public BitMatrix(int rows, int cols) {
		nRows = rows;
		nCols = cols;
		data = new BigInteger[nCols];
	}

	public void initialize(SecureRandom rnd) {
		for (int i = 0; i < nCols; i++)
			data[i] = new BigInteger(nRows, rnd);
	}

	public BitMatrix transpose() {
		return NaiveTranspose(this);
	}

	static public BitMatrix NaiveTranspose(BitMatrix a) {
		BitMatrix b = new BitMatrix(a.nCols, a.nRows);

		for (int i = 0; i < a.nRows; i++)
			b.data[i] = BigInteger.ZERO;

		for (int j = 0; j < a.nCols; j++)
			for (int i = 0; i < a.nRows; i++)
				if (a.data[j].testBit(i))
					b.data[i] = b.data[i].setBit(j);
		return b;
	}

	static public BitMatrix COtranspose(BitMatrix a) {
		BitMatrix b = new BitMatrix(a.nCols, a.nRows);
		for (int i = 0; i < a.nRows; i++)
			b.data[i] = BigInteger.ZERO;
		COtranspose(a, b, 0, 0, a.nRows, a.nCols);
		return b;
	}

	static public void COtranspose(BitMatrix a, BitMatrix b, int startx,
			int starty, int endx, int endy) {

		if (endy - starty == 1 && endx - startx == 1) {
			if (a.data[starty].testBit(startx))
				b.data[startx] = b.data[startx].setBit(starty);
			return;
		} else if (endy - starty < endx - startx) {

			int midx = (startx + endx) / 2;
			COtranspose(a, b, startx, starty, midx, endy);
			COtranspose(a, b, midx, starty, endx, endy);
		} else {
			int midy = (starty + endy) / 2;
			COtranspose(a, b, startx, starty, endx, midy);
			COtranspose(a, b, startx, midy, endx, endy);
		}
	}
}