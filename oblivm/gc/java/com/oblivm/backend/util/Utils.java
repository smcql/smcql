package com.oblivm.backend.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.BitSet;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Party;

public class Utils {
	public static int logFloor(int n) {
		int w = 0;
		n--;
		while(n > 0) {
			w ++;
			n >>= 1;
		}
		return w == 0 ? 1 : w;
	}

	
	public static <T>void print(CompEnv<T> env, String name, T[] data, T[] data2, T con) throws Exception {
		int a = toInt(env.outputToAlice(data));
		int ab = toInt(env.outputToAlice(data2));
		boolean cc = env.outputToAlice(con);
		
		if(cc)
		if(env.getParty() == Party.Alice && a != 0)
			System.out.println(name +" "+ a+" "+ab);
				
	}
	
	public static <T>void print(CompEnv<T> env, String name, T[] data) throws Exception {
		long a =  toSignedInt(env.outputToAlice(data));

		if(env.getParty() == Party.Alice && a != 0)
			System.out.println(name +" "+ a);
	}


	public static <T>void print(CompEnv<T> env, String name, T data) throws Exception {
		boolean a  = env.outputToAlice(data);

		if(env.getParty() == Party.Alice)
			System.out.println(name +" "+ a);
	}
	
	public static Boolean[] toBooleanArray(boolean[] a) {
		Boolean[] res = new Boolean[a.length];
		for (int i = 0; i < a.length; i++)
			res[i] = a[i];
		return res;
	}
	
	public static boolean[] tobooleanArray(Boolean[] a) {
		boolean[] res = new boolean[a.length];
		for (int i = 0; i < a.length; i++)
			res[i] = a[i];
		return res;
	}
	
	public static boolean[] fromInt(int value, int width) {
		boolean[] res = new boolean[width];
		for (int i = 0; i < width; i++)
			res[i] = (((value >> i) & 1) == 0) ? false : true;
		
		return res;
	}
	
	public static int toInt(boolean[] value) {
		
		int res = 0;
		for (int i = 0; i < value.length; i++)
			res =  (value[i]) ? (res | (1<<i)) : res;
		
		return res;
	}

	public static long toUnSignedInt(boolean[] v) {
		long result = 0;
		for(int i = 0; i < v.length; ++i) {
			if(v[i])
				result += ((long)1<<i);
		}
		return result;
	}
	
	public static long toSignedInt(boolean [] v) {
		int i = 0;
		if(v[v.length-1] == false) return toUnSignedInt(v);
		
		boolean[] c2 = new boolean[v.length];
		while(v[i] != true){
			c2[i] = v[i];
			++i;
		}
		c2[i] = v[i];
		++i;
		for(; i < v.length; ++i)
			c2[i] = !v[i];
		return toUnSignedInt(c2)*-(long)(1);
	}
	
	public static boolean[] fromLong(long value, int width) {
		boolean[] res = new boolean[width];
		for (int i = 0; i < width; i++)
			res[i] = (((value >> i) & 1) == 0) ? false : true;
		
		return res;
	}
	
	public static long toLong(boolean[] value) {
/*		BitSet bits = new BitSet(64);

		System.out.print("To long received: ");
		for(int i = 0; i < value.length; ++i) {
			if(value[i]) {
				bits.set(i);
				System.out.print("1");
			}
			else {
				System.out.print("0");

			}
		}
		
		System.out.println(".");
		long[] longs = bits.toLongArray();
		
		if(longs.length > 0) {
			return longs[0];
		}
		return 0L; */

   long res = 0;
	for (int i = 0; i < value.length; i++) {
			res =  (value[i]) ? (res | (1L<<i)) : res;// 1L!! not 1!!
		}
		return res;
	}

	public static double toFloat(boolean[] value, int widthV, int widthP) {
		boolean[]v = Arrays.copyOfRange(value, 1, 1+widthV);
		boolean[]p = Arrays.copyOfRange(value, 1+widthV, value.length);

		double result = value[0] ? -1 : 1;
		long value_v = Utils.toUnSignedInt(v);
		long value_p = Utils.toSignedInt(p);
		result = result * value_v;
		result = result * Math.pow(2, value_p);
		BigDecimal b = new BigDecimal(result);
		return b.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue(); // 6 is should not be fixed.
	}
	
	public static boolean[] fromFloat(double d, int widthV, int widthP) {
			boolean s;
			boolean[] v,p;			
			v = new boolean[widthV];
			p = new boolean[widthP];
			s = d < 0;
			if (d == 0) {
				for(int i  = 0; i < widthV; ++i)
					v[i] = false;
				for(int i  = 0; i < widthP; ++i)
					p[i] = false;
				p[widthP-1]=true;
			} else {
			d = s ? -1*d:d;
			int pInt = 0;
			
			double lower_bound = Math.pow(2, widthV-1);
			double upper_bound = Math.pow(2, widthV);
			while(d < lower_bound) {
				d*=2;
				pInt--;
			}
			
			while(d >= upper_bound) {
				d/=2;
				pInt++;
			}
			
			p = Utils.fromInt(pInt, widthP);
			long tmp = (long) (d+0.000001);//a hack...
			v = Utils.fromLong(tmp, widthV);
			}
			boolean[] result = new boolean[1+widthV+widthP];
			result[0] = s;
			System.arraycopy(v, 0, result, 1, v.length);
			System.arraycopy(p, 0, result, 1+v.length, p.length);
			return result;
		}
	
	final static int[] mask = { 0b00000001, 0b00000010, 0b00000100, 0b00001000,
			0b00010000, 0b00100000, 0b01000000, 0b10000000 };

	public static boolean[] fromBigInteger(BigInteger bd, int length) {
		byte[] b = bd.toByteArray();
		boolean[] result = new boolean[length];
		for (int i = 0; i < b.length; ++i) {
			for (int j = 0; j < 8 && i * 8 + j < length; ++j)
				result[i * 8 + j] = (((b[b.length - i - 1] & mask[j]) >> j) == 1);
		}
		return result;
	}

	public static BigInteger toBigInteger(boolean[] b) {
		BigInteger res = new BigInteger("0");
		BigInteger c = new BigInteger("1");
		for (int i = 0; i < b.length; i++) {
			if (b[i])
				res = res.add(c);
			c = c.multiply(new BigInteger("2"));
		}
		return res;
	}
	
	public static boolean[] fromFixPoint(double a, int width, int offset) {
		a *= Math.pow(2, offset);
		return Utils.fromLong( (long) a, width);
	}
	
	public static double toFixPoint(boolean[] b, int offset) {
		double a = toSignedInt(b);
		a /= Math.pow(2, offset);
		return a;
	}

	public static boolean[] flatten(boolean[][] data) {
		int length = 0;
		for (int i = 0; i < data.length; i++) {
			length += data[i].length;
		}
		boolean[] ret = new boolean[length];
		int pos = 0;
		for (int i = 0; i < data.length; i++) {
			System.arraycopy(data[i], 0, ret, pos, data[i].length);
			pos += data[i].length;
		}
		return ret;
	}

	public static boolean[] flatten(boolean[][][] data) {
		int length = 0;
		for (int i = 0; i < data.length; i++) {
			for (int j = 0; j < data[0].length; j++) {
				length += data[i][j].length;
			}
		}
		boolean[] ret = new boolean[length];
		int pos = 0;
		for (int i = 0; i < data.length; i++) {
			for (int j = 0; j < data[0].length; j++) {
				System.arraycopy(data[i][j], 0, ret, pos, data[i][j].length);
				pos += data[i][j].length;
			}
		}
		return ret;
	}

	public static <T> T[] flatten(CompEnv<T> env, T[] ... data) {
		int length = 0;
		for (int i = 0; i < data.length; i++) {
			length += data[i].length;
		}
		T[] ret = env.newTArray(length);
		int pos = 0;
		for (int i = 0; i < data.length; i++) {
			System.arraycopy(data[i], 0, ret, pos, data[i].length);
			pos += data[i].length;
		}
		return ret;
	}

	public static <T> T[][] flatten(CompEnv<T> env, T[][] ... data) {
		int length = 0;
		for (int i = 0; i < data.length; i++) {
			length += data[i][0].length;
		}
		T[][] ret = env.newTArray(data[0].length, length);
		int pos = 0;
		for (int i = 0; i < data.length; i++) {
			for (int j = 0; j < data[0].length; j++) {
				System.arraycopy(data[i][j], 0, ret[j], pos, data[i][j].length);
			}
			pos += data[i][0].length;
		}
		return ret;
	}

	public static <T> void unflatten(T[][] flat, T[][] ... x) {
		int pos = 0;
		for (int i = 0; i < x.length; i++) {
			for (int j = 0; j < x[i].length; j++) {
				System.arraycopy(flat[j], pos, x[i][j], 0, x[i][j].length);
			}
			pos += x[i][0].length;
		}
	}

	public static <T> void unflatten(T[] flat, T[] ... x) {
		int pos = 0;
		for (int i = 0; i < x.length; i++) {
			System.arraycopy(flat, pos, x[i], 0, x[i].length);
			pos += x[i].length;
		}
	}

	public static <T> void unflatten(T[] flat, T[][][] x) {
		int pos = 0;
		for (int i = 0; i < x.length; i++) {
			for(int j = 0; j < x[0].length; j++) {
				System.arraycopy(flat, pos, x[i][j], 0, x[i][j].length);
				pos += x[i][j].length;
			}
		}
	}

	private static double getMega(double bytes) {
		return bytes/(1024.0 * 1024);
	}

	public static int log2(int n){
	    if(n <= 0) {
	    	throw new IllegalArgumentException();
	    }
	    return 31 - Integer.numberOfLeadingZeros(n);
	}

	public static int log2Ceil(int n) {
		int m = log2(n);
		if((1 << m) < n) m ++;
		return m;
	}
	
	public static double getRandom() {
		double ret = Utils.RAND[Utils.RAND_CNT];
		Utils.RAND_CNT = (Utils.RAND_CNT + 1) % Utils.RAND_LIM;
		return ret;
	}

	public static void generateRandomNumbers() throws FileNotFoundException,
		IOException {
		Utils.RAND = new double[Utils.RAND_LIM];
		BufferedReader reader = new BufferedReader(new FileReader("in/rand.out"));
		for (int i = 0; i < Utils.RAND_LIM; i++) {
			Utils.RAND[i] = Double.parseDouble(reader.readLine());
		}
		reader.close();
	}

	public static double RAND[];
	public static int RAND_CNT = 0;
	public static int RAND_LIM = 10000000;
	
	public static byte[] toByte(int value) {
		byte[] b = new byte[4];
		for(int i=0; i<4; ++i) {
			b[i] = (byte)(value & ((1<<8) -1));
			value >>= 8;
		}
		return b;
	}
	
	public static int fromByte(byte[] b) {
		int value = 0;
		for(int i=3; i>=0; --i) {
			int t = b[i];
			if(t < 0) {
				t += 1 << 8;
			}
			value = (value << 8) | t;
		}
		return value;
	}
}