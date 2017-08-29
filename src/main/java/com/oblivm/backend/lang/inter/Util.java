package com.oblivm.backend.lang.inter;

import java.lang.reflect.Array;
import java.util.Arrays;

import com.oblivm.backend.circuits.arithmetic.IntegerLib;
import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.oram.SecureArray;
import com.oblivm.backend.util.Utils;

public class Util {
	public static<T> T[][] intToArray(T[] intInput, int bitSize, int arraySize) {
		@SuppressWarnings("unchecked")
		T[][] ret = (T[][])Array.newInstance((Class<T>)intInput[0].getClass(), arraySize, bitSize);
		for(int i=0; i<arraySize; ++i) {
			for(int j=0; j<bitSize; ++j) {
				ret[i][j] = intInput[i * bitSize + j];
			}
		}
		return ret;
	}
	
	public static<T> SecureArray<T> intToSecArray(CompEnv<T> env, T[] intInput, int bitSize, int arraySize) throws Exception {
		SecureArray<T> secArray = new SecureArray<T>(env, arraySize, bitSize);
		IntegerLib<T> intLib = new IntegerLib<T>(env); 
		for(int i=0; i<arraySize; ++i) {
			T[] idx = intLib.toSignals(i);		
			secArray.write(idx, Arrays.copyOfRange(intInput, i*bitSize, (i+1)*bitSize));
		}
//		for(int i=0; i<arraySize; ++i) {
//			System.out.println("Input["+i+"] = "+Utils.toInt(env.outputToAlice(secArray.read(intLib.toSignals(i)))));
//		}
		return secArray;
	}
	
	private static <T> T[] concat(T[] first, T[] second) {
		  T[] result = Arrays.copyOf(first, first.length + second.length);
		  System.arraycopy(second, 0, result, first.length, second.length);
		  return result;
	}
	
	public static<T> T[] secToIntArray(CompEnv<T> env, SecureArray<T> secInput) throws Exception {
		return secToIntArray(env, secInput, secInput.dataSize, secInput.length);
	}	
	
	public static<T> T[] secToIntArray(CompEnv<T> env, SecureArray<T> secInput, int bitSize, int arraySize) throws Exception {
		T[] intArray = null;
		IntegerLib<T> intLib = new IntegerLib<T>(env);
		for(int i=0; i<arraySize; ++i) {
			if(i==0) {
				T[] idx = intLib.toSignals(i);
				intArray = secInput.read(idx);
			} else {
				intArray = concat(intArray, secInput.read(intLib.toSignals(i)));
			}
		}
		
		return intArray;
	}
}
