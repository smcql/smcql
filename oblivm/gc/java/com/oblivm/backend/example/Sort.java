package com.oblivm.backend.example;

import java.util.Arrays;

import com.oblivm.backend.circuits.BitonicSortLib;
import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.util.EvaRunnable;
import com.oblivm.backend.util.GenRunnable;
import com.oblivm.backend.util.Utils;

public class Sort {
	static public<T> void compute(CompEnv<T> gen, T[][] inputB){
		BitonicSortLib<T> lib = new  BitonicSortLib<T>(gen); 
		lib.sort(inputB, lib.SIGNAL_ONE);
	}
	
	public static class Generator<T> extends GenRunnable<T> {
		T[][] inputB;

		@Override
		public void prepareInput(CompEnv<T> gen) {
			inputB = gen.newTArray(4000, 0);
			T[] scTemp = gen.inputOfBob(new boolean[4000*16]);
			for(int i = 0; i < inputB.length; ++i)
				inputB[i] = Arrays.copyOfRange(scTemp, i*16, i*16+16);
		}
		
		@Override
		public void secureCompute(CompEnv<T> gen) {
			compute(gen, inputB);
		}
		@Override
		public int[] prepareOutput(CompEnv<T> gen) {
			return new int[0];
		}
	}
	
	public static class Evaluator<T> extends EvaRunnable<T> {
		T[][] inputB;
		T[] scResult;
		
		@Override
		public void prepareInput(CompEnv<T> gen) {
			inputB = gen.newTArray(4000, 0);
			boolean[] temp = new boolean[4000*16];
			for(int i = 0; i < 4000; ++i)
				System.arraycopy(Utils.fromInt(CompEnv.rnd.nextInt(), 16), 0, temp, 16*i, 16);
			
			T[] scTemp = gen.inputOfBob(temp);
			for(int i = 0; i < inputB.length; ++i)
				inputB[i] = Arrays.copyOfRange(scTemp, i*16, i*16+16);
		}
		
		@Override
		public void secureCompute(CompEnv<T> gen) {
			compute(gen, inputB);
		}
		
		@Override
		public int[] prepareOutput(CompEnv<T> gen) {
			return new int[0];
		}
	}
}
