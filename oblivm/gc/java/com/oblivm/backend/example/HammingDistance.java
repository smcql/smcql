package com.oblivm.backend.example;

import com.oblivm.backend.circuits.arithmetic.IntegerLib;
import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.gc.BadLabelException;
import com.oblivm.backend.util.EvaRunnable;
import com.oblivm.backend.util.GenRunnable;

public class HammingDistance {
	
	static public<T> T[] compute(CompEnv<T> gen, T[] inputA, T[] inputB){
		return  new IntegerLib<T>(gen).hammingDistance(inputA, inputB);
	}

	
	public static class Generator<T> extends GenRunnable<T> {

		T[] inputA;
		T[] inputB;
		T[] scResult;
		
		@Override
		public void prepareInput(CompEnv<T> gen) {
			boolean[] in = new boolean[10000];
			for(int i = 0; i < in.length; ++i)
				in[i] = CompEnv.rnd.nextBoolean();
			inputA = gen.inputOfAlice(in);
			gen.flush();
			inputB = gen.inputOfBob(new boolean[10000]);
		}
		
		@Override
		public void secureCompute(CompEnv<T> gen) {
			scResult = compute(gen, inputA, inputB);
		}
		@Override
		public int[] prepareOutput(CompEnv<T> gen) throws BadLabelException {
			System.out.println(gen.outputToAlice(scResult));
			return new int[0];
		}
		
	}
	
	public static class Evaluator<T> extends EvaRunnable<T> {
		T[] inputA;
		T[] inputB;
		T[] scResult;
		
		@Override
		public void prepareInput(CompEnv<T> gen) {
			boolean[] in = new boolean[10000];
			for(int i = 0; i < in.length; ++i)
				in[i] = CompEnv.rnd.nextBoolean();
			inputA = gen.inputOfAlice(new boolean[10000]);
			gen.flush();
			inputB = gen.inputOfBob(in);
		}
		
		@Override
		public void secureCompute(CompEnv<T> gen) {
			scResult = compute(gen, inputA, inputB);
		}
		
		@Override
		public int[] prepareOutput(CompEnv<T> gen) throws BadLabelException {
			gen.outputToAlice(scResult);
			return new int[0];
		}
	}
}
