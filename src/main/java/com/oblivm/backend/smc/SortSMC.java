package com.oblivm.backend.smc;

import java.io.IOException;
import java.util.Arrays;

import com.oblivm.backend.circuits.BitonicSortLib;
import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.lang.inter.Input;
import com.oblivm.backend.lang.inter.input.BitArrayInput;
import com.oblivm.backend.util.EvaRunnable;
import com.oblivm.backend.util.GenRunnable;
import com.oblivm.backend.util.Utils;

public class SortSMC {
	static public<T> void compute(CompEnv<T> gen, T[][] concatenatedArrays){
		BitonicSortLib<T> lib = new  BitonicSortLib<T>(gen); 
		lib.sort(concatenatedArrays, lib.SIGNAL_ONE);
	}
	
	public static <T> T[] concat(T[] first, T[] second) {
		  T[] result = Arrays.copyOf(first, first.length + second.length);
		  System.arraycopy(second, 0, result, first.length, second.length);
		  return result;
	}
	
	public static class Generator<T> extends GenRunnable<T> {
		public Input inputAlice;
		public int lenA, lenB;
		T[] inputA;
		T[] inputB;
		T[] scResult;

		public Generator(String aInput) {
			try {
				inputAlice =  BitArrayInput.read(aInput);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		@Override
		public void prepareInput(CompEnv<T> env) {
			boolean[] in = inputAlice.readAll();
			lenA = in.length;
			lenB = 0;
			try {
				os.write(Utils.toByte(lenA));
				os.flush();
				// retrieve size of B from second party
				lenB = Utils.fromByte(this.readBytes(4));
				this.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
			inputA = env.inputOfAlice(in);
			env.flush();
			inputB = env.inputOfBob(new boolean[lenB]);
			System.out.println("Alice has LenA="+lenA+"\tLenB="+lenB);
		}
		
		@Override
		public void secureCompute(CompEnv<T> env) {
			T[] concat = concat(inputA, inputB);
			T[][] scTemp =  env.newTArray(concat.length/32, 0);
			for(int i = 0; i < scTemp.length; i++)
				scTemp[i] = Arrays.copyOfRange(concat, i*32, i*32+32);
			compute(env, scTemp);
			T[] output = null;
			for(int i=0; i<scTemp.length; i++) {
				if(output==null) {
					output = scTemp[i];
				} else {
					output = concat(output, scTemp[i]);
				}
			}
			scResult = output;
		}
		
		@Override
		public int[] prepareOutput(CompEnv<T> env) throws Exception {
			System.out.println("preparing output");
			boolean[] output = env.outputToAlice(scResult);
			env.channel.os.write(new byte[]{0});
			env.flush();
			int[] result = new int[output.length/32]; //magic number for integer
			for(int i=0; i<output.length; i+=32) {
				result[i/32] = Utils.toInt(Arrays.copyOfRange(output, i, i+32));
			}
			return result;
		}
	}
	
	public static class Evaluator<T> extends EvaRunnable<T> {
		public Input inputBob;
		public int lenA, lenB;
		T[] inputA;
		T[] inputB;
		T[] scResult;
		
		public Evaluator(String bInput) {
			try {
				inputBob = BitArrayInput.read(bInput);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		
		@Override
		public void prepareInput(CompEnv<T> env) {
			boolean[] in = inputBob.readAll();
			lenA = 0;
			lenB = in.length;
			try {
				// just reading filesize
				lenA = Utils.fromByte(this.readBytes(4));
				// ship to gen
				os.write(Utils.toByte(lenB));
				os.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
			inputA = env.inputOfAlice(new boolean[lenA]);
			env.flush();
			inputB = env.inputOfBob(in);
			System.out.println("Bob has LenA="+lenA+"\tLenB="+lenB);
		}
		
		@Override
		public void secureCompute(CompEnv<T> env) {
			T[] concat = concat(inputA, inputB);
			T[][] scTemp =  env.newTArray(concat.length/32, 0);
			for(int i = 0; i < scTemp.length; i++)
				scTemp[i] = Arrays.copyOfRange(concat, i*32, i*32+32);
			compute(env, scTemp);
			T[] output = null;
			for(int i=0; i<scTemp.length; i++) {
				if(output==null) {
					output = scTemp[i];
				} else {
					output = concat(output, scTemp[i]);
				}
			}
			scResult = output;
			System.out.println("Completed secure compute.");
		}
		
		@Override
		public int[] prepareOutput(CompEnv<T> env) throws Exception {
			env.outputToAlice(scResult);
			env.channel.is.read(new byte[]{0});
			env.flush();
			System.out.println("Done preparing output.");
			int[] result = new int[1];
			result[0] = 0;
			return result;
		}
	}
}
