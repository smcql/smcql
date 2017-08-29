package com.oblivm.backend.lang.inter;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;

import com.oblivm.backend.flexsc.CompEnv;
/***
 * Copyright (C) 2015 by Chang Liu <liuchang@cs.umd.edu>
 */
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.lang.inter.input.BitArrayInput;
import com.oblivm.backend.lang.inter.input.BitFileInput;
import com.oblivm.backend.oram.SecureArray;
import com.oblivm.backend.util.EvaRunnable;
import com.oblivm.backend.util.GenRunnable;
import com.oblivm.backend.util.Utils;

public class MainRunnable {
	
	public static class Generator<T> extends GenRunnable<T> implements java.io.Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = -3762973620194026160L;

		/**
		 * 
		 */


		public Input inputAlice;
		
		public String className; 
		public int lenA, lenB;
		protected int honestBrokerPort;
		protected String honestBrokerHost;
		protected SecureArray<T> aliceArrayInput;
		protected boolean[] output;
		
		
		// takes byte array input as string
		public Generator(String className, String aInput, String hbHost, int hbPort) {
			this.className = className;
			try {
				if (aInput.endsWith(".txt")) {
					inputAlice = BitFileInput.open(aInput);
				} else {
					inputAlice =  BitArrayInput.read(aInput);
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			this.honestBrokerHost = hbHost;
			this.honestBrokerPort = hbPort;
		}
		
		
		// takes byte array input 
		public Generator(String className, SecureArray<T> aInput, String hbHost, int hbPort) {
					this.className = className;
					this.honestBrokerHost = hbHost;
					this.honestBrokerPort = hbPort;
					this.aliceArrayInput = aInput;
					
				}
				
		ISecureRunnable<T> runnable;

		T[] inputA;
		T[] inputB;
		T[] scResult;
		
		@SuppressWarnings("unchecked")
		@Override
		public void prepareInput(CompEnv<T> env) {
			try {	
				Class<?> cl = Class.forName(className);
				Constructor<?> ctor = cl.getConstructors()[0];
				runnable = (ISecureRunnable<T>)ctor.newInstance(env);
			} catch (Exception e) {
				e.printStackTrace();
			}
			boolean[] in = inputAlice.readAll();
			lenA = in.length;
			lenB = 0;
			try {
				os.write(Utils.toByte(lenA));
				os.flush();
				// retrieve sizeof(B) from second party
				lenB = Utils.fromByte(this.readBytes(4));
				this.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}

			System.out.println("First bit " + in[0]);
			System.out.println("Env mode: " + env.mode);
			
			inputA = env.inputOfAlice(in);
			System.out.println("input of alice produces has tArrays of " + inputA.getClass());
			env.flush();
			inputB = env.inputOfBob(new boolean[lenB]);
			
		}
		
		
		
		@Override
		public void secureCompute(CompEnv<T> env) throws Exception {		
			scResult = runnable.main(lenA, lenB, inputA, inputB);
			System.out.println("Received " + scResult.length + " bits.");
		}
		
		@Override
		public int[] prepareOutput(CompEnv<T> gen) throws Exception {
			System.out.println("preparing output");
			output = gen.outputToAlice(scResult); 
			gen.channel.os.write(new byte[]{0});
			gen.flush();
						
			int[] result = null;
			int remainder = output.length % 32;
			boolean[] temp = output;
			if (remainder > 0) {
				boolean[] padding = new boolean[remainder];
				temp = ArrayUtils.addAll(padding, output);
			} 
			
			result = new int[temp.length/32]; //magic number for integer
			for(int i=0; i<temp.length; i+=32) {
				result[i/32] = Utils.toInt(Arrays.copyOfRange(output, i, i+32));
			}
			
			return result;
		}
		
		public boolean[] getOutput() {
			return output;
		}
		
	}
	
	public static class Evaluator<T> extends EvaRunnable<T> implements java.io.Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		public Input inputBob;
		public String className; 
		public int lenA, lenB;
		private int honestBrokerPort;
		private String honestBrokerHost;
		protected SecureArray<T> bobArrayInput;
		
		public Evaluator(String className, String bInput, String hbHost, int hbPort) {
			this.className = className;
			try {
				if (bInput.endsWith(".txt")) {
					inputBob = BitFileInput.open(bInput);
				} else {
					inputBob = BitArrayInput.read(bInput);
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			this.honestBrokerHost = hbHost;
			this.honestBrokerPort = hbPort;
		}
		
		
		// takes byte array input 
		public Evaluator(String className, SecureArray<T> bInput, String hbHost, int hbPort) {
					this.className = className;
					this.honestBrokerHost = hbHost;
					this.honestBrokerPort = hbPort;
					bobArrayInput = bInput;
				}
	
		public void setHonestBroker(String host, int port) {
			this.honestBrokerHost = host;
			this.honestBrokerPort = port;
		}
		
		ISecureRunnable<T> runnable;
		
		T[] inputA;
		T[] inputB;
		T[] scResult;
		
		@SuppressWarnings("unchecked")
		@Override
		public void prepareInput(CompEnv<T> env) {
			boolean[] in = inputBob.readAll();
			lenA = 0;
			lenB = in.length;
			 String workingDir = System.getProperty("java.class.path");
			 System.out.println("Class loader working from " + workingDir);
			
			try {
				// just reading filesize
				lenA = Utils.fromByte(this.readBytes(4));
				// ship to gen
				os.write(Utils.toByte(lenB));
				os.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
	
				Class<?> cl = Class.forName(className);
				Constructor<?> ctor = cl.getConstructors()[0];
				runnable = (ISecureRunnable<T>)ctor.newInstance(env);
			} catch (Exception e) {
				e.printStackTrace();
			}
			inputA = env.inputOfAlice(new boolean[lenA]);
			env.flush();
			inputB = env.inputOfBob(in);
			System.out.println("Bob has LenA="+lenA+"\tLenB="+lenB);
		}
		
		@Override
		public void secureCompute(CompEnv<T> env) throws Exception {
			scResult = runnable.main(lenA, lenB, inputA, inputB);
			System.out.println("Completed secure compute");
		}
		
		@Override
		public int[] prepareOutput(CompEnv<T> env) throws Exception {
			int output = Utils.toInt(env.outputToAlice(scResult));
			env.channel.is.read(new byte[]{0});
			env.flush();
			System.out.println("Done preparing output.");
						
			int[] result = new int[1];
			result[0] = output;
			return result;
		}
	}

}
