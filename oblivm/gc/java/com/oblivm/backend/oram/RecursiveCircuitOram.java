// Copyright (C) 2014 by Xiao Shaun Wang <wangxiao@cs.umd.edu>
package com.oblivm.backend.oram;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Party;

public class RecursiveCircuitOram<T> implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7878090853097780235L;
	/**
	 * 
	 */
	public LinearScanOram<T> baseOram;
	public ArrayList<CircuitOram<T>> clients = new ArrayList<>();
	public int lengthOfIden;
	int recurFactor;
	int cutoff;
	int capacity;

	Party p;

	public RecursiveCircuitOram() {
		// needed for serialization
	}
	
	public RecursiveCircuitOram(CompEnv<T> env, int N, int dataSize,
			int cutoff, int recurFactor, int capacity, int sp) {
		init(env, N, dataSize, cutoff, recurFactor, capacity, sp);
	}
	
	public RecursiveCircuitOram(CompEnv<T> env, int N, int dataSize,
			int cutoff, int recurFactor) {
		init(env, N, dataSize, cutoff, recurFactor, 3, 80);
	}

	// with default params
	public RecursiveCircuitOram(CompEnv<T> env, int N, int dataSize) {
		init(env, N, dataSize, 1<<6, 8, 3, 80);
	}

	public void setInitialValue(int initial) {
		clients.get(0).setInitialValue(initial);
	}
	void init(CompEnv<T> env, int N, int dataSize, int cutoff, int recurFactor,
			int capacity, int sp) {
		this.p = env.party;
		this.cutoff = cutoff;
		this.recurFactor = recurFactor;
		this.capacity = capacity;
		CircuitOram<T> oram = new CircuitOram<T>(env, N, dataSize, capacity, sp);
		lengthOfIden = oram.lengthOfIden;
		clients.add(oram);
		int newDataSize = oram.lengthOfPos * recurFactor, newN = (1 << oram.lengthOfIden)
				/ recurFactor;
		while (newN > cutoff) {
			oram = new CircuitOram<T>(env, newN, newDataSize, capacity, sp);
			clients.add(oram);
			newDataSize = oram.lengthOfPos * recurFactor;
			newN = (1 << oram.lengthOfIden) / recurFactor;
		}
		CircuitOram<T> last = clients.get(clients.size() - 1);
		baseOram = new LinearScanOram<T>(env, (1 << last.lengthOfIden),
				last.lengthOfPos);
	}

	public T[] read(T[] iden) {
		T[][] poses = travelToDeep(iden, 1);
		CircuitOram<T> currentOram = clients.get(0);
		boolean[] oldPos = baseOram.lib.declassifyToBoth(poses[0]);

		T[] res = currentOram.read(iden, oldPos, poses[1]);
		return res;
	}

	public void write(T[] iden, T[] data) {
		T[][] poses = travelToDeep(iden, 1);
		CircuitOram<T> currentOram = clients.get(0);

		boolean[] oldPos = baseOram.lib.declassifyToBoth(poses[0]);
		currentOram.write(iden, oldPos, poses[1], data);
	}
	
	public void write(T[] iden, T[] data, T dummy) {
		T[][] poses = travelToDeep(iden, 1);
		CircuitOram<T> currentOram = clients.get(0);

		currentOram. write(iden, poses[0], poses[1], data, dummy);
	}

	public T[] access(T[] iden, T[] data, T op) {
		T[][] poses = travelToDeep(iden, 1);
		CircuitOram<T> currentOram = clients.get(0);

		boolean[] oldPos = baseOram.lib.declassifyToBoth(poses[0]);
		return currentOram.access(iden, oldPos, poses[1], data, op);
	}

	public T[][] travelToDeep(T[] iden, int level) {
		if (level == clients.size()) {
			T[] baseMap = baseOram.readAndRemove(baseOram.lib.padSignal(iden, baseOram.lengthOfIden));
			T[] ithPos = baseOram.lib.rightPublicShift(iden,
					baseOram.lengthOfIden);// iden>>baseOram.lengthOfIden;

			T[] pos = extract(baseMap, ithPos,
					clients.get(level - 1).lengthOfPos);

			T[] newPos = baseOram.lib
					.randBools(clients.get(level - 1).lengthOfPos);
			put(baseMap, ithPos, newPos);
			baseOram.putBack(baseOram.lib.padSignal(iden, baseOram.lengthOfIden), baseMap);
			T[][] result = baseOram.env.newTArray(2, 0);
			result[0] = pos;
			result[1] = newPos;
			return result;
		} else {
			CircuitOram<T> currentOram = clients.get(level);

			T[][] poses = travelToDeep(subIdentifier(iden, currentOram),
					level + 1);

			boolean[] oldPos = baseOram.lib.declassifyToBoth(poses[0]);

			T[] data = currentOram.readAndRemove(
					subIdentifier(iden, currentOram), oldPos, true);
			T[] ithPos = currentOram.lib.rightPublicShift(iden,
					currentOram.lengthOfIden);// iden>>currentOram.lengthOfIden;//iden/(1<<currentOram.lengthOfIden);

			T[] pos = extract(data, ithPos, clients.get(level - 1).lengthOfPos);
			T[] tmpNewPos = baseOram.lib
					.randBools(clients.get(level - 1).lengthOfPos);
			put(data, ithPos, tmpNewPos);
			currentOram.putBack(subIdentifier(iden, currentOram), poses[1],
					data);
			T[][] result = currentOram.env.newTArray(2, 0);
			result[0] = pos;
			result[1] = tmpNewPos;
			return result;
		}
	}

	public T[] subIdentifier(T[] iden, OramParty<T> o) {
		// int a = iden & ((1<<o.lengthOfIden)-1);//(iden % (1<<o.lengthOfIden))
		return o.lib.padSignal(iden, o.lengthOfIden);
	}

	public T[] extract(T[] array, T[] ithPos, int length) {
		int numberOfEntry = array.length / length;
		T[] result = Arrays.copyOfRange(array, 0, length);
		for (int i = 1; i < numberOfEntry; ++i) {
			T hit = baseOram.lib.eq(baseOram.lib.toSignals(i, ithPos.length),
					ithPos);
			result = baseOram.lib.mux(result,
					Arrays.copyOfRange(array, i * length, (i + 1) * length),
					hit);
		}
		return result;
	}

	public void put(T[] array, T[] ithPos, T[] content) {
		int numberOfEntry = array.length / content.length;
		for (int i = 0; i < numberOfEntry; ++i) {
			T hit = baseOram.lib.eq(baseOram.lib.toSignals(i, ithPos.length),
					ithPos);
			T[] tmp = baseOram.lib.mux(
					Arrays.copyOfRange(array, i * content.length, (i + 1)
							* content.length), content, hit);
			System.arraycopy(tmp, 0, array, i * content.length, content.length);
		}
	}
	
	
	public void serializeToDisk(String dstFilename) {
		 try
	      {
	         FileOutputStream fileOut =
	         new FileOutputStream(dstFilename);
	         ObjectOutputStream out = new ObjectOutputStream(fileOut);
	         out.writeObject(this);
	         out.close();
	         fileOut.close();
	      }catch(IOException i)
	      {
	          i.printStackTrace();
	      }
	}
	
	// not static because of <T>
	public  RecursiveCircuitOram<T> deserializeFromDisk(String srcFilename) {
		RecursiveCircuitOram<T> ret = null;
		 try
	      {
	         FileInputStream fileIn = new FileInputStream("/tmp/employee.ser");
	         ObjectInputStream in = new ObjectInputStream(fileIn);
	         Object o = in.readObject();
	         if(o instanceof RecursiveCircuitOram<?>) {
	        	 ret = (RecursiveCircuitOram<T>) o;
	         }
	         
	         in.close();
	         fileIn.close();
	      }catch(IOException i)
	      {
	         i.printStackTrace();
	         return ret;
	      }catch(ClassNotFoundException c)
	      {
	         System.out.println("RecursiveCircuitOram<T> not found.");
	         c.printStackTrace();
	         return ret;
	      }
		 return ret;
		 
	}
}
