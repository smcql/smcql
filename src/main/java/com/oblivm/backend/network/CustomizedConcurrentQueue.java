package com.oblivm.backend.network;

public class CustomizedConcurrentQueue {
	int capacity;
	int size = 0;
	byte[] data;
	boolean finished = false;
	public CustomizedConcurrentQueue(int capacity) {
		this.capacity = capacity;
		data = new byte[capacity];
	}
	public void destory() {
		finished = true;
	}
	
	public  void insert (byte[] in) {
		while (in.length+atomic(null, 3) > capacity){}
		atomic(in, 1);
	}
	
	public synchronized int atomic(byte[] in, int op) {
		if(op == 1) {
			System.arraycopy(in, 0, data, size, in.length);
			size +=in.length;
			return 0;
		}
		else if(op == 3) {
			return size;
		}
		else {
			System.arraycopy(data, 0, in, 0, size);
			int oldsize = size;
			size = 0;
			return oldsize;
		}
	}
	
	public  int pop(byte[] d) {
		int res =  atomic(d, 2);
		if(res == 0 && finished){
//			System.out.println("!"); 
			return -1;
		}
		else return res;
	}
}