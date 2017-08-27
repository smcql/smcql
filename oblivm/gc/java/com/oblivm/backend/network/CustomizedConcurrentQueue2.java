package com.oblivm.backend.network;



public class CustomizedConcurrentQueue2 {
	int capacity;
	int head = 0;
	int tail = 0;
	byte[] data;
	boolean finished = false;
	public CustomizedConcurrentQueue2(int capacity) {
		this.capacity = capacity;
		data = new byte[capacity];
	}

	public void destory() {
		finished = true;
	}

	 public  int insert(byte[] in) {
		int remains = capacity - head;
		if(remains >= in.length) {
			System.arraycopy(in, 0, data, head, in.length);
			head +=in.length;
		}
		else {
			System.arraycopy(in, 0, data, head, remains);
			System.arraycopy(in, remains, data, 0, in.length-remains);
			head = in.length-remains;
		}
		return 0;
	}

	 synchronized public  int pop(byte[] d) {
		int h = head;
		int size = (h - tail + capacity) % capacity;
//		if(h != 0)
//		System.out.println(h+" "+size+" "+tail+" "+size);
		if(finished && size == 0) return -1;

		if(size == 0) return 0;
//		System.out.println(size);


		if(h > tail) {
			System.arraycopy(data, tail, d, 0, h-tail);
		} else {
			System.arraycopy(data, tail, d, 0, capacity-tail);
			System.arraycopy(data, 0, d, capacity-tail, h);
		}
//		System.out.println("!!");

		tail = h;
		return size;
	}
}
