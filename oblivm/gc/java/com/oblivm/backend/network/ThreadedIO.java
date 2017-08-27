package com.oblivm.backend.network;

import java.io.OutputStream;

public class ThreadedIO implements Runnable {
	public CustomizedConcurrentQueue queue;
	OutputStream os;
	public ThreadedIO(CustomizedConcurrentQueue queue2, OutputStream os) {
		this.queue = queue2;
		this.os = os;
	}

	byte[] res = new byte[Network.NetworkThreadedQueueSize];
	public void run() {	
		try {			
			while(true) {
				int len  = queue.pop(res);
				if(len == -1)return;
				if(len != 0) {
					os.write(res, 0, len);
					os.flush();
				}
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
	}
}
