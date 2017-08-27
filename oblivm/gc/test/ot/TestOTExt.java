package ot;

import java.security.SecureRandom;

import org.junit.Assert;
import org.junit.Test;

import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.ot.OTExtReceiver;
import com.oblivm.backend.ot.OTExtSender;

public class TestOTExt {
	static int n = 100000;
	GCSignal[][] m;
	boolean[] c;
	GCSignal[] rcvd;
	
	static SecureRandom rnd = new SecureRandom("abc".getBytes());
	
	class SenderRunnable extends com.oblivm.backend.network.Server implements Runnable {
		OTExtSender snd;
		SenderRunnable () {}
		
		public void run() {
			
			try {
				listen(54321);

				snd = new OTExtSender(80, this);
				m = new GCSignal[n][2];
				for (int i = 0; i < n; i++) {
					m[i][0] = GCSignal.freshLabel(rnd);
					m[i][1] = GCSignal.freshLabel(rnd);
					snd.send(m[i]);
				}
				
				disconnect();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	class ReceiverRunnable extends com.oblivm.backend.network.Client implements Runnable {
		OTExtReceiver rcv;
		ReceiverRunnable () {}
		
		public void run() {
			try {
				connect("localhost", 54321);
				
				rcv = new OTExtReceiver(this);
				c = new boolean[n];
				rcvd = new GCSignal[n];
				for (int i = 0; i < n; i++) {
					c[i] = rnd.nextBoolean();
					rcvd[i] = rcv.receive(c[i]);
				}
				
				disconnect();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	public void test1Case() throws Exception {
		SenderRunnable sender = new SenderRunnable();
		ReceiverRunnable receiver = new ReceiverRunnable();
		Thread tSnd = new Thread(sender);
		Thread tRcv = new Thread(receiver);
		tSnd.start(); 
		tRcv.start(); 
		tSnd.join();

		for (int i = 0; i < n; i++)
			Assert.assertEquals(m[i][c[i]?1:0], rcvd[i]);
	}

	@Test
	public void testAllCases() throws Exception {
		System.out.println("Testing OT Extension...");
		test1Case();
	}
}