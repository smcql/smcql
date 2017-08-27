package ot;

import java.security.SecureRandom;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.ot.NPOTReceiver;
import com.oblivm.backend.ot.NPOTSender;

public class TestNPOTMany {
	static int n = 50000;
	GCSignal[][] m;
	boolean[] c;
	GCSignal[] rcvd;
	
	static SecureRandom rnd;
	static {
		try{
			rnd = SecureRandom.getInstance("SHA1PRNG");
			rnd.setSeed ("initial seed".getBytes ());
			System.out.println(rnd.nextLong());
		} catch (Exception e) { e.printStackTrace(); System.exit(1); }
	}

	class SenderRunnable extends com.oblivm.backend.network.Server implements Runnable {
		NPOTSender snd;
		SenderRunnable () {}
		
		public void run() {
			try {
//				SecureRandom rnd = SecureRandom.getInstance("SHA1PRNG");
//				rnd.setSeed ("initial seed".getBytes ());
//				System.out.println(rnd.nextLong());

				listen(54321);

				m = new GCSignal[n][2];
				for (int i = 0; i < n; i++) {
					m[i][0] = GCSignal.freshLabel(rnd);
					m[i][1] = GCSignal.freshLabel(rnd);
				}
				snd = new NPOTSender(80, this);
				snd.send(m);
				
				disconnect();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	class ReceiverRunnable extends com.oblivm.backend.network.Client implements Runnable {
		NPOTReceiver rcv;
		ReceiverRunnable () {}
		
		public void run() {
			try {
				connect("localhost", 54321);
				
				rcv = new NPOTReceiver(this);
				c = new boolean[n];
				for (int i = 0; i < n; i++)
					c[i] = rnd.nextBoolean();
				rcvd = rcv.receive(c);
				
				disconnect();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	public void runThreads() throws Exception {
		SenderRunnable sender = new SenderRunnable();
		ReceiverRunnable receiver = new ReceiverRunnable();
		Thread tSnd = new Thread(sender);
		Thread tRcv = new Thread(receiver);
		tSnd.start(); 
		tRcv.start(); 
		tSnd.join();

		for (int i = 0; i < n; i++) {
			try {
				Assert.assertEquals(m[i][c[i]?1:0], rcvd[i]);
			} catch (AssertionError e) {
				System.out.println("rcvd[" + i + "]: " + rcvd[i].toHexStr());
				System.out.println("m[" + i + "][c[" + i + "]]: " + m[i][c[i]?1:0].toHexStr());
				
				System.out.println("rcvd[" + i + "]: " + Arrays.toString(rcvd[i].bytes));
				System.out.println("m[" + i + "][c[" + i + "]]: " + Arrays.toString(m[i][c[i]?1:0].bytes));
				throw e;
			}
		}
	}
	
	@Test
	public void testAllCases() throws Exception {
		System.out.println("Testing NPOT...");
		runThreads();
	}
}