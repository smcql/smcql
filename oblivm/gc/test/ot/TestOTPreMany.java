package ot;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.ot.OTPreprocessReceiver;
import com.oblivm.backend.ot.OTPreprocessSender;

public class TestOTPreMany {
	static final int n = 50000;
	GCSignal[][] m;
	boolean[] c;
	GCSignal[] rcvd;

	class SenderRunnable extends com.oblivm.backend.network.Server implements Runnable {
		OTPreprocessSender snd;
		SenderRunnable () {}

		public void run() {
			SecureRandom rnd = new SecureRandom();
			try {
				listen(54321);
				snd = new OTPreprocessSender(80, this);
				
				for(int k = 0; k < 100; ++k) {
					m = new GCSignal[n][2];
					for (int i = 0; i < n; i++) {
						m[i][0] = GCSignal.freshLabel(rnd);
						m[i][1] = GCSignal.freshLabel(rnd);
					}

					double t1 = System.nanoTime();
					snd.send(m);
					os.flush();
					double t2 = System.nanoTime() -t1;
					System.out.println(t2/1000000000);
				}
				disconnect();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	class ReceiverRunnable extends com.oblivm.backend.network.Client implements Runnable {
		OTPreprocessReceiver rcv;
		ReceiverRunnable () {}

		public void run() {
			try {
				connect("localhost", 54321);
				rcv = new OTPreprocessReceiver(this);

				
				for(int k = 0; k < 100; ++k) {
					c = new boolean[n];
					Random rnd = new Random();
					for (int i = 0; i < n; i++)
						c[i] = rnd.nextBoolean();

					double t1 = System.nanoTime();
					rcvd = rcv.receive(c);
					os.flush();
					double t2 = System.nanoTime() -t1;

					System.out.println(t2/1000000000);
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
		tRcv.join();

		for (int i = 0; i < n; i++) {
			//		System.out.println(m[c?1:0].toHexStr());
			//		System.out.println(rcvd.toHexStr());
			//			System.out.println(i);
			try {
				Assert.assertEquals(rcvd[i], m[i][c[i]?1:0]);
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
		System.out.println("Testing OT Extension...");
		test1Case();
	}
}