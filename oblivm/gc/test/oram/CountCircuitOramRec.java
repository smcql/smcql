package oram;

import org.junit.Test;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Flag;
import com.oblivm.backend.flexsc.Mode;
import com.oblivm.backend.flexsc.PMCompEnv;
import com.oblivm.backend.flexsc.Party;
import com.oblivm.backend.oram.RecursiveCircuitOram;
import com.oblivm.backend.util.Utils;

public class CountCircuitOramRec {

	public  static void main(String args[]) throws Exception {
		for(int i = 9; i <=16 ; i++) {
			GenRunnable gen = new GenRunnable(12345, 18, 3, 32, 4, 6);
			EvaRunnable eva = new EvaRunnable("localhost", 12345);
			Thread tGen = new Thread(gen);
			Thread tEva = new Thread(eva);
			tGen.start();
			Thread.sleep(10);
			tEva.start();
			tGen.join();
		}
	}
	@Test
	public void runThreads() throws Exception {
		GenRunnable gen = new GenRunnable(12345, 16, 3, 32, 8, 6);
		EvaRunnable eva = new EvaRunnable("localhost", 12345);
		Thread tGen = new Thread(gen);
		Thread tEva = new Thread(eva);
		tGen.start();
		Thread.sleep(10);
		tEva.start();
		tGen.join();
		System.out.print("\n");
	}

	final static int writeCount = 1;
	final static int readCount = 0;//(1 << 7);

	public CountCircuitOramRec() {
	}

	public static class GenRunnable extends com.oblivm.backend.network.Server implements Runnable {
		int port;
		int logN;
		int N;
		int recurFactor;
		int cutoff;
		int capacity;
		int dataSize;
		int logCutoff;

		GenRunnable(int port, int logN, int capacity, int dataSize,
				int recurFactor, int logCutoff) {
			this.port = port;
			this.logN = logN;
			this.N = 1 << logN;
			this.recurFactor = recurFactor;
			this.logCutoff = logCutoff;
			this.cutoff = 1 << logCutoff;
			this.dataSize = dataSize;
			this.capacity = capacity;
		}

		public void run() {
			try {
				listen(port);

				os.write(logN);
				os.write(recurFactor);
				os.write(logCutoff);
				os.write(capacity);
				os.write(dataSize);
				os.flush();

//				System.out.println("\nlogN recurFactor  cutoff capacity dataSize");
//				System.out.println(logN + " " + recurFactor + " " + cutoff
//						+ " " + capacity + " " + dataSize);

				@SuppressWarnings("unchecked")
				CompEnv<Boolean> env = CompEnv.getEnv(Mode.COUNT, Party.Alice, this);
				RecursiveCircuitOram<Boolean> client = new RecursiveCircuitOram<Boolean>(
						env, N, dataSize, cutoff, recurFactor, capacity, 80);

				for (int i = 0; i < writeCount; ++i) {
					int element = i % N;

					Flag.sw.ands = 0;
					Boolean[] scData = client.baseOram.env.inputOfAlice(Utils
							.fromInt(element, dataSize));
					os.flush();
//					Flag.sw.startTotal();
					((PMCompEnv)env).statistic.flush();
					client.write(client.baseOram.lib.toSignals(element), scData);
					System.out.println(logN+"\t"+((PMCompEnv)env).statistic.andGate);
//					double t = Flag.sw.stopTotal();
//					System.out.println(Flag.sw.ands + " " + t / 1000000000.0
//							+ " " + Flag.sw.ands / t * 1000);
//					Flag.sw.addCounter();
//
//					Runtime rt = Runtime.getRuntime();
//					double usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024.0 / 1024.0;
//					System.out.println("mem: " + usedMB);
				}

				for (int i = 0; i < readCount; ++i) {
					int element = i % N;
					Boolean[] scb = client.read(client.baseOram.lib
							.toSignals(element));
					boolean[] b = client.baseOram.env.outputToAlice(scb);

					// Assert.assertTrue(Utils.toInt(b) == element);
					if (Utils.toInt(b) != element)
						System.out.println("inconsistent: " + element + " "
								+ Utils.toInt(b));
				}

				os.flush();

				disconnect();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	public static class EvaRunnable extends com.oblivm.backend.network.Client implements Runnable {

		String host;
		int port;

		EvaRunnable(String host, int port) {
			this.host = host;
			this.port = port;
		}

		public void run() {
			try {
				connect(host, port);

				int logN = is.read();
				int recurFactor = is.read();
				int logCutoff = is.read();
				int cutoff = 1 << logCutoff;
				int capacity = is.read();
				int dataSize = is.read();

				int N = 1 << logN;
//				System.out
//						.println("\nlogN recurFactor  cutoff capacity dataSize");
//				System.out.println(logN + " " + recurFactor + " " + cutoff
//						+ " " + capacity + " " + dataSize);

				@SuppressWarnings("unchecked")
				CompEnv<Boolean> env = CompEnv.getEnv(Mode.COUNT, Party.Bob, this);
				RecursiveCircuitOram<Boolean> server = new RecursiveCircuitOram<Boolean>(
						env, N, dataSize, cutoff, recurFactor, capacity, 80);
				for (int i = 0; i < writeCount; ++i) {
					int element = i % N;
					Boolean[] scData = server.baseOram.env
							.inputOfAlice(new boolean[dataSize]);
//					Flag.sw.startTotal();
					server.write(server.baseOram.lib.toSignals(element), scData);
//					 Flag.sw.stopTotal();
//					 Flag.sw.addCounter();
//					printStatistic();
				}

				int cnt = 0;
				for (int i = 0; i < readCount; ++i) {
					int element = i % N;
					Boolean[] scb = server.read(server.baseOram.lib
							.toSignals(element));
					server.baseOram.env.outputToAlice(scb);
					if (i % N == 0)
						System.out.println(cnt++);
				}

				disconnect();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
}