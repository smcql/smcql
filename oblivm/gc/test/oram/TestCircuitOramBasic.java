package oram;

import java.security.SecureRandom;
import java.util.Arrays;

import org.junit.Test;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Mode;
import com.oblivm.backend.flexsc.Party;
import com.oblivm.backend.oram.CircuitOram;
import com.oblivm.backend.util.Utils;

//import gc.Boolean;

public class TestCircuitOramBasic {
	final int N = 1 << 6;
	final int capacity = 3;
	int[] posMap = new int[N];
	int writecount = N;
	int readcount = N * 10;
	int dataSize = 32;

	public TestCircuitOramBasic() {
		SecureRandom rng = new SecureRandom();
		for (int i = 0; i < posMap.length; ++i)
			posMap[i] = rng.nextInt(N);
	}

	SecureRandom rng = new SecureRandom();
	boolean breaksignal = false;

	class GenRunnable extends com.oblivm.backend.network.Server implements Runnable {
		int port;

		GenRunnable(int port) {
			this.port = port;
		}

		public int[][] idens;
		public boolean[][] du;
		public int[] stash;

		public void run() {
			try {
				listen(port);

				int data[] = new int[N + 1];
				@SuppressWarnings("unchecked")
				CompEnv<Boolean> env = CompEnv.getEnv(Mode.VERIFY, Party.Alice, this);
				CircuitOram<Boolean> client = new CircuitOram<Boolean>(env, N,
						dataSize, capacity, 80);
				System.out.println("logN:" + client.logN + ", N:" + client.N);

				for (int i = 0; i < writecount; ++i) {
					int element = i % N;

					int oldValue = posMap[element];
					int newValue = rng.nextInt(1 << client.lengthOfPos);
					System.out.println(element + " " + oldValue + " "
							+ newValue);
					data[element] = 2 * element + 1;
					// long t1 = System.currentTimeMillis();
					Boolean[] scNewValue = client.env.inputOfAlice(Utils
							.fromInt(newValue, client.lengthOfPos));
					Boolean[] scData = client.env.inputOfAlice(Utils.fromInt(
							data[element], client.lengthOfData));
					client.write(client.lib.toSignals(element),
							Utils.fromInt(oldValue, client.lengthOfPos),
							scNewValue, scData);
					System.out.println(client.toString());

					os.write(0);
					posMap[element] = newValue;
					os.flush();

					// long t2 = System.currentTimeMillis() - t1;
					Runtime rt = Runtime.getRuntime();
					double usedMB = (rt.totalMemory() - rt.freeMemory()) / 1024.0 / 1024.0;
					System.out.println("mem: " + usedMB);

					posMap[element] = newValue;
				}

				for (int i = 0; i < readcount; ++i) {
					int element = i % N;
					int oldValue = posMap[element];
					int newValue = rng.nextInt(1 << client.lengthOfPos);
					System.out.println(element + " " + oldValue + " "
							+ newValue);

					Boolean[] scNewValue = client.env.inputOfAlice(Utils
							.fromInt(newValue, client.lengthOfPos));
					Boolean[] scb = client.read(client.lib.toSignals(element),
							Utils.fromInt(oldValue, client.lengthOfPos),
							scNewValue);

					boolean[] b = client.env.outputToAlice(scb);
					os.write(0);
					posMap[element] = newValue;
					os.flush();

					// Assert.assertTrue(Utils.toInt(b.data) == data[element]);
					if (Utils.toInt(b) != data[element]) {
						System.out.println("inconsistent: " + element + " "
								+ Utils.toInt(b) + " " + data[element] + " "
								+ posMap[element]);
					}

				}

				idens = new int[client.tree.length][];
				du = new boolean[client.tree.length][];

				for (int j = 1; j < client.tree.length; ++j) {
					idens[j] = new int[client.tree[j].length];
					for (int i = 0; i < client.tree[j].length; ++i)
						idens[j][i] = (int) client.tree[j][i].iden;
				}

				for (int j = 1; j < client.tree.length; ++j) {
					du[j] = new boolean[client.tree[j].length];
					for (int i = 0; i < client.tree[j].length; ++i)
						du[j][i] = client.tree[j][i].isDummy;
				}

				stash = new int[client.queue.length];
				for (int j = 0; j < client.queue.length; ++j)
					stash[j] = (int) client.queue[j].iden;

				os.flush();

				disconnect();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	class EvaRunnable extends com.oblivm.backend.network.Client implements Runnable {
		String host;
		int port;
		public int[][] idens;
		public boolean[][] du;
		public int[] stash;

		EvaRunnable(String host, int port) {
			this.host = host;
			this.port = port;
		}

		public void run() {
			try {
				connect(host, port);

				// CircuitOramServer<Boolean> server = new
				// CircuitOramServer<Boolean>(is, os, N, dataSize, Party.Bob,
				// capacity, Mode.REAL, 80);
				@SuppressWarnings("unchecked")
				CompEnv<Boolean> env = CompEnv.getEnv(Mode.VERIFY, Party.Bob, this);
				CircuitOram<Boolean> server = new CircuitOram<Boolean>(env, N,
						dataSize, capacity, 80);

				for (int i = 0; i < writecount; ++i) {
					int element = i % N;
					int oldValue = posMap[element];
					Boolean[] scNewValue = server.env
							.inputOfAlice(new boolean[server.lengthOfPos]);
					Boolean[] scData = server.env
							.inputOfAlice(new boolean[server.lengthOfData]);

					server.write(server.lib.toSignals(element),
							Utils.fromInt(oldValue, server.lengthOfPos),
							scNewValue, scData);
					server.toString();

					// server.access(oldValue);
					is.read();
				}

				for (int i = 0; i < readcount; ++i) {
					int element = i % N;
					int oldValue = posMap[element];
					Boolean[] scNewValue = server.env
							.inputOfAlice(new boolean[server.lengthOfPos]);
					Boolean[] scb = server.read(server.lib.toSignals(element),
							Utils.fromInt(oldValue, server.lengthOfPos),
							scNewValue);

					server.env.outputToAlice(scb);
					is.read();
				}

				idens = new int[server.tree.length][];
				du = new boolean[server.tree.length][];
				for (int j = 1; j < server.tree.length; ++j) {
					idens[j] = new int[server.tree[j].length];
					for (int i = 0; i < server.tree[j].length; ++i)
						idens[j][i] = (int) server.tree[j][i].iden;
				}

				for (int j = 1; j < server.tree.length; ++j) {
					du[j] = new boolean[server.tree[j].length];
					for (int i = 0; i < server.tree[j].length; ++i)
						du[j][i] = server.tree[j][i].isDummy;
				}

				stash = new int[server.queue.length];
				for (int j = 0; j < server.queue.length; ++j)
					stash[j] = (int) server.queue[j].iden;
				os.flush();

				disconnect();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	GenRunnable gen = new GenRunnable(1234);
	EvaRunnable eva = new EvaRunnable("localhost", 1234);

	@Test
	public void runThreads() throws Exception {
		Thread tGen = new Thread(gen);
		Thread tEva = new Thread(eva);
		tGen.start();
		Thread.sleep(10);
		tEva.start();
		tGen.join();
		printTree(gen, eva);
		System.out.println(Arrays.toString(xor(gen.stash, eva.stash)));
		System.out.print("\n");

		System.out.println();
	}

	public void printTree(GenRunnable gen, EvaRunnable eva) {
		int k = 1;
		int i = 1;
		for (int j = 1; j < gen.idens.length; ++j) {
			System.out.print("[");
			int[] a = xor(gen.idens[j], eva.idens[j]);
			boolean[] bb = xor(gen.du[j], eva.du[j]);
			for (int p = 0; p < eva.idens[j].length; ++p)
				if (bb[p])
					System.out.print("d,");
				else
					System.out.print(a[p] + ",");
			System.out.print("]");
			if (i == k) {
				k = k * 2;
				i = 0;
				System.out.print("\n");
			}
			++i;
		}
		System.out.print("\n");
	}

	public boolean[] xor(boolean[] a, boolean[] b) {
		boolean[] res = new boolean[a.length];
		for (int i = 0; i < res.length; ++i)
			res[i] = a[i] ^ b[i];
		return res;

	}

	public int[] xor(int[] a, int[] b) {
		int[] res = new int[a.length];
		for (int i = 0; i < res.length; ++i)
			res[i] = a[i] ^ b[i];
		return res;

	}

}