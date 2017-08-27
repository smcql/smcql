package oram;

import java.security.SecureRandom;

import org.junit.Test;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Mode;
import com.oblivm.backend.flexsc.PMCompEnv;
import com.oblivm.backend.flexsc.Party;
import com.oblivm.backend.oram.CircuitOram;
import com.oblivm.backend.util.Utils;


public class CountCircuitOramBasic {
	int N;
	int logN;
	final int capacity = 4;
	int writecount = 1;
	int dataSize;

	public CountCircuitOramBasic() {
	}

	SecureRandom rng = new SecureRandom();
	boolean breaksignal = false;

	class GenRunnable extends com.oblivm.backend.network.Server implements Runnable {
		int port;

		GenRunnable(int port) {
			this.port = port;
		}


		public void run() {
			try {
				listen(port);

				@SuppressWarnings("unchecked")
				CompEnv<Boolean> env = CompEnv.getEnv(Mode.COUNT, Party.Alice, this);
				CircuitOram<Boolean> client = new CircuitOram<Boolean>(env, N,
						dataSize, capacity, 80);
//				System.out.println("logN:" + client.logN + ", N:" + client.N);

				for (int i = 0; i < writecount; ++i) {
					Boolean[] scNewValue = client.env.inputOfAlice(Utils
							.fromInt(1, client.lengthOfPos));
					Boolean[] scData = client.env.inputOfAlice(Utils.fromInt(
							1, client.lengthOfData));
					PMCompEnv pm = (PMCompEnv) env;
					pm.statistic.flush();
					client.write(client.lib.toSignals(1),
							Utils.fromInt(1, client.lengthOfPos),
							scNewValue, scData);

					System.out.println(pm.statistic.andGate);
					os.flush();
					
				}

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


		EvaRunnable(String host, int port) {
			this.host = host;
			this.port = port;
		}

		public void run() {
			try {
				connect(host, port);

				@SuppressWarnings("unchecked")
				CompEnv<Boolean> env = CompEnv.getEnv(Mode.COUNT, Party.Bob, this);
				CircuitOram<Boolean> server = new CircuitOram<Boolean>(env, N,
						dataSize, capacity, 80);

				for (int i = 0; i < writecount; ++i) {
					Boolean[] scNewValue = server.env
							.inputOfAlice(new boolean[server.lengthOfPos]);
					Boolean[] scData = server.env
							.inputOfAlice(new boolean[server.lengthOfData]);
					
					server.write(server.lib.toSignals(1),
							Utils.fromInt(1, server.lengthOfPos),
							scNewValue, scData);
				}


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
		for(int i = 10; i <= 30; i+=2 ) {
			this.logN = 10;
			this.N = 1<<logN;
			this.dataSize = logN*2+10;//i+i+i+i+32+32;
			System.out.print(i+"\t");
		Thread tGen = new Thread(gen);
		Thread tEva = new Thread(eva);
		tGen.start();
		Thread.sleep(10);
		tEva.start();
		tGen.join();
//		System.out.println();
		}
	}

}