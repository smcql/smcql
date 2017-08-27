package harness;


import java.math.BigInteger;

import org.junit.Test;

import com.oblivm.backend.circuits.arithmetic.IntegerLib;
import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Flag;
import com.oblivm.backend.flexsc.Mode;
import com.oblivm.backend.flexsc.Party;
import com.oblivm.backend.util.Utils;

public class TestSpeed extends TestHarness {

	public <T>T[] secureCompute(T[] a, T[] b, CompEnv<T> env) {
		IntegerLib<T> lib = new IntegerLib<T>(env);
		T[] res = null;
		

		Flag.sw.ands = 0;

		double t1 = System.nanoTime();
		for(int i = 0; i < 100; ++i) {
//			System.out.println(i+" "+env.getParty()+" "+System.currentTimeMillis()%10000/1000.0);
//			res = lib.and(a, b);
			
//			 T[] rr = env.newTArray(1);
//			 rr[0]  = re[0];
			
			T[] re = lib.and(a, b);
//			lib.declassifyToBoth(rr);
//			env.flush();
			double t2 = System.nanoTime();
			double t = (t2-t1)/1000000000.0;
			System.out.println(i+" "+t +"\t"+ env.numOfAnds/t+" "+env.getParty());

		}
		
		return res;
	}
	int LEN = 1024*10;
	class GenRunnable<T> extends com.oblivm.backend.network.Server implements Runnable {
		boolean[] z;

		public void run() {
			try {
				listen(54321);
				@SuppressWarnings("unchecked")
				CompEnv<T> gen = CompEnv.getEnv(Mode.REAL, Party.Alice, this);

				T[] a = gen.inputOfAlice(Utils.fromBigInteger(BigInteger.ONE, LEN));
				T[] b = gen.inputOfBob(new boolean[LEN]);

				T[] d = secureCompute(a, b, gen);
				os.flush();

				disconnect();

			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	class EvaRunnable<T> extends com.oblivm.backend.network.Client implements Runnable {
		public double andgates;
		public double encs;

		public void run() {
			try {
				connect("localhost", 54321);
				@SuppressWarnings("unchecked")
				CompEnv<T> env = CompEnv.getEnv(Mode.REAL, Party.Bob, this);

				T[] a = env.inputOfAlice(new boolean[LEN]);
				T[] b = env.inputOfBob(Utils.fromBigInteger(BigInteger.ONE, LEN));
				
				T[] d = secureCompute(a, b, env);
				
				os.flush();


				disconnect();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	@Test
	public <T>void runThreads() throws Exception {
		GenRunnable<T> gen = new GenRunnable<T>();
		EvaRunnable<T> env = new EvaRunnable<T>();
		Thread tGen = new Thread(gen);
		Thread tEva = new Thread(env);
		tGen.start();
		Thread.sleep(5);
		tEva.start();
		tGen.join();
		tEva.join();
	}
	
	public static void main(String args[]) throws Exception {
		 TestSpeed test = new TestSpeed();
		 if(args.length == 0){
			 	GenRunnable gen = test.new GenRunnable();
				EvaRunnable env = test.new EvaRunnable();
				Thread tGen = new Thread(gen);
				Thread tEva = new Thread(env);
				tGen.start();
				Thread.sleep(5);
				tEva.start();
				tGen.join();
				tEva.join();
		 }
		 if(new Integer(args[0]) == 0)
			 test.new GenRunnable().run();
		 else test.new EvaRunnable().run();
	}
}
