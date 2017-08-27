package oram;

import com.oblivm.backend.flexsc.Flag;

import oram.TestCircuitOramRec.GenRunnable;

public class TestCircuitOramRecServer {

	public  static void main(String args[]) throws Exception {
		for(int i = 12; i <=20 ; i+=2) {
			Flag.sw.flush();
			GenRunnable gen = new GenRunnable(12345, i, 3, 32, 8, 6);
			gen.run();
			Flag.sw.print();
			System.out.print("\n");
			//asdasdasdas
		}
	}
}