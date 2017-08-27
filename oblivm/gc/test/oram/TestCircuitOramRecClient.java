package oram;

import com.oblivm.backend.flexsc.Flag;

import oram.TestCircuitOramRec.EvaRunnable;

public class TestCircuitOramRecClient {

	public  static void main(String args[]) throws Exception {
		for(int i = 8; i <=24 ; i+=4) {
			Flag.sw.flush();
			EvaRunnable eva = new EvaRunnable("localhost", 12345);
			eva.run();
			Flag.sw.print();
			System.out.print("\n");
		}
	}
}