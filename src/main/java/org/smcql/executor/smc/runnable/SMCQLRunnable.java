package org.smcql.executor.smc.runnable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.smcql.db.data.Tuple;
import org.smcql.executor.smc.ExecutionSegment;
import org.smcql.executor.smc.OperatorExecution;
import org.smcql.executor.smc.SecureBufferPool;
import org.smcql.executor.smc.SecureQueryTable;
import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.util.EvaRunnable;
import com.oblivm.backend.util.GenRunnable;
import com.oblivm.backend.util.Utils;


public class SMCQLRunnable {
	
	// just obliv-needed stuff that is different for gen and eva
	public static class Generator<T> extends GenRunnable<T> implements java.io.Serializable, SMCRunnable {
		/**
		 * 
		 */
		private static final long serialVersionUID = -3762973620194026160L;

		/**
		 * 
		 */

		
		
		
		ExecutionSegment runSpec;
		SMCQLRunnableImpl<T> impl;
		
		public Generator(ExecutionSegment spec) throws Exception {

			runSpec = spec;
			this.m = runSpec.runConf.smcMode;
			this.port = runSpec.runConf.port;
			impl = new SMCQLRunnableImpl<T>(runSpec, this);
			
		}

	
		

		private void writeObject(ObjectOutputStream out) throws IOException {			
			out.writeInt(port);
			out.writeObject(args);
			out.writeObject(impl);
			out.writeObject(runSpec);
		}
		
		@SuppressWarnings("unchecked")
		private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {			
			port = ois.readInt();
			args = (String[])ois.readObject();
			impl = (SMCQLRunnableImpl<T>)ois.readObject();
			runSpec = (ExecutionSegment)ois.readObject();
		}

		
	
				

		
		// walk exec segment tree, instantiate all classes and prepare any plaintext inputs
		@Override
		public void prepareInput(CompEnv<T> env) { 			
		}
		

		
		@Override
		public void secureCompute(CompEnv<T> env) throws Exception {
			
			impl.secureCompute(env);

		}
		
		
		
		
		
			

			
		@Override
		public int[] prepareOutput(CompEnv<T> env) throws Exception {
			impl.prepareOutput(env);
			return null;
		}
		
		
		public SecureQueryTable getSecureOutput() throws Exception {
			return SecureBufferPool.getInstance().readRecord(runSpec.rootNode);
		}
		
		public void sendInt(int toSend) {
			this.writeByte(Utils.toByte(toSend), 4);
			this.flush();
		}
		
		public int getInt() {
			int out = Utils.fromByte(this.readBytes(4));
			return out;
		}














		@Override
		public ExecutionSegment getSegment() {
			return runSpec;
		}



		@Override
		public void sendTuple(Tuple toSend) {
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();

			try {
				ObjectOutputStream oos = new ObjectOutputStream(buffer);
				oos.writeObject(toSend);
				oos.close();
				byte[] rawData = buffer.toByteArray();
				
				this.writeByte(rawData);
				this.flush();

			
			} catch (IOException e) {
				e.printStackTrace();
			}


			
		}


		@Override
		public Tuple getTuple() {

			byte[] rawBytes = this.readBytes();
			this.flush();
			
			ByteArrayInputStream bis = new ByteArrayInputStream(rawBytes);
			ObjectInput in;
			try {
				in = new ObjectInputStream(bis);
				return (Tuple) in.readObject();

			} catch (Exception e) {
				e.printStackTrace();
			}

		
		return null;
		
		}











		@Override
		public SecureQueryTable getOutput() {
			return impl.getOutput();
		}


		@Override
		public OperatorExecution getRootOperator() {
			return runSpec.rootNode;
		}



	}
	
	public static class Evaluator<T> extends EvaRunnable<T> implements java.io.Serializable, SMCRunnable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
	


		
		
		ExecutionSegment runSpec;
		SMCQLRunnableImpl<T> impl;

	
		public Evaluator(ExecutionSegment spec) throws Exception {

			runSpec = spec;
						
			this.m = runSpec.runConf.smcMode;
			this.port = runSpec.runConf.port;
			this.host =  runSpec.runConf.host;
	
			impl = new SMCQLRunnableImpl<T>(runSpec, this);
		}


		
		// walk exec segment tree, instantiate all classes and prepare any plaintext inputs
		@Override
		public void prepareInput(CompEnv<T> env) { 
		// pushing this all down to secureCompute to handle multiple operators
		}
		

		private void writeObject(ObjectOutputStream out) throws IOException {
			out.writeObject(runSpec);
			out.writeObject(impl);
			out.writeInt(port);
			out.writeObject(args);
		}
		
		@SuppressWarnings("unchecked")
		private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
			runSpec = (ExecutionSegment)ois.readObject();
			impl = (SMCQLRunnableImpl<T>)ois.readObject();
			port = ois.readInt();
			args = (String[])ois.readObject();
		}

		
		@Override
		public void secureCompute(CompEnv<T> env) throws Exception {
			impl.secureCompute(env);
			
		}
		
		
		
		

		
		@Override
		public int[] prepareOutput(CompEnv<T> env) throws Exception {
			impl.prepareOutput(env);
			return null;
		}
			
		
		
		
		public void sendInt(int toSend) {
			this.writeByte(Utils.toByte(toSend), 4);
			this.flush();
		}
		
		public int getInt() {
			int out = Utils.fromByte(this.readBytes(4));
			return out;
		}



















	
		@Override
		public ExecutionSegment getSegment() {
			return runSpec;
		}











		@Override
		public void sendTuple(Tuple toSend) {
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();

			try {
				ObjectOutputStream oos = new ObjectOutputStream(buffer);
				oos.writeObject(toSend);
				oos.close();
				byte[] rawData = buffer.toByteArray();

				this.writeByte(rawData);
				this.flush();

			
			} catch (IOException e) {
				e.printStackTrace();
			}


			
		}






		@Override
		public Tuple getTuple() {


			byte[] rawBytes = this.readBytes();
			
			ByteArrayInputStream bis = new ByteArrayInputStream(rawBytes);
			ObjectInput in;
			try {
				in = new ObjectInputStream(bis);
				return (Tuple) in.readObject();

			} catch (Exception e) {
				e.printStackTrace();
			}

		
		return null;
		
		}



		@Override
		public SecureQueryTable getOutput() {
			return impl.getOutput();
		}



		@Override
		public OperatorExecution getRootOperator() {
			return runSpec.rootNode;
		}





	

	};
	
}
