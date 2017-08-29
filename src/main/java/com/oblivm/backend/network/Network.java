package com.oblivm.backend.network;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Mode;
import com.oblivm.backend.gc.GCSignal;

public class Network {
	protected Socket sock;
	protected ServerSocket serverSock;
	public CustomizedConcurrentQueue queue;
	ThreadedIO threadedio;
	public InputStream is;
	public OutputStream os;
	Thread thd;
	boolean  THREADEDIO = true;
	static int NetworkThreadedQueueSize = 1024*256;
	public void setUpThread() {
		if(THREADEDIO) {
			queue = new CustomizedConcurrentQueue(NetworkThreadedQueueSize);
			threadedio = new ThreadedIO(queue, os);
			thd = new Thread(threadedio);
			thd.start();
		}
	}

	public Network() {
		
	}

	public Network(InputStream is, OutputStream os, Socket sock) {
		this.is = is;
		this.os = os;
		this.sock = sock;
	}

	public void disconnect() {
		try {
			if(THREADEDIO) {
				queue.destory();
				os.flush();
				thd.join();

			}
			os.flush();
			// protocol payloads are received.
			if(sock != null) {
				sock.close();
			}
			if(serverSock!= null){
				serverSock.close();
				}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void flush() {
		try {
			os.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public byte[] readBytes(int len) {
		byte[] temp = new byte[len];
		try {
			int remain = len;
			while (0 < remain) {
				int readBytes;

				readBytes = is.read(temp, len - remain, remain);
				if (readBytes != -1) {
					remain -= readBytes;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return temp;
	}

	public void readBytes(byte[] temp) {
//		byte[] temp = new byte[len];
		try {
			int remain = temp.length;
			while (0 < remain) {
				int readBytes;

				readBytes = is.read(temp, temp.length - remain, remain);
				if (readBytes != -1) {
					remain -= readBytes;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	public byte[] readBytes() {
		byte[] lenBytes = readBytes(4);
		int len = ByteBuffer.wrap(lenBytes).getInt();
		return readBytes(len);
	}

	public void writeByte(byte[] data) {
		writeByte(ByteBuffer.allocate(4).putInt(data.length).array(), 4);
		writeByte(data, data.length);
	}

	public void writeByte(byte[] data, int length) {
		try {
			if(THREADEDIO){
				queue.insert(data);
//			System.out.println(data.length);
			}
			else {
				os.write(data);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public void writeByte(byte data) {
		try {
			if(THREADEDIO)
				queue.insert(new byte[]{data});
			else {
				os.write(data);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void writeBI(BigInteger bi) {
		writeByte(bi.toByteArray());
	}

	public BigInteger readBI() {
		byte[] rep = readBytes();

		return new BigInteger(rep);
	}

	public void writeInt(int i) {
		writeByte(ByteBuffer.allocate(4).putInt(i).array(), 4);
	}

	public int readInt() {
		return ByteBuffer.wrap(readBytes(4)).getInt();
	}	

	public <T> void send(T[][][] data, CompEnv<T> env) throws IOException {
		for (int i = 0; i < data.length; i++) {
			send(data[i], env);
		}
	}

	public <T> void send(T[][] data, CompEnv<T> env) throws IOException {
		for (int i = 0; i < data.length; i++) {
			send(data[i], env);
		}
	}

	public <T> T[][] read(int length1, int length2, CompEnv<T> env) throws IOException {
		T[][] ret = env.newTArray(length1, 1);
		for (int i = 0; i < length1; i++) {
			ret[i] = read(length2, env);
		}
		return ret;
	}

	public <T> T[][][] read(int length1, int length2, int length3, CompEnv<T> env) throws IOException {
		T[][][] ret = env.newTArray(length1, 1, 1);
		for (int i = 0; i < length1; i++) {
			ret[i] = read(length2, length3, env);
		}
		return ret;
	}

	public <T> void send(T[] data, CompEnv<T> env) throws IOException {
		for (int i = 0; i < data.length; i++) {
			send(data[i], env);
		}
	}

	public <T> void send(T data, CompEnv<T> env) throws IOException {
		Mode mode = env.getMode();
		if (mode == Mode.REAL) {
			GCSignal gcData = (GCSignal) data;
			gcData.send(this);
		} else if(mode == Mode.VERIFY) {
			writeBoolean((Boolean) data);
		} else if (mode == Mode.COUNT) {

		}
	}

	public <T> T[] read(int length, CompEnv<T> env) throws IOException {
		T[] ret = env.newTArray(length);
		for (int i = 0; i < length; i++) {
			ret[i] = read(env);
		}
		return ret;
	}

	@SuppressWarnings("unchecked")
	public <T> T read(CompEnv<T> env) throws IOException {
		Mode mode = env.getMode();
		if (mode == Mode.REAL || mode == Mode.OPT || mode == Mode.OFFLINE) {
			GCSignal signal = GCSignal.receive(this);
			return (T) signal;
		} else if(mode == Mode.VERIFY) {
			Boolean vData = readBoolean();
			return (T) vData;
		} else if (mode == Mode.COUNT) {
			return env.ZERO();
		}
		// shouldn't happen;
		return null;
	}

	public boolean readBoolean() throws IOException {
		int read = readInt();
		return read == 1;
	}

	public void writeBoolean(boolean data) throws IOException {
		int sen = data ? 1 : 0;
		writeInt(sen);
	}
}
