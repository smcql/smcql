package com.oblivm.backend.network;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;

import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.io.output.CountingOutputStream;

import com.oblivm.backend.flexsc.Flag;

public class Client extends Network {

	CountingOutputStream cos;
	CountingInputStream cis;

	public void connect(String server, int port) throws InterruptedException {
		try{
			while (true) {
				try {
					sock = new java.net.Socket(server, port); // create socket and
					// connect
					if (sock != null)
						break;
				} catch(IOException e){
					Thread.sleep(10);
				}
			}
			if (Flag.countIO) {
				cos = new CountingOutputStream(sock.getOutputStream());
				cis = new CountingInputStream(sock.getInputStream());
				os = new BufferedOutputStream(cos);
				is = new BufferedInputStream(cis);
			} else {
				os = new BufferedOutputStream(sock.getOutputStream());
				is = new BufferedInputStream(sock.getInputStream());

			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		setUpThread();
	}

	public void printStatistic() {
		if (Flag.countIO) {
			System.out.println("\n********************************\n"
					+ "Data Sent from Client :" + cos.getByteCount() / 1024.0
					/ 1024.0 + "MB\n" + "Data Sent to Client :"
					+ cis.getByteCount() / 1024.0 / 1024.0 + "MB"
					+ "\n********************************");
		}
	}
}
