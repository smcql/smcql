package com.oblivm.backend.network;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.ServerSocket;

public class Server extends Network {

	public void listen(int port) {
		try {
			serverSock = new ServerSocket(port);
			sock = serverSock.accept(); // wait for client to connect

			os = new BufferedOutputStream(sock.getOutputStream());
			is = new BufferedInputStream(sock.getInputStream());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // create socket and bind to port
		setUpThread();	
	}
}
