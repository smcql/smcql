package com.oblivm.backend.gc.offline;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

public class FileReader {
	byte[] data;
	int pos = 0;
	public FileReader(String name) {
		try {
			File file = new File(name);
			FileInputStream fis;
			fis = new FileInputStream(file);
			data = new byte[(int) file.length()];
			fis.read(data);
			fis.close();
		} catch ( IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public void read(byte[] a) {
		System.arraycopy(data, pos, a, 0, a.length);
		pos += a.length;
	}
	
	public byte[] read(int len){
		byte[] res = Arrays.copyOfRange(data, pos, pos+len);
		pos += len;
		return res;
	}
	
	static public void main(String[] args) {
		double t1 = System.nanoTime();
		FileReader a = new FileReader("table");
		double t2 = System.nanoTime();
		System.out.println(t2-t1);
		System.out.println(a.data.length);
		byte[] b = new byte[10];
		a.read(b);
	}
}