package com.oblivm.backend.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import org.apache.commons.cli.ParseException;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Flag;
import com.oblivm.backend.flexsc.Mode;
import com.oblivm.backend.flexsc.Party;

public abstract  class EvaRunnable<T> extends com.oblivm.backend.network.Client implements Runnable {
	public abstract void prepareInput(CompEnv<T> gen) throws Exception;
	public abstract void secureCompute(CompEnv<T> gen) throws Exception;
	public abstract int[] prepareOutput(CompEnv<T> gen) throws Exception;
	protected Mode m;
	protected int port;
	protected String host;
	protected String[] args;
	public boolean verbose = true;

	public void setParameter(Mode m, String host, int port, String[] args){
		this.m = m;
		this.port = port;
		this.host = host;
		this.args = args;
	}

	public void setParameter(Mode m, String host, int port){
		this.m = m;
		this.port = port;
		this.host = host;
	}

	public int[] runCore() {
		int[] result = new int[0];
		try {
			double s = 0;
			double e = 0;
			try {
				if(verbose)
					System.out.println("connecting");
	            connect(host, port);
	            if(verbose)
	                System.out.println("connected");
				
				@SuppressWarnings("unchecked")
	            CompEnv<T> env = CompEnv.getEnv(m, Party.Bob, this);
	            System.out.println("Env: " + env);

				Flag.sw.startTotal();

	            s = System.nanoTime();
	            prepareInput(env);
	            os.flush();
	            secureCompute(env);
	            os.flush();
	            result = prepareOutput(env);
	            os.flush();
	            Flag.sw.stopTotal();
	            e = System.nanoTime();
			} finally {
				disconnect();
				if(verbose){
					System.out.println("Eva running time:"+(e-s)/1e9);
	     //           System.out.println("Number Of AND Gates:"+env.numOfAnds);
	            }
			}	
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}	
		return result;
	}
	
	public void run() {
		try {
			runCore();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public void loadConfig() {
		loadConfig("Config.conf");
	}
	
	public void loadConfig(String fileName) {
		File file = new File(fileName);
		Scanner scanner;
		String host = null;
		int port = 0;
		Mode mode = null;

		try {
			scanner = new Scanner(file);
			while(scanner.hasNextLine()) {
				String a = scanner.nextLine();
				String[] content = a.split(":");
				if(content.length == 2) {
					if(content[0].equals("Host"))
						host = content[1].replace(" ", "");
					else if(content[0].equals("Port"))
						port = new Integer(content[1].replace(" ", ""));
					else if(content[0].equals("Mode"))
						mode = Mode.getMode(content[1].replace(" ", ""));
					else{}
				}
			}
			scanner.close();			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		this.setParameter(mode, host, port);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ParseException, ClassNotFoundException {
		Class<?> clazz = Class.forName(args[0]+"$Evaluator");
		EvaRunnable run = (EvaRunnable) clazz.newInstance();
		String[] input = new String[1];
		input[0] = args[1];
		System.out.println("Generator Input: " + input[0]);
		run.args = input;
		run.loadConfig();
		run.run();
		
		if(Flag.CountTime)
			Flag.sw.print();
		if(Flag.countIO)
			run.printStatistic();
	}
}
