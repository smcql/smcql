/***
 * Copyright (C) 2015 by Chang Liu <liuchang@cs.umd.edu>
 */
package com.oblivm.backend.lang.inter;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import com.oblivm.backend.flexsc.Flag;
import com.oblivm.backend.flexsc.Mode;
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.oram.SecureArray;
import com.oblivm.backend.util.EvaRunnable;
import com.oblivm.backend.util.GenRunnable;

/**
 * A single entry point to call EvaRunnable and GenRunnable. This class should be used as the entry point for the jar file.
 *
 */
public class Cmd {
	
	
	public static void run(String packageName, String host, int port, String mode, SecureArray<GCSignal> lhs, SecureArray<GCSignal> rhs) {
		
	}
	
	
	@SuppressWarnings("rawtypes")
	public static void main(String[] args) throws Exception {
		ArgumentParser ap = ArgumentParsers.newArgumentParser("Cmd");
		ap.addArgument("file").nargs("*").help("File to compile");
		ap.addArgument("--config").setDefault("Config.conf").help("Config file");
		ap.addArgument("-la", "--lenghAlice")
			.help("input length of Alice");
		ap.addArgument("-t", "--type")
			.choices("gen", "eva")
			.setDefault("gen")
			.help("Whether it is the generator or the evaluator");
		ap.addArgument("-i", "--input")
			.help("Input file");
		ap.addArgument("-c", "--class")
			.help("Runnable Class");
		ap.addArgument("-hp")
    		.setDefault("-1");
		ap.addArgument("-hh")
    		.setDefault("none");
		Namespace ns = null;
		try {
			ns = ap.parseArgs(args);
		} catch (ArgumentParserException e) {
			ap.handleError(e);
			System.exit(1);
		}
		
		int hbPort = Integer.parseInt(ns.getString("hp"));
		String hbHost = ns.getString("hh");

		if(ns.getString("type").equals("gen")) {
			GenRunnable gen = new MainRunnable.Generator(ns.getString("class"), ns.getString("input"), hbHost, hbPort);
			gen.loadConfig(ns.getString("config"));
			gen.runCore();
			if(Flag.CountTime)
				Flag.sw.print();
		} else {
			EvaRunnable eva = new MainRunnable.Evaluator(ns.getString("class"), ns.getString("input"), hbHost, hbPort);
			eva.loadConfig(ns.getString("config"));
			eva.runCore();
			if(Flag.CountTime)
				Flag.sw.print();
			if(Flag.countIO)
				eva.printStatistic();
		}
	}


	public static int[] run(String[] args) throws Exception {
        ArgumentParser ap = ArgumentParsers.newArgumentParser("Cmd");
        ap.addArgument("file").nargs("*").help("File to compile");
        ap.addArgument("--config").setDefault("Config.conf").help("Config file");
        ap.addArgument("-la", "--lenghAlice")
            .help("input length of Alice");
        ap.addArgument("-t", "--type")
            .choices("gen", "eva")
            .setDefault("gen")
            .help("Whether it is the generator or the evaluator");
        ap.addArgument("-i", "--input")
            .help("Input file");
		ap.addArgument("-p", "--port")
			.setDefault("54321");
		ap.addArgument("-ho", "--host")
			.setDefault("localhost");
		ap.addArgument("-m", "--mode")
			.setDefault("REAL");
        ap.addArgument("-c", "--class")
            .help("Runnable Class");
        ap.addArgument("-hp")
			.setDefault("-1");
        ap.addArgument("-hh")
			.setDefault("none");
 
        Namespace ns = null;
                
		int[] result = new int[0];
		try {
            ns = ap.parseArgs(args);
        } catch (ArgumentParserException e) {
            ap.handleError(e);
            System.exit(1);
        }
	
		result = aggregateFunctions(ns);
		
		return result;
    }
	
	@SuppressWarnings("rawtypes")
	private static int[] aggregateFunctions(Namespace ns) throws Exception {
		Mode mode = Mode.getMode(ns.getString("mode").replace(" ", ""));
		String host = ns.getString("host").replace(" ", "");
		int port = Integer.parseInt(ns.getString("port").replace(" ", ""));
		String className = ns.getString("class");
		String input = ns.getString("input");
		String type = ns.getString("type");
		int honestBrokerPort = Integer.parseInt(ns.getString("hp"));
		String honestBrokerHost = ns.getString("hh");
		int[] result = new int[0];
		
        if(type.equals("gen")) {
            GenRunnable gen = new MainRunnable.Generator(className, input, honestBrokerHost, honestBrokerPort);
            gen.setParameter(mode, port);
			result = gen.runCore();
            if(Flag.CountTime)
                Flag.sw.print();
        } else {
            EvaRunnable eva = new MainRunnable.Evaluator(className, input, honestBrokerHost, honestBrokerPort);
            eva.setParameter(mode, host, port);
			result = eva.runCore();
            if(Flag.CountTime)
                Flag.sw.print();
            if(Flag.countIO)
                eva.printStatistic();
        }
        
		return result;
	}
}
