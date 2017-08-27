package org.smcql.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.smcql.config.SystemConfiguration;
import org.smcql.db.schema.SecureSchemaLookup;
import org.smcql.executor.smc.OperatorExecution;
import org.smcql.plan.SecureRelRoot;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelDataTypeField.SecurityPolicy;
import org.smcql.type.SecureRelRecordType;


public class Utilities {
	
	
    
	 public static String getSMCQLRoot() {
	        String    root = System.getProperty("smcql.root"); // for remote systems
	       
	        if(root != null) {
	            return root;
	        }
	       
	        // fall back to local path
	        URL location = Utilities.class.getProtectionDomain().getCodeSource().getLocation();
	        String path = location.getFile();
	       
	        // chop off trailing "/bin/src/"
	        if(path.endsWith("src/")) { // ant build
	            path = path.substring(0, path.length()-"src/".length());
	        }
	       
	        if(path.endsWith("bin/")) { // eclipse and ant build
	            path = path.substring(0, path.length() - "/bin/".length());
	        }
	       
	        if(path.endsWith("target/classes/")) 
	            path = path.substring(0, path.length() - "/target/classes/".length());

	        if(path.endsWith("target/smcql-open-source-0.5.jar"))
	        	path = path.substring(0, path.length() - "/target/smcql-open-source-0.5.jar".length());
	        
	        return path;
	    }	
	 
	 
     public static String getCodeGenRoot() {
         return getSMCQLRoot() + "/conf/smc/operators";
 }
 
 public static String getCodeGenTarget() {
         return getSMCQLRoot() + "/bin";
 }
 
	 public static List<String> readFile(String filename) throws IOException  {				  
			

				List<String> lines = Files.readAllLines(Paths.get(filename), StandardCharsets.UTF_8);
				return lines;
					
			
		}

	 public static void writeFile(String fname, String contents) throws FileNotFoundException, UnsupportedEncodingException {
         String path = FilenameUtils.getFullPath(fname);
         File f = new File(path);
         f.mkdirs();

         PrintWriter writer = new PrintWriter(fname, "UTF-8");
         writer.write(contents);
         writer.close();


	 }

		public static byte[] readGeneratedClassFile(String packageName) throws IOException {

			String filename = Utilities.getCodeGenTarget() + "/"  + packageName.replace('.', '/') + "/NoClass.class";
			return readBinaryFile(filename);
		}

		public static byte[] readBinaryFile(String filename) throws IOException {
		 	  Path p = FileSystems.getDefault().getPath("", filename);
		 	  return Files.readAllBytes(p);	 
		}


		public static SecureRelRecordType getOutSchemaFromString(String sql) throws Exception {
			SecureRelRoot relRoot = new SecureRelRoot("anonymous", sql);
			
			return relRoot.getPlanRoot().getSchema();
			
		}


		public static boolean isCTE(OperatorExecution src) {
			String packageName = src.packageName;
			String pkg = packageName.substring(packageName.lastIndexOf('.') + 1);
			pkg = pkg.replaceAll("\\d", "");
			return pkg.equals("CommonTableExpressionScan");
		}
		public static void mkdir(String path) throws Exception {
			
			String cmd = "mkdir -p "  + path;
			
			CommandOutput output = runCmd(cmd);
			
			if(output.exitValue != 0 && output.exitValue != 1) { // 1 = already exists
				throw new Exception("Failed to create path " + path + "!");
			}
			
			
		}
		
	public static void cleanDir(String path) throws Exception {
			
			String cmd = "rm -rf "  + path + "/*" ;
			CommandOutput output = runCmd(cmd);
			
			if(output.exitValue != 0) {
				throw new Exception("Failed to clear out " + path + "!");
			}
			
			
		}
		
	public static CommandOutput runCmd(String aCmd) throws IOException, InterruptedException {
		
		String[] cmd = StringUtils.split(aCmd, ' ');
		return runCmd(cmd);
	}
	
	
	
	
	public static CommandOutput runCmd(String[] cmd) throws IOException, InterruptedException {
		File dir = new File(Utilities.getSMCQLRoot());
		Process p = java.lang.Runtime.getRuntime().exec(cmd, null, dir);

		BufferedReader stderr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
		BufferedReader stdout = new BufferedReader(new InputStreamReader(p.getInputStream()));

		String err, out;
		
		CommandOutput cmdOutput = new CommandOutput();
		
		err = stderr.readLine();
		out = stdout.readLine();
		while(err != null || out != null) {
			if(err != null) {
				cmdOutput.output += err + "\n";
				err = stderr.readLine();
			}
			if(out != null) {
				cmdOutput.output += out + "\n";
				out = stdout.readLine();
			}
		}		
		
		p.waitFor();
		
		cmdOutput.exitValue = p.exitValue();
		return cmdOutput;
	}


	public static String getOperatorId(String packageName) {
		int idx = packageName.lastIndexOf('.');
		return packageName.substring(idx+1, packageName.length());
	
	}

	public static String getTime() {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yy HH:mm:ss");
       return sdf.format(cal.getTime());
	}

	public static float getElapsed(Date start, Date end) {
		return  (end.getTime() - start.getTime()) / 1000F; // ms --> secs
		
	}

	public static boolean dirsEqual(String lhs, String rhs) throws IOException, InterruptedException {
		String cmd = "diff -r " + lhs + " " + rhs;


		CommandOutput output =  runCmd(cmd);
		if(output.exitValue != 0) {
			System.out.println("diff: " + output.output);
		}
		return output.exitValue == 0;
	}

	
	public static boolean isMerge(OperatorExecution op) {
		if(op.packageName.endsWith(".merge"))
			return true;
		return false;
	}


	public static SecureRelDataTypeField lookUpAttribute(String table, String attr) throws Exception {
		SystemConfiguration conf = SystemConfiguration.getInstance();
		SchemaPlus tables = conf.getPdnSchema();
		Table lookupTable = tables.getTable(table);
		JavaTypeFactory typeFactory = conf.getCalciteConnection().getTypeFactory();
		
		RelRecordType rowType = (RelRecordType) lookupTable.getRowType(typeFactory);
		RelDataTypeField fieldType = rowType.getField(attr, false, false);
		
		SecureSchemaLookup lookup = SecureSchemaLookup.getInstance();
		SecurityPolicy policy = lookup.getPolicy(table, attr);
		
		SecureRelDataTypeField result = new SecureRelDataTypeField(fieldType, policy);
		result.setStoredAttribute(attr);
		result.setStoredTable(table);
		return result;
	}

}
