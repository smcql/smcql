package org.smcql.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class FileUtils {
	public static String readSQL(String filename) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
	    String sql = "";
	    String line;
	    while ((line = br.readLine()) != null) {
	    	
	    	line = line.replaceAll("\\-\\-.*$", ""); // delete any comments
	        sql = sql + line + " ";
	    }
	    br.close();
	    sql = sql.replaceAll("`", "");

	    return sql;
	}
}
