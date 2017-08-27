package org.smcql.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.linq4j.tree.ConditionalExpression;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.util.PGInterval;
import org.smcql.executor.config.RunConfig.ExecutionMode;
import org.smcql.plan.operator.Operator;
import org.smcql.plan.operator.Project;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelRecordType;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.select.Pivot;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

public class CodeGenUtils {

	public static String getBitmask(String src, int size, int idx) {
		String ret = new String(src) + "$" + idx;
		idx += size - 1; // inclusive
		ret += "~" + idx + "$";
		++idx;
		
		return ret;
	}
	

	
	// read in template file and replace all of its variables with intended values
	// source file is a relative path
	public static String generateFromTemplate(String srcFile, Map<String, String> variables) throws IOException {
		srcFile =  Utilities.getCodeGenRoot() + "/" + srcFile;
		
		List<String> template = Utilities.readFile(srcFile);
		
		//while(incompleteSubstitution(template)) {
			for(int i = 0; i < template.size(); ++i) {
				for(String v : variables.keySet()) {
					// variable references in template start with "$"
					String line = template.get(i);
					template.set(i, line.replace("$" + v, variables.get(v)));
				}
			}
		//}
		
		return StringUtils.join(template.toArray(), "\n");
	}
	
	public static boolean incompleteSubstitution(List<String> lines) {
		for(String line : lines) {
			if(hasVariable(line)) {	
				return true;
			}
		}
		return false;
	}
	
	public static boolean hasVariable(String src) {
		
		if(!src.contains("$")) {
			return false;
		}

		String[] splits = src.split("\\$");
		
		
		for(int i = 1; i < splits.length; ++i) {
			String s = splits[i];
			char c = s.charAt(0);
			if((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) { // if it is a character
				return true;
			}
		}
		return false;
	}
	
	public static String searchAndReplace(List<String> source, Map<String, String> variables) {
		String ret = new String();
		
		for(String line : source) {
			for(String v : variables.keySet()) {
				// variable references in template start with "$"
				line = line.replace(v, variables.get(v));
			}
			ret += line + "\n";
		}
		
		return ret;

	}
	

	public static String replaceTable(String sql, String beforeName, String afterName) throws JSQLParserException {
		Statement stmt = CCJSqlParserUtil.parse(sql);
		SelectDeParser deparser = new SelectDeParser() {

		    @Override
		    public void visit(Table tableName) {
		    	String tName = tableName.getFullyQualifiedName();
		    	if(tName.equals(beforeName))
		    		tName = afterName;
		    	
		        super.getBuffer().append(tName);
		        Pivot pivot = tableName.getPivot();
		        if (pivot != null) {
		            pivot.accept(this);
		        }
		        Alias alias = tableName.getAlias();
		        if (alias != null) {
		            super.getBuffer().append(alias);
		        }
		    }

		};
		
		StringBuilder b = new StringBuilder();
		deparser.setBuffer(b);
		
		stmt.accept((StatementVisitor) deparser); // adjusts outAttribute for winagg case

		return b.toString();
	}
	
	
	
	public static String timeIntervalsToSeconds(String src) throws Exception {
		String dst = src;
		
		while(dst.toUpperCase().contains(" INTERVAL ")) {
			int startInterval = dst.indexOf(" INTERVAL '") + 11; // " INTERVAL '", does not include '
			int endInterval = dst.indexOf("'", startInterval);
			String intervalText = dst.substring(startInterval, endInterval);
			long intervalSeconds = sqlIntervalToSeconds(intervalText);
			long intervalms = intervalSeconds * 1000; // timestamps reported in ms
			dst = dst.replaceAll("INTERVAL '" + intervalText + "'", Long.toString(intervalms));
			
		}
		
		return dst;

	}
	
	public static long sqlIntervalToSeconds(String interval) throws Exception {
		PGInterval pg = new PGInterval(interval);
		Double secs = pg.getSeconds();
		int minutes = pg.getMinutes();
		int days = pg.getDays();
		int years = pg.getYears();
		
		int months = pg.getMonths();
		if(months != 0) {
			throw new Exception("Months not yet implemented!");
		}
		
		long intervalSeconds = secs.longValue();
		intervalSeconds += minutes * 60;
		intervalSeconds += days * 24 * 60 * 60;
		intervalSeconds += years * 31557600;
		
		return intervalSeconds;
		
	}
	
	
	public static String getBitmask(List<SecureRelDataTypeField> attrs, SecureRelDataTypeField ref) {
		int startIdx = 0;
		boolean found = false;
		for(SecureRelDataTypeField r : attrs) {
			if(r.getName().equals(ref.getName()) 
					&& r.getType().equals(ref.getType())) {
				found = true;
				break;
			}
			else {
				startIdx += r.size();
			}
		}
		
		assert(found);
		
		int endIdx = startIdx + ref.size();
		
		String mask =  "$" + startIdx + "~" + endIdx + "$";
		return mask;
	}
	
		public static String getBitmask(SecureRelRecordType srcSchema, SecureRelDataTypeField r)  {
		
		return getBitmask(srcSchema.getSecureFieldList(), r);
	}
	
	
	public static boolean isSecureLeaf(Operator o) {
		
		if(o.getExecutionMode() == ExecutionMode.Plain) { // seq scans always plaintext, so this covered here
			return false;
		}
		
		List<Operator> sources = o.getSources();

		for(Operator s : sources) {
			if(s.getExecutionMode() == ExecutionMode.Plain) {
				return true;
			}
		}
		return false;
		
	}
	
	public static String writeFields(SecureRelRecordType srcSchema, String srcName, String dstName) throws Exception {
		
		String ret = new String();
		
		for(SecureRelDataTypeField field : srcSchema.getAttributes()) {
			String bitmask =  CodeGenUtils.getBitmask(srcSchema, field);
			ret += dstName + bitmask + " = " + srcName + bitmask + ";\n        ";
		}
			
		return ret;

	}
}
