package org.smcql.type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import net.sf.jsqlparser.statement.create.table.ColDataType;

// singleton for converting from sql types to smc types and vice versa
// file format: sql name, smc name, sizeof(entry)
public class TypeMap {
	   private static TypeMap instance = null;
	   private final Map<String, String> sql2smc = new HashMap<String, String>();
	   private final Map<String, String> smc2sql = new HashMap<String, String>();
	   private final Map<String, Integer> sqlSize = new HashMap<String, Integer>();
	   
	   protected TypeMap()  {
		   // hardcoded since types rarely change
		   String[] typeSpecs = {"integer,int,64",
		                       "boolean,int,1",
				   			   "varchar,int,8",
		                       "timestamp,int,64"};
		   
			for(String line : typeSpecs) {

		    	String[] types = line.split(",");
			    sql2smc.put(types[0], types[1]);
			    smc2sql.put(types[1], types[0]);
			    int size = Integer.parseInt(types[2]);
			    sqlSize.put(types[0], size);

		    }
		    
	   }
	   
	   public static TypeMap getInstance() {
	      if(instance == null) {
	         instance = new TypeMap();
	      }
	   
	      return instance;
	   }
	   
	   
	   public String toSMC(String sql) {
		    return sql2smc.get(sql);
	   }
	   
	   public String toSQL(String smc) {
		   return smc2sql.get(smc);
	   }
	   
	   
	   public int getSize(List<SecureRelDataTypeField> attrs) {
		   int summedSize = 0;
		   for(SecureRelDataTypeField r : attrs) {
			   summedSize += this.sizeof(r);
		   }
		   
		  return summedSize;
	   }
	   
	   public int sizeof(SecureRelDataTypeField attribute) {
		   RelDataType type = attribute.getBaseField().getType();
		   SqlTypeName sqlType = type.getSqlTypeName();
		   if(SqlTypeName.CHAR_TYPES.contains(sqlType)) {
			   int precision = (type.getPrecision() == 2147483647) ? 32 : type.getPrecision();
			   return precision * sqlSize.get("varchar");
		   }

		   if(sqlType == SqlTypeName.INTEGER) 
			   return sqlSize.get("integer");
		   
		   if(sqlType == SqlTypeName.BIGINT)
			   return sqlSize.get("integer");
		   
		   if(SqlTypeName.DATETIME_TYPES.contains(sqlType))
			   return sqlSize.get("timestamp");

		   if(sqlType == SqlTypeName.BOOLEAN) {
			   return sqlSize.get("boolean");
		   }
		   // all others not yet implemented
		   return 0;
	   }
	   

	}
