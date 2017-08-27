package org.smcql.db.data.field;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.Timestamp;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.smcql.type.SecureRelDataTypeField;

public class FieldFactory implements Serializable {

	public static Field get(SecureRelDataTypeField s, ResultSet r, int resultIdx) throws Exception {

		   RelDataType type = s.getBaseField().getType();
		   SqlTypeName sqlType = type.getSqlTypeName();
		   if(SqlTypeName.CHAR_TYPES.contains(sqlType))
				return new CharField(s, r.getString(resultIdx));
			

		   if(sqlType == SqlTypeName.INTEGER || sqlType == SqlTypeName.BIGINT) 
				return new IntField(s, r.getInt(resultIdx));

		   
		   if(SqlTypeName.DATETIME_TYPES.contains(sqlType)) {
			    Timestamp t = r.getTimestamp(resultIdx);
			    return new TimestampField(s, r.getTimestamp(resultIdx));
		   }

		   if(SqlTypeName.BOOLEAN_TYPES.contains(sqlType))
			   return new BooleanField(s, r.getBoolean(resultIdx));
		   
		   throw new NullPointerException("Type " + sqlType + " is not defined!");		
	}

	public static Field get(SecureRelDataTypeField s) throws Exception {

		   RelDataType type = s.getBaseField().getType();
		   SqlTypeName sqlType = type.getSqlTypeName();
		   if(SqlTypeName.CHAR_TYPES.contains(sqlType))
				return new CharField(s);
			

		   if(sqlType == SqlTypeName.INTEGER || sqlType == SqlTypeName.BIGINT) 
				return new IntField(s);

		   
		   if(SqlTypeName.DATETIME_TYPES.contains(sqlType)) {
			    return new TimestampField(s);
		   }

		   throw new NullPointerException("Type " + sqlType + " is not defined!");
	}
	
	
	
}
