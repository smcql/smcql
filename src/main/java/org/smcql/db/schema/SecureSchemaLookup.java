package org.smcql.db.schema;

import org.smcql.config.SystemConfiguration;
import org.smcql.executor.config.WorkerConfiguration;
import org.smcql.type.SecureRelDataTypeField.SecurityPolicy;
import org.smcql.util.Utilities;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

// decorator for calcite schema
// adds management of security policy for attributes
public class SecureSchemaLookup {

	
    Map<String, Map<String, SecurityPolicy>> accessPolicies;
    
    static SecureSchemaLookup instance;
    
    protected SecureSchemaLookup(WorkerConfiguration config) throws ClassNotFoundException, SQLException {
		accessPolicies = new HashMap<String, Map<String, SecurityPolicy>>();
		String publicQuery = "SELECT table_name, column_name FROM information_schema.column_privileges WHERE grantee='public_attribute'";
		String protectedQuery = "SELECT table_name, column_name FROM information_schema.column_privileges WHERE grantee='protected_attribute'";

		Connection dbConnection = config.getDbConnection();

		initializeSecurityPolicy(publicQuery, dbConnection, SecurityPolicy.Public);
		initializeSecurityPolicy(protectedQuery, dbConnection, SecurityPolicy.Protected);
		
	}
	
    public static SecureSchemaLookup getInstance() throws Exception {
    	if(instance == null) {
    		SystemConfiguration conf = SystemConfiguration.getInstance();
    		instance = new SecureSchemaLookup(conf.getHonestBrokerConfig());
    	}
    	return instance;
    }
	
	void initializeSecurityPolicy(String sql, Connection c, SecurityPolicy policy) throws SQLException {
		Statement st = c.createStatement();
		ResultSet rs = st.executeQuery(sql);
		
		while (rs.next()) {
			String table = rs.getString(1);
			String attr = rs.getString(2);
			if(accessPolicies.containsKey(table)) {
				accessPolicies.get(table).put(attr, policy);
			}
			else {
				Map<String, SecurityPolicy> tableEntries = new HashMap<String, SecurityPolicy>();
				tableEntries.put(attr, policy);
				accessPolicies.put(table, tableEntries);
			}
		}

	}
	
	
	public Map<String, SecurityPolicy> tablePolicies(String tableName) {
		return accessPolicies.get(tableName);
	}
	
	public SecurityPolicy getPolicy(String table, String attr) {
		if(!accessPolicies.containsKey(table) || !accessPolicies.get(table).containsKey(attr))
			return SecurityPolicy.Private;
		return accessPolicies.get(table).get(attr);
	}
}
