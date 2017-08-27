package org.smcql.executor.config;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.smcql.config.SystemConfiguration;
import org.smcql.util.Utilities;

// read config and connect to psql instances
public class ConnectionManager {
	
	private Map<String, WorkerConfiguration> workers;
	private Map<Integer, WorkerConfiguration> workersById;
	private List<String> hosts;
	
	private static ConnectionManager instance = null;
	
	
	protected ConnectionManager() throws Exception {

		workers = new LinkedHashMap<String, WorkerConfiguration>();
		hosts = new ArrayList<String>();
		
		
		workersById = new HashMap<Integer, WorkerConfiguration>();
		initialize();
	}
	

	   public static ConnectionManager getInstance() throws Exception {
		      if(instance == null) {
		         instance = new ConnectionManager();
		      }
		   
		      return instance;
		   }

	   
	public void reinitialize() {
		instance = null;
	}
	
	
	private void initialize() throws Exception, SQLException {
		List<String> hosts = null;
		
		String connectionParameters = System.getProperty("smcql.connections.str");
		if(connectionParameters != null) {
			hosts = Arrays.asList(StringUtils.split(connectionParameters, '\n'));
		}
		
		else {
			String connectionsFile = SystemConfiguration.getInstance().getProperty("data-providers");		
			String configHosts = Utilities.getSMCQLRoot() + "/" + connectionsFile;

			 hosts = Utilities.readFile(configHosts);
		}
		
		for(String h : hosts) {
			parseConnection(h);
		}
		
	}
	
	
	private void parseConnection(String c) throws NumberFormatException, Exception, SQLException {
		if(c.startsWith("#")) { // comment in spec
			return;
		}
     

		WorkerConfiguration worker = new WorkerConfiguration(c);

		if(!hosts.contains(worker.hostname)) 
			hosts.add(worker.hostname);

		workers.put(worker.workerId,  worker);
		workersById.put(worker.dbId, worker);
	}

	
 	// list of all hostnames in smcql deployment
	public List<String> getHosts() {
		return hosts;
	}
	
	
	public List<WorkerConfiguration> getWorkerConfigurations() {
		return new ArrayList<WorkerConfiguration>(workers.values());
	}
	
	public Set<String> getDataSources() {
		return  workers.keySet();
	}
	
	public WorkerConfiguration getWorker(String workerId) {
		return workers.get(workerId);
	}
	
	public Connection getConnection(String workerId) throws SQLException, ClassNotFoundException {
		return workers.get(workerId).getDbConnection();
	}
	
	public Connection getConnectionById(int id) throws SQLException, ClassNotFoundException {
		return workersById.get(id).getDbConnection();
	}
	
		
	
	
	// get first connection
	public String getAlice() {
		List<String> keys = new ArrayList<String>(workers.keySet());
		return keys.get(0);
	}
	
	// get second connection
	public String getBob() {
		List<String> keys = new ArrayList<String>(workers.keySet());
		return keys.get(1);
		
	}

	
}

