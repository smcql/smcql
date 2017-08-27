package org.smcql.executor.step;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.smcql.codegen.CodeGenerator;
import org.smcql.config.SystemConfiguration;
import org.smcql.db.data.QueryTable;
import org.smcql.executor.config.RunConfig;
import org.smcql.executor.plaintext.SqlQueryExecutor;
import org.smcql.executor.smc.OperatorExecution;
import org.smcql.plan.operator.Filter;
import org.smcql.plan.operator.Operator;
import org.smcql.plan.operator.Project;
import org.smcql.type.SecureRelRecordType;

public class PlaintextStep implements ExecutionStep, Serializable {

	transient Operator srcOperator;
	
	RunConfig runConf;
	ExecutionStep parent;
	List<ExecutionStep> children;
	List<Integer> sites;
	boolean visited = false;
	String packageName = null;
	OperatorExecution exec;

	public PlaintextStep(Operator op, RunConfig run, ExecutionStep child) throws Exception {
		srcOperator = op;
		runConf = run;
		packageName = srcOperator.getPackageName();
		
		children = new ArrayList<ExecutionStep>();
		if(child != null)
			children.add(child);
		
		exec = new OperatorExecution(this);
		exec.setSourceSQL(generate());
	}
	
	public PlaintextStep(Operator op, RunConfig run, ExecutionStep lhsChild, ExecutionStep rhsChild) throws Exception {
		srcOperator = op;
		runConf = run;
		packageName = srcOperator.getPackageName();
		
		children = new ArrayList<ExecutionStep>();
		if(lhsChild != null)
			children.add(lhsChild);
		
		if(rhsChild != null)
			children.add(rhsChild);
		
		exec.setSourceSQL(generate());
	}
	
	public void addChild(ExecutionStep child) {
		children.add(child);
	}
	
	
	@Override
	public String generate() throws Exception {
		return srcOperator.getPlainOperator().generate();
	}

	@Override
	public String getPackageName() {
		return srcOperator.getPackageName();
	}

	public void setParent(ExecutionStep e) {
		parent = e;
	}
	
	@Override
	public OperatorExecution getExec() {
		return exec;
	}
	
	@Override
	public ExecutionStep getParent() {
		return parent;
	}

	@Override
	public ExecutionStep getChild(int idx) {
		if(children.isEmpty() || (children.size() -1  < idx))
			return null;
		
		return children.get(idx);
	}

	@Override
	public SecureRelRecordType getInSchema() {
		return srcOperator.getInSchema();
	}

	@Override
	public SecureRelRecordType getSchema() {
		return srcOperator.getSchema();
	}

	@Override
	public RunConfig getRunConfig() {
		return runConf;
	}

	@Override
	public Operator getSourceOperator() {
		return srcOperator;
	}


	public QueryTable execute(String db) throws Exception {
		String sql = this.generate();
		
		
		SqlQueryExecutor qe = new SqlQueryExecutor();
		return qe.plainQuery(srcOperator.getSchema(), sql);
	}
	
	

	@Override
	public CodeGenerator getCodeGenerator() {
		return srcOperator;
	}

	@Override
	public List<ExecutionStep> getChildren() {
		return children;
	}
	
	public List<Integer> getSites() {
		return sites;
	}
	
	public void addSites(List<Integer> list) {
		for (Integer i : list) {
			if (!sites.contains(i))
				sites.add(i);
		}
	}

	@Override
	public boolean visited() {
		return visited;
	}

	@Override
	public void visit() {
		 visited = true;
	}

	@Override
	public void setHostname(String host) {
		runConf.host = host;
		
	}

	@Override
	public SecureRelRecordType getSchema(boolean forSecureLeaf) {
		// schema will be a function of whether secure leaf has an AVG aggregate
		return srcOperator.getSchema();
	}

	@Override
	public String printTree() {
		return appendOperator(this, new String(), "");
	}
	
	private String appendOperator(ExecutionStep step, String src, String linePrefix) {
		src += linePrefix + step.getSourceOperator() + "\n";
		linePrefix += "    ";
		for(ExecutionStep child : step.getChildren()) {
			src = appendOperator(child, src, linePrefix);
		}
		return src;
	}
}