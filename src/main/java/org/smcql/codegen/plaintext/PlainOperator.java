package org.smcql.codegen.plaintext;

import java.util.ArrayList;
import java.util.List;

import org.smcql.codegen.CodeGenerator;
import org.smcql.codegen.sql.SqlGenerator;
import org.smcql.config.SystemConfiguration;
import org.smcql.db.data.Tuple;
import org.smcql.executor.config.RunConfig.ExecutionMode;
import org.smcql.plan.execution.slice.statistics.SliceStatistics;
import org.smcql.plan.execution.slice.statistics.StatisticsCollector;
import org.smcql.plan.operator.Filter;
import org.smcql.plan.operator.Operator;
import org.smcql.plan.operator.Project;
import org.smcql.plan.operator.SeqScan;
import org.smcql.plan.slice.SliceKeyDefinition;
import org.smcql.type.SecureRelRecordType;

// thin layer on top of Operator for SQL generation
public class PlainOperator implements CodeGenerator {

	Operator planNode;
	List<PlainOperator> children = null;
	private List<Tuple> sliceValues = null;
	private List<Tuple> complementValues = null;
	
	public PlainOperator(Operator src, Operator ...childNodes ) throws Exception {
		planNode = src;
		children = getChildren();
	}
	
	protected List<PlainOperator> getChildren() {
		if (children != null)
			return children;
		
		List<PlainOperator> children = new ArrayList<PlainOperator>();

		for(Operator o : planNode.getChildren()) {
			if(o.getPlainOperator() != null)
				children.add(o.getPlainOperator());
		}
		
		return children;
	}
	
	private List<PlainOperator> getSources() {
		List<PlainOperator> sources = new ArrayList<PlainOperator>();

		for(Operator o : planNode.getSources()) {
			if(o.getPlainOperator() != null)
				sources.add(o.getPlainOperator());
		}
		
		return sources;
	}
	
	@Override
	public String generate() throws Exception {
		Operator parent = planNode.getParent();
		Operator sqlOp = planNode;
		if (sqlOp instanceof SeqScan) {
			while (parent instanceof Filter || parent instanceof Project) {
				sqlOp = parent;
				parent = parent.getParent();
			}
		}
		
		return SqlGenerator.getSourceSql(sqlOp, SystemConfiguration.DIALECT);		
	}

	@Override
	public String getPackageName() {
		return null;
	}

	@Override
	public SecureRelRecordType getInSchema() {
		return null;
	}

	@Override
	public SecureRelRecordType getSchema() {
		return null;
	}

	@Override
	public String destFilename(ExecutionMode e) {
		return null;
	}

	@Override
	public void compileIt() throws Exception {		
	}

	@Override
	public SecureRelRecordType getSchema(boolean asSecureLeaf) {
		return planNode.getSchema(asSecureLeaf);
	}

	@Override
	public String generate(boolean asSecureLeaf) throws Exception {
		return null;
	}
	
	
	private String getComplementPredicate() {
		String complementPredicate = "(";
		for (int i=0; i < complementValues.size(); i++) {
			Tuple t = complementValues.get(i);
			String val = t.getField(0).toString();
			
			if (i > 0) 
				complementPredicate += ", "; 
			
			complementPredicate += val;
		}
		complementPredicate += ")";
		
		return (complementPredicate.equals("()")) ? null : complementPredicate;
	}
	//inserts complement values into the query
	public String generatePlaintextForSliceComplement(String userQuery) throws Exception {
		List<String> predicates = planNode.getSliceKey().getFilters();
		String complementPredicate = getComplementPredicate();
		String result = userQuery;
		
		if (complementPredicate == null)
			return null;
		
		String complementString = "";
		int[] indices = {2, 5};
		for (int i=0; i<predicates.size(); i++) {
			String table = predicates.get(i).split("\\.")[0];
			String from = (String) userQuery.subSequence(userQuery.indexOf("FROM"), userQuery.indexOf("WHERE"));
			String varName = planNode.getSliceKey().getAttributes().get(0).getName();
			
			if (from.split(" ").length > 2) 
				table = from.split(" ")[indices[i]];
			
			if (i > 0)
				complementString += " AND ";
			
			complementString += table + "." + varName + " IN " + complementPredicate;
		}
		result = result.replaceFirst("WHERE ", "WHERE " + complementString + " AND ");
		
		if (result.indexOf("COUNT(") > 0)
			result = result.replace("COUNT(", "").replaceFirst("\\)", "");
			
		return result;
	}
	
	private void inferSlicePredicate(SliceStatistics stats) throws Exception {
		sliceValues = new ArrayList<Tuple>(stats.getDistributedValues().keySet());
		complementValues = new ArrayList<Tuple>(stats.getSingleSiteValues().keySet());
		
		for(PlainOperator o : this.getSources()) 
			o.inferSlicePredicate(stats);
	}
	
	public void inferSlicePredicates(SliceKeyDefinition def) throws Exception {
		if(planNode.getExecutionMode() == ExecutionMode.Slice) {
			SliceStatistics stats = StatisticsCollector.collect(def);
			sliceValues = new ArrayList<Tuple>(stats.getDistributedValues().keySet());
			complementValues = new ArrayList<Tuple>(stats.getSingleSiteValues().keySet());
			
			for(PlainOperator src : getSources()) {
				src.inferSlicePredicate(stats);
			}
		} else {
			for(PlainOperator src : getSources()) {
				src.inferSlicePredicates(def);
			}
		}
        
    }

	public List<Tuple> getComplementValues() {
		return complementValues;
	}
	
	public List<Tuple> getSliceValues() {
		return sliceValues;
	}
    
}
    
    
