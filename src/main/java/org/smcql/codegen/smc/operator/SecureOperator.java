package org.smcql.codegen.smc.operator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.smcql.codegen.CodeGenerator;
import org.smcql.codegen.smc.DynamicCompiler;
import org.smcql.codegen.smc.operator.support.RexNodeUtilities;
import org.smcql.codegen.smc.operator.support.SortMethod;
import org.smcql.executor.config.RunConfig.ExecutionMode;
import org.smcql.executor.step.ExecutionStep;
import org.smcql.executor.step.PlaintextStep;
import org.smcql.plan.operator.Filter;
import org.smcql.plan.operator.Join;
import org.smcql.plan.operator.Operator;
import org.smcql.plan.operator.Project;
import org.smcql.plan.operator.Sort;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelRecordType;
import org.smcql.util.CodeGenUtils;
import org.smcql.util.Utilities;

public class SecureOperator implements CodeGenerator, Serializable {

	protected Operator planNode;
	protected String structName;
	protected String generatedCode = null;
	
	protected List<Filter> filters;
	protected List<Project> projects;
	protected List<ExecutionStep> merges;
	
	// not equal to Operator.children b/c of projects and filters getting folded into code gen for larger op
	protected List<SecureOperator> children;

	
	public SecureOperator(Operator o) throws Exception {
		planNode = o;		
		children = new ArrayList<SecureOperator>();
		
		for (Operator child : planNode.getChildren()) {
			SecureOperator secureChild = child.getSecureOperator();
			if (secureChild != null)
				children.add(secureChild);
		}
		
		planNode.setSecureOperator(this);
		filters = new ArrayList<Filter>();
		projects = new ArrayList<Project>();
		merges = new ArrayList<ExecutionStep>();
	}	
	
	

	public String generate() throws Exception {
		return null;
	}
	
	
	
	public SecureRelRecordType getSchema() {
		if(!projects.isEmpty())
			return projects.get(0).getSchema();
		if (!merges.isEmpty())
			return merges.get(0).getCodeGenerator().getSchema();
		
		return planNode.getSchema();
	}

	
	
	public Operator getPlanNode() {
		return planNode;
	}
	
	public void addProject(Project aProject) {
		aProject.setSecureOperator(this);
		projects.add(aProject);
	}
	
	
	public void addFilter(Filter aFilter) {
		aFilter.setSecureOperator(this);
		filters.add(aFilter);
	}
	
	public List<ExecutionStep> getMerges() {
		return merges;
	}

	public void setMerges(List<ExecutionStep> merges) {
		this.merges = merges;
	}
	
	protected Operator getCorrectChild(Operator child, int mergeIndex) {
	    while (child instanceof Project || child instanceof Filter) {
	    	child = child.getChild(0); 
	    	if (!merges.isEmpty() && merges.get(mergeIndex).getSourceOperator().isSplittable()) {
	    		ExecutionStep m = merges.get(mergeIndex);
	    		child = m.getChild(0).getSourceOperator();
	    	}
	    		
	    }
	    
	    return child;
	}
	
	private String rewriteForJoin(String condition, String srcName) {
		String result = condition;
		boolean right = false;
		while (true) {
			int i = result.indexOf(srcName);
			if (i < 0)
				break;
			
			String replacement = (right) ? "rTuple" : "lTuple";
			result = result.replaceFirst(srcName, replacement);
			
			right = !right;
		}
		return result;
	}
	
	private void handleFilters(Map<String, String> variables) {
		if (this.filters.isEmpty()) {
			variables.put("applyFilter", "ret = 1;");
		} else {
			String condition = "";
			String assigns = "";
			int index = 0;
			String srcName = "tuple";
			for (Filter f : this.filters) {
				String filterCond = RexNodeUtilities.flattenFilter(f, srcName, Integer.parseInt(variables.get("sSize")));
				condition += (index == 0) ? filterCond : " && " + filterCond;
				index++;
			}
			
			if (planNode instanceof Join)
				condition = rewriteForJoin(condition, srcName);
			
			String filterStr = assigns + "\n\tif (" + condition + ") {\n\t\tret = 1;\n\t}";
			variables.put("applyFilter", filterStr);
		}		
	}
	
	private void handleProjects(Map<String, String> variables, SecureRelRecordType childSchema) throws Exception {
		if(!projects.isEmpty()) {
			String projectVars = RexNodeUtilities.flattenProjection(projects.get(0), "srcTuple", "dstTuple", Integer.parseInt(variables.get("sSize")));
			variables.put("writeDst", projectVars);
		}
		else {
			String writeDstContent = CodeGenUtils.writeFields(childSchema, "srcTuple", "dstTuple");
			variables.put("writeDst", writeDstContent);
		}
	}
	
	public Map<String, String> baseVariables() throws Exception {
		// variables: size, sortKeySize signal, fid, bitmask
		Map<String, String> variables = new HashMap<String, String>();
	   
		// tuple size in bits
		String tupleSize = Integer.toString(planNode.getSchema(true).size());
		variables.put("size", tupleSize);
		
		// for ops with different schemas between input and output
		// overridden by Join
		Operator child = getCorrectChild(planNode.getChild(0), 0);
		SecureRelRecordType childSchema = (merges.isEmpty()) ? child.getSchema(true) : merges.get(0).getCodeGenerator().getSchema();
		String srcSize = Integer.toString(childSchema.size());
		String dstSize = (projects.isEmpty()) ? variables.get("size") : Integer.toString(projects.get(0).getSchema().size());
		
		variables.put("sSize", srcSize);
		variables.put("dSize", dstSize);
		
		handleFilters(variables);
		handleProjects(variables, childSchema);
		
		variables.put("packageName", planNode.getPackageName());
		
		if(!(planNode instanceof Sort) && !(planNode instanceof Join)) {
			if(child == null || children.isEmpty() || this.sharesComputeOrder(children.get(0))) {
				// no sort since it will either be handled by the SQL writer or by the child operator
				variables.put("sortInput", "");  // calls sort method
				variables.put("sortMethod", ""); // declares sort method
			}	
			else {
				SortMethod sm = null;
				if(planNode.isSplittable()) {
					sm = new SortMethod(planNode, planNode.secureComputeOrder());
				}
				else { 
					sm = new SortMethod(child, planNode.secureComputeOrder());
				}
				
				try {
					variables.put("sortInput", sm.sortInvocation("v", "aPrime"));
					variables.put("sortMethod", sm.sorter());
				} catch (Exception e) {
					variables.put("sortInput", "");
					variables.put("sortMethod", "");
				}
				
			
			}
		}
			
		
		return variables;

	}


	
	public boolean sharesComputeOrder(SecureOperator op) {
		List<SecureRelDataTypeField> opOrder = op.getPlanNode().secureComputeOrder();
		List<SecureRelDataTypeField> myOrder = planNode.secureComputeOrder();
		
		if(opOrder.equals(myOrder)) {
			return true;
		}
		return false;
		
	}
	
	




	@Override
	public String getPackageName() {
		return planNode.getPackageName();
	}

	@Override
	public SecureRelRecordType getInSchema() {
		return planNode.getInSchema();
	}



	
	@Override
	 public String destFilename(ExecutionMode e) {
		return planNode.destFilename(e);
	}

	@Override
	public void compileIt() throws Exception {
		String code = generate();
	
		if(code != null)
			DynamicCompiler.compileOblivLang(code, this.getPackageName());		
	}



	@Override
	public String generate(boolean asSecureLeaf) throws Exception {
		return this.generate();
	}



	@Override
	public SecureRelRecordType getSchema(boolean asSecureLeaf) {
		return getSchema();
	}







	

	

	


	
	
	
}
