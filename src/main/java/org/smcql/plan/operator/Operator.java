package org.smcql.plan.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.smcql.codegen.CodeGenerator;
import org.smcql.codegen.plaintext.PlainOperator;
import org.smcql.codegen.smc.operator.SecureOperator;
import org.smcql.config.SystemConfiguration;
import org.smcql.executor.config.RunConfig.ExecutionMode;
import org.smcql.plan.SecureRelNode;
import org.smcql.plan.ShadowRelNode;
import org.smcql.plan.slice.SliceKeyDefinition;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelDataTypeField.SecurityPolicy;
import org.smcql.type.SecureRelRecordType;
import org.smcql.util.RelNodeMatcher;
import org.smcql.util.Utilities;


// for execution mode planning over a RelNode
public abstract class Operator implements CodeGenerator {
	
	protected SecureRelNode baseRelNode;
	protected ShadowRelNode unoptimizedRelNode = null;
	private SecureOperator secureOperator;
	protected PlainOperator plaintextGenerator;
	protected List<Operator> children;
	
	protected Operator parent;
	
	SliceKeyDefinition sliceKey;
	
	Logger logger;
	
	String operatorId;
	
	protected boolean sliceAgnostic = false;
	
	protected boolean splittable = false;
	
	protected boolean blocking = false;
	
	ExecutionMode  executionMode = ExecutionMode.Secure;
	String queryName;
	
	
	public Operator(String name, SecureRelNode src, Operator... childOps) throws Exception {
		baseRelNode = src;
		src.setPhysicalNode(this);
		children = new ArrayList<Operator>();
		operatorId = SystemConfiguration.getInstance().getOperatorId();
		
		for(Operator op : childOps) {
			children.add(op);
			op.setParent(this);
		}
		
		
		logger = SystemConfiguration.getInstance().getLogger();
		queryName = name.replaceAll("-", "_");
		plaintextGenerator = new PlainOperator(this);
	}
	 
	public ShadowRelNode getShadowRelNode() { 
		return unoptimizedRelNode;
	}
	
	public void inferExecutionMode() {
		for(Operator op : children) {
			op.inferExecutionMode();
			
		}
		
		ExecutionMode maxChild = maxChildMode(); 
		SecurityPolicy maxAccess = maxAccessLevel(); // most sensitive thing it computes on
		List<SecureRelDataTypeField> sliceAttrs = getSliceAttributes();
		
		String msg = "For " + baseRelNode.getRelNode().getRelTypeName() + " have max child " + maxChild + " and max access " + maxAccess + " slice key " + sliceAttrs;
		logger.log(Level.FINE, msg);

		if(maxChild.compareTo(ExecutionMode.Plain) <= 0 && maxAccess == SecurityPolicy.Public) {
			executionMode = ExecutionMode.Plain;
			return;
		}


		
		
		
		if(maxChild.compareTo(ExecutionMode.Plain) <= 0 & !sliceAttrs.isEmpty()) {
			executionMode = ExecutionMode.Slice;
			sliceKey = new SliceKeyDefinition(sliceAttrs);
			sliceKey.addFilters(sliceAttrs.get(0).getFilters(), this.getSchema());
			return;
		}
		
		
		
		if(maxChild == ExecutionMode.Slice) {
			boolean sliceAble = true;
			for(Operator op : children) {
				if(!SliceKeyDefinition.sliceCompatible(op, this)) {
					sliceAble = false;
				}
			
			}
		
			if(sliceAble) {
				executionMode = ExecutionMode.Slice;
				sliceKey = new SliceKeyDefinition(this);
				//For multiple filters
				for (int i=0; i< this.getChildren().size(); i++) {
					sliceKey.mergeFilters(this.getChild(i).getSliceKey().getFilters());
				}
				return;
			}
		}
		
		// secure mode default
		
		
	}
	
		
	public void addChild(Operator op) {
		children.add(op);
		op.setParent(this);
	}
	
	public void addChildren(List<Operator> ops) {
		children.addAll(ops);	
		for(Operator op : ops)
			op.setParent(this);
	}
	
	public void setParent(Operator op) {
		parent = op;
	}

	public SecureRelNode getSecureRelNode() {
		return baseRelNode;
	}
	
	public List<Operator> getChildren() {
		return children;
	}
	
	

	// what fields does this operator reveal information on?
	// some, like SeqScan and Project reveal nothing in their output based on the contents of the tuples they process
	// thus they "computeOn" nothing
	public List<SecureRelDataTypeField> computesOn() {
		return new ArrayList<SecureRelDataTypeField>();
	}

	protected SecurityPolicy maxAccessLevel() {
		List<SecureRelDataTypeField> accessed = computesOn();
		SecurityPolicy policy = SecurityPolicy.Public;
		
		for(SecureRelDataTypeField field : accessed) {
			if(field.getSecurityPolicy().compareTo(policy) >  0)
				policy = field.getSecurityPolicy();
		}
		
		return policy;
	}
	
	
	protected ExecutionMode maxChildMode() {
		ExecutionMode maxMode = children.get(0).executionMode;
		
		//join
		if(children.size() == 2)
			if(children.get(1).executionMode.compareTo(maxMode) > 0)
				maxMode = children.get(1).executionMode;

		return maxMode;
	}

	// schema of output 
	public SecureRelRecordType getSchema() {
		return baseRelNode.getSchema();
	}

	// schema of output 
	public SecureRelRecordType getSchema(boolean asSecureLeaf) {
		return baseRelNode.getSchema();
	}

	
	// for all but SeqScan and join
	public SecureRelRecordType getInSchema() {
		return children.get(0).getSchema();
	}
	

	// for all except sort
	public int getLimit() {
		return  -1;
		
	}
	
	public List<SecureRelDataTypeField> getSliceAttributes() {
		return new ArrayList<SecureRelDataTypeField>();
	}
	
	boolean sliceAble() {
		if(this.sliceAgnostic == true && this.maxChildMode() != ExecutionMode.Secure)
			return true;
		return false;
	}
	
	
	
	public String toString() {
		
		String ret = baseRelNode.getRelNode().getRelTypeName() + "-" + executionMode + ", schema:" + getSchema();
		List<SecureRelDataTypeField> sliceAttrs = this.getSliceAttributes();
		if(!sliceAttrs.isEmpty())
			ret += ", slice key: " + sliceAttrs;

		return ret;
	}
	
	// some operators have a slice key equal to that of their children
	// only covers attrs in key that appear in op's schema
	// examples: distinct, project
	protected List<SecureRelDataTypeField> derivedSliceKey() {
		
		assert(this.sliceAgnostic == true);
		List<SecureRelDataTypeField> baseKey;
		
		// if has a sliced child
		if(children.get(0).getExecutionMode() == ExecutionMode.Slice) {
			baseKey = children.get(0).getSliceAttributes();
		}
		else { 
			Operator parentPtr = parent;
			while(parentPtr != null && parentPtr.sliceAgnostic) {
				parentPtr = parentPtr.parent;
			}

			// no well-defined slice keys
			if(parentPtr == null || parentPtr.sliceAgnostic)
				return new ArrayList<SecureRelDataTypeField>();
			
			baseKey = parentPtr.getSliceAttributes();
			
		}
		
		List<SecureRelDataTypeField> derivedKey = new ArrayList<SecureRelDataTypeField>();
		SecureRelRecordType schema = getSchema();
			
		for(SecureRelDataTypeField f : baseKey) {
			for(SecureRelDataTypeField l : schema.getSecureFieldList()) {
				if(l.equals(f)) {
					derivedKey.add(l);
					break;
				}
			}

		}

		
		return derivedKey;
	}



	public ExecutionMode getExecutionMode() {
		return executionMode;
	}



	public Operator getChild(int i) {
		return children.get(i);
	}


	public String getOpName() {
		String fullName = this.getClass().toString();
		int startIdx = fullName.lastIndexOf('.') + 1;
		return fullName.substring(startIdx);
	}

	public String getPackageName() {
		String opName = getOpName();

        return "org.smcql.generated." + queryName + "." + opName + operatorId;
	}


	
	public List<Operator> getSources() {
		return children;
	}


	// if a SMC operator implementation leverages the order of tuples for comparisons, codify that in this function
	// otherwise empty set = order agnostic
	// order is implicitly ascending
	public List<SecureRelDataTypeField> secureComputeOrder() {
		return new ArrayList<SecureRelDataTypeField>();
	}



	public String getOperatorId() {
		return operatorId;
	}



	public boolean isSplittable() {
		return splittable;
	}



	public String getQueryId() {
		return queryName;
	}



	public String destFilename(ExecutionMode e) {
        if(e == ExecutionMode.Plain) {
        	return Utilities.getCodeGenTarget() + "/" + getQueryId() +  "/sql/" + getOperatorId() + "_" + getOpName() + ".sql";
        }

        return Utilities.getCodeGenTarget() + "/" + getQueryId() +  "/smc/" + getOperatorId() + "_" + getOpName() + ".lcc";
	}


	@Override
	public String generate() throws Exception {
		return plaintextGenerator.generate();
	}

	
	@Override
	public String generate(boolean asSecureLeaf) throws Exception {
		return null;
	}

	public void compileIt() throws Exception {
		// nothing to do
	}


	
	public boolean sharesComputeOrder(Operator o) {
		return secureComputeOrder().equals(o.secureComputeOrder());
	}



	public boolean isBlocking() {
		return blocking;
	}



	public Operator getParent() {
		return parent;
	}



	public boolean sharesExecutionProperties(Operator op) {
		if(executionMode != op.getExecutionMode())
			return false;
		
		if(executionMode == ExecutionMode.Slice && op.getExecutionMode() == this.executionMode) {
			if(!SliceKeyDefinition.sliceCompatible(this, op)) {
				return false;
			}
			
		}
		return true;
	}



	public PlainOperator getPlainOperator() {
		return plaintextGenerator;
	}



	public SliceKeyDefinition getSliceKey() {
		return (this.sliceKey == null) ? new SliceKeyDefinition(this) : sliceKey;
	}
	
	public void addSqlGenerationNode(RelNode project) {
		if (project == null)
			return;
		
		RelNode next = project;
		RelNodeMatcher matcher = new RelNodeMatcher();
		if (matcher.matches(project, baseRelNode.getRelNode())) {
			unoptimizedRelNode = new ShadowRelNode(project);
			next = (project.getInputs().isEmpty()) ? null : project.getInput(0);
		}
		
		if (!children.isEmpty())
			children.get(0).addSqlGenerationNode(next);
	}

	public SecureOperator getSecureOperator() {
		return secureOperator;
	}

	public void setSecureOperator(SecureOperator secureOperator) {
		this.secureOperator = secureOperator;
	}
	
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof Operator))
			return false;
		return this.toString().equals(o.toString());
	}
	
	//returns first non-Project, non-Filter child Op
	public Operator getNextValidChild() {
		Operator child = children.get(0);
		while (child instanceof Project || child instanceof Filter) {
			child = child.getChild(0);
		}
		return child;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 31).append(this.toString()).toHashCode();
	}
}
