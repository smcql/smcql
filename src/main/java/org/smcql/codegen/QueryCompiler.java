
package org.smcql.codegen;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.smcql.codegen.plaintext.PlainOperator;
import org.smcql.codegen.smc.operator.SecureOperator;
import org.smcql.codegen.smc.operator.SecureOperatorFactory;
import org.smcql.codegen.smc.operator.support.MergeMethod;
import org.smcql.config.SystemConfiguration;
import org.smcql.executor.config.ConnectionManager;
import org.smcql.executor.config.RunConfig;
import org.smcql.executor.config.RunConfig.ExecutionMode;
import org.smcql.executor.smc.BasicSecureQueryTable;
import org.smcql.executor.smc.ExecutionSegment;
import org.smcql.executor.smc.OperatorExecution;
import org.smcql.executor.smc.SecureBufferPool;
import org.smcql.executor.step.ExecutionStep;
import org.smcql.executor.step.PlaintextStep;
import org.smcql.executor.step.SecureStep;
import org.smcql.plan.SecureRelRoot;
import org.smcql.plan.operator.Aggregate;
import org.smcql.plan.operator.CommonTableExpressionScan;
import org.smcql.plan.operator.Filter;
import org.smcql.plan.operator.Join;
import org.smcql.plan.operator.Operator;
import org.smcql.plan.operator.Project;
import org.smcql.plan.operator.Sort;
import org.smcql.plan.operator.WindowAggregate;
import org.smcql.type.SecureRelDataTypeField;
import org.smcql.type.SecureRelRecordType;
import org.smcql.util.ClassPathUpdater;
import org.smcql.util.CodeGenUtils;
import org.smcql.util.Utilities;

import com.oblivm.backend.flexsc.Mode;



public class QueryCompiler {
	

	Map<ExecutionStep, String> sqlCode;
	Map<ExecutionStep, String> smcCode;
	Map<Operator,ExecutionStep> allSteps;
	
	String queryId;
	
	List<String> smcFiles;
	List<String> sqlFiles;
	List<ExecutionSegment> executionSegments;
	
	String userQuery = null;

	SecureRelRoot queryPlan;
	
	ExecutionStep compiledRoot;
	
	Mode mode = Mode.REAL;
	String generatedClasspath = null;
	
	public QueryCompiler(SecureRelRoot q) throws Exception {
	
		queryPlan = q;
		smcFiles = new ArrayList<String>();
		sqlFiles = new ArrayList<String>();
		sqlCode = new HashMap<ExecutionStep, String>();
		smcCode = new HashMap<ExecutionStep, String>();
		executionSegments = new ArrayList<ExecutionSegment>();

		allSteps = new HashMap<Operator, ExecutionStep>();
	
		queryId = q.getName();
		Operator root = q.getPlanRoot();
		
		// set up space for .class files
		generatedClasspath = Utilities.getSMCQLRoot()+ "/bin/org/smcql/generated/" + queryId;
		Utilities.mkdir(generatedClasspath);
		Utilities.cleanDir(generatedClasspath);
		
		
		// single plaintext executionstep if no secure computation detected
		if(root.getExecutionMode() == ExecutionMode.Plain) {
			compiledRoot = generatePlaintextStep(root);
			ExecutionSegment segment = createSegment(compiledRoot);
			executionSegments.add(segment);
			compiledRoot.getExec().parentSegment = segment;
		}
		else {  // recurse
			compiledRoot = addOperator(root, new ArrayList<Operator>());
		}

		inferExecutionSegment(compiledRoot);
	}
	
	public QueryCompiler(SecureRelRoot q, String sql) throws Exception {
		
		queryPlan = q;
		smcFiles = new ArrayList<String>();
		sqlFiles = new ArrayList<String>();
		sqlCode = new HashMap<ExecutionStep, String>();
		smcCode = new HashMap<ExecutionStep, String>();
		executionSegments = new ArrayList<ExecutionSegment>();
		userQuery = sql;

		allSteps = new HashMap<Operator, ExecutionStep>();
	
		queryId = q.getName();
		Operator root = q.getPlanRoot();
		
		// set up space for .class files
		generatedClasspath = Utilities.getSMCQLRoot()+ "/bin/org/smcql/generated/" + queryId;
		Utilities.mkdir(generatedClasspath);
		Utilities.cleanDir(generatedClasspath);
		
		
		// single plaintext executionstep if no secure computation detected
		if(root.getExecutionMode() == ExecutionMode.Plain) {
			compiledRoot = generatePlaintextStep(root);
			ExecutionSegment segment = createSegment(compiledRoot);
			executionSegments.add(segment);
			compiledRoot.getExec().parentSegment = segment;
		}
		else {  // recurse
			compiledRoot = addOperator(root, new ArrayList<Operator>());
		}

		inferExecutionSegment(compiledRoot);
	}
	
	public QueryCompiler(SecureRelRoot q, Mode m) throws Exception {
		
		queryPlan = q;
		mode = m;
		smcFiles = new ArrayList<String>();
		sqlFiles = new ArrayList<String>();
		sqlCode = new HashMap<ExecutionStep, String>();
		smcCode = new HashMap<ExecutionStep, String>();
		executionSegments = new ArrayList<ExecutionSegment>();

		allSteps = new HashMap<Operator, ExecutionStep>();
	
		queryId = q.getName();
		Operator root = q.getPlanRoot();
		
		// single plaintext executionstep if no secure computation detected
		if(root.getExecutionMode() == ExecutionMode.Plain) {
			compiledRoot = generatePlaintextStep(root);
			ExecutionSegment segment = createSegment(compiledRoot);
			executionSegments.add(segment);
		}
		else {  // recurse
			compiledRoot = addOperator(root, new ArrayList<Operator>());
		}
		
		inferExecutionSegment(compiledRoot);
	}
	
	
	public List<ExecutionSegment> getSegments() {
		return executionSegments;
	}
	
	public void writeToDisk() throws Exception {
		
		String targetPath = Utilities.getCodeGenTarget() + "/" + queryId;
		Utilities.cleanDir(targetPath);


		Utilities.mkdir(targetPath + "/sql");

		Utilities.mkdir(targetPath + "/smc");
		
		
		for(Entry<ExecutionStep, String> e : sqlCode.entrySet()) {
			CodeGenerator cg = e.getKey().getCodeGenerator();
			String targetFile = cg.destFilename(ExecutionMode.Plain);
			sqlFiles.add(targetFile);
			Utilities.writeFile(targetFile, e.getValue());
		}
		
		for(Entry<ExecutionStep, String> e : smcCode.entrySet()) {
			CodeGenerator cg = e.getKey().getCodeGenerator();
			String targetFile = cg.destFilename(ExecutionMode.Secure);
			smcFiles.add(targetFile);
			if(e.getValue() != null) // no ctes
				Utilities.writeFile(targetFile, e.getValue());
		}

	}	
	
	public List<String> getClasses() throws IOException, InterruptedException {
	
		File path = new File(Utilities.getCodeGenTarget() + "/org/smcql/generated/" +queryId);
		String[] extensions = new String[1];
		extensions[0] = "class";
		Collection<File> files = FileUtils.listFiles(path, extensions, true);
		List<String> filenames = new ArrayList<String>();
		
		for(File f : files) {
			filenames.add(f.toString());
		}
		return filenames;
	}
	
	public void loadClasses() throws IOException, InterruptedException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		List<String> classFiles = getClasses();
		for(String classFile : classFiles) {
			ClassPathUpdater.add(classFile);
		}
	}
	
		
	public ExecutionStep getRoot() {
		return compiledRoot;
	}

	
	private ExecutionStep addOperator(Operator o, List<Operator> opsToCombine) throws Exception {

		if(o instanceof CommonTableExpressionScan) {
			Operator child = o.getSources().get(0);
			SecureBufferPool.getInstance().addPointer(o.getPackageName(), child.getPackageName());
		} 
	
		if(allSteps.containsKey(o)) {
			return allSteps.get(o);
		}

		if(o.getExecutionMode() == ExecutionMode.Plain) { // child of a secure leaf
			return generatePlaintextStep(o);
		}
		
		// secure case
		List<ExecutionStep> merges = new ArrayList<ExecutionStep>();
		List<ExecutionStep> localChildren = new ArrayList<ExecutionStep>();
		for(Operator child : o.getSources()) {
			List<Operator> nextToCombine = new ArrayList<Operator>();
			while (child instanceof Filter || child instanceof Project) {
				if (child instanceof Filter) {
					opsToCombine.add(child);
				} else {
					nextToCombine.add(child);
				}
				child = child.getChild(0);
			}
			
			if(child.getExecutionMode() != o.getExecutionMode()) { // secure leaf 
				ExecutionStep childSource = null;
				Operator tmp = o;
				if (child.getExecutionMode() == ExecutionMode.Plain) {
					Operator plain = (o.isSplittable() && !(o instanceof WindowAggregate)) ? o : child;
					childSource = generatePlaintextStep(plain);
				} else {
					childSource = addOperator(child, nextToCombine);
				}
				ExecutionStep mergeStep = addMerge(tmp, childSource);
				childSource.setParent(mergeStep);
				localChildren.add(mergeStep);
				merges.add(mergeStep);
			} else {
				ExecutionStep e = addOperator(child, nextToCombine);
				localChildren.add(e);
			}
				
		}  // end iterating over children

		ExecutionStep secStep = null;
		if(o instanceof Sort) {
			Operator sortChild = o.getChild(0);
			if(CodeGenUtils.isSecureLeaf(o) || sortChild.sharesComputeOrder(o)) { // implement splittable join
				secStep = localChildren.get(0); 
			}
		}

		if(secStep == null) {
			secStep = generateSecureStep(o, localChildren, opsToCombine, merges);
		}
		
		return secStep;
	}
	
	
	//adds the given step to the global collections and adds execution information to the step
	private void processStep(PlaintextStep step) throws Exception {
		Operator op = step.getSourceOperator();
		
		String sql = op.generate();
		allSteps.put(op, step);
		sqlCode.put(step, sql);
	}
	
	//creates the PlaintextStep plan from the given operator and parent step
	private PlaintextStep generatePlaintextStep(Operator op, PlaintextStep prevStep) throws Exception {
		RunConfig pRunConf = new RunConfig();
		pRunConf.port = 54321; // does not matter for plaintext
		pRunConf.smcMode = mode;
				
		if (prevStep == null) {
			PlaintextStep result = new PlaintextStep(op, pRunConf, null); 
			processStep(result);
			return result;
		}
		
		PlaintextStep curStep = prevStep;
		if (op.isBlocking() || op.getParent() == null || prevStep == null) {
			curStep = new PlaintextStep(op, pRunConf, null);
			curStep.setParent(prevStep);
			if (prevStep != null)
				prevStep.addChild(curStep);
			processStep(curStep);
		}	
		
		for (Operator child : op.getChildren()) {	
			PlaintextStep nextStep = generatePlaintextStep(child, curStep);		
			if (nextStep != null) {
				curStep.addChild(nextStep);
				nextStep.setParent(curStep);
			}
		}
		
		return  curStep;
	}
	
	private ExecutionStep generatePlaintextStep(Operator op) throws Exception {
		return generatePlaintextStep(op, null);
	}
	
	// join not yet implemented for split execution
	// child.getSourceOp may be equal to op for split execution 
	private ExecutionStep addMerge(Operator op, ExecutionStep child) throws Exception {
		// merge input tuples with other party
		MergeMethod merge = null;
		if(op instanceof Join) { // inserts merge for specified child
			Operator childOp = child.getSourceOperator();
			Join joinOp = (Join) op;
			Operator leftChild = joinOp.getChild(0).getNextValidChild();
			Operator rightChild = joinOp.getChild(1).getNextValidChild();
			
			boolean isLhs = (leftChild == childOp); 
			List<SecureRelDataTypeField> orderBy = (isLhs) ? leftChild.secureComputeOrder() : rightChild.secureComputeOrder();
			merge = new MergeMethod(op, child, orderBy);	
			
		} else {
			 merge = new MergeMethod(op, child, op.secureComputeOrder());
		}
		merge.compileIt();
		
		RunConfig mRunConf = new RunConfig();
		mRunConf.port = (SystemConfiguration.getInstance()).readAndIncrementPortCounter();
		mRunConf.smcMode = mode;
		
		 if(child.getSourceOperator() instanceof CommonTableExpressionScan) {
			 String src = child.getPackageName();
			 String dst = merge.getPackageName();
			 SecureBufferPool.getInstance().addPointer(src, dst);
		 }

		SecureStep mergeStep = new SecureStep(merge, op, mRunConf, child, null);
		child.setParent(mergeStep);
		smcCode.put(mergeStep, merge.generate());
		return mergeStep;
	}
	
	
	private ExecutionStep generateSecureStep(Operator op, List<ExecutionStep> children, List<Operator> opsToCombine, List<ExecutionStep> merges) throws Exception {
		SecureOperator secOp = SecureOperatorFactory.get(op);
		if (!merges.isEmpty())
			secOp.setMerges(merges);
		
		for (Operator cur : opsToCombine) {
			if (cur instanceof Filter) {
				secOp.addFilter((Filter) cur);
			} else if (cur instanceof Project) {
				secOp.addProject((Project) cur);
			}
		}
		secOp.compileIt();
	
		RunConfig sRunConf = new RunConfig();
		
		sRunConf.port = (SystemConfiguration.getInstance()).readAndIncrementPortCounter();
		sRunConf.smcMode = mode;
		sRunConf.host = getAliceHostname();
		
		SecureStep smcStep = null;

		if(children.size() == 1) {
			ExecutionStep child = children.get(0);
			smcStep = new SecureStep(secOp, op, sRunConf, child, null);
			child.setParent(smcStep);
		}
		else if(children.size() == 2) {// join
			ExecutionStep lhsChild = children.get(0);
			ExecutionStep rhsChild = children.get(1);
			smcStep = new SecureStep(secOp, op, sRunConf, lhsChild, rhsChild);
			lhsChild.setParent(smcStep);
			rhsChild.setParent(smcStep);
		}
		else {
			throw new Exception("Operator cannot have >2 children.");
		}
		
		allSteps.put(op, smcStep);
		String code = secOp.generate();
		smcCode.put(smcStep, code);
		return smcStep;
	}

	public Map<ExecutionStep, String> getSMCCode() {
		return smcCode;
	}
	
	public Map<ExecutionStep, String> getSQLCode() {
		return sqlCode;
	}
	
	private String getAliceHostname() throws Exception {
		ConnectionManager cm = ConnectionManager.getInstance(); 
		String alice = cm.getAlice();
		return cm.getWorker(alice).hostname;
	}
	
	private void inferExecutionSegment(ExecutionStep step) throws Exception  {
		if(step instanceof PlaintextStep) 
			return;
		
		SecureStep secStep = (SecureStep) step;
		
		if(secStep.getExec().parentSegment != null) {
			return;
		}
		
		// if root node
		if(secStep.getParent() == null) {
			ExecutionSegment segment = createSegment(secStep);
			executionSegments.add(segment);
			secStep.getExec().parentSegment = segment;
		
		}
		else { // non-root
			Operator parentOp = secStep.getParent().getSourceOperator();
			Operator localOp = secStep.getSourceOperator();
			SecureStep parent = (SecureStep) step.getParent();
	
			if(localOp.sharesExecutionProperties(parentOp)) { // same segment
				secStep.getExec().parentSegment = parent.getExec().parentSegment;
				
			}
			else { // create new segment
				ExecutionSegment current = createSegment(secStep);
				executionSegments.add(current);	
				secStep.getExec().parentSegment = current;
			}
			
			
		}
		
		List<ExecutionStep> sources = secStep.getChildren();
		for(ExecutionStep s : sources) 
			inferExecutionSegment(s);
	}

	public SecureRelRoot getPlan() {
		return queryPlan;
	}
	
	private ExecutionSegment createSegment(ExecutionStep secStep) throws Exception {
		ExecutionSegment current = new ExecutionSegment();
		current.rootNode = secStep.getExec();
		
		current.runConf = secStep.getRunConfig();
		current.outSchema = new SecureRelRecordType(secStep.getSchema());
		current.executionMode = secStep.getSourceOperator().getExecutionMode();
		
		if(secStep.getSourceOperator().getExecutionMode() == ExecutionMode.Slice && userQuery != null) {
			current.sliceSpec = secStep.getSourceOperator().getSliceKey();
			
			PlainOperator sqlGenRoot = secStep.getSourceOperator().getPlainOperator();		
			sqlGenRoot.inferSlicePredicates(current.sliceSpec);
			current.sliceValues = sqlGenRoot.getSliceValues();
			current.complementValues = sqlGenRoot.getComplementValues();
			current.sliceComplementSQL = sqlGenRoot.generatePlaintextForSliceComplement(userQuery); //plaintext query for single site values
		}

		return current;
		
	}
	
	Byte[] toByteObject(byte[] primitive) {
	    Byte[] bytes = new Byte[primitive.length];
	    int i = 0;
	    for(byte b: primitive)
	    	bytes[i++] = Byte.valueOf(b); 
	    return bytes;
	}
	
}
