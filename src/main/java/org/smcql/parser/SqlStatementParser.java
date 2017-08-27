package org.smcql.parser;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.commons.dbcp2.BasicDataSource;
import org.smcql.config.SystemConfiguration;
import org.smcql.executor.config.WorkerConfiguration;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.rules.*;


// parse and validate a sql statement against a schema
public class SqlStatementParser {
	
	SchemaPlus sharedSchema;
	CalciteConnection calciteConnection;
	Planner planner;
	FrameworkConfig config;
	HepPlanner optimizer;
	
	
	public SqlStatementParser() throws Exception {
		SystemConfiguration pdnConfig = SystemConfiguration.getInstance();
		config = pdnConfig.getCalciteConfiguration();
		calciteConnection = pdnConfig.getCalciteConnection();
		sharedSchema = pdnConfig.getPdnSchema();
		
		planner = new PlannerImpl(config);
		 
		// configure optimizer
		   HepProgramBuilder builder = new HepProgramBuilder();
		  
		    builder.addRuleClass(ReduceExpressionsRule.class);
		    builder.addRuleClass(FilterJoinRule.class);
		    builder.addRuleClass(JoinPushTransitivePredicatesRule.class);
		    builder.addRuleClass(ProjectMergeRule.class);
		    builder.addCommonRelSubExprInstruction(); 
		    builder.addRuleClass(AggregateExpandDistinctAggregatesRule.class);
		    builder.addRuleClass(ProjectToWindowRule.class);
		    builder.addRuleClass(SortProjectTransposeRule.class);
		    builder.addRuleClass(FilterMergeRule.class);
		    builder.addRuleClass(ProjectWindowTransposeRule.class);
		    builder.addRuleClass(FilterProjectTransposeRule.class);
		    builder.addRuleClass(FilterMergeRule.class);
		    
		    
		    optimizer = new HepPlanner(builder.build());
		    optimizer.addRule(ProjectToWindowRule.PROJECT);
		    
		    
		       
		    optimizer.addRule(ReduceExpressionsRule.FILTER_INSTANCE);
		    optimizer.addRule(ReduceExpressionsRule.CALC_INSTANCE);
		    optimizer.addRule(ReduceExpressionsRule.PROJECT_INSTANCE);
		    optimizer.addRule(ReduceExpressionsRule.JOIN_INSTANCE);
		    optimizer.addRule(FilterProjectTransposeRule.INSTANCE);

		    optimizer.addRule(FilterJoinRule.FILTER_ON_JOIN);
		    optimizer.addRule(FilterJoinRule.JOIN);
		    optimizer.addRule(JoinPushTransitivePredicatesRule.INSTANCE);
		    
		    optimizer.addRule(AggregateExpandDistinctAggregatesRule.INSTANCE);
		    optimizer.addRule(SortProjectTransposeRule.INSTANCE);
		    optimizer.addRule(FilterTableScanRule.INSTANCE);
		   optimizer.addRule(ProjectWindowTransposeRule.INSTANCE);
		    
		    optimizer.addRule(FilterMergeRule.INSTANCE);
		    optimizer.addRule(ProjectMergeRule.INSTANCE);
		    
	}
	
	
	public SqlNode parseSQL(String sql) throws SqlParseException, ValidationException  {
		SqlNode parsed =  planner.parse(sql);
		parsed = planner.validate(parsed);
		return parsed;
	}
	
	
	public RelDataTypeFactory getTypeFactory() {
		return planner.getTypeFactory();
	}
	public RelRoot compile(SqlNode sqlRoot) throws RelConversionException {
		RelRoot root =  planner.rel(sqlRoot);
		
		return root;
	}
	
	public FrameworkConfig getConfig() { 
		return config;
	}
	
	
	// method for minimizing the fields at each RelNode 
	// this reduces the permissions we need to run many operators
	// generalization of SqlToRelTestBase
	  public RelRoot convertSqlToRelMinFields(String sql) throws SqlParseException {
	      assert(sql != null);
	      final SqlNode sqlQuery = planner.parse(sql);
			
	      final RelDataTypeFactory typeFactory = planner.getTypeFactory();
	     

	      final Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
	    	        CalciteSchema.from(sharedSchema),
	    	        false,
	    	        CalciteSchema.from(sharedSchema).path(null),
	    	        (JavaTypeFactory) typeFactory);

	          
	      final SqlValidator validator = new LocalValidatorImpl(config.getOperatorTable(), catalogReader, typeFactory,
	              conformance());
	      validator.setIdentifierExpansion(true);
	      
	      final SqlToRelConverter converter =
	          createSqlToRelConverter(
	              validator,
	              catalogReader,
	              typeFactory);
	      
	      converter.setTrimUnusedFields(true);
	      final SqlNode validatedQuery = validator.validate(sqlQuery);
	      RelRoot root =
	          converter.convertQuery(validatedQuery, false, true);
	      assert(root != null);
	      converter.setTrimUnusedFields(true);
	      root = root.withRel(converter.trimUnusedFields(true, root.rel));
	      return root;
	    }

	
	  // from PlannerImpl, here b/c of protected method
	  private SqlConformance conformance() {
		    final Context context = config.getContext();
		    if (context != null) {
		      final CalciteConnectionConfig connectionConfig =
		          context.unwrap(CalciteConnectionConfig.class);
		      if (connectionConfig != null) {
		        return connectionConfig.conformance();
		      }
		    }
		    return SqlConformance.DEFAULT;
		  }

	  
	// use optimizer to get operators into a canonical form
	// very basic optimizer
	public RelRoot optimize(RelRoot relRoot) {
		
		     optimizer.setRoot(relRoot.project());

		 		    
		    RelNode out = optimizer.findBestExp();
		    return RelRoot.of(out, relRoot.kind);

	}

	
	public RelRoot trimFields(RelRoot root)  {

	      final RelDataTypeFactory typeFactory = planner.getTypeFactory();
	     

	      final Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
	    	        CalciteSchema.from(sharedSchema),
	    	        false,
	    	        CalciteSchema.from(sharedSchema).path(null),
	    	        (JavaTypeFactory) typeFactory);

	          
	      final SqlValidator validator = new LocalValidatorImpl(config.getOperatorTable(), catalogReader, typeFactory,
	              conformance());
	      validator.setIdentifierExpansion(true);
	      
	      final SqlToRelConverter converter =
	          createSqlToRelConverter(
	              validator,
	              catalogReader,
	              typeFactory);
	      
	      converter.setTrimUnusedFields(true);
	      
	      final boolean ordered = !root.collation.getFieldCollations().isEmpty();
	      return root.withRel(converter.trimUnusedFields(ordered, root.rel));
	      
	      
	}
	
	public RelRoot mergeProjects(RelRoot root) {
		   HepProgramBuilder builder = new HepProgramBuilder();
		   builder.addRuleClass(ProjectMergeRule.class);
		   builder.addRuleClass(ProjectRemoveRule.class);
		   HepPlanner merger = new HepPlanner(builder.build());
		    merger.addRule(ProjectMergeRule.INSTANCE);
		    merger.addRule(ProjectRemoveRule.INSTANCE);
		    merger.setRoot(root.project());

		    RelNode out = merger.findBestExp();
		    return RelRoot.of(out, root.kind);
		
	}	
	
	public RelRoot logicalCalc(RelRoot root) {
		   HepProgramBuilder builder = new HepProgramBuilder();
		    builder.addRuleClass(FilterToCalcRule.class);
		    builder.addRuleClass(ProjectToCalcRule.class);
		    builder.addRuleClass(FilterCalcMergeRule.class);
		    builder.addRuleClass(ProjectCalcMergeRule.class);
		    builder.addRuleClass(CalcMergeRule.class);

		    
		    HepPlanner calcMaker = new HepPlanner(builder.build());
		    calcMaker.addRule(FilterToCalcRule.INSTANCE);
		    calcMaker.addRule(ProjectToCalcRule.INSTANCE);
		    calcMaker.addRule(FilterCalcMergeRule.INSTANCE);
		    calcMaker.addRule(ProjectCalcMergeRule.INSTANCE);
		    calcMaker.addRule(CalcMergeRule.INSTANCE);
		    
		    calcMaker.setRoot(root.project());

		    RelNode out = calcMaker.findBestExp();
		    return RelRoot.of(out, root.kind);
		
	}
	
	
	
	
    protected SqlToRelConverter createSqlToRelConverter(
            final SqlValidator validator,
            final Prepare.CatalogReader catalogReader,
            final RelDataTypeFactory typeFactory) {
          final RexBuilder rexBuilder = new RexBuilder(typeFactory);

          RelOptCluster cluster =
              RelOptCluster.create(optimizer, rexBuilder);

          return new SqlToRelConverter(null, validator, catalogReader, cluster,
              StandardConvertletTable.INSTANCE);
        }


	
	private class LocalValidatorImpl extends SqlValidatorImpl {
	    protected LocalValidatorImpl(
	        SqlOperatorTable opTab,
	        SqlValidatorCatalogReader catalogReader,
	        RelDataTypeFactory typeFactory,
	        SqlConformance conformance) {
	      super(opTab, catalogReader, typeFactory, conformance);
	    }

	};
	
}
