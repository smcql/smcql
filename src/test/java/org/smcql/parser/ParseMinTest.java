package org.smcql.parser;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.smcql.BaseTest;
import org.smcql.config.SystemConfiguration;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelRoot;

public class ParseMinTest  extends  BaseTest {

	
	protected void setUp() throws Exception {
		super.setUp();
	}
	
	public void testComorbidity() throws Exception {

		String expectedPlan =  "LogicalSort(sort0=[$1], dir0=[DESC], fetch=[10])\n"
				 + "  LogicalAggregate(group=[{0}], cnt=[COUNT()])\n"
				 + "    LogicalProject(major_icd9=[$12])\n"
				 + "      JdbcTableScan(table=[[cdiff_cohort_diagnoses]])\n";

		
		runTest("comorbidity", expectedPlan);
		
	}
	
	public void testCDiff() throws Exception {
		String expectedPlan = "LogicalAggregate(group=[{0}])\n"
				 + "  LogicalProject(patient_id=[$0])\n"
				 + "    LogicalJoin(condition=[AND(=($0, $3), >=(/INT(CAST(-($1, $4, FLAG(DAY))):INTEGER NOT NULL, 86400000), 15), <=(/INT(CAST(-($1, $4, FLAG(DAY))):INTEGER NOT NULL, 86400000), 56), =(+($2, 1), $5))], joinType=[inner])\n"
				 + "      LogicalWindow(window#0=[window(partition {0} order by [1] rows between UNBOUNDED PRECEDING and CURRENT ROW aggs [ROW_NUMBER()])])\n"
				 + "        LogicalProject(patient_id=[$0], timestamp_=[$2])\n"
				 + "          LogicalProject(patient_id=[$0], icd9=[$8], timestamp_=[$10])\n"
				 + "            LogicalFilter(condition=[=($8, '008.45')])\n"
				 + "              JdbcTableScan(table=[[cdiff_cohort_diagnoses]])\n"
				 + "      LogicalWindow(window#0=[window(partition {0} order by [1] rows between UNBOUNDED PRECEDING and CURRENT ROW aggs [ROW_NUMBER()])])\n"
				 + "        LogicalProject(patient_id=[$0], timestamp_=[$2])\n"
				 + "          LogicalProject(patient_id=[$0], icd9=[$8], timestamp_=[$10])\n"
				 + "            LogicalFilter(condition=[=($8, '008.45')])\n"
				 + "              JdbcTableScan(table=[[cdiff_cohort_diagnoses]])\n";

		runTest("cdiff", expectedPlan);
			
	}
		
	
	public void testAsprinCount() throws Exception {
		String expectedPlan =  "LogicalAggregate(group=[{}], rx_cnt=[COUNT($0)])\n"
				 + "  LogicalAggregate(group=[{0}])\n"
				 + "    LogicalProject(patient_id=[$0])\n"
				 + "      LogicalJoin(condition=[AND(=($0, $2), <=($1, $3))], joinType=[inner])\n"
				 + "        LogicalProject(patient_id=[$0], timestamp_=[$2])\n"
				 + "          LogicalFilter(condition=[LIKE($1, '414%')])\n"
				 + "            LogicalProject(patient_id=[$0], icd9=[$8], timestamp_=[$10])\n"
				 + "              JdbcTableScan(table=[[mi_cohort_diagnoses]])\n"
				 + "        LogicalProject(patient_id=[$0], timestamp_=[$2])\n"
				 + "          LogicalFilter(condition=[LIKE(LOWER($1), '%aspirin%')])\n"
				 + "            LogicalProject(patient_id=[$0], medication=[$4], timestamp_=[$7])\n"
				 + "              JdbcTableScan(table=[[mi_cohort_medications]])\n";

		
		runTest("aspirin-count", expectedPlan);
	}

	
	public void runTest(String testName, String expectedPlan) throws Exception {
		String sql = super.readSQL(testName);
		RelRoot testRoot =  parser.convertSqlToRelMinFields(sql);
		
		testRoot = parser.optimize(testRoot); // optimized to represent in a fine granularity for more smc avoidance
		testRoot = parser.trimFields(testRoot); // use minimal set of fields to avoid triggering unnecessary SMC
	
		String planStr = RelOptUtil.toString(testRoot.project()); // , SqlExplainLevel.ALL_ATTRIBUTES);


		Logger logger = SystemConfiguration.getInstance().getLogger();
		
		
		logger.log(Level.INFO, "Parsed plan:\n" + planStr);
		
		assertEquals(expectedPlan, planStr);
		

	}
		
		


	
	
	

	
		
}
