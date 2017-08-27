package org.smcql.util;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;

public class RelNodeMatcher implements ReflectiveVisitor {
	
	private final ReflectUtil.MethodDispatcher<Boolean> dispatcher;
	
	public RelNodeMatcher() {
		dispatcher = ReflectUtil.createMethodDispatcher(Boolean.class, this, "compare", RelNode.class, RelNode.class);
	}
	
	protected Boolean dispatch(RelNode r1, RelNode r2) {
		return dispatcher.invoke(r1, r2);
	}
	
	public boolean matches(RelNode r1, RelNode r2) {
		if (!r1.getClass().equals(r2.getClass()))
			return false;
		
		return dispatch(r1, r2);
	}
	
	public boolean compare(RelNode r1, RelNode r2) {
		throw new AssertionError("Need to implement " + r1.getClass().getName() + ", " + r2.getClass().getName());
	}
	
	public Boolean compare(Filter r1, RelNode r2) {
		Filter comp = (Filter) r2;
		return r1.getRowType().equals(comp.getRowType())
				&& r1.getVariablesSet().equals(comp.getVariablesSet())
				&& r1.getCondition().equals(comp.getCondition());
	}
	
	public Boolean compare(Project r1, RelNode r2) {
		Project comp = (Project) r2;
		return r1.getRowType().equals(comp.getRowType())
				&& r1.getChildExps().equals(comp.getChildExps());		
	}
	
	public Boolean compare(Aggregate r1, RelNode r2) {
		Aggregate comp = (Aggregate) r2;
		return r1.getGroupSets().equals(comp.getGroupSets()) 
				&& r1.getAggCallList().equals(comp.getAggCallList())
				&& r1.getRowType().equals(comp.getRowType())
				&& r1.getGroupSet().equals(comp.getGroupSet());
	}
	
	public Boolean compare(TableScan r1, RelNode r2) {
		TableScan comp = (TableScan) r2;
		return r1.getRowType().equals(comp.getRowType())
				&& r1.getTable().equals(comp.getTable());
	}
	
	public Boolean compare(Sort r1, RelNode r2) {
		Sort comp = (Sort) r2;
		return r1.getRowType().equals(comp.getRowType())
				&& r1.getChildExps().equals(comp.getChildExps())
				&& r1.getCollation().equals(comp.getCollation())
				&& ((r1.fetch == null && comp.fetch == null) || r1.fetch.equals(comp.fetch))
				&& ((r1.offset == null && comp.offset == null) || r1.offset.equals(comp.offset));
	}
}
