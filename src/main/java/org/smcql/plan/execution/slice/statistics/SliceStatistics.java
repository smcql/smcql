package org.smcql.plan.execution.slice.statistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.smcql.db.data.Tuple;
import org.smcql.db.data.field.Field;
import org.smcql.db.data.field.IntField;
import org.smcql.plan.slice.SliceKeyDefinition;


public class SliceStatistics {
	SliceKeyDefinition sliceKey = null;
	private Map<Tuple, SlicePlacement> singleSiteValues; // value --> (site id, count): Slice complement (can process on single host), no SMC
	private Map<Tuple, List<SlicePlacement > > distributedValues; // appears on greater than one host: keys that get run, one SMC instance per key, how I partition SMC
	
	public SliceStatistics(SliceKeyDefinition key) {
		singleSiteValues = new HashMap<Tuple, SlicePlacement >();
		distributedValues = new HashMap<Tuple, List<SlicePlacement>>();
		sliceKey = key;
		
	}
	
	


	public SliceStatistics(SliceStatistics a) {
		sliceKey = a.sliceKey;
		singleSiteValues = new HashMap<Tuple, SlicePlacement>(a.singleSiteValues);
		distributedValues = new HashMap<Tuple, List<SlicePlacement>>(a.distributedValues);
		
	}





	public SliceStatistics() {
		singleSiteValues = new HashMap<Tuple, SlicePlacement >();
		distributedValues = new HashMap<Tuple, List<SlicePlacement>>();

	}




	// each tuple has the form attrs, site_id, count
	public void addDataSource(Tuple t) throws Exception {

		Tuple sliceValues = new Tuple();
		// collect values
		int i;
		int siteId = 0;
		int valueCount = 0;
		
		for(i = 0; i < t.getAttributeCount()-2; ++i) {
			sliceValues.addField(t.getField(i));
		}
		
		// site id
		Field f = t.getField(i);
		if(f instanceof IntField) {
			siteId = (int) ((IntField) f).getValue();
		}
		else {
			throw new Exception("Fields not aligned for statistics collection.");
		}
		
		++i;
		f = t.getField(i);
		if(f instanceof IntField) {
			valueCount = (int) ((IntField) f).getValue();
		}
		else {
			throw new Exception("Fields not aligned for statistics collection.");
		}
		
		
		SlicePlacement place = new SlicePlacement(siteId, valueCount);
		addEntry(sliceValues, place);
	}
	
	public void addEntry(Tuple sliceValues, SlicePlacement place) {
		if(distributedValues.containsKey(sliceValues)) {
			if(!matchAndAdd(sliceValues, place)) {				
				distributedValues.get(sliceValues).add(place);
			}
		}
		// have exactly one previous entry
		else if(singleSiteValues.containsKey(sliceValues)) {
			SlicePlacement existing = singleSiteValues.remove(sliceValues);
			if(existing.getSiteId() == place.getSiteId()) { // same site, happens when we merge on join equality predicates
				existing.setCount(existing.getCount() + place.getCount());
				singleSiteValues.put(sliceValues, existing);
			}
			else {
				List<SlicePlacement> distributed = new ArrayList<SlicePlacement>();
				distributed.add(existing);
				distributed.add(place);
				distributedValues.put(sliceValues, distributed);
			}
		}
		else {
			singleSiteValues.put(sliceValues, place);
		}

	}
	
	public void insertDistributedEntry(Tuple sliceValues, List<SlicePlacement> places) {
		distributedValues.put(sliceValues, places);
	}
	
	public void addSingulerKey(Tuple sliceValues) {
		singleSiteValues.put(sliceValues, null);
	}
	
	
	private boolean matchAndAdd(Tuple t, SlicePlacement place) {
		
		int siteId = place.getSiteId();
		
		for(int i = 0; i < distributedValues.get(t).size(); ++i) {
			SlicePlacement s = distributedValues.get(t).get(i);
			if(s.getSiteId() == place.getSiteId()) {
				int newTotal = s.getCount() + place.getCount();
				SlicePlacement sp = new SlicePlacement(siteId, newTotal);
				distributedValues.get(t).set(i, sp);
				return true;
			}
		}
		
		return false;
	}




	
	public static SliceStatistics intersectStatistics(SliceStatistics lhs, SliceStatistics rhs) {
		SliceStatistics merged = new SliceStatistics();
		merged.sliceKey = new SliceKeyDefinition(lhs.sliceKey.getAttributes());
		merged.sliceKey.addAttributes(rhs.sliceKey.getAttributes());
		
		List<Tuple> lhsKeys = new ArrayList<Tuple>(lhs.singleSiteValues.keySet());
		lhsKeys.addAll(lhs.distributedValues.keySet());
		
		List<Tuple> rhsKeys = new ArrayList<Tuple>(rhs.singleSiteValues.keySet());
		rhsKeys.addAll(rhs.distributedValues.keySet());
		
		for(Tuple t : lhsKeys) {
			if(rhsKeys.contains(t)) {
				merged.distributedValues.put(t, null);
			}
			else {
				merged.singleSiteValues.put(t, null);
			}
		}
		
		for(Tuple t : rhsKeys) {
			if(!lhsKeys.contains(t)) {
				merged.singleSiteValues.put(t, null);
			}
		}
		
		return merged;

	
	}
	
	  public boolean equals(Object obj) {
		
		if(!(obj instanceof SliceStatistics)) {
			return false;
		}

		SliceStatistics cmp = (SliceStatistics) obj;

		if(!sliceKey.equals(cmp.sliceKey)) {
 			return false;
		}
		
		if(this.singleSiteValues.size() != cmp.singleSiteValues.size()) {
			return false;
		}
				
		for(Tuple t : singleSiteValues.keySet()) {
			SlicePlacement ap = cmp.singleSiteValues.get(t);
		
			if(!ap.equals(singleSiteValues.get(t))) {
				return false;
			}
		}
		
		


		if(this.distributedValues.size() != cmp.distributedValues.size()) {
			return false;
		}


		
		for(Tuple t : distributedValues.keySet()) {
			List<SlicePlacement> cap = cmp.distributedValues.get(t);
			List<SlicePlacement> tap = this.distributedValues.get(t);
			
			if(cap.size() != tap.size()) {
				return false;
			}
			
			for(SlicePlacement ap : cap) {

				if(!tap.contains(ap)) {

					return false;
				}
			}
		}
		
		return true;
		
	}
	
	@Override
	public String toString() {
		String ret = new String();
		ret += "Key: " + sliceKey + "\n";
		ret += "Single site values: ";

		List<Tuple> tuples = new ArrayList<Tuple>(singleSiteValues.keySet());

		java.util.Collections.sort(tuples);
		
		for(Tuple t : tuples) {
			SlicePlacement ap = singleSiteValues.get(t);
			ret += "<" + t + ", " + ap + "> ";
		}
		
		ret += "\n";
		
		tuples = new ArrayList<Tuple>(distributedValues.keySet());
		java.util.Collections.sort(tuples);
			
		ret += "Distributed values: ";
		for(Tuple t : tuples) {
			List<SlicePlacement> ap = distributedValues.get(t);
			java.util.Collections.sort(ap);
					
			ret += "<" + t + ", " + ap + "> ";
		}
		ret += "\n";

		return ret;
		
	}
	
	
	public int getSingleSiteValueCount() {
		return singleSiteValues.size();
	}
	
	public int getDistributedValueCount() {
		return distributedValues.size();
	}




	public Map<Tuple, List<SlicePlacement > > getDistributedValues() {
		return distributedValues;
	}




	public void setDistributedValues(Map<Tuple, List<SlicePlacement > > distributedValues) {
		this.distributedValues = distributedValues;
	}




	public Map<Tuple, SlicePlacement> getSingleSiteValues() {
		return singleSiteValues;
	}




	public void setSingleSiteValues(Map<Tuple, SlicePlacement> singleSiteValues) {
		this.singleSiteValues = singleSiteValues;
	}
	
	public SliceKeyDefinition getKey() {
		return sliceKey;
	}




	
}
