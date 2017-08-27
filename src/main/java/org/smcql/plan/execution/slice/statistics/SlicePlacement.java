package org.smcql.plan.execution.slice.statistics;

import java.util.Comparator;

// basically a pair for keeping track of where a slice key has values
public class SlicePlacement implements Comparator<SlicePlacement>, Comparable<SlicePlacement> {
	private int siteId;
	private int count;
	
	
	public SlicePlacement(int s, int c) {
		siteId = s;
		count = c;
	}
	
	public int getSiteId() {
		return siteId;
	}
	public void setSiteId(int siteId) {
		this.siteId = siteId;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	
	@Override
	public boolean equals(Object o) {
		if(o instanceof SlicePlacement) {
			SlicePlacement ap = (SlicePlacement) o;
			if(ap.count == this.count && ap.siteId == this.siteId) {
				return true;
			}
		}
		
		return false;
	}
	
	public String toString() {
		return new String("(" + siteId + ", " + count + ")" );
	}

	@Override
	public int compareTo(SlicePlacement o) {
		int cmp = compareInt(siteId, o.siteId);
		if(cmp != 0) {
			return cmp;
		}
		
		return compareInt(count, o.count);
		
	}

	@Override
	public int compare(SlicePlacement o1, SlicePlacement o2) {
		return o1.compareTo(o2);
	}

	private int compareInt(int lhs, int rhs) {
		Integer l = new Integer(lhs);
		Integer r = new Integer(rhs);
		return l.compareTo(r);
	}
	
	
	
}
