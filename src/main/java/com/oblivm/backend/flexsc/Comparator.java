package com.oblivm.backend.flexsc;


public interface Comparator<T> {
	public abstract T compare(T[] a, T[] b) throws Exception;
}
