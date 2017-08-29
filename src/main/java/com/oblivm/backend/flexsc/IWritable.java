package com.oblivm.backend.flexsc;

// for compiler generated code
public interface IWritable<T1 extends IWritable<T1, T2>, T2> {
	public int numBits();

	public T2[] getBits() throws Exception;

	public T1 newObj(T2[] data) throws Exception;
	
	default T1 fake() throws Exception {
		return newObj(getBits());
	};
	
	default T1 muxFake(T2 dummy) throws Exception {
		return fake();
	}

}
