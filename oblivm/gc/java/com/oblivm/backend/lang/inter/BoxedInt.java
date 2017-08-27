package com.oblivm.backend.lang.inter;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.IWritable;

public class BoxedInt<T1> implements IWritable<BoxedInt<T1>, T1> {
	public T1[] value;
	public CompEnv<T1> env;
	
	public BoxedInt(CompEnv<T1> env, T1[] data) {
		this.value = data;
		this.env = env;
	}
	
	public BoxedInt(CompEnv<T1> env, T1 data) {
		this.value = env.newTArray(1);
		this.value[0] = data;
		this.env = env;
	}
	
	@Override
	public int numBits() {
		return value.length;
	}

	@Override
	public T1[] getBits() {
		return value;
	}

	@Override
	public BoxedInt<T1> newObj(T1[] data) throws Exception {
		return new BoxedInt<T1>(env, data);
	}
}
