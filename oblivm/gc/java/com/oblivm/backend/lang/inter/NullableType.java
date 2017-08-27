package com.oblivm.backend.lang.inter;

import com.oblivm.backend.circuits.arithmetic.IntegerLib;
import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.IWritable;
import com.oblivm.backend.flexsc.Mode;

public class NullableType<T1, T2 extends IWritable<T2, T1>> implements IWritable<NullableType<T1, T2>, T1> {
	public T1 isNull;
	public T2 value;
	public CompEnv<T1> env;
	public IntegerLib<T1> intLib;
	
	public NullableType(CompEnv<T1> env, T2 data, T1 isNull) {
		this.value = data;
		this.isNull = isNull;
		this.env = env;
		this.intLib = new IntegerLib<T1>(env);
	}
	
	@Override
	public int numBits() {
		return value.numBits() + 1;
	}

	@Override
	public T1[] getBits() throws Exception {
		T1[] ret = env.newTArray(numBits());
		ret[0] = isNull;
		System.arraycopy(value.getBits(), 0, ret, 1, ret.length - 1);
		return ret;
	}

	@Override
	public NullableType<T1, T2> newObj(T1[] data) throws Exception {
		T1[] ret = env.newTArray(data.length - 1);
		System.arraycopy(data, 1, ret, 0, ret.length);
		return new NullableType<T1, T2>(env, value.newObj(ret), data[0]);
	}
	
	public NullableType<T1, T2> mux(T1 is, NullableType<T1, T2> obj) throws Exception {
		if(env.mode == Mode.COUNT) {
			intLib.mux(value.getBits(), obj.value.getBits(), is);
			intLib.mux(isNull, obj.isNull, is);
			return this;
		}
		return new NullableType<T1, T2>(env, 
										value.newObj(intLib.mux(value.getBits(), obj.getBits(), is)),
										intLib.mux(isNull, obj.isNull, is));
	}

	public T2 muxFake() throws Exception {
		if(env.mode == Mode.COUNT) {
			intLib.mux(value.getBits(), getFake().getBits(), isNull);
			return value;
		}
		return value.newObj(intLib.mux(value.getBits(), getFake().getBits(), isNull));
	}
	
	public T2 getFake() throws Exception {
		if(env.mode == Mode.COUNT) {
			return value;
		}
		return value.fake();
	}
}
