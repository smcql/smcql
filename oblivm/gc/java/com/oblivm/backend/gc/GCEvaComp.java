package com.oblivm.backend.gc;

import java.io.IOException;
import java.util.Arrays;

import com.oblivm.backend.flexsc.Flag;
import com.oblivm.backend.flexsc.Mode;
import com.oblivm.backend.flexsc.Party;
import com.oblivm.backend.network.Network;
import com.oblivm.backend.ot.FakeOTReceiver;
import com.oblivm.backend.ot.OTExtReceiver;
import com.oblivm.backend.ot.OTPreprocessReceiver;
import com.oblivm.backend.ot.OTReceiver;

public abstract class GCEvaComp extends GCCompEnv{

	/**
	 * 
	 */
	private static final long serialVersionUID = -809050851637963057L;

	OTReceiver rcv;

	protected long gid = 0;

	public GCEvaComp(Network channel, Mode mode) {
		super(channel, Party.Bob, mode);

		if (Flag.FakeOT)
			rcv = new FakeOTReceiver(channel);
		else if (Flag.ProprocessOT)
			rcv = new OTPreprocessReceiver(channel);
		else
			rcv = new OTExtReceiver(channel);

	}

	public GCSignal inputOfAlice(boolean in) {
		Flag.sw.startOT();
		GCSignal signal = GCSignal.receive(channel);
		Flag.sw.stopOT();
		return signal;
	}

	public GCSignal inputOfBob(boolean in) {
		Flag.sw.startOT();
		GCSignal signal = null;
		try {
			signal = rcv.receive(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Flag.sw.stopOT();
		return signal;
	}

	public GCSignal[] inputOfBob(boolean[] x) {
		GCSignal[] ret = new GCSignal[x.length];
		for(int i = 0; i < x.length; i+=Flag.OTBlockSize) {
			GCSignal[] tmp = inputOfBobInter(Arrays.copyOfRange(x, i, Math.min(i+Flag.OTBlockSize, x.length)));
			System.arraycopy(tmp, 0, ret, i, tmp.length);
		}
		return ret;
	}

	public GCSignal[] inputOfBobInter(boolean[] x) {
		Flag.sw.startOT();
		GCSignal[] signal = null;
		try {
			signal = rcv.receive(x);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Flag.sw.stopOT();
		return signal;
	}

	public GCSignal[] inputOfAlice(boolean[] x) {
		Flag.sw.startOT();
		GCSignal[] result = new GCSignal[x.length];
		for (int i = 0; i < x.length; ++i)
			result[i] = GCSignal.receive(channel);
		Flag.sw.stopOT();
		return result;
	}

	public boolean outputToAlice(GCSignal out) {
		if (!out.isPublic())
			out.send(channel);
		return false;
	}

	public boolean outputToBob(GCSignal out) {
		if (out.isPublic())
			return out.v;

		GCSignal lb = GCSignal.receive(channel);
		if (lb.equals(out))
			return false;
		else
			return true;
	}

	public boolean[] outputToAlice(GCSignal[] out) {
		boolean[] result = new boolean[out.length];
		for (int i = 0; i < result.length; ++i) {
			if (!out[i].isPublic())
				out[i].send(channel);
		}

		channel.flush();		

		for (int i = 0; i < result.length; ++i)
			result[i] = false;
		return result;
	}

	public boolean[] outputToBob(GCSignal[] out) {
		boolean[] result = new boolean[out.length];
		for (int i = 0; i < result.length; ++i) {
			result[i] = outputToBob(out[i]);
		}
		return result;
	}

	public GCSignal xor(GCSignal a, GCSignal b) {
		if (a.isPublic() && b.isPublic())
			return  ((a.v ^ b.v) ?new GCSignal(true):new GCSignal(false));
		else if (a.isPublic())
			return a.v ? not(b) : b;
		else if (b.isPublic())
			return b.v ? not(a) : a;
		else
			return a.xor(b);
	}

	public GCSignal not(GCSignal a) {
		if (a.isPublic())
			return ((!a.v) ?new GCSignal(true):new GCSignal(false));
		else {
			return a;
		}
	}
}
