package com.oblivm.backend.gc;

import java.io.IOException;
import java.util.Arrays;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Flag;
import com.oblivm.backend.flexsc.Mode;
import com.oblivm.backend.flexsc.Party;
import com.oblivm.backend.network.Network;
import com.oblivm.backend.ot.FakeOTSender;
import com.oblivm.backend.ot.OTExtSender;
import com.oblivm.backend.ot.OTPreprocessSender;
import com.oblivm.backend.ot.OTSender;

public abstract class GCGenComp extends GCCompEnv {

	/**
	 * 
	 */
	private static final long serialVersionUID = -582119636455451625L;
	static public GCSignal R = null;
	static {
		R = GCSignal.freshLabel(CompEnv.rnd);
		R.setLSB();
	}

	OTSender snd;
	protected long gid = 0;

	public GCGenComp(Network channel, Mode mode) {
		super(channel, Party.Alice, mode);

		if (Flag.FakeOT)
			snd = new FakeOTSender(80, channel);
		else if(Flag.ProprocessOT)
			snd = new OTPreprocessSender(80, channel);
		else
			snd = new OTExtSender(80, channel);
	}

	public static GCSignal[] genPairForLabel(Mode mode) {
		GCSignal[] label = new GCSignal[2];
		if(mode != Mode.OFFLINE || !Flag.offline)
			label[0] = GCSignal.freshLabel(rnd);
		if(mode == Mode.OFFLINE) {
			if(Flag.offline) {
				label[0] = new GCSignal(com.oblivm.backend.gc.offline.GCGen.fread.read(10));
			}
			else 
				label[0].send(com.oblivm.backend.gc.offline.GCGen.fout);
		}
		label[1] = R.xor(label[0]);
		return label;
	}
	
	public static GCSignal[] genPair() {
		GCSignal[] label = new GCSignal[2];
		label[0] = GCSignal.freshLabel(rnd);
		label[1] = R.xor(label[0]);
		return label;
	}

	public GCSignal inputOfAlice(boolean in) {
		Flag.sw.startOT();
		GCSignal[] label = genPairForLabel(mode);
		Flag.sw.startOTIO();
		label[in ? 1 : 0].send(channel);
		flush();
		Flag.sw.stopOTIO();
		Flag.sw.stopOT();
		return label[0];
	}

	public GCSignal inputOfBob(boolean in) {
		Flag.sw.startOT();
		GCSignal[] label = genPairForLabel(mode);
		try {
			snd.send(label);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Flag.sw.stopOT();
		return label[0];
	}

	public GCSignal[] inputOfAlice(boolean[] x) {
		Flag.sw.startOT();
		GCSignal[][] pairs = new GCSignal[x.length][2];
		GCSignal[] result = new GCSignal[x.length];
		for (int i = 0; i < x.length; ++i) {
			pairs[i] = genPairForLabel(mode);
			result[i] = pairs[i][0];
		}
		Flag.sw.startOTIO();
		for (int i = 0; i < x.length; ++i)
			pairs[i][x[i] ? 1 : 0].send(channel);
		flush();
		Flag.sw.stopOTIO();
		Flag.sw.stopOT();
		return result;
	}

	public GCSignal[] inputOfBob(boolean[] x) {
		Flag.sw.startOT();
		GCSignal[][] pair = new GCSignal[x.length][2];
		for (int i = 0; i < x.length; ++i)
			pair[i] = genPairForLabel(mode);
		try {
			snd.send(pair);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		GCSignal[] result = new GCSignal[x.length];
		for (int i = 0; i < x.length; ++i)
			result[i] = pair[i][0];
		Flag.sw.stopOT();
		return result;
	}

	protected boolean gatesRemain = false;

	public boolean outputToAlice(GCSignal out) {

		if (gatesRemain) {
			gatesRemain = false;
			flush();
		}
		
		if (out.isPublic())
			return out.v;

		GCSignal lb = GCSignal.receive(channel);
		
		
		if (lb.equals(out))
			return false;
		else if (lb.equals(R.xor(out)))
			return true;

		try {
			throw new Exception("bad label at final output.");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(-1);
		}
		return false;
	}

	public boolean outputToBob(GCSignal out) {
		if (!out.isPublic())
			out.send(channel);
		return false;
	}

	public boolean[] outputToBob(GCSignal[] out) {
		boolean[] result = new boolean[out.length];

		for (int i = 0; i < result.length; ++i) {
			if (!out[i].isPublic())
				out[i].send(channel);
		}
		flush();

		for (int i = 0; i < result.length; ++i)
			result[i] = false;
		return result;
	}

	public boolean[] outputToAlice(GCSignal[] out) {
		boolean[] result = new boolean[out.length];
		for (int i = 0; i < result.length; ++i) {
			result[i] = outputToAlice(out[i]);
		}
		return result;
	}
	
	public GCSignal xor(GCSignal a, GCSignal b) {
		
		if (a.isPublic() && b.isPublic())
			return new GCSignal(a.v ^ b.v);
		else if (a.isPublic())
			return a.v ? not(b) : new GCSignal(b);
		else if (b.isPublic())
			return b.v ? not(a) : new GCSignal(a);
		else {
			return a.xor(b);
		}
	}

	public GCSignal not(GCSignal a) {
		if (a.isPublic())
			return new GCSignal(!a.v);
		else
			return R.xor(a);
	}
}