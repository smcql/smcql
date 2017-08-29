package com.oblivm.backend.gc.regular;

import com.oblivm.backend.flexsc.Flag;
import com.oblivm.backend.flexsc.Mode;
import com.oblivm.backend.gc.GCEvaComp;
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.network.Network;

public class GCEva extends GCEvaComp implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7130564226213305739L;
	/**
	 * 
	 */

	Garbler gb;
	GCSignal[][] gtt = new GCSignal[2][2];

	public GCEva(Network channel) {
		super(channel, Mode.REAL);
		gb = new Garbler();
		gtt[0][0] = GCSignal.ZERO;
		gtt[0][1] = GCSignal.newInstance(new byte[10]);
		gtt[1][0] = GCSignal.newInstance(new byte[10]);
		gtt[1][1] = GCSignal.newInstance(new byte[10]);
	}

	private void receiveGTT() {
		try {
			Flag.sw.startGCIO();
			GCSignal.receive(channel, gtt[0][1]);
			GCSignal.receive(channel, gtt[1][0]);
			GCSignal.receive(channel, gtt[1][1]);
//			gtt[1][0] = GCSignal.receive(channel);
//			gtt[1][1] = GCSignal.receive(channel);
			Flag.sw.stopGCIO();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public GCSignal and(GCSignal a, GCSignal b) {
		Flag.sw.startGC();

		GCSignal res;
		if (a.isPublic() && b.isPublic())
			res = new GCSignal(a.v && b.v);
		else if (a.isPublic())
			res = a.v ? b : new GCSignal(false);
		else if (b.isPublic())
			res = b.v ? a : new GCSignal(false);
		else {
			++numOfAnds;
			res = new GCSignal(new byte[10]);
			receiveGTT();

			int i0 = a.getLSB();
			int i1 = b.getLSB();

			gb.dec(a, b, gid, gtt[i0][i1], res);
			gid++;
		}
		Flag.sw.stopGC();
		return res;
	}
}