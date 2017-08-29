package com.oblivm.backend.gc.offline;

import com.oblivm.backend.flexsc.Flag;
import com.oblivm.backend.flexsc.Mode;
import com.oblivm.backend.gc.GCEvaComp;
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.network.Network;

public class GCEva extends GCEvaComp {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2292745553477289840L;
	Garbler gb;
	GCSignal[][] gtt = new GCSignal[2][2];

	public GCEva(Network channel) {
		super(channel, Mode.OFFLINE);
		gb = new Garbler();
		gtt[0][0] = GCSignal.ZERO;
	}

	private void receiveGTT() {
		try {
			Flag.sw.startGCIO();
			gtt[0][1] = GCSignal.receive(channel);
			gtt[1][0] = GCSignal.receive(channel);
			gtt[1][1] = GCSignal.receive(channel);
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
			res = ( (a.v && b.v) ? new GCSignal(true) :new GCSignal(false));
		else if (a.isPublic())
			res = a.v ? b :new GCSignal(false);
		else if (b.isPublic())
			res = b.v ? a :new GCSignal(false);
		else {
			++numOfAnds;
			receiveGTT();

			int i0 = a.getLSB();
			int i1 = b.getLSB();

			res = gb.dec(a, b, gid, gtt[i0][i1]);
			gid++;
		}
		Flag.sw.stopGC();
		return res;
	}
	
	public void setOffline(boolean offline) {
		   Flag.offline = offline;
		}

}