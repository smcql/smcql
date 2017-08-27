package com.oblivm.backend.gc.regular;

import com.oblivm.backend.flexsc.Flag;
import com.oblivm.backend.flexsc.Mode;
import com.oblivm.backend.gc.GCGenComp;
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.network.Network;

public class GCGen extends GCGenComp implements java.io.Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3189976774842043421L;
	/**
	 * 
	 */

	Garbler gb;

	public GCGen(Network channel) {
		super(channel, Mode.REAL);
		gb = new Garbler();
		for(int i = 0; i < 2; ++i) {
			labelL[i] = new GCSignal(new byte[10]);
			labelR[i] = new GCSignal(new byte[10]);
			lb[i] = new GCSignal(new byte[10]);
			toSend[0][i] = new GCSignal(new byte[10]);
			toSend[1][i] = new GCSignal(new byte[10]);
		}
	}

	private GCSignal[][] gtt = new GCSignal[2][2];
	private GCSignal[][] toSend = new GCSignal[2][2];
	private GCSignal labelL[] = new GCSignal[2];
	private GCSignal labelR[] = new GCSignal[2];
	private GCSignal[] lb = new GCSignal[2];

	private GCSignal garble(GCSignal a, GCSignal b) {
		labelL[0] = a;
		GCSignal.xor(R, labelL[0], labelL[1]);
		labelR[0] = b;
		GCSignal.xor(R, labelR[0], labelR[1]);

		int cL = a.getLSB();
		int cR = b.getLSB();

		
		gb.enc(labelL[cL], labelR[cR], gid, GCSignal.ZERO, lb[cL & cR]);
		GCSignal.xor(R, lb[cL & cR], lb[1 - (cL & cR)]);

		gtt[0 ^ cL][0 ^ cR] = lb[0];
		gtt[0 ^ cL][1 ^ cR] = lb[0];
		gtt[1 ^ cL][0 ^ cR] = lb[0];
		gtt[1 ^ cL][1 ^ cR] = lb[1];

		if (cL != 0 || cR != 0)
			gb.enc(labelL[0], labelR[0], gid,
					gtt[0 ^ cL][0 ^ cR], toSend[0 ^ cL][0 ^ cR]);
		if (cL != 0 || cR != 1)
			gb.enc(labelL[0], labelR[1], gid,
					gtt[0 ^ cL][1 ^ cR], toSend[0 ^ cL][1 ^ cR]);
		if (cL != 1 || cR != 0)
			gb.enc(labelL[1], labelR[0], gid,
					gtt[1 ^ cL][0 ^ cR], toSend[1 ^ cL][0 ^ cR]);
		if (cL != 1 || cR != 1)
			gb.enc(labelL[1], labelR[1], gid,
					gtt[1 ^ cL][1 ^ cR], toSend[1 ^ cL][1 ^ cR]);

		return GCSignal.newInstance(lb[0].bytes);
	}

	private void sendGTT() {
		try {
			Flag.sw.startGCIO();
			toSend[0][1].send(channel);
			toSend[1][0].send(channel);
			toSend[1][1].send(channel);
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
			res = ( (a.v && b.v) ? new GCSignal(true): new GCSignal(false));
		else if (a.isPublic())
			res = a.v ? b : new GCSignal(false);
		else if (b.isPublic())
			res = b.v ? a : new GCSignal(false);
		else {
			++numOfAnds;
			GCSignal ret;
			ret = garble(a, b);

			sendGTT();
			gid++;
			gatesRemain = true;
			res = ret;
		}
		Flag.sw.stopGC();
		return res;
	}

}
