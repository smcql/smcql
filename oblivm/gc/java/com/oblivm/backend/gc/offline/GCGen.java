package com.oblivm.backend.gc.offline;

import java.io.BufferedOutputStream;
import java.io.IOException;

import com.oblivm.backend.flexsc.CompEnv;
import com.oblivm.backend.flexsc.Flag;
import com.oblivm.backend.flexsc.Mode;
import com.oblivm.backend.gc.GCGenComp;
import com.oblivm.backend.gc.GCSignal;
import com.oblivm.backend.network.Network;

public class GCGen extends GCGenComp {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6454404832601009846L;

	Garbler gb;

	public static BufferedOutputStream fout = null;
	public static FileReader fread = null;

	
	public GCGen(Network channel) {
		super(channel, Mode.OFFLINE);
		gtt[0][1] = GCSignal.freshLabel(CompEnv.rnd);
		gtt[1][0] = GCSignal.freshLabel(CompEnv.rnd);
		gtt[1][1] = GCSignal.freshLabel(CompEnv.rnd);
		gb = new Garbler();
	}
	
	private GCSignal[][] gtt = new GCSignal[2][2];
	
	private GCSignal labelL[] = new GCSignal[2];
	private GCSignal labelR[] = new GCSignal[2];
	GCSignal[] lb = new GCSignal[2];;
	private GCSignal garble(GCSignal a, GCSignal b) {
		labelL[0] = a;
		labelL[1] = R.xor(labelL[0]);
		labelR[0] = b;
		labelR[1] = R.xor(labelR[0]);

		int cL = a.getLSB();
		int cR = b.getLSB();

		lb[cL & cR] = gb.enc(labelL[cL], labelR[cR], gid, GCSignal.ZERO);
		lb[1 - (cL & cR)] = R.xor(lb[cL & cR]);

		gtt[0 ^ cL][0 ^ cR] = lb[0];
		gtt[0 ^ cL][1 ^ cR] = lb[0];
		gtt[1 ^ cL][0 ^ cR] = lb[0];
		gtt[1 ^ cL][1 ^ cR] = lb[1];

		if (cL != 0 || cR != 0)
			gtt[0 ^ cL][0 ^ cR] = gb.enc(labelL[0], labelR[0], gid,
					gtt[0 ^ cL][0 ^ cR]);
		if (cL != 0 || cR != 1)
			gtt[0 ^ cL][1 ^ cR] = gb.enc(labelL[0], labelR[1], gid,
					gtt[0 ^ cL][1 ^ cR]);
		if (cL != 1 || cR != 0)
			gtt[1 ^ cL][0 ^ cR] = gb.enc(labelL[1], labelR[0], gid,
					gtt[1 ^ cL][0 ^ cR]);
		if (cL != 1 || cR != 1)
			gtt[1 ^ cL][1 ^ cR] = gb.enc(labelL[1], labelR[1], gid,
					gtt[1 ^ cL][1 ^ cR]);
		return lb[0];
	}

	public double t;
	private GCSignal readGateFromFile() {
		fread.read(gtt[0][1].bytes);
		fread.read(gtt[1][0].bytes);
		fread.read(gtt[1][1].bytes);
		GCSignal a = new GCSignal(fread.read(10));
		return a;
	}

	private void writeGateToFile(GCSignal a){
		gtt[0][1].send(fout);
		gtt[1][0].send(fout);
		gtt[1][1].send(fout);
		a.send(fout);
		try {
			fout.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void sendGTT() {
		try {
			Flag.sw.startGCIO();
			gtt[0][1].send(channel);
			gtt[1][0].send(channel);
			gtt[1][1].send(channel);
			Flag.sw.stopGCIO();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
public void setOffline(boolean offline) {
	   Flag.offline = offline;
	}
	

	public GCSignal and(GCSignal a, GCSignal b) {

		Flag.sw.startGC();
		GCSignal res = null;
		if (a.isPublic() && b.isPublic())
			res = ( (a.v && b.v) ? new GCSignal(true):new GCSignal(false));
		else if (a.isPublic())
			res = a.v ? b : new GCSignal(false);
		else if (b.isPublic())
			res = b.v ? a : new GCSignal(false);
		else {
			++numOfAnds;
			if(Flag.offline) {
				res = readGateFromFile();
			} else {
				res = garble(a, b);
				writeGateToFile(res);
				if (gid % 100000 == 0) {
					System.out.println(gid);
				}
			}
			sendGTT();
			gid++;
			gatesRemain = true;
		}
		Flag.sw.stopGC();
		return res;
	}

}