package ot;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;

import org.junit.Test;

import com.oblivm.backend.ot.Cipher;

public class TestCipher {

	@Test
	public void test() {
		Cipher cipher = new Cipher();
		
		BigInteger m = new BigInteger(1, new byte[]{0, 51, -81, -8, -31, -11, -63, -33, 119, 88});
		BigInteger key = new BigInteger("64262963087917280318431012909038837736482277929104660937487767756585560270313423100003447879793943534544898299133874580590962268751730143486877768478515769127147932418781496095196661929062654753004661130108482201546032073987859299216292408047794239234079919579897865647529268432034346756682537588983138342000");
		BigInteger c = cipher.encrypt(key.toByteArray(), m, 80);
//		System.err.println(Arrays.toString(m.toByteArray()));
		BigInteger mp = cipher.decrypt(key.toByteArray(), c, 80);
//		System.out.println(Arrays.toString(mp.toByteArray()));
		assertEquals(m, mp);
	}

}
