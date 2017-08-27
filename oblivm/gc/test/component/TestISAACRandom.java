package component;

import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.Security;

import com.oblivm.backend.rand.ISAACProvider;

public class TestISAACRandom {
	public static void main(String[] args) throws NoSuchAlgorithmException {
		// Add the provider to make Java aware of ISAAC
		Security.addProvider(new ISAACProvider());

		// Instanciate ISAAC
		SecureRandom isaac = SecureRandom.getInstance("ISAACRandom");

		// The engine's constructor (which handles all calls to the algorithm)
		// auto-seeds
		// the new instance. Setting the seed is optional. See ISAACEngine.
		// Two instances will never (unless SecureRandom's getSeed method
		// returns the same data)
		// return the same random data. Try running this demo twice and compare
		// the output...
		//
		// isaac.setSeed (new byte[] {1, 2, 3, 4, 5});

		// Fetch some random data
		for (int i = 0; i < 10; i++) {
			for (int j = 0; j < 10; j++) {
				// Get 4 fresh random bytes...
				byte[] random = new byte[4];
				isaac.nextBytes(random);

				// ... and make an integer from these bytes ...
				int randomInt = new BigInteger(random).intValue();

				// "Prettyfy" the integer
				String output = Integer.toHexString(randomInt);
				while (output.length() < 8)
					output = "0" + output;

				// Print it
				System.out.print(output + " ");
			}
			System.out.println("");
		}
	}
}
