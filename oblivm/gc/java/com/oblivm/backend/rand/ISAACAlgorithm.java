package com.oblivm.backend.rand;

/**
 * <h3>ISAAC: a fast cryptographic pseudo-random number generator</h3>
 * 
 * ISAAC (Indirection, Shift, Accumulate, Add, and Count) generates 32-bit random numbers.<br>
 * ISAAC has been designed to be cryptographically secure and is inspired by RC4.<br>
 * Cycles are guaranteed to be at least 2<sup>40</sup> values long, and they are 2<sup>8295</sup>
 * values long on average.<br>
 * The results are uniformly distributed, unbiased, and unpredictable unless you know the seed.<br>
 * <br> 
 * This is the original implementation by Bob Jenkins with some minor changes.<br>
 * <br>
 * <b>Changelog:</b><br>
 * <ul>
 * <li>050325: Added <code>supplementSeed(int[] seed )</code> method; made all variables private</li>
 * <li>050320: Use <code>System.arraycopy()</code> in <code>ISAACAlgorithm(int[] seed)</code></li>
 * <li>980224: Translate to Java</li>
 * <li>970719: Use context, not global variables, for internal state</li>
 * <li>960327: Creation (addition of randinit, really)</li>
 * </ul>
 * 
 * @author  Bob Jenkins
 * @version 050325
 */
class ISAACAlgorithm {
	private static final int SIZEL = 8;					// Log of size of rsl[] and mem[]
	private static final int SIZE  = 1 << SIZEL;		// Size of rsl[] and mem[]
	private static final int MASK  = (SIZE - 1) << 2;	// For pseudorandom lookup
	private              int count;						// Count through the results in rsl[]
    private              int rsl[];						// The results given to the user
	private              int mem[];						// The internal state
	private              int a;							// Accumulator
	private              int b;							// The last result
	private              int c;							// Counter, guarantees cycle is at least 2^40
	
	/**
	 * This constructor creates and initializes an new instance without using a seed.<br>
	 * Equivalent to <code>randinit(ctx,FALSE)</code> in the C implementation.
	 */
	ISAACAlgorithm () {
		mem = new int[SIZE];
		rsl = new int[SIZE];
		
		init (false);
	}
	
	/**
	 * This constructor creates and initializes an new instance using a user-provided seed.<br>
	 * Equivalent to <code>randinit(ctx, TRUE)</code> after putting seed in <code>randctx</code>
	 * in the C implementation.
	 * 
	 * @param seed The seed.
	 */
	ISAACAlgorithm (int[] seed) {
		mem = new int[SIZE];
		rsl = new int[SIZE];
		
		// This is slow and throws an ArrayIndexOutOfBoundsException if seed.length > rsl.length ...
		/* for (int i = 0; i < seed.length; ++i) rsl[i] = seed[i]; */
		// ... this is faster and safe:
		System.arraycopy (seed, 0, rsl, 0, (seed.length <= rsl.length) ? seed.length : rsl.length);
		
		init (true);
	}
	
	/**
	 * Generate 256 results.<br>
	 * This is a small (not fast) implementation.
	 */
	private final void isaac () {
		int i, x, y;
		
		b += ++c;
		for (i = 0; i < SIZE; ++i) {
			x = mem[i];
			switch (i & 3) {
				case 0: a ^= a <<  13; break;
				case 1: a ^= a >>>  6; break;
				case 2: a ^= a <<   2; break;
				case 3: a ^= a >>> 16; break;
			}
			a += mem[(i + SIZE / 2) & (SIZE - 1)];
			mem[i] = y = mem[((x         ) & MASK) >> 2] + a + b;
			rsl[i] = b = mem[((y >> SIZEL) & MASK) >> 2] + x;
		}
	}
	
	/**
	 * Initialize or reinitialize this instance.
	 * 
	 * @param flag If <code>true</code> then use the seed (which the constructor placed in
	 *             <code>rsl[]</code>) for initialization.
	 */
	private final void init (boolean flag) {
		int i;
		int a, b, c, d, e, f, g, h;
		a = b = c = d = e = f = g = h = 0x9e3779b9;		// The golden ratio
		
		for (i = 0; i < 4; ++i) {
			a ^= b <<  11; d += a; b += c;
			b ^= c >>>  2; e += b; c += d;
			c ^= d <<   8; f += c; d += e;
			d ^= e >>> 16; g += d; e += f;
			e ^= f <<  10; h += e; f += g;
			f ^= g >>>  4; a += f; g += h;
			g ^= h <<   8; b += g; h += a;
			h ^= a >>>  9; c += h; a += b;
		}
		
		for (i = 0; i < SIZE; i += 8) {		// Fill in mem[] with messy stuff
			if (flag) {
				a += rsl[i    ]; b += rsl[i + 1]; c += rsl[i + 2]; d += rsl[i + 3];
				e += rsl[i + 4]; f += rsl[i + 5]; g += rsl[i + 6]; h += rsl[i + 7];
			}
			a ^= b <<  11; d += a; b += c;
			b ^= c >>>  2; e += b; c += d;
			c ^= d <<   8; f += c; d += e;
			d ^= e >>> 16; g += d; e += f;
			e ^= f <<  10; h += e; f += g;
			f ^= g >>>  4; a += f; g += h;
			g ^= h <<   8; b += g; h += a;
			h ^= a >>>  9; c += h; a += b;
			mem[i    ] = a; mem[i + 1] = b; mem[i + 2] = c; mem[i + 3] = d;
			mem[i + 4] = e; mem[i + 5] = f; mem[i + 6] = g; mem[i + 7] = h;
		}
		
		if (flag) {							// Second pass: makes all of seed affect all of mem[]
			for (i = 0; i < SIZE; i += 8) {
				a += mem[i    ]; b += mem[i + 1]; c += mem[i + 2]; d += mem[i + 3];
				e += mem[i + 4]; f += mem[i + 5]; g += mem[i + 6]; h += mem[i + 7];
				a ^= b <<  11; d += a; b += c;
				b ^= c >>>  2; e += b; c += d;
				c ^= d <<   8; f += c; d += e;
				d ^= e >>> 16; g += d; e += f;
				e ^= f <<  10; h += e; f += g;
				f ^= g >>>  4; a += f; g += h;
				g ^= h <<   8; b += g; h += a;
				h ^= a >>>  9; c += h; a += b;
				mem[i    ] = a; mem[i + 1] = b; mem[i + 2] = c; mem[i + 3] = d;
				mem[i + 4] = e; mem[i + 5] = f; mem[i + 6] = g; mem[i + 7] = h;
			}
		}
		
		isaac ();
		count = SIZE;
	}
	
	/**
	 * Get a random integer value.
	 */
	final int nextInt () {
		if (0 == count--) {
			isaac ();
			count = SIZE - 1;
		}
		
		return (rsl[count]);
	}
	
	/**
	 * Reseeds this random object.<br>
	 * The given seed supplements (using bitwise xor), rather than replaces, the existing seed.
	 * 
	 * @param seed An integer array containing the seed.
	 */
	final void supplementSeed (int[] seed) {
		for (int i = 0; i < seed.length; i++)
			mem[i % mem.length] ^= seed[i];
	}
}
