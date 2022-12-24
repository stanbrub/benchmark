package io.deephaven.verify.util;

import java.math.BigInteger;
import java.util.Base64;
import java.util.Random;

/**
 * Provide unique Ids for file naming
 */
public class Ids {
	static final long years50 = 1577847600000L;
	static final Random random = new Random();
	static private long count = 0;
	static private long lastTime = System.currentTimeMillis();
	
	/**
	 * Return a unique identifier (not a UUID)
	 * ex. Fd7YDsw.1.bjSAVA
	 * @return the unique name
	 */
	static public String uniqueName() {
		long now = System.currentTimeMillis();
		if(now > lastTime) {
			lastTime = now;
			count = 0;
		}
		
		String time = toBase64(now - years50);
		String cnt = Long.toHexString(++count);
		String rand = toBase64(random.nextInt());
		return time + '.' + cnt + '.' + rand;
	}
	
	static String toBase64(long num) {
		String s = Base64.getUrlEncoder().encodeToString(BigInteger.valueOf(num).toByteArray());
		return s.replace("=", "");
	}

}
