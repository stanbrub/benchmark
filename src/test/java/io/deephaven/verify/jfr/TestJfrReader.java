package io.deephaven.verify.jfr;

import static org.junit.Assert.assertEquals;
import org.junit.*;

@Ignore
public class TestJfrReader {
	
	@Test
	public void getEventNames() {
		JfrReader jfr = new JfrReader(getClass().getResource("server.jfr"));
		assertEquals("Wrong method names", "", jfr.getEventNames().toString());
	}

}
