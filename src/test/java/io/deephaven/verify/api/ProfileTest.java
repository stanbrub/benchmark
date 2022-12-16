package io.deephaven.verify.api;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class ProfileTest {
	
	@Test
	public void property() {
		Profile profile = new Profile("test-profile.properties");
		
		assertEquals("Wrong host value", "localhost:8080", profile.property("test.host", "n/a"));
		assertEquals("Wrong nano str value", "1 nanos", profile.property("test.duration1", "n/a"));
		assertEquals("Wrong nano value", 1, profile.propertyAsDuration("test.duration1", "n/a").toNanos());
		assertEquals("Wrong millis value", 2, profile.propertyAsDuration("test.duration2", "n/a").toMillis());
		assertEquals("Wrong seconds value", 3, profile.propertyAsDuration("test.duration3", "n/a").toSeconds());
		assertEquals("Wrong minutes value", 4, profile.propertyAsDuration("test.duration4", "n/a").toMinutes());
		
		assertEquals("Wrong profile name", System.getProperty("user.dir"), profile.property("user.dir", "n/a"));
		assertEquals("Wrong default value", 11, profile.propertyAsDuration("this.prop.is.not.anywhere", "11 nanos").toNanos());
	}
	
}
