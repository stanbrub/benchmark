package io.deephaven.verify.util;

import static org.junit.jupiter.api.Assertions.*;
import java.util.LinkedHashSet;
import org.junit.jupiter.api.*;

public class IdsTest {
	@Test
	public void uniqueName() {
		var ids = new LinkedHashSet<String>();
		int count = 100000;
		for(int i = 0; i < count; i++) {
			ids.add(Ids.uniqueName());
		}
		assertEquals(count, ids.size(), "Wrong unique count");
		
		for(String id: ids) {
			assertTrue(id.matches("[A-Za-z0-9_-]+[.][a-f0-9]+[.][A-Za-z0-9_-]+"), "Wrong id format");
		}
	}
	
	@Test
	public void getFileSafeName() {
		assertEquals("This_is_a_test", Ids.getFileSafeName("This is a test"), "Wrong safe name");
	}
	
}
