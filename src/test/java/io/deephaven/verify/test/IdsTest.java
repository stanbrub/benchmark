package io.deephaven.verify.test;

import static org.junit.Assert.*;
import java.util.LinkedHashSet;
import org.junit.Test;
import io.deephaven.verify.util.Ids;

public class IdsTest {
	@Test
	public void uniqueName() {
		var ids = new LinkedHashSet<String>();
		int count = 100000;
		for(int i = 0; i < count; i++) {
			ids.add(Ids.uniqueName());
		}
		assertEquals("Wrong unique count", count, ids.size());
		
		for(String id: ids) {
			assertTrue("Wrong id format", id.matches("[A-Za-z0-9_-]+[.][a-f0-9]+[.][A-Za-z0-9_-]+"));
		}
	}
	
}
