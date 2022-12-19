package io.deephaven.verify.api;

import static org.junit.Assert.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.junit.*;

import io.deephaven.verify.util.Timer;

public class VerifyResultTest {
	final private Path resultFile = getResource("test-result.csv");

	@Test
	public void single() throws Exception {
		VerifyResult result = new VerifyResult("mytest", resultFile);
		Files.deleteIfExists(result.file);
		assertFalse("Result file exists: " + result.file, Files.exists(result.file));
		
		result.setup(timer(20), 1234);
		result.test(timer(123), 1234);
		result.teardown(timer(11), 1234);
		Thread.sleep(200);
		result.commit();
		
		assertTrue("Missing result file: " + result.file, Files.exists(result.file));
		
		List<String[]> csv = getResult(result);
		assertEquals("Wrong line count", 2, csv.size());
		assertEquals("Wrong header", "[name, timestamp, duration, setup, test, teardown]", Arrays.toString(csv.get(0)));
		assertEquals("Wrong name", "mytest", csv.get(1)[0]);
		assertEquals("Wrong timestamp", result.timer.beginTime, Long.parseLong(csv.get(1)[1]));
		assertTrue("Wrong duration", Float.parseFloat(csv.get(1)[2]) >= 0.200);
		assertEquals("Wrong setup rate", 61700.0f, Float.parseFloat(csv.get(1)[3]), 0.0);
		assertEquals("Wrong test rate", 10032.5205f, Float.parseFloat(csv.get(1)[4]), 0.01);
		assertEquals("Wrong teardown rate", 112181.82f, Float.parseFloat(csv.get(1)[5]), 0.01);
	}
	
	@Test
	public void multi() throws Exception {
		VerifyResult result = new VerifyResult("mytest", resultFile);
		Files.deleteIfExists(result.file);
		assertFalse("Result file exists: " + result.file, Files.exists(result.file));
		
		result.setup(timer(20), 1234);
		result.test(timer(123), 1234);
		result.teardown(timer(11), 1234);
		result.commit();
		
		result = new VerifyResult("mytest2", resultFile);
		result.setup(timer(30), 2345);
		result.test(timer(321), 2345);
		result.teardown(timer(12), 2345);
		result.commit();
		
		assertTrue("Missing result file: " + result.file, Files.exists(result.file));
		
		List<String[]> csv = getResult(result);
		assertEquals("Wrong line count", 3, csv.size());
		assertEquals("Wrong header", "[name, timestamp, duration, setup, test, teardown]", Arrays.toString(csv.get(0)));
		assertEquals("Wrong name", "mytest", csv.get(1)[0]);
		assertEquals("Wrong name", "mytest2", csv.get(2)[0]);
		
	}
	
	private Path getResource(String fileName) {
		try {
			return Paths.get(getClass().getResource("test-profile.properties").toURI()).resolveSibling(fileName);
		} catch(Exception ex) {
			throw new RuntimeException("Failed to get resource dir", ex);
		}
	}
	
	private List<String[]> getResult(VerifyResult result) throws Exception {
		return Files.lines(result.file).map(v->v.split(",")).toList();
	}
	
	private Timer timer(int millis) {
		return new DTimer(millis);
	}

	static class DTimer extends Timer {
		Duration duration;
		DTimer(int durationMillis) {
			this.duration = Duration.ofMillis(durationMillis);
		}
		
		@Override
		public Duration duration() {
			return duration;
		}
	}
	
}
