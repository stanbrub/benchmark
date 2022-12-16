package io.deephaven.verify.api;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.*;
import io.deephaven.verify.util.Log;
import io.deephaven.verify.util.Timer;

/**
 * Represents the results of a query instance. Results are collected for each and
 * appended to a CSV file when the API is closed after each test. The results focus 
 * on rates for steps in a test like setup, test, teardown.
 */
final public class VerifyResult {
	static final String resultFileName = "verify-results.csv";
	static final String[] header = {"name", "timestamp", "duration", "setup", "test", "teardown"};
	static Path resultFile = null;
	final String name;
	final Timer timer;
	final Map<String,Object> rates;
	final Path file;

	VerifyResult(String name) {
		this(name, initializeResultPath(".", resultFileName));
	}
	
	VerifyResult(String name, Path resultFile) {
		this.name = name;
		this.file = resultFile;
		this.timer = Timer.start();
		this.rates = new LinkedHashMap<>();
		initializeRates(rates);
	}

	/**
	 * Record a setup rate for this result instance
	 * @param duration the run duration for setup
	 * @param count the processed item count (e.g. rowCount) 
	 * @return this result instance
	 */
	public VerifyResult setup(Duration duration, long count) {
		rates.put("setup", new Rate(duration, count));
		return this;
	}
	
	/**
	 * Record a test rate for this result instance
	 * @param duration the run duration for test
	 * @param count the processed item count (e.g. rowCount) 
	 * @return this result instance
	 */
	public VerifyResult test(Duration duration, long count) {
		rates.put("test", new Rate(duration, count));
		return this;
	}
	
	/**
	 * Record a teardown rate for this result instance
	 * @param duration the run duration for teardown
	 * @param count the processed item count (e.g. rowCount) 
	 * @return this result instance
	 */
	public VerifyResult teardown(Duration duration, long count) {
		rates.put("teardown", new Rate(duration, count));
		return this;
	}
	
	/**
	 * Save the collected results a csv file
	 */
	public void commit() {
		List<String> head = Arrays.stream(header).toList();
		if(!hasHeader()) writeLine(head, file);
		
		var m = new HashMap<String,Object>(rates);
		m.put("name", name);
		m.put("timestamp", timer.beginTime);
		m.put("duration", toSeconds(timer.duration()));
		Log.info("Result: %s", m);
		writeLine(head.stream().map(h->m.get(h)).toList(), file);
		initializeRates(rates);
	}
	
	private boolean hasHeader() {
		return Files.exists(file);
	}
	
	private void initializeRates(Map<String,Object> rates) {
		Arrays.stream(header).forEach(h->rates.put(h, 0));		// Preserve key order
	}
	
	static void writeLine(Collection<?> values, Path file) {
		try(BufferedWriter out = Files.newBufferedWriter(file, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
			out.write(String.join(",", values.stream().map(v->v.toString()).toList()));
			out.newLine();
		} catch(Exception ex) {
			throw new RuntimeException("Failed to write result to file: " + file, ex);
		}
	}
	
	// Use toMillis() because toSeconds() loses the fraction
	static float toSeconds(Duration duration) {
		return duration.toMillis() / 1000.0f;
	}
	
	static Path initializeResultPath(String dir, String resultFileName) {
		if(resultFile != null) return resultFile;
		
		resultFile = Paths.get(dir, resultFileName).toAbsolutePath();
		try {
			Files.deleteIfExists(resultFile); 
			writeLine(Arrays.stream(header).toList(), resultFile);
			return resultFile;
		} catch(Exception ex) {
			throw new RuntimeException("Failed to delete result file: " + resultFile, ex);
		}
	}
	
	static record Rate(Duration duration, long rowCount) {
		@Override
		public String toString() {
			return "" + (rowCount / toSeconds(duration));
		}
	}

}
