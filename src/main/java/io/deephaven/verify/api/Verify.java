package io.deephaven.verify.api;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import io.deephaven.verify.util.Log;
import io.deephaven.verify.util.Metrics;
import io.deephaven.verify.util.Timer;

/**
 * The root accessor class for the API.  Use <code>Verify.create(this)</code> in a typical
 * JUnit test to start things off
 */
final public class Verify {
	static final Profile profile = new Profile();
	static final Platform platform = new Platform();

	static public Verify create(Object testInst) {
		return create(testInst.getClass().getSimpleName());
	}
	
	static public Verify create(String name) {
		setSessionTimeout();
		Verify v = new Verify();
		v.setName(name);
		return v;
	}

	final VerifyResult result;
	final List<Future<Metrics>> futures = new ArrayList<>();
	final List<Closeable> closeables = new ArrayList<>();
	final List<Metrics> metrics = new ArrayList<>();
	
	Verify() {
		this.result = new VerifyResult();
	}
	
	public void setName(String name) {
		if(name == null || name.isBlank()) throw new RuntimeException("No blank Verify names allowed");
		this.result.setName(name);
	}
	
	/**
	 * Get a property from the profile, System, Environment or return a default value
	 * @param name the property name
	 * @param defaultValue value to return if the property does not exist
	 * @return the property value or default
	 */
	public String property(String name, String defaultValue) {
		return profile.property(name, defaultValue);
	}
	
	/**
	 * Get an integral property from the profile, System, Environment or return a default value
	 * @param name the property name
	 * @param defaultValue value to return if the property does not exist
	 * @return the property value or default
	 */
	public long propertyAsIntegral(String name, String defaultValue) {
		return profile.propertyAsIntegral(name, defaultValue);
	}
	
	/**
	 * Get an integral property from the profile, System, Environment or return a default value. 
	 * Values are specified to match the following regular expression:<p/>
	 * <code>[0-9]+ ( nanos | nano | millis | milli | seconds | second | minutes | minute )</code>
	 * @param name the property name
	 * @param defaultValue value to return if the property does not exist
	 * @return the property value or default
	 */
	public Duration propertyAsDuration(String name, String defaultValue) {
		return profile.propertyAsDuration(name, defaultValue);
	}

	/**
	 * Start configuring a table
	 * @param name the name of the table
	 * @return a table configuration instance
	 */
	public VerifyTable table(String name) {
		return addCloseable(new VerifyTable(this, name));
	}
	
	/**
	 * Start configuring a query
	 * @param logic the query logic that will be executed through a session
	 * @return a query configuration instance
	 */
	public VerifyQuery query(String logic) {
		return addCloseable(new VerifyQuery(this, logic));
	}
	
	/**
	 * Wait for all previously executed asynchronous tasks (e.g. generators, queries) to finish
	 * before moving on
	 */
	public void awaitCompletion() {
		for(Future<Metrics> f: futures) {
			awaitCompletion(f);
		}
		futures.clear();
	}
	
	/**
	 * Starts and returns a timer
	 * @return a timer
	 */
	public Timer timer() {
		return Timer.start();
	}
	
	/**
	 * Get the result for this Verify instance (e.g. test) used for collecting rates
	 * @return the result instance
	 */
	public VerifyResult result() {
		return result;
	}
	
	/**
	 * Finish all running tasks (e.g. queries, generators), close any I/O, and append any
	 * results to the file system
	 */
	public void close() {
		for(Closeable c: closeables) {
			try {
				c.close();
			} catch(Exception ex) {
				throw new RuntimeException("Failed to close: " + c.getClass().getSimpleName(), ex);
			}
		}
		closeables.clear();
		result.commit();
		platform.ensureCommit();
	}
	
	Metrics awaitCompletion(Future<Metrics> future) {
		try {
			long secs = propertyAsDuration("default.completion.timeout", "5 minutes").toSeconds();
			Metrics m = future.get(secs, TimeUnit.SECONDS);
			metrics.add(m);
			Log.info("Metrics: %s", m);
			return m;
		} catch(Exception ex) {
			throw new RuntimeException("Timed out waiting for completion", ex);
		}
	}
	
	<T extends Closeable> T addCloseable(T closeable) {
		closeables.add(closeable);
		return closeable;
	}
	
	<T extends Future<Metrics>> T addFuture(T future) {
		futures.add(future);
		return future;
	}
	
	static private void setSessionTimeout() {
		Duration timeout = profile.propertyAsDuration("default.completion.timeout", "5 minutes");
		System.setProperty("deephaven.session.executeTimeout", timeout.toString());
	}

}
