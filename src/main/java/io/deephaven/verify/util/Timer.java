package io.deephaven.verify.util;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class Timer {
	static public long now() {
		return System.currentTimeMillis();
	}
	
	static public Timer start() {
		return new Timer();
	}
	
	final public long beginTime = now();
	
	private Timer() {
	}
	
	public Duration duration() {
		return Duration.of(now() - beginTime, ChronoUnit.MILLIS);
	}

}
