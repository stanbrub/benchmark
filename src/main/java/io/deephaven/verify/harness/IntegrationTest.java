package io.deephaven.verify.harness;

import java.util.ArrayList;
import java.util.List;

import org.junit.*;

abstract public class IntegrationTest {
	final private TestState state = TestState.create(this);

	abstract public void setup();
	abstract public void test();
	abstract public void validate();
	abstract public void teardown();
	
	@Before
	final public void setup0() throws Throwable {
		try {
			long beginTime = state.fullBeginTime = mark();
			setup();
			state.setupTime = mark() - beginTime;
		} catch(Throwable ex) {
			state.failureStep = "setup";
			state.failure = ex;
			throw removeThisTrace(ex);
		}
	}
	
	@Test
	final public void localRun() throws Throwable {
		try {
			long beginTime = mark();
			test();
			state.testTime = mark() - beginTime;
		} catch(Throwable ex) {
			state.failureStep = "test";
			state.failure = ex;
			throw removeThisTrace(ex);
		}
		
		try {
			long beginTime = mark();
			validate();
			state.validateTime = mark() - beginTime;
		} catch(Throwable ex) {
			state.failureStep = "validate";
			state.failure = ex;
			throw removeThisTrace(ex);
		}
	}

	@After
	final public void teardown0() throws Throwable {
		try {
			long beginTime = mark();
			teardown();
			state.teardownTime = mark() - beginTime;
			state.fullEndTime = mark();
		} catch(Throwable ex) {
			state.failureStep = "teardown";
			state.failure = ex;
			throw removeThisTrace(ex);
		}
		TestResults.writeTestResult(state, TestConfig.get().out);
	}
	
	private long mark() {
		return System.currentTimeMillis();
	}
	
	private Throwable removeThisTrace(Throwable ex) {
		StackTraceElement[] trace = ex.getStackTrace();
		List<StackTraceElement> newTrace = new ArrayList<>(trace.length);
		for(int i = 0; i < trace.length; i++) {
			if(trace[i].getClassName().contains("IntegrationTest")) continue;
			newTrace.add(trace[i]);
		}
		trace = newTrace.toArray(new StackTraceElement[newTrace.size()]);
		ex.setStackTrace(trace);
		return ex;
	}
}
