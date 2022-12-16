package io.deephaven.verify.harness;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

public class Integration extends BlockJUnit4ClassRunner {
	private LocalState state = null;
	
	public Integration(Class<?> klass) throws InitializationError {
		super(klass);
	}
	
	@Override
	protected Statement withBeforeClasses(Statement statement) {
		return new BenchStatement(super.withBeforeClasses(statement), "BeforeAll");
	}
	
	@Override
	protected Statement withBefores(FrameworkMethod method, Object target, Statement statement) {
		return new BenchStatement(super.withBefores(method, target, statement), "Before");
	}

    @Override
	protected Statement methodInvoker(FrameworkMethod method, Object test) {
    	if(state == null) state = new LocalState();
    	state.testName = method.getDeclaringClass().getSimpleName();
    	return new BenchStatement(super.methodInvoker(method, test), "Test");
	}
    
    @Override
	protected Statement withAfters(FrameworkMethod method, Object target, Statement statement) {
		return new BenchStatement(super.withAfters(method, target, statement), "After");
	}
    
    @Override
	protected Statement withAfterClasses(Statement statement) {
		return new BenchStatement(super.withAfterClasses(statement), "AfterAll");
	}
    
    void printBenchmarks() {
    	TestState test = TestState.create(state.testName);
    	test.fullBeginTime = state.times.get("BeforeBeginTime");
    	test.fullEndTime = state.times.get("AfterEndTime");
    	test.setupTime = state.times.get("TestBeginTime") - state.times.get("BeforeBeginTime");
    	test.testTime = state.times.get("TestEndTime") - state.times.get("TestBeginTime");
    	test.teardownTime = state.times.get("AfterEndTime") - state.times.get("BeforeEndTime");
    	TestResults.writeTestResult(test, new PrintWriter(System.out, true));
    }

    static long mark() {
		return System.nanoTime() / 1000000; //System.currentTimeMillis();
	}
    
    static class LocalState {
    	String testName = null;
    	Map<String,Long> times = new HashMap<>();
    	
    	void addBenchmark(String step, long beginTime, long endTime) {
    		times.put(step + "BeginTime", beginTime);
    		times.put(step + "EndTime", endTime);
    	}
    }

    class BenchStatement extends Statement {
    	final Statement delegate;
    	final String step;
    	
    	BenchStatement(Statement delegate, String step) {
    		this.delegate = delegate;
    		this.step = step;
    	}
    	
		@Override
		public void evaluate() throws Throwable {
			if(state == null) state = new LocalState();
			long beginTime = mark();
			try {
				System.out.println("Start: " + step);
				delegate.evaluate();
				System.out.println("End: " + step);
			} finally {
				state.addBenchmark(step, beginTime, mark());
				if(step.equals("After")) printBenchmarks();
			}
		}
 
    }
    
}
