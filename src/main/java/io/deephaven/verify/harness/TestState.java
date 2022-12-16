package io.deephaven.verify.harness;

class TestState {
	static TestState create(Object testInst) {
		return new TestState(testInst.getClass().getSimpleName());
	}
	
	static TestState create(String name) {
		return new TestState(name);
	}
	
	final String name;
	long fullBeginTime = 0;
	long fullEndTime = 0;
	long setupTime = 0; 
	long testTime = 0;
	long validateTime = 0;
	long teardownTime = 0;
	String failureStep = null;
	Throwable failure = null;
	
	private TestState(String name) {
		this.name = name;
	}
	
	String status() {
		return (failureStep==null)?"Pass":"Fail";
	}
	
	long fullRunTime() {
		return fullEndTime - fullBeginTime;
	}
}