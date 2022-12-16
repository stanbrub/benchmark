package io.deephaven.verify.harness;

import java.io.PrintWriter;

class TestConfig {
	static private TestConfig inst = null;
	
	static public void set(PrintWriter out) {
		if(inst == null) inst = new TestConfig(out);
	}
	
	static public TestConfig get() {
		if(inst == null) set(new PrintWriter(System.out, true));
		return inst;
	}
	
	final PrintWriter out;
	
	TestConfig(PrintWriter out) {
		this.out = out;
	}
}
