package io.deephaven.verify.harness;

import java.io.PrintWriter;

public class TestResults {
	static boolean wroteTestHeaders = false;
	static boolean wroteHardwareHeaders = false;
	
	static void writeTestResult(TestState test, PrintWriter out) {
		if(!wroteTestHeaders) writeTestHeaders(out);
		String csv = String.join(",", str(test.name), str(test.status()), time(test.fullRunTime()), time(test.setupTime), 
			time(test.testTime), time(test.validateTime), str(test.failureStep), str(test.failure));
		out.println(csv);	
	}
	
	static private void writeTestHeaders(PrintWriter out) {
		wroteTestHeaders = true;
		String header = String.join(",", "name", "status", "full(ms)", "setup(ms)", "test(ms)", "validate(ms)", 
			"teardown(ms)", "failureStep", "failureMsg");
		out.println(header);
	}
	
	static private String time(long tms) {
		return Long.toString(tms);
	}
	
	static private String str(Object val) {
		return (val==null)?"":('"' + val.toString().replace('"', '\'') + '"');
	}
}
