package io.deephaven.verify.api;

import static java.nio.file.StandardOpenOption.*;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

class QueryLog implements Closeable {
	final Object testInst;
	final Path parent;
	final Path logFile;
	final List<String> queries = new ArrayList<>();
	final boolean isTest;
	private String name = null;
	
	QueryLog(Path parent, Object testInst) {
		this.testInst = testInst;
		this.parent = parent;
		this.logFile = getLogFile(parent, testInst);
		this.isTest = isTest(testInst);
	}
	
	public void close() {
		if(queries.isEmpty()) return;
		if(!Files.exists(logFile)) {
			write("# Test Class - " + testInst.getClass().getName(), 2);
		}
		write("## Test - " + name, 2);
		for(int i = 0, n = queries.size(); i < n; i++) {
			write("### Query " + (i+1), 1);
			write("````", 1);
			write(queries.get(i), 0);
			write("````", 2);
		}
	}
	
	void setName(String name) {
		this.name = name;
	}
	
	void logQuery(String query) {
		if(name == null) throw new RuntimeException("Set a test name before logging a query");
		if(isTest) queries.add(query);
	}
	
	private void write(String text, int newLineCount) {
		try(BufferedWriter out = Files.newBufferedWriter(logFile, CREATE, APPEND)) {
			out.write(text);
			for(int i = 0; i < newLineCount; i++) out.newLine();
		} catch(Exception ex) {
			throw new RuntimeException("Failed to write to query log: " + logFile, ex);
		}
	}
	
	static Path getLogFile(Path parent, Object testInst) {
		Path logFile = parent.resolve("test-logs/" + testInst.getClass().getName() + ".query.md");
		try {
			Files.createDirectories(logFile.getParent());
			return logFile;
		} catch(Exception ex) {
			throw new RuntimeException("Failed to create query log directory" + logFile.getParent());
		}
	}
	
	static boolean isTest(Object inst) {
		for(Method m: inst.getClass().getMethods()) {
			for(Annotation a: m.getAnnotations()) {
				String str = a.toString();
				if(str.matches(".*[.]Test[(].*")) return true;
			}
		}
		return false;
	}
	
}
