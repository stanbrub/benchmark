package io.deephaven.verify.api;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import io.deephaven.verify.util.Ids;

class QueryLog {
	final private Path parent;
	private String name;
	private Path logFile = null;
	private int logCount = 0;
	
	QueryLog(Path parent) {
		this.parent = parent;
	}
	
	Path getLogFile() {
		return logFile;
	}

	void setName(String name) {
		this.name = name;
		this.logFile = parent.resolve(Ids.getFileSafeName(name + ".query.md"));
	}
	
	void logQuery(String query) {
		if(logFile == null) throw new RuntimeException("Set a name before logging a query");
		
		try(BufferedWriter out = Files.newBufferedWriter(logFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
			if(logCount == 0) {
				out.write("# " + name);
				out.newLine();
				out.newLine();
			}
			out.write("## Query " + ++logCount);
			out.newLine();
			out.write("````");
			out.newLine();
			out.write(query);
			out.write("````");
			out.newLine();
			out.newLine();
		} catch(Exception ex) {
			throw new RuntimeException("Failed to write to query log: " + logFile, ex);
		}
	}
	
}
