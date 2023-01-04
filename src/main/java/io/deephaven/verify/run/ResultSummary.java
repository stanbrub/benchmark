package io.deephaven.verify.run;

import static java.nio.file.StandardOpenOption.*;
import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import io.deephaven.verify.api.Verify;
import io.deephaven.verify.util.Ids;

public class ResultSummary {
	static final String headerPrefix = "run-id";
	final Path rootDir;
	final Path summaryFile;
	
	public ResultSummary(Path rootDir) {
		this.rootDir = rootDir;
		this.summaryFile = rootDir.resolve("verify-summary-results.csv");
	}

	public void summarize() {
		if(!Files.exists(rootDir)) {
			System.out.println("Skipping summary because of missing output directory: " + rootDir);
		}
		try(BufferedWriter out = Files.newBufferedWriter(summaryFile, CREATE, WRITE, TRUNCATE_EXISTING)) {
			boolean isHeaderWritten = false;
			for(Path resultFile: getResultFiles(rootDir)) {
				String runId = resultFile.getParent().getFileName().toString();
				List<String> lines = Files.lines(resultFile).toList();
				for(int i = 0, n = lines.size(); i < n; i++) {
					if(!isHeaderWritten) isHeaderWritten = writeSummaryHeader(lines.get(i), out);
					else if(i == 0) continue;
					else writeSummaryLine(runId, lines.get(i), out);
				};
			}
		} catch(Exception ex) {
			throw new RuntimeException("Failed to write summary results: " + summaryFile, ex);
		}
	}
	
	boolean writeSummaryHeader(String header, BufferedWriter out) throws Exception {
		out.write(headerPrefix + ',' + header);
		out.newLine();
		return true;
	}
	
	void writeSummaryLine(String runTimestamp, String resultLine, BufferedWriter out) throws Exception {
		out.write(runTimestamp.replace("run-", "") + ',');
		out.write(resultLine);
		out.newLine();
	}

	List<Path> getResultFiles(Path rootDir) {
		try {
			List<Path> resultFiles = new ArrayList<>();
			Files.newDirectoryStream(rootDir).forEach(d->{
				if(!Ids.isRunId(d.getFileName())) return;
				if(!Files.isDirectory(d)) return;
				Path resultFile = d.resolve(Verify.resultFileName);
				if(Files.exists(resultFile)) resultFiles.add(resultFile);
			});
			return resultFiles;
		} catch(Exception ex) {
			throw new RuntimeException("Failed to get result files from root directory: " + rootDir);
		}
	}

}
