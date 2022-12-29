package io.deephaven.verify.util;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public class Filer {
	
	/**
	 * Delete the given directory recursively
	 * @param dir the directory to delete
	 */
	static public void deleteAll(Path dir) {
		try {
			if(!Files.exists(dir)) return;
			Files.walk(dir)
				.sorted(Comparator.reverseOrder())
				.map(Path::toFile).forEach(File::delete);
		} catch(Exception ex) {
			throw new RuntimeException("Failed to delete directory: " + dir);
		}
	}
	
}
