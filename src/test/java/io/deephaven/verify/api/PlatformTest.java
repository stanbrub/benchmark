package io.deephaven.verify.api;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.*;
import io.deephaven.verify.connect.CsvTable;
import io.deephaven.verify.connect.ResultTable;

public class PlatformTest {

	@Test
	public void ensureCommit() throws Exception {
		Path outFile = Paths.get(getClass().getResource("test-profile.properties").toURI()).resolveSibling("platform-test.out");
		var platform = new LocalPlatform(outFile);
		platform.ensureCommit();
		
		var lines = Files.readAllLines(outFile);
		assertEquals(15, lines.size(), "Wrong row count");
		assertEquals("node,name,value", lines.get(0), "Wrong header");
		assertTrue(lines.get(1).matches("test-runner,java.version,[0-9.]+"), "Wrong values: " + lines.get(1));
		assertTrue(lines.get(3).matches("test-runner,java.class.version,[0-9.]+"), "Wrong values: " + lines.get(3));
		assertTrue(lines.get(7).matches("test-runner,java.max.memory,[0-9]+[.][0-9]{2}G"), "Wrong values: " + lines.get(7));
		assertEquals("deephaven-engine,java.version,17.0.5", lines.get(8), "Wrong values");
		assertEquals("deephaven-engine,java.class.version,61.0", lines.get(10), "Wrong values");
		assertEquals("deephaven-engine,java.max.memory,42.00G", lines.get(14), "Wrong values");
	}
	
	
	static class LocalPlatform extends Platform {
		LocalPlatform(Path file) {
			super(file);
		}
		
		@Override
		protected ResultTable fetchResult(String query) {
			var csv = 
			"""
			Type|Key|Value
			----+---+-----
			runtime-mx.sys-props|os.name|Linux                                                                                               
			runtime-mx.sys-props|os.version|5.15.79.1-microsoft-standard-WSL2                                                                   
			runtime-mx.sys-props|java.vm.name|OpenJDK 64-Bit Server VM                                                                            
			runtime-mx.sys-props|java.version|17.0.5                                                                                              
			runtime-mx.sys-props|java.class.version|61.0                                                                                                
			memory-mx.heap|max|45097156608                                                                                         
			system-info.cpu|logical|12 		
			""";
			return new CsvTable(csv, "|");
		}
	}
	
}
