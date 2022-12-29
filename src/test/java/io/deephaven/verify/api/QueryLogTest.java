package io.deephaven.verify.api;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.*;
import org.junit.jupiter.api.*;

public class QueryLogTest {
	
	@Test
	public void logQuery() throws Exception {
		Path outParent = Paths.get(getClass().getResource("test-profile.properties").toURI()).getParent();
		Files.deleteIfExists(QueryLog.getLogFile(outParent, this));
		
		var qlog = new QueryLog(outParent, this);
		qlog.setName(getClass().getSimpleName());
		qlog.logQuery(
		"""
		setup test
		"""
		);
		qlog.setName("1st Test");
		qlog.logQuery(
		"""
		query1 
		query line
		"""
		);
		
		qlog.logQuery(
		"""
		query2
		query line
		"""
		);
		
		qlog.close();
		
		
		var expected = 
		"""
		# Test Class - io.deephaven.verify.api.QueryLogTest

		## Test - 1st Test
		
		### Query 1
		````
		setup test
		````
		
		### Query 2
		````
		query1
		query line
		````
		
		### Query 3
		````
		query2
		query line
		````	
		""".trim().replace("\r", "");
		var text = new String(Files.readAllBytes(qlog.logFile)).trim().replace("\r", "");
		assertEquals(expected, text);
		
	}

}
