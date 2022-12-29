package io.deephaven.verify.api;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.*;
import org.junit.jupiter.api.*;

public class QueryLogTest {
	
	@Test
	public void logQuery() throws Exception {
		Path outParent = Paths.get(getClass().getResource("test-profile.properties").toURI()).getParent();
		var qlog = new QueryLog(outParent);
		qlog.setName("This is a Test Name");
		Files.deleteIfExists(qlog.getLogFile());
		qlog.logQuery(
		"""
		from deephaven import agg
		from deephaven.table import Table
		from deephaven.ugp import exclusive_lock
		
		kafka_stock_trans = verify_api_kafka_consume('stock_trans', 'append')
		verify_api_await_table_size(kafka_stock_trans, 100000)
		"""
		);
		
		qlog.logQuery(
		"""
		from deephaven import agg
		
		kafka_stock_trans = verify_api_kafka_consume('stock_trans', 'append')
		verify_api_await_table_size(kafka_stock_trans, 20000})
		"""
		);
		
		var lines = Files.readAllLines(qlog.getLogFile());
		assertEquals(20, lines.size(), "Wrong file line count");
		assertEquals("# This is a Test Name", lines.get(0), "Wrong title");
		assertEquals("", lines.get(1), "Should be blank");
		assertEquals("## Query 1", lines.get(2), "Should be a query title with count");
		assertEquals("````", lines.get(3), "Should be execute quotes");
		assertEquals("from deephaven import agg", lines.get(4), "Should be import");
		assertEquals("## Query 2", lines.get(12), "Should be a query title with count");
	}
}
