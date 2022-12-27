package io.deephaven.verify.connect;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class CsvTableTest {
	String csv = 
		"""
		RowPosition|     RowKey|    symbol|            AvgPrice|     Total|            RecCount
		----------+----------+----------+--------------------+----------+--------------------
	         0|         0|GIS       |                 5.0|      10.0|                   2
	         1|         1|AAPL      |                 6.0|      10.0|                   2
	         2|         2|MSFT      |                 7.0|      14.0|                   2
		""";
	
	@Test
	public void fromBarrageCsv() {
		CsvTable table = new CsvTable(csv, "|");
		
		assertEquals("Wrong columns", "[RowPosition, RowKey, symbol, AvgPrice, Total, RecCount]", table.getColumnNames().toString());
		assertEquals("Wrong row count", 3, table.getRowCount());
		assertEquals("Wrong row value", "0", table.getValue(0, "RowPosition"));
		assertEquals("Wrong row value", "MSFT", table.getValue(2, "symbol"));
		assertEquals("Wrong sum agg", 18.0, table.getSum("AvgPrice"));
	}
	
	@Test
	public void findRows() {
		CsvTable table = new CsvTable(csv, "|");
		ResultTable other = table.findRows("Total", "10.0");
		assertEquals("Wrong row count", 2, other.getRowCount());
		
		other = table.findRows("Total", "14.0");
		assertEquals("Wrong row count", 1, other.getRowCount());
		assertEquals("Wrong row key", "MSFT", other.getValue(0, "symbol"));
	}

}
