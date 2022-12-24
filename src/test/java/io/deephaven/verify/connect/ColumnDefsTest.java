package io.deephaven.verify.connect;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class ColumnDefsTest {

	@Test
	public void add() {
		ColumnDefs columnDefs = new ColumnDefs()
			.add("symbol", "string", "ABC[1-11]")
			.add("price", "float", "[100-105]")
			.add("priceAgain", "int", "[100-105]");
		
		assertEquals("Wrong def count", 3, columnDefs.columns.size());
		
		assertEquals("Wrong field count", "string", columnDefs.columns.get(0).type());
		assertEquals("Wrong field name", "symbol", columnDefs.columns.get(0).name());
		assertEquals("Wrong field maker", "StringMaker", columnDefs.columns.get(0).maker().getClass().getSimpleName());
		assertEquals("Wrong field next", "ABC6", columnDefs.nextValue(0, 1));
		
		assertEquals("Wrong field count", "float", columnDefs.columns.get(1).type());
		assertEquals("Wrong field name", "price", columnDefs.columns.get(1).name());
		assertEquals("Wrong field maker", "FloatMaker", columnDefs.columns.get(1).maker().getClass().getSimpleName());
		assertEquals("Wrong field maker", "[100.0, 101.0, 102.0, 103.0, 104.0, 105.0]", columnDefs.columns.get(1).maker().values.toString());
		assertEquals("Wrong field next", 100.0f, columnDefs.nextValue(1, 1));
		
		assertEquals("Wrong field maker", "[100, 101, 102, 103, 104, 105]", columnDefs.columns.get(2).maker().values.toString());
		assertEquals("Wrong field next", 103, columnDefs.nextValue(2, 1));
		assertEquals("Wrong field next", 105, columnDefs.nextValue(2, 0));
	}
	
	@Test
	public void getQuotedColumns() {
		ColumnDefs columnDefs = new ColumnDefs()
			.add("symbol", "string", "ABC[1-11]")
			.add("price", "float", "[100-105]")
			.add("priceAgain", "int", "[100-105]");
		
		assertEquals("Wrong field next", "\"symbol\", \"price\", \"priceAgain\"", columnDefs.getQuotedColumns());
	}
	
	@Test
	public void getMaxValueCount() {
		ColumnDefs columnDefs = new ColumnDefs()
			.add("symbol", "string", "ABC[1-10]")
			.add("price", "float", "[100-105]")
			.add("priceAgain", "int", "[100-105]");
		
		assertEquals("Wrong row count", 10, columnDefs.getMaxValueCount());
	}
	
	@Test
	public void describe() {
		ColumnDefs columnDefs = new ColumnDefs()
			.add("symbol", "string", "ABC[1-10]")
			.add("price", "float", "[100-105]")
			.add("priceAgain", "int", "[100-105]");
		
		assertEquals("Wrong toString", 
			"""
			name,type,values
			symbol,string,ABC[1-10]
			price,float,[100-105]
			priceAgain,int,[100-105]
			""", columnDefs.describe());
		
		columnDefs.setFixed();
		
		assertEquals("Wrong toString",
			"""
			name,type,values
			symbol,string,ABC[1-10]
			price,float,[100-105]
			priceAgain,int,[100-105]
			""", columnDefs.describe());
	}
	
}
