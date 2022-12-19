package io.deephaven.verify.tests.query;

import static org.junit.Assert.*;
import org.junit.*;
import io.deephaven.verify.api.Verify;

public class KafkaToParquetStream {
	final Verify api = Verify.create(this);
	final long scaleRowCount = api.propertyAsIntegral("scale.row.count", "1000");
	
	@Test
	public void makeParquetFile() {
		var tm = api.timer();
		api.table("orders").random()
			.add("symbol", "string", "SYM[1-1000]")
			.add("price", "float", "[10-20]")
			.add("qty", "int", "1")
			.generateParquet();
		
		api.result().test(tm, scaleRowCount);
		
		var query = 
		"""
		from deephaven.parquet import read
		from deephaven import agg

		orders = read("/data/orders.parquet")
		result = orders.view(formulas=["qty"]).agg_by([agg.sum_("RecCount = qty")], "qty")	
		""";
		
		api.query(query).fetchAfter("result", table->{
			assertEquals("Wrong record count", scaleRowCount, table.getSum("RecCount").longValue());
		}).execute();
		api.awaitCompletion();
		
	}

	@After
	public void teardown() {
		api.close();
	}

}
