package io.deephaven.verify.tests.query;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.*;
import io.deephaven.verify.api.Verify;

public class KafkaToParquetStreamTest {
	final Verify api = Verify.create(this);
	final long scaleRowCount = api.propertyAsIntegral("scale.row.count", "1000");
	
	@Test
	public void makeParquetFile() {
		api.table("orders").random()
			.add("symbol", "string", "SYM[1-1000]")
			.add("price", "float", "[10-20]")
			.add("qty", "int", "1")
			.generateParquet();
		
		var query = 
		"""
		from deephaven.parquet import read
		from deephaven import agg

		orders = read("/data/orders.parquet")
		result = orders.view(formulas=["qty"]).agg_by([agg.sum_("RecCount = qty")], "qty")	
		""";
		
		var tm = api.timer();
		api.query(query).fetchAfter("result", table->{
			assertEquals(scaleRowCount, table.getSum("RecCount").longValue(), "Wrong record count");
		}).execute();
		api.awaitCompletion();
		api.result().test(tm, scaleRowCount);
		
	}

	@AfterEach
	public void teardown() {
		api.close();
	}

}
