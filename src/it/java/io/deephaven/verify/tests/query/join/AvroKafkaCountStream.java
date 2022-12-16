package io.deephaven.verify.tests.query.join;

import static org.junit.Assert.*;
import org.junit.*;
import io.deephaven.verify.api.Verify;
import io.deephaven.verify.util.Timer;

public class AvroKafkaCountStream {
	final Verify api = Verify.create(this);
	private long scaleRowCount = api.propertyAsIntegral("scale.row.count", "1000");
	
	@Before
	public void setup() {
		long symCnt = 100000;
		api.table("stock_info").fixed()
			.add("symbol", "string", "SYM[1-" + symCnt + "]")
			.add("description", "string", "ABC[1-" + symCnt + "] CORP")
			.add("exchange", "string", "EXCHANGE[1-10]")
			.generateAvro();
		
		api.awaitCompletion();
		
		api.table("stock_trans").random()
			.add("symbol", "string", "SYM[1-" + symCnt + "]")
			.add("price", "float", "[100-200]")
			.add("buys", "int", "[1-100]")
			.add("sells", "int", "[1-100]")
			.generateAvro();
	}

	@Test
	public void joinFromKafkaStream() {
		var query = 
		"""
		from deephaven import agg
		from deephaven import kafka_consumer as kc
		from deephaven.stream.kafka.consumer import TableType, KeyValueSpec
		from deephaven.table import Table
		from deephaven.ugp import exclusive_lock
			
		kafka_stock_trans = kc.consume(
			{ 'bootstrap.servers' : '${kafka.consumer.addr}', 'schema.registry.url' : 'http://${schema.registry.addr}' },
			'stock_trans', partitions=None, offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
			key_spec=KeyValueSpec.IGNORE, value_spec=kc.avro_spec('stock_trans_record', schema_version='1'),
			table_type=TableType.append())

		record_count = kafka_stock_trans.view(formulas=['rec_count=1']).agg_by([agg.sum_('RecordCount=rec_count')])
		
		def wait_ticking_table_update(table: Table, row_count: int):
			with exclusive_lock():
				recCount = 0
				recColumn = table.j_table.getColumnSource('RecordCount')
				while recCount < row_count:
					table.j_table.awaitUpdate()
					recCount = recColumn.get(0)

		wait_ticking_table_update(record_count, ${scale.row.count})
		
		""";

		var tm = Timer.start();
		api.query(query).fetchAfter("record_count", table->{
			int recCount = table.getSum("RecordCount").intValue();
			assertEquals("Wrong record count", scaleRowCount, recCount);
		}).execute();
		api.awaitCompletion();
		api.result().test(tm.duration(), scaleRowCount);
	}

	@After
	public void teardown() {
		var tm = Timer.start();
		api.close();
		api.result().teardown(tm.duration(), scaleRowCount);
	}

}
