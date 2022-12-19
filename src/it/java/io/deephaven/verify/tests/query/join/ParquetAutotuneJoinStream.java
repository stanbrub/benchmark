package io.deephaven.verify.tests.query.join;

import static org.junit.Assert.*;
import org.junit.*;
import io.deephaven.verify.api.Verify;

public class ParquetAutotuneJoinStream {
	final Verify api = Verify.create(this);
	private long scaleRowCount = api.propertyAsIntegral("scale.row.count", "10000");
	
	@Before
	public void setup() {
		var tm = api.timer();
		long symCnt = 100000;
		api.table("stock_info").fixed()
			.add("symbol", "string", "SYM[1-" + symCnt + "]")
			.add("description", "string", "ABC[1-" + symCnt + "] CORP")
			.add("exchange", "string", "EXCHANGE[1-10]")
			.generateParquet();

		api.table("stock_trans").random()
			.add("symbol", "string", "SYM[1-" + symCnt + "]")
			.add("price", "float", "[100-200]")
			.add("buys", "int", "[1-100]")
			.add("sells", "int", "[1-100]")
			.generateParquet();
		
		api.result().setup(tm, scaleRowCount + symCnt);
	}

	@Test
	public void joinFromAutotuneStream() {
		var query = 
		"""
		from deephaven.time import now
		from deephaven import agg
		from deephaven.parquet import read
		from deephaven.table import Table
		from deephaven.ugp import exclusive_lock

		p_stock_info = read('/data/stock_info.parquet')
			
		p_stock_trans = read('/data/stock_trans.parquet').select(formulas=['symbol','price','buys','sells'])
		autotune = jpy.get_type('io.deephaven.engine.table.impl.select.AutoTuningIncrementalReleaseFilter')
		relation_filter = autotune(0, 1000000, 1.0, True)
			
		stock_info = p_stock_info.view(formulas=['symbol', 'description', 'exchange'])
		stock_trans = p_stock_trans.where(relation_filter).view(formulas=['symbol', 'timestamp=now()', 'price', 'buys', 'sells', 'rec_count=1'])
		
		aggs = [
			agg.avg('AvgPrice=price'), agg.min_('LowPrice=price'), agg.max_('HighPrice=price'),
			agg.sum_('Buys=buys'), agg.sum_('Sells=sells'), agg.sum_('RecordCount=rec_count') 
		]
		
		by = ['symbol', 'description', 'exchange']
		
		formulas = [
			'Symbol=symbol', 'Description=description', 'Exchange=exchange', 'AvgPrice',
			'LowPrice', 'HighPrice', 'Volume=Buys+Sells', 'RecordCount'
		]
		
		stock_volume = stock_trans.join(stock_info, on=['symbol']).agg_by(aggs, by).view(formulas)
		stock_exchange = stock_volume.agg_by([agg.sum_('Volume'), agg.sum_('RecordCount')], by=['Exchange'])
		record_count = stock_exchange.agg_by([agg.sum_('RecordCount')])
		
		relation_filter.start()
		relation_filter.waitForCompletion()
		
		""";

		var tm = api.timer();
		api.query(query).fetchAfter("record_count", table->{
			int recCount = table.getSum("RecordCount").intValue();
			assertEquals("Wrong record count", scaleRowCount, recCount);
		}).execute();
		
		api.awaitCompletion();
		api.result().test(tm, scaleRowCount);
	}

	@After
	public void teardown() {
		var tm = api.timer();
		api.close();
		api.result().teardown(tm, scaleRowCount);
	}

}
