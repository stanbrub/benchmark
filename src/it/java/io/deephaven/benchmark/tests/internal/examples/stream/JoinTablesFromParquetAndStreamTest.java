/* Copyright (c) 2022-2023 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.tests.internal.examples.stream;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.*;
import io.deephaven.benchmark.api.Bench;

/**
 * Join two tables that are read from parquet files. Demonstrates how to configure the tables, generate parquet files,
 * read parquet into Deephaven tables, and query the tables. (These queries are the same as
 * {@link JoinTablesFromKafkaStreamTest})
 */
public class JoinTablesFromParquetAndStreamTest {
    final Bench api = Bench.create(this);
    private long scaleRowCount = api.propertyAsIntegral("scale.row.count", "10000");

    @BeforeEach
    public void setup() {
        api.table("stock_info").fixed()
                .add("symbol", "string", "SYM[1-10000]")
                .add("description", "string", "ABC[1-10000] CORP")
                .add("exchange", "string", "EXCHANGE[1-10]")
                .generateParquet();

        api.table("stock_trans")
                .add("symbol", "string", "SYM[1-10000]")
                .add("price", "float", "[100-200]")
                .add("buys", "int", "[1-100]")
                .add("sells", "int", "[1-100]")
                .generateParquet();
    }

    /**
     * Join two tables from parquet files generated by the test where one table is fixed size and the other is scaled:
     * <ol>
     * <li>Generate two parquet files; stock_info and stock_trans</li>
     * <li>Read two corresponding tables in Deephaven Engine the from parquet files
     * <li>Join the two tables and do some aggregations</li>
     * <li>End the query when all data has been consumed and aggregated</li>
     * </ol>
     * Properties (e.g. ${kafka.consumer.addr}) are automatically filled in during query execution.
     */
    @Test
    public void joinTwoTablesFromParquetViews() {
        api.setName("Join Two Tables Using Parquet File Views");

        var query = """
        from deephaven.time import now
        from deephaven import agg
        from deephaven.parquet import read

        p_stock_info = read('/data/stock_info.parquet')
        p_stock_trans = read('/data/stock_trans.parquet')

        stock_info = p_stock_info.view(formulas=['symbol', 'description', 'exchange'])
        stock_trans = p_stock_trans.view(formulas=['symbol', 'timestamp=now()', 'price', 'buys', 'sells', 'rec_count=1'])

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
        """;

        var tm = api.timer();
        api.query(query).fetchAfter("record_count", table -> {
            int recCount = table.getSum("RecordCount").intValue();
            assertEquals(scaleRowCount, recCount, "Wrong record count");
        }).execute();

        api.awaitCompletion();
        api.result().test("deephaven-engine", tm, scaleRowCount);
    }

    /**
     * Join two tables from incremental release of parquet files generated by the test where one table is fixed size and
     * the other is scaled:
     * <ol>
     * <li>Generate two parquet files; stock_info and stock_trans</li>
     * <li>Read two corresponding tables in Deephaven Engine through autotune incremental release
     * <li>Join the two tables and do some aggregations</li>
     * <li>End the query when all data has been consumed and aggregated</li>
     * </ol>
     * Properties (e.g. ${kafka.consumer.addr}) are automatically filled in during query execution.
     */
    @Test
    public void joinTwoTablesFromParquetFileIncRelease() {
        api.setName("Join Two Tables Using Incremental Release of Parquet File Records");

        var query = """
        from deephaven.time import now
        from deephaven import agg
        from deephaven.parquet import read

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
        api.query(query).fetchAfter("record_count", table -> {
            int recCount = table.getSum("RecordCount").intValue();
            assertEquals(scaleRowCount, recCount, "Wrong record count");
        }).execute();

        api.awaitCompletion();
        api.result().test("deephaven-engine", tm, scaleRowCount);
    }

    @AfterEach
    public void teardown() {
        api.close();
    }

}
