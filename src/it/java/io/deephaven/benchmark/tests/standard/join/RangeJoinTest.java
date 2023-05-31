/* Copyright (c) 2022-2023 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.tests.standard.join;

import org.junit.jupiter.api.*;
import io.deephaven.benchmark.tests.standard.StandardTestRunner;

/**
 * Standard tests for the aj (As Of-Join) table operation. The first N-1 match columns are exactly matched. The last
 * match column is used to find the key values from the right table that are closest to the values in the left table
 * without going over the left value
 */
public class RangeJoinTest {
    final StandardTestRunner runner = new StandardTestRunner(this);

    @BeforeEach
    public void setup() {
        runner.setRowFactor(8);
        runner.tables("timed");
        var query = """
        right = timed.view(['event_time=timestamp', 'event_value=int5']).snapshot()
        
        from deephaven.agg import group
        aggs = [
            group(cols=["events=event_value"]),     
        ]
        """;
        runner.addSetupQuery(query);
    }

    @Test
    public void rangeJoinOn1Col4ItemVector() {
        var q = "timed.range_join(right, on=['<- timestamp <= event_time <= timestamp2 ->'], aggs=aggs)";
        runner.test("RangeJoin- Join On 1 Col 4 Item Vector", q, "timestamp", "timestamp2", "int5");
    }
    
    @Test
    public void rangeJoinOn1Col2ItemVector() {
        var q = "timed.range_join(right, on=['timestamp < event_time < timestamp2'], aggs=aggs)";
        runner.test("RangeJoin- Join On 1 Col 2 Item Vector", q, "timestamp", "timestamp2", "int5");
    }

}
