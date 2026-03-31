/* Copyright (c) 2026-2026 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.tests.train;

import org.junit.jupiter.api.*;

/**
 * Standard tests for the whereIn table operation. Filters rows of data from the source table where the rows match
 * column values in the filter table.
 */
@Tag("Iterate")
public class FilterTrainTest {
    final TrainTestRunner runner = new TrainTestRunner(this);

    void setup(double rowFactor) {
        runner.tables(rowFactor, "timed");
        var setup = """
        from deephaven.column import string_col, int_col
        where_filter = new_table([
        	string_col("set1", ['1', '2', '3', '4', '5', '6', '7', '8']),
        	string_col("set2", ['10', '20', '30', '40', '50', '60', '70', '80']),
        	int_col("set3", [-1, -2, -3, -4, 1, 2, 3, 4])
        ])
        """;
        runner.addSetupQuery(setup);
    }

    @Test
    void filter1Col() {
        setup(390);
        var q = "timed.where_in(where_filter, cols=['key1 = set1']).where(['key1 < `4`'])";
        runner.test("Filter- 1 Col", 0, q, "key1", "num1");
    }

    @Test
    void filter3Cols() {
        setup(390);
        var q = """
        timed.where_in(where_filter, cols=['key1 = set1', 'key2 = set2', 'key3 = set3']) \
            .where(filters=["key1 = '1'", "key2 < '100'", "key3 in -2, -1, 0, 1, 2"])
        """;
        runner.test("Filter- 3 Cols", 0, q, "key1", "key2", "key3", "num1");
    }

}
