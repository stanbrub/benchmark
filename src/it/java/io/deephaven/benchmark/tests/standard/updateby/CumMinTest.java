/* Copyright (c) 2022-2024 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.tests.standard.updateby;

import org.junit.jupiter.api.*;
import io.deephaven.benchmark.tests.standard.StandardTestRunner;

/**
 * Standard tests for the updateBy table operation. Calculates a cumulative minimum for specified columns and places the
 * result into a new column for each row.
 */
public class CumMinTest {
    final StandardTestRunner runner = new StandardTestRunner(this);

    void setup(int staticRowFactor, int staticFactor, int incRowFactor, int incFactor) {
        runner.setRowFactors(staticRowFactor, incRowFactor);
        runner.setScaleFactors(staticFactor, incFactor);
        runner.tables("timed");
        runner.addSetupQuery("from deephaven.updateby import cum_min");
    }

    @Test
    void cumMin0Group1Col() {
        setup(4, 35, 4, 25);
        var q = "timed.update_by(ops=cum_min(cols=['X=num1']))";
        runner.test("CumMin- No Groups 1 Cols", q, "num1");
    }

    @Test
    void cumMin1Group1Col() {
        setup(4, 7, 4, 1);
        var q = "timed.update_by(ops=cum_min(cols=['X=num1']), by=['key1'])";
        runner.test("CumMin- 1 Group 100 Unique", q, "key1", "num1");
    }

    @Test
    void cumMin2Group1Col() {
        setup(3, 2, 3, 1);
        var q = "timed.update_by(ops=cum_min(cols=['X=num1']), by=['key1','key2'])";
        runner.test("CumMin- 2 Groups 10K Unique Vals", q, "key1", "key2", "num1");
    }

    @Test
    void cumMin3Groups1Col() {
        setup(3, 1, 1, 1);
        var q = "timed.update_by(ops=cum_min(cols=['X=num1']), by=['key1','key2','key3'])";
        runner.test("CumMin- 3 Groups 100K Unique Combos", q, "key1", "key2", "key3", "num1");
    }

}
