/* Copyright (c) 2026-2026 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.tests.train;

import org.junit.jupiter.api.*;

/**
 * Standard tests for the aggBy table operation. Applies basic math aggregations to table data
 */
public class AggByTrainTest {
    final TrainTestRunner runner = new TrainTestRunner(this);

    @BeforeEach
    void setup() {
        runner.setRowFactor(2);
        runner.tables("source");

        var setupStr = """
        from deephaven import agg
        
        aggs = [
           agg.sum_('Sum=num1'), agg.std('Std=num2'), agg.min_('Min=num1'), agg.max_('Max=num2'),
           agg.avg('Avg=num1'), agg.var('Var=num2'), agg.count_('num1')
        ]
        """;
        runner.addSetupQuery(setupStr);
    }

    @Test
    void mathComboAggBy7Ops0Groups() {
        var q = "source.agg_by(aggs)";
        runner.test("MathCombo-AggBy- 7 Ops No Groups", 1, q, "num1", "num2");
    }

    @Test
    void mathComboAggBy7Ops2Groups() {
        var q = "source.agg_by(aggs, by=['key1', 'key2'])";
        runner.test("MathCombo-AggBy- 7 Ops 2 Groups 10K Unique Combos ", 10100, q, "key1", "key2", "num1", "num2");
    }

}
