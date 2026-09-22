/* Copyright (c) 2026-2026 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.tests.train;

import org.junit.jupiter.api.*;

/**
 * Training tests for the formula table operations (e.g. udf, inline). See <code>TrainTestRunner</code> for more
 * information.
 */
public class InlineFormulaTrainTest {
    final TrainTestRunner runner = new TrainTestRunner(this);

    void setup(double staticRowFactor, double incRowFactor) {
        runner.tables(staticRowFactor, incRowFactor, "timed");
    }

    @Test
    void formulaInline() {
        setup(467, 467);
        var q = "timed.view(['New1 = (float)((num2 + num1) / 2)', 'New2 = (float)(num1 + num2)']).sum_by()";
        runner.test("InlineFormula- 2 Calcs", 1, q, "num1", "num2");
    }

}
