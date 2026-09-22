/* Copyright (c) 2026-2026 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.tests.train;

import org.junit.jupiter.api.*;

/**
 * Training tests for the formula table operations (e.g. udf, inline). See <code>TrainTestRunner</code> for more
 * information.
 */
public class UserFormulaTrainTest {
    final TrainTestRunner runner = new TrainTestRunner(this);

    void setup(double staticRowFactor, double incRowFactor) {
        runner.tables(staticRowFactor, incRowFactor, "timed");
    }

    @Test
    void formulaUdf() {
        setup(9, 9);
        var setup = """
        def f_py(num1: float, num2: float) -> float:
            return (num2 + num1) / 2
        def f_np(num1: np.float64, num2: np.float64) -> np.float64:
            return num1 + num2
        """;
        runner.addSetupQuery(setup);
        var q = "timed.view(['New1 = f_py(num1, num2)','New2 = f_np(num1, num2)']).sum_by()";
        runner.test("UserFormula- 2 Calcs", 1, q, "num1", "num2");
    }

}
