/* Copyright (c) 2022-2024 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.tests.standard.formula;

import org.junit.jupiter.api.*;
import io.deephaven.benchmark.tests.standard.StandardTestRunner;

/**
 * Standard tests for running user-defined functions. These tests are meant to be compared, and so use the same data.
 * <p>
 * Note: When scaling row count, vector size should not get bigger. That would cause more than one axis change and
 * invalidate any expected comparisons.
 * <p>
 * Note: The "No Hints" tests have casts to make them equivalent to the hints tests, otherwise the return value would
 * always be a PyObject and not really the same test. They use two formulas to achieve this, otherwise vectorization
 * would not happen on "No Hints" benchmarks.
 */
public class StatedFormulaTest {
    final StandardTestRunner runner = new StandardTestRunner(this);

    @BeforeEach
    void setup() {
        runner.setRowFactor(1);
        runner.tables("source");
        runner.setScaleFactors(1, 0);
        var q = """
        import jpy, math
        SelectColumnFactory = jpy.get_type('io.deephaven.engine.table.impl.select.SelectColumnFactory')
        SelectColumn = jpy.get_type('io.deephaven.engine.table.impl.select.SelectColumn')
        List = jpy.get_type('java.util.List')
        
        def hypo_calc(val1, val2):
            return math.sqrt(math.pow(math.fabs(val1),2) + math.pow(math.fabs(val2),2))
        
        stated_column = SelectColumnFactory.getExpression(f'Value=hypo_calc(num1, num2)')
        """;
        runner.addSetupQuery(q);
    }

    @Test
    void statefulUdfSum() {
        var q = "source.update(List.of(stated_column))";
        runner.test("Stated-UDF- 2 Doubles Stateful", q, "num1", "num2");
    }

    @Test
    void statelessUdfSum() {
        var setup = "stated_column = SelectColumn.ofStateless(stated_column)";
        runner.addSetupQuery(setup);
        var q = "source.update(List.of(stated_column))";
        runner.test("Stated-UDF- 2 Doubles Stateless", q, "num1", "num2");
    }

}
