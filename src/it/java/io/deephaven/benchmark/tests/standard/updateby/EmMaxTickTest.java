/* Copyright (c) 2022-2023 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.tests.standard.updateby;

import org.junit.jupiter.api.*;
import io.deephaven.benchmark.tests.standard.StandardTestRunner;

/**
 * Standard tests for the updateBy table operation. Calculates a tick-based exponential moving maximum for specified
 * columns and places the result into a new column for each row.
 */
public class EmMaxTickTest {
    final StandardTestRunner runner = new StandardTestRunner(this);

    @BeforeEach
    public void setup() {
        runner.setRowFactor(6);
        runner.tables("timed");
        runner.addSetupQuery("from deephaven.updateby import emmax_tick");
    }

    @Test
    public void emMaxTick0Group1Col() {
        runner.setScaleFactors(20, 12);
        var q = "timed.update_by(ops=emmax_tick(decay_ticks=100,cols=['X=int5']))";
        runner.test("EmMaxTick- No Groups 1 Col", q, "int5");
    }

    @Test
    public void emMaxTick0Group2Cols() {
        runner.setScaleFactors(10, 7);
        var q = "timed.update_by(ops=emmax_tick(decay_ticks=100,cols=['X=int5','Y=int10']))";
        runner.test("EmMaxTick- No Groups 2 Cols", q, "int5", "int10");
    }

    
    @Test
    public void emMaxTick1Group1Col() {
        runner.setScaleFactors(7, 1);
        var q = "timed.update_by(ops=emmax_tick(decay_ticks=100,cols=['X=int5']), by=['str100'])";
        runner.test("EmMaxTick- 1 Group 100 Unique Vals 1 Col", q, "str100", "int5");
    }

    @Test
    public void emMaxTick1Group2Cols() {
        runner.setScaleFactors(5, 1);
        var q = "timed.update_by(ops=emmax_tick(decay_ticks=100,cols=['X=int5','Y=int10']), by=['str100'])";
        runner.test("EmMaxTick- 1 Group 100 Unique Vals 2 Cols", q, "str100", "int5", "int10");
    }

    @Test
    public void emMaxTick2GroupsInt() {
        runner.setScaleFactors(5, 1);
        var q = "timed.update_by(ops=emmax_tick(decay_ticks=100,cols=['X=int5']), by=['str100','str150'])";
        runner.test("EmMaxTick- 2 Groups 15K Unique Combos 1 Col Int", q, "str100", "str150",
                "int5");
    }

    @Test
    public void emMaxTick2GroupsFloat() {
        runner.setScaleFactors(5, 1);
        var q = "timed.update_by(ops=emmax_tick(decay_ticks=100,cols=['X=float5']), by=['str100','str150'])";
        runner.test("EmMaxTick- 2 Groups 15K Unique Combos 1 Col Float", q, "str100", "str150",
                "float5");
    }

}
