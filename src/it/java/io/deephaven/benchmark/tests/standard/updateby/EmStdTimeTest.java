/* Copyright (c) 2022-2023 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.tests.standard.updateby;

import org.junit.jupiter.api.*;
import io.deephaven.benchmark.tests.standard.StandardTestRunner;

/**
 * Standard tests for the updateBy table operation. Calculates a time-based exponential moving standard deviation for
 * specified columns and places the result into a new column for each row.
 */
public class EmStdTimeTest {
    final StandardTestRunner runner = new StandardTestRunner(this);

    @BeforeEach
    public void setup() {
        runner.setRowFactor(6);
        runner.tables("timed");
        runner.addSetupQuery("from deephaven.updateby import emstd_time");
    }

    @Test
    public void emStdTime0Group1Col() {
        var q = "timed.update_by(ops=emstd_time(ts_col='timestamp', decay_time='PT2S', cols=['X=int5']))";
        runner.test("EmStdTime- No Groups 1 Col", q, "int5", "timestamp");
    }

    @Test
    public void emStdTime1Group1Col() {
        var q = "timed.update_by(ops=emstd_time(ts_col='timestamp', decay_time='PT2S', cols=['X=int5']), by=['str100'])";
        runner.test("EmStdTime- 1 Group 100 Unique Vals 1 Col", q, "str100", "int5", "timestamp");
    }

    @Test
    public void emStdTime2GroupsInt() {
        var q = "timed.update_by(ops=emstd_time(ts_col='timestamp', decay_time='PT2S', cols=['X=int5']), by=['str100','str150'])";
        runner.test("EmStdTime- 2 Groups 15K Unique Combos 1 Col Int", q, "str100", "str150",
                "int5", "timestamp");
    }

    @Test
    public void emStdTime2GroupsFloat() {
        var q = "timed.update_by(ops=emstd_time(ts_col='timestamp', decay_time='PT2S', cols=['X=float5']), by=['str100','str150'])";
        runner.test("EmStdTime- 2 Groups 15K Unique Combos 1 Col Float", q, "str100", "str150",
                "float5", "timestamp");
    }

}
