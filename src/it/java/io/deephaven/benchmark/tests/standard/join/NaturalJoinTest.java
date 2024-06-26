/* Copyright (c) 2022-2024 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.tests.standard.join;

import org.junit.jupiter.api.*;
import io.deephaven.benchmark.tests.standard.StandardTestRunner;

/**
 * Standard tests for the natural_join table operation. Column values will be added to each left row from exactly one
 * matched row from the right table or null values if no match
 */
public class NaturalJoinTest {
    final StandardTestRunner runner = new StandardTestRunner(this);

    void setup(int rowFactor) {
        runner.setRowFactor(rowFactor);
        runner.tables("source", "right");
    }

    @Test
    void NaturalJoinOn1Col() {
        setup(2);
        var q = "source.natural_join(right, on=['key5 = r_key5'])";
        runner.test("NaturalJoin- Join On 1 Col", q, "key5", "num1");
    }

    @Test
    void NaturalJoinOn2Cols() {
        setup(6);
        var q = "source.natural_join(right, on=['key1 = r_wild', 'key2 = r_key2'])";
        runner.test("NaturalJoin- Join On 2 Cols", q, "key1", "key2", "num1");
    }
    
    @Test
    void NaturalJoinOn3Cols() {
        setup(6);
        var q = "source.natural_join(right, on=['key1 = r_wild', 'key2 = r_key2', 'key1 = r_key1'])";
        runner.test("NaturalJoin- Join On 3 Cols", q, "key1", "key2", "num1");
    }

}
