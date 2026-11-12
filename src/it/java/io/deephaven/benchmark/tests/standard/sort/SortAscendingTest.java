/* Copyright (c) 2022-2026 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.tests.standard.sort;

import org.junit.jupiter.api.*;
import io.deephaven.benchmark.tests.standard.StandardTestRunner;

/**
 * Standard tests for the ascending sort table operation. Sorts rows of data from the source table according to the
 * defined columns
 */
public class SortAscendingTest {
    final StandardTestRunner runner = new StandardTestRunner(this);

    void setup(double staticRowFactor, int staticFactor, double incRowFactor, int incFactor) {
        runner.setRowFactors(staticRowFactor, incRowFactor);
        setup(staticFactor, incFactor);
    }

    void setup(int staticFactor, int incFactor) {
        runner.tables("source");
        runner.setScaleFactors(staticFactor, incFactor);
    }

    @Test
    void sort1Col() {
        setup(6, 4, 1, 1);
        var q = "source.sort(order_by=['key1'])";
        runner.test("Sort- 1 Col Ascending", q, "key1", "num1");
    }

    @Test
    void sort2Cols() {
        setup(5, 1, 1, 1);
        var q = "source.sort(order_by=['key1', 'key2'])";
        runner.test("Sort- 2 Cols Ascending", q, "key1", "key2", "num1");
    }

    @Test
    void sort3Cols() {
        setup(5, 1, 0.45, 1);
        var q = "source.sort(order_by=['key1', 'key2', 'key3'])";
        runner.test("Sort- 3 Cols Ascending", q, "key1", "key2", "key3", "num1");
    }

    /** Twin of {@link #sort1ColAfterLastBy()}. first_by sends the sort adds only. */
    @Test
    void sort1ColAfterFirstBy() {
        setup(1, 7, 0.85, 1);
        var q = "source.first_by(by=['key5']).sort(order_by=['num1'])";
        runner.test("Sort- 1 Col After FirstBy", 1000000, q, "key5", "num1");
    }

    /** Twin of {@link #sort1ColAfterFirstBy()}. last_by sends the sort modifies to the sort column. */
    @Test
    void sort1ColAfterLastBy() {
        setup(1, 7, 0.85, 1);
        var q = "source.last_by(by=['key5']).sort(order_by=['num1'])";
        runner.test("Sort- 1 Col After LastBy", 1000000, q, "key5", "num1");
    }

    /** Twin of {@link #sort1ColAfterLastByWhere()}. The sort receives adds only. */
    @Test
    void sort1ColAfterFirstByWhere() {
        setup(1, 7, 0.85, 1);
        var q = "source.first_by(by=['key5']).where(['num1 < 2']).sort(order_by=['num1'])";
        runner.test("Sort- 1 Col After FirstBy Where", 1000000, q, "key5", "num1");
    }

    /** Twin of {@link #sort1ColAfterFirstByWhere()}. Modified rows that fail the filter reach the sort as removes. */
    @Test
    void sort1ColAfterLastByWhere() {
        setup(1, 7, 0.85, 1);
        var q = "source.last_by(by=['key5']).where(['num1 < 2']).sort(order_by=['num1'])";
        runner.test("Sort- 1 Col After LastBy Where", 1000000, q, "key5", "num1");
    }

}
