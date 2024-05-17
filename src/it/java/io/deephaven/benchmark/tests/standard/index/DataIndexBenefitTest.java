/* Copyright (c) 2022-2024 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.tests.standard.index;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import io.deephaven.benchmark.tests.standard.StandardTestRunner;

/**
 * Standard tests for using the <code>data_index</code> and assessing its performance benefit to operations where it
 * applies
 */
@TestMethodOrder(OrderAnnotation.class)
public class DataIndexBenefitTest {
    final StandardTestRunner runner = new StandardTestRunner(this);

    void setup(int rowFactor, int staticFactor, int incFactor, String... groupCols) {
        runner.setRowFactor(rowFactor);
        runner.tables("right");
        runner.groupedTable("source", groupCols);
        runner.setScaleFactors(staticFactor, incFactor);

        var setup = """
        from deephaven.experimental.data_index import data_index
        QueryTable = jpy.get_type('io.deephaven.engine.table.impl.QueryTable')
        QueryTable.setMemoizeResults(False)
        where_filter = empty_table(1000).update([
            'set1=``+(ii % 10)', 'set2=``+(ii % 11)', 'set3=(int)(ii % 8)',
        ])
        """;
        runner.addSetupQuery(setup);
    }

    @Test
    @Order(1)
    void dataIndexWhereInNoIndex() {
        setup(1, 250, 225);

        var op = """
        source.where_in(where_filter, cols=['key1 = set1', 'key2 = set2', 'key4 = set3'])
        QueryTable.setMemoizeResults(True)
        """;
        runner.test("DataIndex-WhereIn No Index 1M Unique Combos", 999900, op, "num1", "key1", "key2", "key4");
    }

    @Test
    @Order(2)
    void dataIndexWhereInIndexedStatic() {
        setup(1, 48, 0, "key1", "key2", "key4");

        var preOp = """
        source_idx = data_index(source, ['key1','key2','key4'])
        source_idx.table
        """;
        runner.addPreOpQuery(preOp);

        var op = """
        source.where_in(where_filter, cols=['key1 = set1', 'key2 = set2', 'key4 = set3'])
        QueryTable.setMemoizeResults(True)
        """;
        runner.test("DataIndex-WhereIn Indexed 1M Unique Combos", 999900, op, "num1", "key1", "key2", "key4");
    }

    @Test
    @Order(3)
    void dataIndexWhereInIndexedInc() {
        setup(1, 0, 2);

        var preOp = """
        source_idx = data_index(source, ['key1','key2','key4'])
        source_idx.table
        """;
        runner.addPreOpQuery(preOp);

        var op = """
        source.where_in(where_filter, cols=['key1 = set1', 'key2 = set2', 'key4 = set3'])
        QueryTable.setMemoizeResults(True)
        """;
        runner.test("DataIndex-WhereIn Indexed 1M Unique Combos", 999900, op, "num1", "key1", "key2", "key4");
    }

    @Test
    @Order(4)
    void dataIndexAvgByNoIndex() {
        setup(1, 6, 0);

        var op = """
        source.avg_by(by=['key1','key2','key4'])
        QueryTable.setMemoizeResults(True)
        """;
        runner.test("DataIndex-AvgBy No Index 1M Unique Combos", 999900, op, "num1", "key1", "key2", "key4");
    }

    @Test
    @Order(5)
    void dataIndexAvgByIndexed() {
        setup(1, 40, 0, "key1", "key2", "key4");

        var preOp = """
        source_idx = data_index(source, ['key1','key2','key4'])
        source_idx.table
        """;
        runner.addPreOpQuery(preOp);

        var op = """
        source.avg_by(by=['key1','key2','key4'])
        QueryTable.setMemoizeResults(True)
        """;
        runner.test("DataIndex-AvgBy Indexed 1M Unique Combos", 999900, op, "num1", "key1", "key2", "key4");
    }


    @Test
    @Order(6)
    void dataIndexSortNoIndex() {
        setup(1, 1, 0);

        var op = """
        source.sort(['key1','key2','key4'])
        QueryTable.setMemoizeResults(True)
        """;
        runner.test("DataIndex-Sort No Index 1M Unique Combos", op, "num1", "key1", "key2", "key4");
    }

    @Test
    @Order(7)
    void dataIndexSortIndexed() {
        setup(1, 45, 0, "key1", "key2", "key4");

        var setup = """
        source_idx = data_index(source, ['key1','key2','key4'])
        source_idx.table
        """;
        runner.addSetupQuery(setup);

        var op = """
        source.sort(['key1','key2','key4'])
        QueryTable.setMemoizeResults(True)
        """;
        runner.test("DataIndex-Sort Indexed 1M Unique Combos", op, "num1", "key1", "key2", "key4");
    }

    @Test
    @Order(8)
    void dataIndexAsOfJoinNoIndex() {
        setup(1, 4, 0);

        var op = """
        source.aj(right, on=['key1 = r_wild', 'key2 = r_key2', 'key4 >= r_key4'])
        QueryTable.setMemoizeResults(True)
        """;
        runner.test("DataIndex-AsOfJoin No Index 1M Unique Combos", op, "num1", "key1", "key2", "key4");
    }

    @Test
    @Order(9)
    void dataIndexAsOfJoinIndexed() {
        setup(1, 20, 0, "key1", "key2", "key4");

        var setup = """
        source_idx = data_index(source, ['key1','key2','key4'])
        source_idx.table
        right_idx = data_index(right, ['r_wild','r_key2','r_key4'])
        right_idx.table
        """;
        runner.addSetupQuery(setup);

        var op = """
        source.aj(right, on=['key1 = r_wild', 'key2 = r_key2', 'key4 >= r_key4'])
        QueryTable.setMemoizeResults(True)
        """;
        runner.test("DataIndex-AsOfJoin Indexed 1M Unique Combos", op, "num1", "key1", "key2", "key4");
    }

}
