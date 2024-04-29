/* Copyright (c) 2022-2024 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.tests.standard.index;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import io.deephaven.benchmark.tests.standard.StandardTestRunner;

/**
 * Standard tests for using the <code>data_index</code>
 */
@TestMethodOrder(OrderAnnotation.class)
public class DataIndexTest {
    final StandardTestRunner runner = new StandardTestRunner(this);

    @BeforeEach
    void setup() {
        runner.setRowFactor(3);
        runner.tables("source", "right");

        var setup = """
        from deephaven.experimental.data_index import data_index
        from deephaven.updateby import rolling_std_tick
        QueryTable = jpy.get_type('io.deephaven.engine.table.impl.QueryTable')
        QueryTable.setMemoizeResults(False)
        contains_row = rolling_std_tick(cols=["Contains=num1"], rev_ticks=20, fwd_ticks=30)
        where_filter = empty_table(50).update([
            'set1=``+(ii % 20)', 'set2=``+(ii % 30)', 'set3=(int)(ii % 40)',
        ])
        """;
        runner.addSetupQuery(setup);
    }

    @Test
    @Order(1)
    void dataIndexCostStatic() {
        runner.setScaleFactors(1, 0);
        
        var op = """
        source_idx = data_index(source, ['key1','key2','key4'])
        result = source_idx.table
        QueryTable.setMemoizeResults(True)
        """;
        runner.test("DataIndex- Index Cost 1M Unique Combos", 999900, op, "num1", "key1", "key2", "key4");
    }
    
    @Test
    @Order(2)
    void dataIndexCostInc() {
        runner.setScaleFactors(0, 1);
        
        var setup = """
        source_idx = data_index(source, ['key1','key2','key4'])
        source_idx.table
        """;
        runner.addPreOpQuery(setup);
        
        var op = """
        result = source
        QueryTable.setMemoizeResults(True)
        """;
        runner.test("DataIndex- Index Cost 1M Unique Combos", op, "num1", "key1", "key2", "key4");
    }
    
    @Test
    @Order(3)
    void dataIndexCostStatic100K() {
        runner.setScaleFactors(1, 0);
        
        var op = """
        source_idx = data_index(source, ['key1','key2','key3'])
        result = source_idx.table
        QueryTable.setMemoizeResults(True)
        """;
        runner.test("DataIndex- Index Cost 100K Unique Combos", 999900, op, "num1", "key1", "key2", "key3");
    }
    
    @Test
    @Order(4)
    void dataIndexCostInc100K() {
        runner.setScaleFactors(0, 1);
        
        var setup = """
        source_idx = data_index(source, ['key1','key2','key3'])
        source_idx.table
        """;
        runner.addPreOpQuery(setup);
        
        var op = """
        result = source
        QueryTable.setMemoizeResults(True)
        """;
        runner.test("DataIndex- Index Cost 100K Unique Combos", op, "num1", "key1", "key2", "key3");
    }
    
    @Test
    @Order(5)
    void dataIndexAvgByNoIndex() {
        runner.setScaleFactors(1, 1);
        
        var op = """
        result = source.avg_by(by=['key1','key2','key4'])
        QueryTable.setMemoizeResults(True)
        """;
        runner.test("DataIndex-AggBy No Index 1M Unique Combos", 999900, op, "num1", "key1", "key2", "key4");
    }
    
    @Test
    @Order(6)
    void dataIndexAvgByIndexed() {
        runner.setScaleFactors(1, 1);

        var preOp = """
        source_idx = data_index(source, ['key1','key2','key4'])
        source_idx.table
        """;
        runner.addPreOpQuery(preOp);

        var op = """
        result = source.avg_by(by=['key1','key2','key4'])
        QueryTable.setMemoizeResults(True)
        """;
        runner.test("DataIndex-AvgBy Indexed 1M Unique Combos", 999900, op, "num1", "key1", "key2", "key4");
    }
    
    @Test
    @Order(7)
    void dataIndexAvgByNoIndex100K() {
        runner.setScaleFactors(1, 1);
        
        var op = """
        source.avg_by(by=['key1','key2','key3'])
        QueryTable.setMemoizeResults(True)
        """;
        runner.test("DataIndex-AggBy No Index 100K Unique Combos", 999900, op, "num1", "key1", "key2", "key3");
    }
    
    @Test
    @Order(8)
    void dataIndexAvgByIndexed100K() {
        runner.setScaleFactors(1, 1);

        var preOp = """
        source_idx = data_index(source, ['key1','key2','key3'])
        source_idx.table
        """;
        runner.addPreOpQuery(preOp);

        var op = """
        source.avg_by(by=['key1','key2','key3'])
        QueryTable.setMemoizeResults(True)
        """;
        runner.test("DataIndex-AvgBy Indexed 100K Unique Combos", 999900, op, "num1", "key1", "key2", "key3");
    }
    
//    @Test
//    void dataIndexSortNoIndex() {
//        runner.setScaleFactors(1, 1);
//        
//        var op = """
//        result = source.sort(['key1','key2','key4'])
//        QueryTable.setMemoizeResults(True)
//        """;
//        runner.test("DataIndex-Sort No Index 1M Unique Combos", op, "num1", "key1", "key2", "key4");
//    }
//    
//    @Test
//    void dataIndexSortIndexed() {
//        runner.setScaleFactors(1, 1);
//
//        var setup = """
//        source_idx = data_index(source, ['key1','key2','key4'])
//        source_idx.table
//        """;
//        runner.addSetupQuery(setup);
//
//        var op = """
//        result = source.sort(['key1','key2','key4'])
//        QueryTable.setMemoizeResults(True)
//        """;
//        runner.test("DataIndex-Sort Indexed 1M Unique Combos", op, "num1", "key1", "key2", "key4");
//    }

//    @Test
//    void dataIndex() {
//        runner.setScaleFactors(1, 1);
//
//        var setup = """
//        source_idx = data_index(source, ['key1','key2','key4'])
//        right_idx = data_index(right, ['r_wild','r_key2','r_key4'])
//        """;
//        runner.addSetupQuery(setup);
//
//        var op = """
//        source.count_by('count', by=['key1','key2','key4'])
//        right.count_by('r_count', by=['r_wild','r_key2','r_key4'])
//        #result = source.where(['key1=`100`','key2=`101`','key4=50'])
//        #result = source.sort(['key1','key2','key4'])
//        #result = source.avg_by(by=['key1','key2','key4'])
//        #result = source.update_by(ops=[contains_row], by=['key1','key2','key4'])
//        #result = source.aj(right, on=['key1 = r_wild', 'key2 = r_key2', 'key4 >= r_key4'])
//        #result = source.group_by(['key1','key2','key4'])
//        for i in range(100):
//            result = source.where_in(where_filter, cols=['key1 = set1', 'key2 = set2', 'key4 = set3'])
//        QueryTable.setMemoizeResults(True)
//        """;
//        runner.test("DataIndex- Indexed 1M Unique Combos", Long.MAX_VALUE, op, "num1", "key1", "key2", "key4");
//    }
//
//    @Test
//    void dataIndexNoIndex() {
//        runner.setScaleFactors(1, 1);
//
//        var op = """
//        source.count_by('count', by=['key1','key2','key4'])
//        right.count_by('r_count', by=['r_wild','r_key2','r_key4'])
//        #result = source.where(['key1=`100`','key2=`101`','key4=50'])
//        #result = source.sort(['key1','key2','key4'])
//        #result = source.avg_by(by=['key1','key2','key4'])
//        #result = source.update_by(ops=[contains_row], by=['key1','key2','key4'])
//        #result = source.aj(right, on=['key1 = r_wild', 'key2 = r_key2', 'key4 >= r_key4'])
//        #result = source.group_by(['key1','key2','key4'])
//        for i in range(100):
//            result = source.where_in(where_filter, cols=['key1 = set1', 'key2 = set2', 'key4 = set3'])
//        QueryTable.setMemoizeResults(True)
//        """;
//        runner.test("DataIndex- No Index 1M Unique Combos", Long.MAX_VALUE, op, "num1", "key1", "key2", "key4");
//    }

}
