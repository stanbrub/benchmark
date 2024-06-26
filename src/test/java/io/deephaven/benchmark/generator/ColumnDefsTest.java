/* Copyright (c) 2022-2024 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.generator;

import static org.junit.jupiter.api.Assertions.*;
import java.util.*;
import java.util.stream.*;
import org.junit.jupiter.api.*;

public class ColumnDefsTest {
    final int cacheSize = 5;

    @Test
    void add() {
        ColumnDefs columnDefs = new ColumnDefs()
                .add("symbol", "string", "ABC[1-11]")
                .add("price", "float", "[100-105]")
                .add("priceAgain", "int", "[100-105]");
        columnDefs.setDefaultDistribution("ascending");

        assertEquals(3, columnDefs.columns.size(), "Wrong def count");

        assertEquals("string", columnDefs.columns.get(0).type(), "Wrong field count");
        assertEquals("symbol", columnDefs.columns.get(0).name(), "Wrong field name");
        assertEquals("StringMaker", columnDefs.columns.get(0).maker().getClass().getSimpleName(), "Wrong maker");

        assertEquals("float", columnDefs.columns.get(1).type(), "Wrong field count");
        assertEquals("price", columnDefs.columns.get(1).name(), "Wrong field name");
        assertEquals("FloatMaker", columnDefs.columns.get(1).maker().getClass().getSimpleName(), "Wrong maker");
    }

    @Test
    void add_Literals() {
        ColumnDefs columnDefs = new ColumnDefs()
                .add("col1", "string", "11")
                .add("col2", "long", "12")
                .add("col3", "int", "13")
                .add("col4", "double", "14")
                .add("col5", "float", "15")
                .add("col6", "timestamp-millis", "16");

        for (int i = 0; i < 10; i++) {
            assertEquals("11", columnDefs.nextValue(0, i, 1));
            assertEquals((Long) 12L, columnDefs.nextValue(1, i, 1));
            assertEquals((Integer) 13, columnDefs.nextValue(2, i, 1));
            assertEquals((Double) 14D, columnDefs.nextValue(3, i, 1));
            assertEquals((Float) 15F, columnDefs.nextValue(4, i, 1));
            assertEquals((Long) 16L, columnDefs.nextValue(5, i, 1));
        }
    }

    @Test
    void getQuotedColumns() {
        ColumnDefs columnDefs = new ColumnDefs()
                .add("symbol", "string", "ABC[1-11]")
                .add("price", "float", "[100-105]")
                .add("priceAgain", "int", "[100-105]");

        assertEquals("\"symbol\",\"price\",\"priceAgain\"", columnDefs.getQuotedColumns(), "Wrong field next");
    }

    @Test
    void getMaxValueCount() {
        ColumnDefs columnDefs = new ColumnDefs()
                .add("symbol", "string", "ABC[1-10]")
                .add("price", "float", "[100-105]")
                .add("priceAgain", "int", "[100-105]");

        assertEquals(10, columnDefs.getMaxValueCount(), "Wrong row count");
    }

    @Test
    void describe() {
        ColumnDefs columnDefs = new ColumnDefs()
                .add("symbol", "string", "ABC[1-10]")
                .add("price", "float", "[100-105]")
                .add("priceAgain", "int", "[100-105]", "runLength");

        assertEquals("""
                name,type,values,distribution
                symbol,string,ABC[1-10],random
                price,float,[100-105],random
                priceAgain,int,[100-105],runlength
                """,
                columnDefs.describe(), "Wrong toString");

        columnDefs = new ColumnDefs()
                .add("symbol", "string", "ABC[1-10]")
                .add("price", "float", "[100-105]")
                .add("priceAgain", "int", "[100-105]", "runLength");
        columnDefs.setDefaultDistribution("ascending");

        assertEquals("""
                name,type,values,distribution
                symbol,string,ABC[1-10],ascending
                price,float,[100-105],ascending
                priceAgain,int,[100-105],runlength
                """,
                columnDefs.describe(), "Wrong toString");
    }

    @Test
    void nextValue_Ascending() {
        ColumnDefs columnDefs = new ColumnDefs(cacheSize).add("v", "int", "[901-907]");
        columnDefs.setDefaultDistribution("ascending");

        assertValuesEqual(columnDefs, 901, 902, 903, 904, 905, 906, 907, 901, 902, 903);
        assertCacheOccurences(columnDefs, "901:1", "902:1", "903:1", "904:1", "905:1", "906:3", "907:3");
    }

    @Test
    void nextValue_StringAscending() {
        ColumnDefs columnDefs = new ColumnDefs(cacheSize).add("v", "string", "s[901-907]");
        columnDefs.setDefaultDistribution("ascending");

        assertValuesEqual(columnDefs, "s901", "s902", "s903", "s904", "s905", "s906", "s907", "s901", "s902", "s903");
        assertCacheOccurences(columnDefs, "s901:1", "s902:1", "s903:1", "s904:1", "s905:1", "s906:3", "s907:3");
    }

    @Test
    void nextValue_Descending() {
        ColumnDefs columnDefs = new ColumnDefs(cacheSize).add("v", "int", "[901-907]");
        columnDefs.setDefaultDistribution("descending");

        assertValuesEqual(columnDefs, -901, -902, -903, -904, -905, -906, -907, -901, -902, -903);
        assertCacheOccurences(columnDefs, "-901:1", "-902:1", "-903:1", "-904:1", "-905:1", "-906:3", "-907:3");
    }

    @Test
    void nextValue_StringDescending() {
        ColumnDefs columnDefs = new ColumnDefs(cacheSize).add("v", "string", "[901-907]s");
        columnDefs.setDefaultDistribution("descending");

        assertValuesEqual(columnDefs, "907s", "906s", "905s", "904s", "903s", "902s", "901s", "907s", "906s", "905s");
        assertCacheOccurences(columnDefs, "901s:3", "902s:3", "903s:1", "904s:1", "905s:1", "906s:1", "907s:1");
    }

    @Test
    void nextValue_Random() {
        var columnDefs1 = new ColumnDefs(cacheSize).add("v", "int", "[901-907]");
        columnDefs1.setDefaultDistribution("random");

        assertValuesEqual(columnDefs1, 904, 904, -901, 902, 902, 906, 906, 904, 904, -903);
        assertCacheOccurences(columnDefs1, "-901:1", "902:1", "-903:1", "904:1", "-905:1", "906:1", "-907:8");

        var columnDefs2 = new ColumnDefs(cacheSize).add("r", "int", "[901-907]");
        columnDefs2.setDefaultDistribution("random");

        assertValuesEqual(columnDefs2, 904, 904, -901, 902, 902, 906, 906, 904, 904, -903);
        assertCacheOccurences(columnDefs2, "-901:1", "902:1", "-903:1", "904:1", "-905:1", "906:1", "-907:8");
    }

    @Test
    void nextValue_RunLength() {
        var columnDefs1 = new ColumnDefs(cacheSize).add("v", "int", "[901-903]", "runLength");

        assertValuesEqual(columnDefs1, 901, 901, 901, -902, -902, -902, 903, 903, 903, 901);
        assertCacheOccurences(columnDefs1, "901:1", "-902:1", "903:1");

        var columnDefs2 = new ColumnDefs(cacheSize).add("v", "int", "[900-902]", "runLength");

        assertValuesEqual(columnDefs2, -900, -900, -900, 901, 901, 901, -902, -902, -902, -900);
        assertCacheOccurences(columnDefs2, "-900:1", "901:1", "-902:1");
    }

    private void assertValuesEqual(ColumnDefs colDefs, Object... expectedVals) {
        int maxValues = cacheSize + 5;
        var vals = IntStream.range(0, maxValues).mapToObj(i -> colDefs.nextValue(0, i, maxValues)).toArray();
        assertArrayEquals(expectedVals, vals, "Wrong generated values");
    }

    private void assertCacheOccurences(ColumnDefs colDefs, String... expectedOccurences) {
        int maxValues = cacheSize * 5;
        var unique = new TreeMap<Object, Set<Integer>>(new AbsComparable());
        IntStream.range(0, maxValues).mapToObj(i -> colDefs.nextValue(0, i, maxValues)).forEach(v -> {
            unique.computeIfAbsent(v, ids -> new HashSet<>());
            unique.get(v).add(System.identityHashCode(v));
        });
        var occurrences = new ArrayList<String>();
        unique.forEach((k, v) -> occurrences.add("" + k + ':' + v.size()));

        assertArrayEquals(expectedOccurences, occurrences.toArray(), "Wrong object cache occurences");
    }

    static class AbsComparable implements Comparator<Object> {
        @Override
        public int compare(Object o1, Object o2) {
            if (o1 instanceof String)
                return ((String) o1).compareTo((String) o2);
            return Integer.compare(Math.abs((Integer) o1), Math.abs((Integer) o2));
        }
    }

}
