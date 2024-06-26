/* Copyright (c) 2022-2024 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.generator;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.*;

public class DFunctionTest {

    @Test
    void linearConvApply() {
        var f = DFunction.get("linearConv", "col1");
        assertEquals(0, (int) f.apply(0, 500, 0, 0, 100), "Wrong min result");
        assertEquals(50, (int) f.apply(0, 500, 250, 0, 100), "Wrong middle result");
        assertEquals(100, (int) f.apply(0, 500, 500, 0, 100), "Wrong max result");

        assertEquals(75, (int) f.apply(-500, 500, 250, 0, 100), "Wrong middle result");
        assertEquals(0, (int) f.apply(0, 500, 250, -50, 50), "Wrong middle result");
        assertEquals(10, (int) f.apply(1, 1, 1, 0, 20), "Wrong same min/max result");
        assertEquals(2000, (int) f.apply(0, 500, 250, 1000, 3000), "Wrong middle scale bigger result");
    }

    @Test
    void runLengthApply() {
        var f = DFunction.get("runLength", "col1");
        assertEquals(0, (int) f.apply(0, 500, 0, 0, 100), "Wrong min result");
        assertEquals(2, (int) f.apply(0, 500, 250, 0, 100), "Wrong middle result");
        assertEquals(5, (int) f.apply(0, 500, 500, 0, 100), "Wrong max result");

        assertEquals(2, (int) f.apply(-500, 500, 250, 0, 100), "Wrong middle result");
        assertEquals(2, (int) f.apply(0, 500, 250, -50, 50), "Wrong middle result");
        assertEquals(0, (int) f.apply(1, 1, 1, 0, 20), "Wrong same min/max result");
        assertEquals(0, (int) f.apply(0, 500, 250, 1000, 3000), "Wrong middle scale bigger result");
    }

    @Test
    void randomApply() {
        var f = DFunction.get("random", "col1");
        assertEquals(15, (int) f.apply(0, 0, 0, 0, 100), "Wrong random result");
        assertEquals(-20, (int) f.apply(0, 0, 0, -50, 50), "Wrong random result");
    }

    @Test
    void ascendingApply() {
        var f = DFunction.get("ascending", "col1");
        assertEquals(1, (int) f.apply(0, 1, 1, 0, 100), "Wrong low result");
        assertEquals(1, (int) f.apply(0, 1, 101, 0, 100), "Wrong rollover result");
        assertEquals(1, (int) f.apply(0, 1, 201, 0, 100), "Wrong rollover result");
        assertEquals(50, (int) f.apply(0, 1, 50, 0, 100), "Wrong middle result");
        assertEquals(99, (int) f.apply(0, 1, 99, 0, 100), "Wrong high result");
    }

    @Test
    void check() {
        try {
            DFunction.check(100, 99, 0, 20);
            assertTrue(false, "Should have thrown 'srcMin is greater than srcMax'");
        } catch (Exception ex) {
            assertEquals("srcMin is greater than srcMax: 100 > 99", ex.getMessage(), "Wrong exception");
        }

        try {
            DFunction.check(2, 200, 100, 99);
            assertTrue(false, "Should have thrown 'dstMin is greater than dstMaxx'");
        } catch (Exception ex) {
            assertEquals("dstMin is greater than dstMax: 100 > 99", ex.getMessage(), "Wrong exception");
        }

    }

}
