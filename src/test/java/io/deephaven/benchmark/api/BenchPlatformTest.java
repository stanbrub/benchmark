/* Copyright (c) 2022-2025 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.api;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import org.junit.jupiter.api.*;
import io.deephaven.benchmark.connect.CachedResultTable;
import io.deephaven.benchmark.connect.ResultTable;

public class BenchPlatformTest {

    @Test
    void commit() throws Exception {
        Path outParent = Paths.get(getClass().getResource("test-profile.properties").toURI()).getParent();
        var platform = new LocalPlatform(outParent, "platform-test.out");
        platform.commit();

        var lines = Files.readAllLines(outParent.resolve("platform-test.out"));
        assertEquals(18, lines.size(), "Wrong row count");
        assertEquals("origin,name,value", lines.get(0), "Wrong header");
        assertTrue(lines.get(1).matches("test-runner,java.version,[0-9.]+"), "Wrong values: " + lines.get(1));
        assertTrue(lines.get(3).matches("test-runner,java.class.version,[0-9.]+"), "Wrong values: " + lines.get(3));
        assertTrue(lines.get(7).matches("test-runner,java.max.memory,[0-9]+"), "Wrong values: " + lines.get(7));
        assertEquals("test-runner,profile.prop2,prop2", lines.get(9), "Wrong values");
        assertEquals("deephaven-engine,java.version,17.0.5", lines.get(11), "Wrong values");
        assertEquals("deephaven-engine,java.class.version,61.0", lines.get(13), "Wrong values");
        assertEquals("deephaven-engine,java.max.memory,25769803776", lines.get(17), "Wrong values");

        platform.add("deephaven-engine", "no.repeat.no.overwrite", "100");
        platform.add("deephaven-engine", "no.repeat.no.overwrite", "200");
        platform.commit();

        lines = Files.readAllLines(outParent.resolve("platform-test.out"));
        assertEquals("deephaven-engine,no.repeat.no.overwrite,100", lines.get(18), "Wrong values");
    }

    @Test
    void getDeephavenVersionFromPom() throws Exception {
        Path outParent = Paths.get(getClass().getResource("test-profile.properties").toURI()).getParent();
        var platform = new LocalPlatform(outParent, "platform-test.out");
        assertEquals("0.22.0", platform.getDeephavenVersion(outParent, "platform-test-pom.xml"));
        assertEquals("Unknown", platform.getDeephavenVersion(outParent, "test-profile.properties"));
    }

    @Test
    void nomalize() {
        assertEquals("one", BenchPlatform.normalize("one"));
        assertEquals("one two", BenchPlatform.normalize("one,two"));
        assertEquals("one two", BenchPlatform.normalize("one, \"two\""));
        assertEquals("one two", BenchPlatform.normalize("   one, 'two'   "));
    }


    static class LocalPlatform extends BenchPlatform {
        LocalPlatform(Path dir, String fileName) {
            super(null, dir, fileName, getProfileProps());
        }

        static Properties getProfileProps() {
            var props = new Properties();
            props.setProperty("profile.prop1", "prop1");
            props.setProperty("profile.prop2", "prop2");
            return props;
        }

        @Override
        protected ResultTable fetchResult(String query) {
            var csv = """
                origin|name|value
                ------+----+-----
                deephaven-engine|java.version|17.0.5
                deephaven-engine|java.vm.name|OpenJDK 64-Bit Server VM
                deephaven-engine|java.class.version|61.0
                deephaven-engine|os.name|Linux
                deephaven-engine|os.version|5.17.0-1029
                deephaven-engine|available.processors|24
                deephaven-engine|java.max.memory|25769803776
            """;
            return CachedResultTable.create(csv, "|");
        }
    }

}
