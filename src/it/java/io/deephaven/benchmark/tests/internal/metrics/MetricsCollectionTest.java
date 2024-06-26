package io.deephaven.benchmark.tests.internal.metrics;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.*;
import io.deephaven.benchmark.api.Bench;

/**
 * Test to see what metrics are available in the remote Deephaven Server. Note. These tests should pass when DH is
 * JVM 17+
 */
public class MetricsCollectionTest {
    final Bench api = Bench.create(this);

    @Test
    public void collect1MetricSet() {
        var query = """
        from time import sleep
        
        bench_api_metrics_snapshot()
        sleep(0.5)
        mymetrics = bench_api_metrics_collect()
        """;

        api.query(query).fetchAfter("mymetrics", table -> {
            assertEquals("timestamp, origin, category, type, name, value, note", formatCols(table.getColumnNames()),
                    "Wrong column names");
            int rowCount = table.getRowCount();
            assertTrue(rowCount > 20, "Wrong row count. Got " + rowCount);
            assertEquals("ClassLoadingImpl", table.getValue(0, "category"), "Wrong bean name");
            assertEquals("TotalLoadedClassCount", table.getValue(0, "name"), "Wrong ");
            assertTrue(table.getValue(3, "value").toString()
                    .matches("init = .* used = .* committed = .* max = .*"));
        }).execute();
    }

    @Test
    public void collect2MetricSets() {
        var query = """
        from time import sleep
        
        bench_api_metrics_snapshot()
        sleep(0.5)
        bench_api_metrics_snapshot()
        mymetrics = bench_api_metrics_collect()
        """;

        api.query(query).fetchAfter("mymetrics", table -> {
            assertEquals("timestamp, origin, category, type, name, value, note", formatCols(table.getColumnNames()),
                    "Wrong column names");
            int rowCount = table.getRowCount();
            assertTrue(rowCount > 40, "Wrong row count. Got " + rowCount);
        }).execute();
    }

    @Test
    public void collectMetricsToFile() throws Exception {
        var query = """
        from time import sleep
        
        bench_api_metrics_snapshot()
        sleep(0.5)
        mymetrics = bench_api_metrics_collect()
        """;

        api.query(query).fetchAfter("mymetrics", table -> {
            int rowCount = table.getRowCount();
            assertTrue(rowCount > 20, "Wrong row count. Got " + rowCount);
            api.metrics().add(table);
        }).execute();
        api.close();

        Path metricsFile = Bench.outputDir.resolve(Bench.metricsFileName);
        assertTrue(Files.exists(metricsFile), "Missing metrics output file");
        var lines = Files.lines(metricsFile).toList();
        assertEquals("benchmark_name,origin,timestamp,category,type,name,value,note", lines.get(0), "Wrong csv header");
        assertTrue(lines.size() > 1, "CSV has no data");
    }

    @AfterEach
    public void teardown() {
        api.close();
    }

    private String formatCols(Collection<String> columns) {
        var cols = new LinkedHashSet<String>(columns);
        cols.remove("RowPosition");
        cols.remove("RowKey");
        return cols.toString().replace("]", "").replace("[", "");
    }

}
