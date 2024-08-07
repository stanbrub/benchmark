/* Copyright (c) 2022-2023 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.api;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import io.deephaven.benchmark.connect.ResultTable;
import io.deephaven.benchmark.metric.Metrics;
import io.deephaven.benchmark.util.Numbers;

/**
 * Represents the metrics gathered during usage of the Bench API. These can include metrics supplied by the API, the
 * result of a query, or supplied by by the user.
 */
final public class BenchMetrics {
    static final String[] header =
            {"benchmark_name", "origin", "timestamp", "name", "value", "note"};
    final List<Metrics> metrics = new ArrayList<>();
    final Path file;
    private String name = null;

    BenchMetrics(Path parent) {
        this(parent, Bench.metricsFileName);
    }

    BenchMetrics(Path parent, String resultFileName) {
        this.file = parent.resolve(resultFileName);
    }

    /**
     * Add the results from a table as metrics to persist to the file system. This table must have columns defined as
     * <code>timestamp, origin, category, type, name, value, note</code>
     * 
     * @param table a table containing metrics
     * @return this instance
     */
    public BenchMetrics add(ResultTable table) {
        assertColumnNames(table);
        for (int r = 0, rn = table.getRowCount(); r < rn; r++) {
            var origin = table.getValue(r, "origin").toString();
            var timestamp = table.getNumber(r, "timestamp").longValue();
            var category = table.getValue(r, "category").toString();
            var mname = table.getValue(r, "name").toString();
            var note = table.getValue(r, "note").toString();
            Metrics m = new Metrics(timestamp, origin, category);
            m.set(mname, Numbers.parseNumber(table.getValue(r, "value")), note);
            metrics.add(m);
        }
        return this;
    }

    /**
     * Add metrics to persist to the file system
     * 
     * @param m metrics to add
     * @return this instance
     */
    public BenchMetrics add(Metrics m) {
        metrics.add(m);
        return this;
    }

    /**
     * Save the collected results to a csv file. Skip results where name starts with "#"
     */
    public void commit() {
        if (name.startsWith("#"))
            return;

        List<String> head = Arrays.stream(header).toList();
        if (!hasHeader())
            writeLine(head, file);

        for (Metrics metric : metrics) {
            for (String name : metric.getNames()) {
                var m = new HashMap<>(metric.getMetric(name));
                m.put("benchmark_name", this.name);
                writeLine(head.stream().map(h -> m.get(h)).toList(), file);
            }
        }
    }

    void setName(String name) {
        this.name = name;
    }

    private void assertColumnNames(ResultTable table) {
        var required = new TreeSet<>(Arrays.asList(header));
        required.remove("benchmark_name"); // Supplied by this class
        var columns = new TreeSet<>(table.getColumnNames());

        for (String req : required) {
            if (!columns.contains(req))
                throw new RuntimeException("Missing required metric column: " + req);
        }
    }

    private boolean hasHeader() {
        return Files.exists(file);
    }

    static void writeLine(Collection<?> values, Path file) {
        try (BufferedWriter out = Files.newBufferedWriter(file, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            out.write(String.join(",", values.stream().map(v -> v.toString()).toList()));
            out.newLine();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to write result to file: " + file, ex);
        }
    }

    static String format(float v) {
        return String.format("%.2f", v);
    }

}
