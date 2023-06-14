/* Copyright (c) 2022-2023 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.tests.standard;

import static org.junit.jupiter.api.Assertions.assertTrue;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import io.deephaven.benchmark.api.Bench;
import io.deephaven.benchmark.metric.Metrics;
import io.deephaven.benchmark.util.Exec;
import io.deephaven.benchmark.util.Timer;

/**
 * A wrapper for the Bench api that allows the running of small (single-operation) tests without requiring the
 * boilerplate logic like imports, parquet reads, time measurement logic, etc. Each <code>test</code> runs two tests;
 * one reading from a static parquet, and the other exercising ticking tables through the
 * <code>AutotuningIncrementalReleaseFilter</code>. Note: This class is meant to keep the majority of single-operations
 * compact and readable, not to cover every possible case. Standard query API code can be used in conjunction as long as
 * conventions are followed (ex. main file is "source")
 */
public class StandardTestRunner {
    final Object testInst;
    final List<String> setupQueries = new ArrayList<>();
    final List<String> supportTables = new ArrayList<>();
    private String mainTable = "source";
    private Bench api;
    private long scaleRowCount;
    private int staticFactor = 1;
    private int incFactor = 1;
    private int rowCountFactor = 1;

    public StandardTestRunner(Object testInst) {
        this.testInst = testInst;
        this.api = initialize(testInst);
        this.scaleRowCount = api.propertyAsIntegral("scale.row.count", "100000");
    }

    /**
     * Get the Bench API instance for this runner
     * 
     * @return the Bench API instance
     */
    public Bench api() {
        return api;
    }

    /**
     * Identify a pre-defined table for use by this runner
     * 
     * @param type
     */
    public void tables(String... names) {
        if (names.length > 0)
            mainTable = names[0];

        for (String name : names) {
            switch (name) {
                case "source":
                    generateSourceTable();
                    break;
                case "right":
                    generateRightTable();
                    break;
                case "timed":
                    generateTimedTable();
                    break;
                default:
                    throw new RuntimeException("Undefined table name: " + name);
            }
        }
    }

    /**
     * Add a query to be run outside the benchmark measurement but before the benchmark query
     * 
     * @param query the query to run before benchmark
     */
    public void addSetupQuery(String query) {
        setupQueries.add(query);
    }

    /**
     * The {@code scale.row.count} property supplies a default for the number of rows generated for benchmark tests.
     * Given that some operations use less memory than others, scaling up the generated rows per operation is more
     * effective than using scale factors {@link #setScaleFactors(int, int)}.
     * 
     * @param rowCountFactor a multipier applied against {@code scale.row.count}
     */
    public void setRowFactor(int rowCountFactor) {
        this.rowCountFactor = rowCountFactor;
        this.scaleRowCount = (long) (api.propertyAsIntegral("scale.row.count", "100000") * rowCountFactor);
    }

    /**
     * Scale static and incremental tests row counts artificially using Deephaven merge operations to avoid the added
     * cost of loading fully generated data into memory.
     * 
     * Simulate a higher row count by specifying a multiplier (Factor) for the row count. For example, if
     * scale.row.count=10, and staticFactor=2, the resulting row count used for the static test and rate will be 20
     * 
     * @param rowCountFactor the multiplier for the row count
     * @param staticFactor the multiplier for (scale.row.count * rowCountFactor) for the static test
     * @param incFactor the multiplier for (scale.row.count * rowCountFactor) for the inc test
     */
    public void setScaleFactors(int staticFactor, int incFactor) {
        this.staticFactor = staticFactor;
        this.incFactor = incFactor;
    }

    /**
     * Run a single operation test through the Bench API with no upper bound expected on the resulting row count
     * 
     * @see #test(String, long, String, String...)
     * @param name the name of the test as it will show in the result file
     * @param operation the operation to run and measure for the benchmark
     * @param loadColumns columns to load from the generated parquet file
     */
    public void test(String name, String operation, String... loadColumns) {
        test(name, 0, operation, loadColumns);
    }

    /**
     * Run a single-operation test through the Bench API. Create and Bench instance if necessary. Run according to the
     * following contract:
     * <ul>
     * <li>Run test from static parquet read</li>
     * <li>If static test duration <code>&lt; scale.elapsed.time.target</code>, scale row count and do it again</li>
     * <li>Run test with auto increment release filter according to the previously determined row count</li>
     * <li>Assert that both static and incremental result tables have the same number of rows</li>
     * <p/>
     * 
     * @param name the name of the test as it will show in the result file
     * @param expectedRowCount the max row count expected from the operation regardless of scale, or zero if the count
     *        has no upper bound
     * @param operation the operation to run and measure for the benchmark
     * @param loadColumns columns to load from the generated parquet file
     */
    public void test(String name, long expectedRowCount, String operation, String... loadColumns) {
        var read = getReadOperation(staticFactor);
        var result = runStaticTest(name, operation, read, loadColumns);
        var rcount = result.resultRowCount();
        var ecount = getExpectedRowCount(expectedRowCount, staticFactor);
        assertTrue(rcount > 0 && rcount <= ecount, "Wrong result Static row count: " + rcount);

//        read = getReadOperation(incFactor);
//        result = runIncTest(name, operation, read, loadColumns);
//        rcount = result.resultRowCount();
//        ecount = getExpectedRowCount(expectedRowCount, incFactor);
//        assertTrue(rcount > 0 && rcount <= ecount, "Wrong result Inc row count: " + rcount);
    }

    long getExpectedRowCount(long expectedRowCount, long scaleFactor) {
        return (expectedRowCount < 1) ? Long.MAX_VALUE : expectedRowCount;
    }

    String getReadOperation(int scaleFactor) {
        var read = "read('/data/${mainTable}.parquet').select(formulas=[${loadColumns}])";

        if (scaleFactor > 1) {
            read = "merge([${readTable}] * ${scaleFactor})".replace("${readTable}", read);
            read = read.replace("${scaleFactor}", "" + scaleFactor);
        }
        return read;
    }

    Result runStaticTest(String name, String operation, String read, String... loadColumns) {
        var staticQuery = """
        ${loadSupportTables}
        ${mainTable} = ${readTable}

        garbage_collect()
        
        ${setupQueries}
        
        bench_api_metrics_snapshot()
        begin_time = time.perf_counter_ns()
        result = ${operation}
        end_time = time.perf_counter_ns()
        bench_api_metrics_snapshot()
        standard_metrics = bench_api_metrics_collect()
        
        stats = new_table([
            double_col("elapsed_nanos", [end_time - begin_time]),
            long_col("processed_row_count", [${mainTable}.size]),
            long_col("result_row_count", [result.size]),
        ])
        """;
        return runTest(name + " -Static", staticQuery, operation, read, loadColumns);
    }

    Result runIncTest(String name, String operation, String read, String... loadColumns) {
        var incQuery = """
        ${loadSupportTables}
        loaded = ${readTable}
        autotune = jpy.get_type('io.deephaven.engine.table.impl.select.AutoTuningIncrementalReleaseFilter')
        source_filter = autotune(0, 1000000, 1.0, True)
        ${mainTable} = loaded.where(source_filter)
        
        garbage_collect()
        
        ${setupQueries}

        bench_api_metrics_snapshot()
        begin_time = time.perf_counter_ns()
        result = ${operation}
        source_filter.start()
        
        from deephaven.execution_context import get_exec_ctx
        get_exec_ctx().update_graph.j_update_graph.requestRefresh()
        source_filter.waitForCompletion()
        end_time = time.perf_counter_ns()
        bench_api_metrics_snapshot()
        standard_metrics = bench_api_metrics_collect()
        
        stats = new_table([
            double_col("elapsed_nanos", [end_time - begin_time]),
            long_col("processed_row_count", [loaded.size]),
            long_col("result_row_count", [result.size]),
        ])
        """;
        return runTest(name + " -Inc", incQuery, operation, read, loadColumns);
    }

    Result runTest(String name, String query, String operation, String read, String... loadColumns) {
        if (api.isClosed())
            api = initialize(testInst);
        api.setName(name);
        query = query.replace("${readTable}", read);
        query = query.replace("${mainTable}", mainTable);
        query = query.replace("${loadSupportTables}", loadSupportTables());
        query = query.replace("${loadColumns}", listStr(loadColumns));
        query = query.replace("${setupQueries}", String.join("\n", setupQueries));
        query = query.replace("${operation}", operation);

        try {
            var result = new AtomicReference<Result>();
            api.query(query).fetchAfter("stats", table -> {
                long loadedRowCount = table.getSum("processed_row_count").longValue();
                long resultRowCount = table.getSum("result_row_count").longValue();
                long elapsedNanos = table.getSum("elapsed_nanos").longValue();
                var r = new Result(loadedRowCount, Duration.ofNanos(elapsedNanos), resultRowCount);
                result.set(r);
            }).fetchAfter("standard_metrics", table -> {
                api.metrics().add(table);
                var metrics = new Metrics(Timer.now(), "test-runner", "setup", "test");
                metrics.set("static_scale_factor", staticFactor);
                metrics.set("inc_scale_factor", incFactor);
                metrics.set("row count factor", rowCountFactor);
            }).execute();
            api.result().test("deephaven-engine", result.get().elapsedTime(), result.get().loadedRowCount());
            return result.get();
        } finally {
            api.close();
        }
    }

    String listStr(String... values) {
        return String.join(", ", Arrays.stream(values).map(c -> "'" + c + "'").toList());
    }

    String loadSupportTables() {
        return supportTables.stream().map(t -> t + " = read('/data/" + t + ".parquet').select()\n")
                .collect(Collectors.joining(""));
    }

    Bench initialize(Object testInst) {
        var query = """
        import time
        from deephaven import new_table, garbage_collect, merge
        from deephaven.column import long_col, double_col
        from deephaven.parquet import read
        """;

        Bench api = Bench.create(testInst);
        restartDocker(api);
        api.query(query).execute();
        return api;
    }

    void restartDocker(Bench api) {
        var timer = api.timer();
        if (!Exec.restartDocker(api.property("docker.compose.file", "")))
            return;
        var metrics = new Metrics(Timer.now(), "test-runner", "setup", "docker");
        metrics.set("restart", timer.duration().toMillis(), "standard");
        api.metrics().add(metrics);
    }

    void generateSourceTable() {
        api.table("source").random()
                .add("int250", "int", "[1-250]")
                .add("int640", "int", "[1-640]")
                .add("int1M", "int", "[1-1000000]")
                .add("float5", "float", "[1-5]")
                .add("str250", "string", "[1-250]")
                .add("str640", "string", "[1-640]")
                .add("str1M", "string", "[1-1000000]")
                .withRowCount(scaleRowCount)
                .generateParquet();
    }

    void generateRightTable() {
        supportTables.add("right");
        api.table("right").fixed()
                .add("r_str250", "string", "[1-250]")
                .add("r_str640", "string", "[1-640]")
                .add("r_int1M", "int", "[1-1000000]")
                .add("r_str1M", "string", "[1-1000000]")
                .add("r_str10K", "string", "[1-100000]")
                .add("r_timestamp", "timestamp-millis", "[1-1000000]")
                .generateParquet();
    }

    void generateTimedTable() {
        long baseTime = 1676557157537L;
        long timeCount = baseTime + scaleRowCount - 1;
        api.table("timed").fixed()
                .add("timestamp", "timestamp-millis", "[" + baseTime + "-" + timeCount + "]")
                .add("timestamp2", "timestamp-millis", "[" + (baseTime + 3) + "-" + (timeCount + 3) + "]")
                .add("int5", "int", "[1-5]")
                .add("int10", "int", "[1-10]")
                .add("float5", "float", "[1-5]")
                .add("str100", "string", "[1-100]")
                .add("str150", "string", "[1-150]")
                .generateParquet();
    }

    record Result(long loadedRowCount, Duration elapsedTime, long resultRowCount) {
    }

}
