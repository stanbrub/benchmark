/* Copyright (c) 2026-2026 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.tests.train;

import io.deephaven.benchmark.tests.standard.StandardTestRunner;

/**
 * A wrapper for the Bench api that allows the running of operational training (think AOT) tests without requiring the
 * boilerplate logic like imports, parquet reads, time measurement logic, etc. Each <code>test</code> runs two
 * benchmarks; one reading from a static parquet, and the other exercising ticking tables through the
 * <code>AutotuningIncrementalReleaseFilter</code>. This is different from the <code>StandardTestRunner</code> in that
 * it runs more than one operation per benchmark and attempts to cover the majority of the query code base with fewer
 * benchmarks. It is meant for training AOT and for "representative" benchmarks used to compare things like JDK/Python
 * versions and GC types.
 */
final public class TrainTestRunner {
    static final int maxRowFactor = 400;
    final StandardTestRunner delegate;
    final long baseRowCount;

    TrainTestRunner(Object testInst) {
        this.delegate = new StandardTestRunner(testInst);
        this.baseRowCount = delegate.getGeneratedRowCount();
        delegate.useMemorySource(false);
        delegate.useLocalParquet(true);
        delegate.setRowFactor(maxRowFactor);
    }

    public void tables(double rowFactor, String... names) {
        delegate.tables(names);
        if (rowFactor > maxRowFactor)
            throw new IllegalArgumentException("Row factor cannot be greater than " + maxRowFactor);
        var q = "%s = %s.head(%d)".formatted(names[0], names[0], (long) (baseRowCount * rowFactor));
        delegate.addSetupQuery(q);
    }

    public void addSetupQuery(String query) {
        delegate.addSetupQuery(query);
    }

    public void test(String name, long maxExpectedRowCount, String operation, String... loadColumns) {
        delegate.addSetupQuery(startJfrQuery);
        delegate.addTeardownQuery(stopJfrQuery);
        delegate.addTeardownQuery(ugpQuery);
        delegate.test(name, maxExpectedRowCount, operation, loadColumns);
    }
    
    static final String startJfrQuery = """
        import jpy
        Recording = jpy.get_type("jdk.jfr.Recording")
        rec = Recording()
        rec.setName("benchmark")
        rec.start()
        """;
    
    static final String stopJfrQuery = """
        Paths = jpy.get_type("java.nio.file.Paths")
        RecordingFile = jpy.get_type("jdk.jfr.consumer.RecordingFile")
        rec.dump(Paths.get("/data/benchmark.jfr"))
        rec.stop()
        rec.close()
        events = RecordingFile.readAllEvents(Paths.get("/data/benchmark.jfr"))

        # Log each event's fields to the console for inspection
        print("=== JFR event dump begin ===")
        for i in range(events.size()):
            e = events.get(i)
            etype = e.getEventType()
            print(f"Event {i}: type={etype.getName()}")
            fields = e.getFields()
            for idx in range(fields.size()):
                fd = fields.get(idx)
                fname = fd.getName()
                fval = e.getValue(fname)
                print(f"  {fname} = {fval}")
            print("--")
        print("=== JFR event dump end ===")

        jfr_rows = []
        for i in range(events.size()):
            e = events.get(i)
            start = e.getStartTime().getEpochSecond() * 1000000000 + e.getStartTime().getNano()
            dur = e.getDuration().getSeconds() * 1000000000 + e.getDuration().getNano()
            jfr_rows.append([str(e.getEventType().getName()), start, dur, str(e)])
        jfr = new_table([
            string_col("origin", ["jfr" for r in jfr_rows]),
            string_col("type", [r[0] for r in jfr_rows]),
            long_col("start_ns", [r[1] for r in jfr_rows]),
            long_col("duration_ns", [r[2] for r in jfr_rows]),
            string_col("detail", [r[3] for r in jfr_rows]),
        ])
        standard_events = merge([standard_events, jfr])
        """;
    
    static final String ugpQuery = """
        from deephaven import write_csv
        import deephaven.perfmon as pm
        ugp = pm.update_performance_log()
        write_csv(ugp, "/data/ugp_cycles.csv")
        """;
}