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
        
        enabled_events=['jdk.GarbageCollection', 'jdk.GCPhasePause', 'jdk.GCPhaseConcurrent', 'jdk.GCCPUTime']
        for n in enabled_events:
            try:
                rec.enable(n)
            except Exception as e:
                print(f"Event Not Enabled: {e}")

        disabled_events=['jdk.ExecutionSample', 'jdk.JavaMonitorEnter', 'jdk.JavaMonitorWait', 'jdk.ThreadSleep', 
            'jdk.SocketRead', 'jdk.SocketWrite']
        for n in disabled_events:
            try:
                rec.disable(_ename)
            except Exception:
                print(f"Event Not Disabled: {e}")

        rec.start()
        """;
    
    static final String stopJfrQuery = """
        Paths = jpy.get_type("java.nio.file.Paths")
        RecordingFile = jpy.get_type("jdk.jfr.consumer.RecordingFile")

        rec.dump(Paths.get("/data/benchmark.jfr"))
        rec.stop()
        rec.close()
        
        events = RecordingFile.readAllEvents(Paths.get("/data/benchmark.jfr"))

        jfr_rows = []

        def getEventValue(ev, field):
            try:
                return ev.getValue(field)
            except Exception:
                return None
                
        def getNanoValue(ev, duration_field):
            val = ev.getValue(duration_field)
            if val is None or str(val) == "null": return 0
            if isinstance(val, int): return val
            if hasattr(val, "size") and hasattr(val, "get"):
                total = 0
                for i in range(val.size()):
                    d = val.get(i)
                    if d is not None and str(d) != "null": total += d.toNanos()
                return total
            if hasattr(val, "toNanos"): return val.toNanos()
            raise TypeError(f"Unsupported JFR value type: {type(val)}")


        for i in range(events.size()):
            e = events.get(i)
            etype = e.getEventType().getName()
            start = e.getStartTime().getEpochSecond() * 1000000000 + e.getStartTime().getNano();

            if etype == 'jdk.GarbageCollection':
                duration = getNanoValue(e, 'duration')
                name = getEventValue(e, 'name')
                value = getNanoValue(e, 'sumOfPauses')
            elif etype == 'jdk.GCPhasePause' or etype == 'jdk.GCPhaseConcurrent':
                duration = getNanoValue(e, 'duration')
                name = getEventValue(e, 'name')
                value = duration
            elif etype == 'jdk.GCCPUTime':
                duration = getNanoValue(e, 'realTime')
                name = "cpuTime"
                value = getNanoValue(e, 'systemTime') + getNanoValue(e, 'userTime')
            else:
                continue

            jfr_rows.append([etype, start, duration, name, value])

        # Only create a table if we saw any GC events
        if len(jfr_rows) > 0:
            jfr_gc = new_table([
                string_col("origin", ["deephaven-engine" for r in jfr_rows]),
                string_col("type", [r[0] for r in jfr_rows]),
                long_col("start_ns", [r[1] for r in jfr_rows]),
                long_col("duration_ns", [r[2] for r in jfr_rows]),
                string_col("name", [r[3] for r in jfr_rows]),
                double_col("value", [r[4] for r in jfr_rows]),
            ])
            standard_events = merge([standard_events, jfr_gc])
        """;
    
    static final String ugpQuery = """
        from deephaven import write_csv
        import deephaven.perfmon as pm
        ugp = pm.update_performance_log()
        write_csv(ugp, "/data/ugp_cycles.csv")
        """;
}