/* Copyright (c) 2026-2026 Deephaven Data Labs and Patent Pending */
package io.deephaven.benchmark.tests.train;

import java.util.Arrays;
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
    static final int maxRowFactor = 40;
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
        delegate.test(name, maxExpectedRowCount, operation, loadColumns);
    }

}
