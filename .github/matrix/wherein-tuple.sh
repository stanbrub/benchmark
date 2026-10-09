# DH-23428 (whereIn without a tuple per row, deephaven-core PR #8782): main vs PR
#
# base: 5a02aaccfa, the merge base of the PR on main, so the only difference is the PR's commits
# pr:   90fba05151, PR head on cpwright's fork
# Both are built from source once per run and reused by every row, so they share the toolchain and
# server-docker image (Temurin 21).
#
# Training rows (the main concern): FilterTrain, the where_in + where benchmark over a Parquet-backed
# table that put G1 under heap pressure in the GC study, where tuples were the largest single
# allocator (studies/2026-08-17_where-heap-improvements).
# - Plain rows: rates plus the GC events TrainTestRunner records itself (pauses, heap after GC).
# - Alloc rows: JFR with the TLAB events, which are not throttled like ObjectAllocationSample, so
#   allocation totals compare across builds. They roughly double the runtime, so their rates don't count.
# Differences from the GC study rows: no -XX:+UseCompactObjectHeaders (JDK 21), so objects are larger
# than with JDK 25. At 24G both builds sat at a full heap after GC and the PR alloc row ran out of
# heap, so the training rows use the GC study's 48G.
# The PR also lowers QueryTable.dataIndexForWhereThreshold, which Parquet location pushdown shares, so
# the chained .where() could change as well as the where_in.
#
# Standard rows (last): WhereIn and WhereNotIn cover all three set kernels: 1 col (per-type fastutil
# map), 2 cols (pregenerated tuple kernel), 3 cols (kernel compiled on first use). Where is the
# control, since the PR does not touch it. Standard tests read a cached in-memory source, so they
# measure the matching itself without Parquet decoding. Default 24G G1.
#
# JFR files stay in the server's data directory and the next matrix run's clear deletes them, so copy
# them off before starting another run.
#
# TSV columns: run_type, run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=6

RTYP='adhoc'
ROWSM=10
DIST=random
# Training rows only; the standard rows keep the default 24G
TRAIN_HEAP=48

BASE=deephaven:5a02aaccfa7689d918ef510705cc627b7da35826
PR=cpwright:90fba051512f39619994f337dbab4d238be37449

TRAIN_PKG=io.deephaven.benchmark.tests.train
TRAIN_CLASSES='FilterTrain'
STD_PKG=io.deephaven.benchmark.tests.standard
# Suffix-free: 'Where' matches only WhereTest
STD_CLASSES='WhereIn,WhereNotIn,Where'

OPTS="-XX:+UseG1GC -Xms${TRAIN_HEAP}g -Xmx${TRAIN_HEAP}g -XX:+AlwaysPreTouch -XX:+UseTransparentHugePages -XX:+UseStringDeduplication -DServerStateTracker.reportIntervalMillis=1000 -DPeriodicUpdateGraph.targetCycleDurationMillis=1000 -Dbench.incLoadTarget=1.00"
PLAIN='<default>'

# Needs docker.compose.stop.timeout > 0 (adhoc has 30) so the recording flushes
alloc_opts() {
    echo "$OPTS -XX:StartFlightRecording=name=bench,filename=/data/$1-%t-%p.jfr,settings=profile,maxsize=1g,jdk.ObjectAllocationInNewTLAB#enabled=true,jdk.ObjectAllocationOutsideTLAB#enabled=true"
}

# label, image, package, classes, test_iterations, config_options
row() { echo -e "$RTYP\t$1\t$2\t$3\t$4\t$5\t$ROWSM\t$DIST\t$6"; }

row train_base_g1       "$BASE" $TRAIN_PKG "$TRAIN_CLASSES" 3 "$OPTS"
row train_pr_g1         "$PR"   $TRAIN_PKG "$TRAIN_CLASSES" 3 "$OPTS"
row train_base_g1_alloc "$BASE" $TRAIN_PKG "$TRAIN_CLASSES" 1 "$(alloc_opts train_base_g1_alloc)"
row train_pr_g1_alloc   "$PR"   $TRAIN_PKG "$TRAIN_CLASSES" 1 "$(alloc_opts train_pr_g1_alloc)"
row wherein_base        "$BASE" $STD_PKG   "$STD_CLASSES"   5 "$PLAIN"
row wherein_pr          "$PR"   $STD_PKG   "$STD_CLASSES"   5 "$PLAIN"
