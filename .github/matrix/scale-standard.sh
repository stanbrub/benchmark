# Run every standard benchmark three times (one pass per row) with the rescaled factors on JVM 25, for comparison
# against the first JVM 25 nightly (2026-11-13, run-Vapbns1, Temurin 25.0.5), which ran this same image. Uses the
# nightly's heap so the results are comparable. Each row is a separate job, so each pass stays under the 6 hour job
# limit, and the shared label puts all runs in one set.
#
# Then profile the benchmarks that regressed most on JVM 25 (mostly incremental updateBy with 100K groups) with
# -Dbench.jfr=profile, once on the last JVM 21 nightly image (2026-11-12) and once on the JVM 25 image, so the
# recordings can be compared side by side. Note that the two images also differ in engine revision.
# TSV columns: run_type, run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=7

RTYP='adhoc'
IMG25=ghcr.io/deephaven/server@sha256:0f0559e7fdcd4d6e73084bca8c0e81f58f896c488a386dd99e22fc5bdd1e23fe
IMG21=ghcr.io/deephaven/server@sha256:618755645bf6d9503eb224401c4262be9f6260efcaaf91bf35c42660da07470a
PKG=io.deephaven.benchmark.tests.standard
ALL='*'
REGRESSED='CumCombo,CumSum,CumMax,CumMin,CumProd,EmMinTick,EmStdTick'
REGRESSED+=',RollingMaxTick,RollingProdTick,RollingAvgTick,MixedCombo,RollingCombo'
ITERS=1
ROWSM=10
DIST=random
OPTS="-Xmx24g"

row() { echo -e "$RTYP\t$1\t$2\t$PKG\t$3\t$ITERS\t$ROWSM\t$DIST\t$4"; }

row scale4     $IMG25 "$ALL"       "$OPTS"
row scale4     $IMG25 "$ALL"       "$OPTS"
row scale4     $IMG25 "$ALL"       "$OPTS"
row scale4     $IMG25 "$ALL"       "$OPTS"
row scale4     $IMG25 "$ALL"       "$OPTS"
row jfr_jvm21  $IMG21 "$REGRESSED" "$OPTS -Dbench.jfr=profile"
row jfr_jvm25  $IMG25 "$REGRESSED" "$OPTS -Dbench.jfr=profile"
