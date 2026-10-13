# Profile the median/percentile regressions that appeared in the 2026-10-13 nightly
#
# Nightly 10-12 ran 9fc7833bbb and nightly 10-13 ran 62726a15ee. Across that step most MedianBy and
# SortedCombo benchmarks gained 10% to 190%, but three regressed, and an 11-iteration adhoc A/B
# confirmed each one (see the 2026-10-13_medianby-percentile study):
#
#   MedianBy- No Groups -Inc                           -15% to -19%
#   SortedCombo-AggBy- 5 Ops 3 Groups 1M Unique Combos -Inc  -9% to -11%
#   SortedCombo-AggBy- 5 Ops No Groups -Static          -7% to  -9%
#
# Throughput is already established, so every row here records JFR. These rows show WHERE the time
# goes; do not read effect sizes off them, since JFR alone has moved some benchmarks by double digits.
#
# Only three commits in that range touch engine code. The rows bracket the prime suspect and carry
# on to the nightly tip, which also covers the other two:
#
#   before  2a29db7a5f  parent of #8858. For engine code it matches nightly 10-12; the commits
#                       between are only examples, docs, a test move and a docstring
#   pr      29668d5d0c  perf: DH-24000: Faster percentile and median aggregations (#8858)
#   tip     62726a15ee  nightly 10-13, adding #8799 (cross join) and #8828 (RowSet negative keys)
#
# Expect before -> pr to carry the whole change and pr -> tip to look the same. If that holds, #8858
# is confirmed and the before/pr profiles answer where.
#
# All three rows are built from source rather than pulling the nightly digests. A pulled image and
# a source build can differ in JDK and OS layers, a confound that once mimicked a 17% regression.
#
# What to look for, per regressing benchmark (pr vs before):
#   No Groups -Inc        time in SsmChunkedPercentileOperator.applyStaged / compactAndCount: one
#                         large Object sort per cycle (the key1/key2 string medians) in place of
#                         many chunk-sized sorts
#   3 Groups 1M -Inc      allocation samples in SsmChunkedPercentileOperator.stage: one staged
#                         WritableChunk per touched bucket per cycle, at ~10 rows per bucket
#   No Groups -Static     DoubleStaticPercentileOperator.append growth and select over a single
#                         200M-value array for median and pct on num1. No hypothesis yet; GC
#                         metrics did not explain it
# The improving benchmarks in the same two classes act as in-run controls.
#
# settings=profile includes jdk.ObjectAllocationSample, which the allocation hypothesis needs.
# JFR needs docker.compose.stop.timeout above 0 or the engine is killed before the recording
# flushes; adhoc-scale-benchmark.properties sets 30.
#
# Class lists omit the Test suffix: ConsoleLauncherUtil appends it.
#
# ITERS=3 gives each benchmark three recordings per row, so a stray recording can be spotted
# without paying for 11. ROWSM=10 and DIST=random match the nightly the regression was found in.
#
# TSV columns: run_type, run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=3

RTYP='adhoc'
PKG=io.deephaven.benchmark.tests.standard
CLASSES='MedianBy,SortedCombo'
DIST=random
ROWSM=10
ITERS=3

BEFORE=deephaven:2a29db7a5fe74744cc5f3fa05b9a18d14f096dd4
PR=deephaven:29668d5d0c56ca3a5d67e98b087e3513dca458a5
TIP=deephaven:62726a15ee2f14516e2e6a6a707f94b943d3a80e

jfr_opts() {
    echo "-Xmx24g -XX:StartFlightRecording=name=bench,filename=/data/$1-%t-%p.jfr,settings=profile,maxsize=200m"
}

row() { echo -e "$RTYP\t$1\t$2\t$PKG\t$CLASSES\t$ITERS\t$ROWSM\t$DIST\t$(jfr_opts "$1")"; }

row medianby_before_jfr "$BEFORE"
row medianby_pr_jfr     "$PR"
row medianby_tip_jfr    "$TIP"
