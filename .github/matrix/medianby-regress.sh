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
# Run 1 (labels medianby_{before,pr,tip}_jfr) settled the commit: before -> pr carried all of it and
# pr -> tip was flat, so #8858 alone is responsible. Its profiles showed that most of the added wall
# time is on the busy thread but in neither Java samples nor GC pauses. In all three benchmarks, the
# one quantity that grows with it is bytes allocated: +2.0 GB, +10.6 GB and +8.0 GB per benchmark.
# The suspicion is first-touch page faults on fresh heap, which the JFR execution sampler cannot see.
#
# This run tests that, and only needs the two commits:
#
#   before  2a29db7a5f  parent of #8858
#   pr      29668d5d0c  perf: DH-24000: Faster percentile and median aggregations (#8858)
#
# Each commit runs under two heap configurations that differ only in -XX:+AlwaysPreTouch:
#
#   xms   -Xms24g -Xmx24g                       heap committed up front, pages faulted on first use
#   pt    -Xms24g -Xmx24g -XX:+AlwaysPreTouch   every heap page faulted in at JVM startup
#
# -Xms24g is required, not optional: AlwaysPreTouch only touches committed memory, and with the
# default -Xms G1 commits as it grows, so the touching would just move into the operation. It also
# changes G1's sizing, which is why the comparison is xms vs pt, never run 1 vs pt. Run 1's
# default-heap numbers compare to the xms rows only as a cross-run sanity check.
#
# How to read it (with jfr_timeline.py and jfr_alloc_thread.py from the study):
#   gap shrinks under pt, before->pr delta mostly gone   first-touch page faults were the cost
#   gap stays under pt                                   it is in the allocation itself (zeroing,
#                                                        humongous slow path), or not allocation;
#                                                        needs a profiler that sees VM frames
# Pre-touch does not remove zeroing or copying, so a partial drop is possible. The 1M-groups
# benchmark also has visible SSM churn CPU (create/clear every cycle) that pt should not change.
#
# Pre-touching 24 GB adds a few seconds to each engine start. That falls outside the measured
# operation but lengthens every row; the four rows run about 75 minutes plus two source builds.
#
# Labels are new: reusing run 1's labels would append run directories to those sets and mix them.
# The JFR filename is the label, so recordings map to their row by name.
#
# Both rows of each commit request the same ref, so the second reuses the image the first built:
# each pair is the same binary. Rows are built from source, not pulled, for the same reason.
#
# settings=profile includes jdk.ObjectAllocationSample. JFR needs docker.compose.stop.timeout above 0
# or the engine is killed before the recording flushes; adhoc-scale-benchmark.properties sets 30.
#
# Class lists omit the Test suffix: ConsoleLauncherUtil appends it. Both classes run in full, so the
# improving benchmarks and SortedCombo No Groups -Inc (a probable fourth, smaller regression) come
# along as controls.
#
# ITERS=3 gives each benchmark three recordings per row. ROWSM=10 and DIST=random match the nightly.
#
# TSV columns: run_type, run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=4

RTYP='adhoc'
PKG=io.deephaven.benchmark.tests.standard
CLASSES='MedianBy,SortedCombo'
DIST=random
ROWSM=10
ITERS=3

BEFORE=deephaven:2a29db7a5fe74744cc5f3fa05b9a18d14f096dd4
PR=deephaven:29668d5d0c56ca3a5d67e98b087e3513dca458a5

XMS='-Xms24g -Xmx24g'
PT='-Xms24g -Xmx24g -XX:+AlwaysPreTouch'

# $1 label, $2 heap options
jfr_opts() {
    echo "$2 -XX:StartFlightRecording=name=bench,filename=/data/$1-%t-%p.jfr,settings=profile,maxsize=200m"
}

# $1 label, $2 image, $3 heap options
row() { echo -e "$RTYP\t$1\t$2\t$PKG\t$CLASSES\t$ITERS\t$ROWSM\t$DIST\t$(jfr_opts "$1" "$3")"; }

row medianby_before_xms "$BEFORE" "$XMS"
row medianby_pr_xms     "$PR"     "$XMS"
row medianby_before_pt  "$BEFORE" "$PT"
row medianby_pr_pt      "$PR"     "$PT"
