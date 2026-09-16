# A/B the 2026-09-16 boundary, where bucketed incremental updateBy lost half its throughput
#
# Nightly, 09-16 against the 09-12..09-15 baseline (that baseline is after the DH-23630 fix and
# before this change, so it is stable at 2-11% spread):
#
#   -59.1%   512K -> 209K   RollingAvgTick- 3 Groups 100K Unique Combos -Inc
#   -56.3%   502K -> 219K   RollingMaxTick- 3 Groups 100K Unique Combos -Inc
#   -54.6%   502K -> 228K   RollingGroupTick- 3 Groups 100K Unique Combos -Inc
#   -42.4%   230K -> 132K   RollingMinTime- 3 Groups 100K Unique Combos -Inc
#   -21.9%   949K -> 741K   EmaTime- 3 Groups 100K Unique Combos -Inc
#   +44.0%  1.06M -> 1.53M  Where- 3 Filters -Static
#   +52.2%   611K -> 930K   DataIndex-WhereIn Indexed 1M Unique Combos -Static
#
# 107 benchmarks lost more than 10%, 42 gained more than 5%. The regressed set is bucketed
# incremental updateBy; the gainers are filter and where paths. Note 09-16 (209-254K) sits BELOW the
# pre-DH-23630 level (332-373K), so this is not merely a reversal of that fix.
#
# Seven commits separate the images and only one touches updateBy:
#   d936e1498  perf: DH-23676: Add RowSetFactory.union and use it for N-way merges (#8543)
#              RowSetFactory (+220), RowSetBuilderRandom, AdaptiveRowSetBuilderRandom,
#              WritableRowSetImpl, and UpdateBy.java itself (42 lines)   <-- suspect
#   2b5802039  fix: DH-23675: SourcePartitionedTable previous values (#8554)   3 lines, unrelated class
#   the remaining five are docs and github-actions pinning
#
# This is the third rowset perf commit in three weeks to trade one workload for another, after
# DH-23407 (2026-08-27) and DH-23190 (2026-07-31).
#
# Classes: the worst Tick family plus its Time variant, the three Combo families, EmaTime, and
# WhereIn for the gain side. WhereIn rather than Where or WhereNotIn because those two are the
# noisiest candidates in the suite -- mean day-to-day CV of 12.8% and 9.5% against WhereIn's 4.6%
# over 31 nightlies -- so they cannot carry a control. WhereIn- 1 Filter Col -Static gained 22.4% at
# this boundary at 7.1% CV.
#
# The quietest affected benchmarks are the Combo families: CumCombo 3.0% mean CV, RollingCombo 4.1%,
# MixedCombo 4.8%, EmaTime 3.4%. Those carry the regression evidence.
#
# JFR is on: unlike the UDF investigation, this regression is java side, where flight recorder can
# actually see it. Requires docker.compose.stop.timeout>0 in the adhoc properties.
#
# TSV columns: run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=2

PKG=io.deephaven.benchmark.tests.standard
CLASSES='RollingGroupTick,RollingGroupTime,EmaTime,CumCombo,RollingCombo,MixedCombo,WhereIn'
DIST=random
ROWSM=10
ITERS=3

# 09-15 nightly, rev e4a7e10a22bc. Baseline.
GOOD=ghcr.io/deephaven/server@sha256:4504e67b9176089fd3d2322309203e6b5bf2d7f901011010a0db85b97dcc6d6b
# 09-16 nightly, rev e2726a000de9. First image containing DH-23676.
BAD=ghcr.io/deephaven/server@sha256:1d8b398ad35620af5ad377981d5a9f32ebc85f54f27ad38a15a21697d5e4914d

jfr_opts() {
    echo "-Xmx24g -XX:StartFlightRecording=name=bench,filename=/data/$1-%t-%p.jfr,settings=profile,maxsize=200m"
}

row() { echo -e "$1\t$2\t$PKG\t$CLASSES\t$ITERS\t$ROWSM\t$DIST\t$(jfr_opts "$3")"; }

row dh23676_good "$GOOD" good
row dh23676_bad "$BAD" bad
