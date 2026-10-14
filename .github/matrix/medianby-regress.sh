# Measure how much of the #8858 median/percentile regressions the follow-up #8914 recovers
#
# #8858 (DH-24000) sped up most MedianBy and SortedCombo benchmarks, many by 2-3x, but four
# regressed (see the 2026-10-13_medianby-percentile study):
#
#   MedianBy- No Groups -Inc                    -15% to -19%  Object TimSort over a multi-million-
#                                                             value staged chunk per cycle
#   SortedCombo-AggBy- 5 Ops 3 Groups 1M -Inc     -7% to -11%  per-group staging at ~0.3 values per
#                                                             group per cycle, plus SSM churn
#   SortedCombo-AggBy- 5 Ops No Groups -Static    -7% to  -9%  growing a 200M-value array by doubling
#   SortedCombo-AggBy- 5 Ops No Groups -Inc       -1% to  -9%  single-group staging growth (weakest)
#
# Two JFR rounds on this matrix found the causes; this round is throughput only, the measurement of
# record. #8914 (draft, Charles Wright) targets them:
#   - Object values stage in shared, bounded windows: no multi-million-value sort per cycle
#   - an emptied lo/hi SSM is kept until both are empty, for every type: no create/clear churn
#   - a zero-key static aggregation presizes its array from the table size: no doubling
#   - primitive values keep per-destination staging, which #8914 itself says leaves primitive
#     columns with 1M keys slower than before #8858
#
# Expectations: MedianBy No Groups -Inc (strings) recovers; SortedCombo No Groups -Static improves;
# SortedCombo 3 Groups 1M -Inc (num1 is a double) recovers only the churn share, so some regression
# likely remains.
#
# Rows, all built from source so the three are the same kind of image:
#
#   before_8858  deephaven:2a29db7a5f  parent of #8858
#   after_8858   deephaven:591c80a5bf  main at #8914's merge base. Besides #8858 it adds #8799 and
#                                      #8828, which the first JFR round showed flat on all 18
#                                      MedianBy/SortedCombo benchmarks
#   after_8914   cpwright:a4e38a4e96   #8914 head, from Charles's fork
#
# before_8858 -> after_8858 re-measures the regressions. after_8858 -> after_8914 is #8914 alone,
# since #8914 is built on exactly that commit. before_8858 -> after_8914 is what, if anything, is
# still regressed with both PRs in. The refs are pinned SHAs: the PR is a draft and may move, and
# results should name the exact commit measured. To measure a newer push, change PR8914 and the
# labels.
#
# A cpwright row clones cpwright/deephaven-core; the build script repoints origin per owner, so
# mixing owners in one matrix is supported.
#
# config_options is <default> (-Xmx24g, no JFR, no pre-touch), matching the nightly. Pre-touch
# would hide the two page-fault regressions, which a user on a fresh server does see.
#
# Not included: larger update cycles via -Dbench.incLoadTarget. At 1M groups the source has only
# ~10 values per group in total, so even a few long cycles cannot give a group enough values per
# cycle to change the result. Testing the cycle-size effect would need its own benchmark.
#
# ITERS=11. The four original regressions (7-19%) would resolve at 7, but the open question for
# #8914 is whether a residual of a few percent remains. 11 tightens each row's median by about 20%
# and leaves room under a Bonferroni threshold for 18 benchmarks x 3 comparisons (p<0.00093). At 7,
# complete separation gives p=0.00058 but a single overlapping run gives 0.0012, which fails it;
# at 11, complete separation gives 0.0000028, with room for a few overlaps. It also matches the
# 11-iteration adhoc sets that first measured the regressions. Class lists omit the Test suffix:
# ConsoleLauncherUtil appends it. Labels are new, so nothing appends to earlier sets.
#
# Expect roughly 6 minutes per iteration per row, about 3.3 hours for the three rows plus three
# source builds.
#
# TSV columns: run_type, run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=3

RTYP='adhoc'
PKG=io.deephaven.benchmark.tests.standard
CLASSES='MedianBy,SortedCombo'
DIST=random
ROWSM=10
ITERS=11
PLAIN='<default>'

BEFORE=deephaven:2a29db7a5fe74744cc5f3fa05b9a18d14f096dd4
MAIN=deephaven:591c80a5bf1c509237f78095ffadde42d4ed0c64
PR8914=cpwright:a4e38a4e966905afc040abcb3cd02ab22c931cf0

row() { echo -e "$RTYP\t$1\t$2\t$PKG\t$CLASSES\t$ITERS\t$ROWSM\t$DIST\t$PLAIN"; }

row medianby_r3_before_8858 "$BEFORE"
row medianby_r3_after_8858  "$MAIN"
row medianby_r3_after_8914  "$PR8914"
