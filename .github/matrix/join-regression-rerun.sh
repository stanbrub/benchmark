# A/B the 2026-07-31 boundary across every join benchmark, to test the DH-23190 trade-off
#
# WHAT WE ALREADY KNOW FROM SOURCE (this run is confirmatory, not exploratory)
#
# e68ce72921 "perf: DH-23190: Improve naturalJoin modification performance. (#8298)" changed left
# add/remove handling from direct per-row insertion to accumulation in a RowSetBuilderSequential
# allocated per hash slot per update cycle, applied in bulk afterwards
# (NaturalJoinModifiedSlotTracker.accumulateLeftRowKey). addLeftAddition is called from the
# add/build path of all eight generated IncrementalNaturalJoinHasher* classes, so the cost is not
# confined to the modify path the PR was optimizing.
#
# Batching amortizes when a slot receives many keys per cycle and is pure overhead when a slot
# receives one key: an object allocation plus a RowSet build to perform a single insert. Benchmark
# key cardinality (StandardTestRunner.generateSourceTable) predicts exactly who loses:
#
#   benchmark                       join keys      distinct    step at 2026-07-31
#   NaturalJoin- 1 Col -Inc         key5          1,000,000            -10.2%
#   ExactJoin-   1 Col -Inc         key5          1,000,000             -6.2%
#   NaturalJoin- 2/3 Cols           key1 x key2      10,100            none
#   ExactJoin-   2/3 Cols           key1 x key2      10,100            none
#
# Only the two high-cardinality (few-rows-per-slot) benchmarks regressed, and both on the same day.
# exactJoin shares the machinery: QueryTable.exactJoinImpl delegates to naturalJoinInternal with
# NaturalJoinType.EXACTLY_ONE_MATCH.
#
# The 2-col and 3-col variants are the real control here: same changed code, low cardinality, no
# regression. They are included automatically since class selection pulls in all six benchmarks
# per class.
#
# IN THE JFR, EXPECT on the bad side for NaturalJoin/ExactJoin 1 Col -Inc: RowSetFactory
# .builderSequential, BasicRowSetBuilderSequential appendKey/build, and raised allocation pressure;
# flat or absent for the 2-col and 3-col variants. If the 1-col and multi-col variants move
# together, the per-slot builder is NOT the mechanism and this attribution is wrong.
#
# IMAGES: the 07-30 nightly ran rev ef1c2239, the 07-31 nightly ran f3fd7fde. Three commits apart:
#   f3fd7fde72  perf: DH-23193: Improve Barrage Low-Level Serialization/Deserialization (#8293)
#   e670ed9d50  docs: DH-22220: Update UI docs for Conditional Formatting (#8303)
#   e68ce72921  perf: DH-23190: Improve naturalJoin modification performance. (#8298)   <-- suspect
# Both digests are the images those nightlies actually ran (07-30 pushed 04:43Z against a 07:02Z
# run, 07-31 pushed 05:21Z against a 07:08Z run), not merely images dated that day.
#
# SCOPE CAVEATS
#
# AsOfJoin, ReverseAsOfJoin and Join are included for breadth and as unaffected-machinery controls.
# They should not move at this boundary. Note AsOfJoin- 3 Cols -Static does NOT have a 10%
# regression to find: its worst forward drop in 345 nightlies (2025-09-01..2026-09-11) is -8.4%,
# and its smoothed level is flat year over year (3.92M in 2025-10, 3.91M in 2026-09). Across the
# window where the 1-col joins fell 11% it rose 8%.
#
# The 1-col join decline is at least two events. Separate and still unexplained: 2026-02-21 ->
# 2026-03-10 (15 nightlies) where NaturalJoin- 1 Col -Inc fell 10.7% and ExactJoin- 1 Col -Inc
# 11.3%, four months before DH-23190. Full-year forward drops are -20.6% and -19.9%, so this
# boundary accounts for roughly half.
#
# Also open against this same code: DH-23211, spurious right modifies when the first/last duplicate
# row key does not change, filed by rcaudy during review of #8298.
#
# ITERS=5 because a 10% effect sits close to the +-5-9% day-to-day noise floor and needs medians;
# the 2026-08-27 updateBy drop was -24% and survived a single shot.
#
# TSV columns: run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=2

PKG=io.deephaven.benchmark.tests.standard
DIST=random
ROWSM=10
ITERS=5

GOOD=ghcr.io/deephaven/server@sha256:602b95e6e9fdde1a683297fa8d0d41ac186c9fb984816cd3f79f9f318f6a67f6
BAD=ghcr.io/deephaven/server@sha256:1941e0c8064c48eb81de8d10133ff336fa098f85ae25e496d4060c314a5d9c28

# Every join class in tests/standard/join: 30 benchmarks, 6 per class (-Inc and -Static x 3 shapes),
# ~10 min per iteration per image -> ~1.7 h for both sides at ITERS=5, 300 JFR recordings.
# Suspects first, then the controls.
#
# Do NOT write the "Test" suffix here. ConsoleLauncherUtil.formatConsoleWildcards rewrites this
# list into ^.*[.](entry1|entry2|...)Test.*$ -- it appends Test itself, so "NaturalJoinTest" would
# compile to (NaturalJoinTest.*)Test.*$ and match nothing. Each bare entry is anchored right after
# the package dot, so "Join" matches JoinTest only and does not also catch NaturalJoinTest,
# ExactJoinTest, AsOfJoinTest or ReverseAsOfJoinTest; likewise "AsOfJoin" does not catch
# ReverseAsOfJoinTest. Verified: these five entries resolve to exactly the five join classes.
CLASSES='NaturalJoin,ExactJoin,AsOfJoin,ReverseAsOfJoin,Join'

# DataIndex-AsOfJoin No Index / Indexed also exist, but only as 4 of the 12 benchmarks in
# index.DataIndexBenefitTest, whose other 8 are WhereIn, AvgBy and Sort. Including it costs 277
# s/iter (+46 min at ITERS=5) and drags in 8 non-join benchmarks, so it is off by default. To add
# it, append ,DataIndexBenefit to CLASSES (PKG already covers the index subpackage recursively).

# JFR per engine JVM. The engine restarts once per benchmark, so each benchmark gets its own
# recording; %t makes the names unique (%p is always 1 in the container). Distinct prefix per side
# because both land in the same directory. Requires docker.compose.stop.timeout>0 in the adhoc
# properties, or the JVM is SIGKILLed before it can write the dump and every file is 0 bytes.
jfr_opts() {
    echo "-Xmx24g -XX:StartFlightRecording=name=bench,filename=/data/$1-%t-%p.jfr,settings=profile,maxsize=200m"
}

# emit <run_label> <docker_image> <jfr_filename_prefix>
row() { echo -e "$1\t$2\t$PKG\t$CLASSES\t$ITERS\t$ROWSM\t$DIST\t$(jfr_opts "$3")"; }

row dh23190_good "$GOOD" good
row dh23190_bad "$BAD" bad
