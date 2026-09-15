# Bisect the 2026-09-12 boundary: the updateBy fix landed and a vectorized-UDF regression appeared
#
# Both events are in the same nightly, verified from the nightly series:
#   RollingGroupTick- 3 Groups 100K Unique Combos -Inc    352K -> 505K   +43.7%   (fix landed)
#   RollingMaxTick-   3 Groups 100K Unique Combos -Inc    364K -> 499K   +37.2%
#   UDF- 2 Doubles to Double Python Hints -Static        4.43M -> 3.63M    -18%   (regression)
#   UDF- 2 Doubles to Double Python Hints Serial -Static 4.40M -> 3.69M    -16%
#   UDF- 2 Doubles to Double Python Hints No Vectorize -Static  0.46M -> 0.45M  flat
#
# The environment is NOT the cause: the two nightlies ran identical Temurin-21.0.12+8, Python 3.12.3,
# numpy 2.5.3, pandas 3.0.5, jpy 2.1.0, pyarrow 25.0.1. Only contourpy and matplotlib differ, and
# neither can touch a select.
#
# EVERY ROW IS A SOURCE BUILD FROM A COMMIT. No prebuilt images are mixed in, so all five rows are
# built and packaged the same way and are directly comparable to each other. The endpoints are the
# revisions the two nightly images were built from, so the series spans exactly the same window the
# nightly data showed, without inheriting differences between the official image pipeline and a local
# build.
#
# Seven commits separate the endpoints; three touch engine code:
#   9fa9e83c55  docs: DOC-621   (== revision of the 09-11 nightly image, == 8cbd379b3^)
#   8cbd379b3   fix:  DH-23579: Keep the packed ArrayContainer shared flag in the short[] itself
#               ArrayContainer, RspArray, RspBitmap, RspIterator, RspRangeBatchIterator,
#               RspRangeIterator, RspReverseIterator                       <-- suspect A
#   5e417cbd1   perf: DH-23630: Skip the SortedRanges insert pre-pass when it cannot pay for itself
#               RspArray, RspBitmap                          <-- suspect B, and the updateBy fix
#   3ed1d774b   fix:  DH-23502: Correct sorted-column pushdown match and range semantics
#               MatchFilter, range filters, binary search kernels          <-- suspect C
#   261121f7c   docs: Fix Amazon Corretto links
#   3d2e31be0   chore: DH-23636: Add gomod ecosystem to Dependabot configuration
#   dd16b9c1c   feat: DH-23447: Generate, ship sourcemaps for JS API
#   c50cd7f2f   chore: Update web version 1.29.0  (== revision of the 09-12 nightly image)
#
# Suspects A and B are both by Charles P. Wright, both dated 2026-09-11, and both touch the same
# RspArray/RspBitmap files DH-23407 did. This is follow-on work in the rowset layer that shipped as
# a pair, which is why the fix and the regression arrived on the same night.
#
# ATTRIBUTION. The rows are cumulative and the ancestry is strictly linear (verified:
# 8cbd379b3^ == 9fa9e83c55, 5e417cbd1^ == 8cbd379b3, 3ed1d774b^ == 5e417cbd1), so each consecutive
# pair isolates exactly one commit:
#   src_base    -> src_dh23579   attributes suspect A (DH-23579)
#   src_dh23579 -> src_dh23630   attributes suspect B (DH-23630, the updateBy fix)
#   src_dh23630 -> src_dh23502   attributes suspect C (DH-23502)
#   src_dh23502 -> src_top       attributes the four non-engine commits, expected to be flat
# src_base -> src_top must reproduce the whole nightly step. If it does not, the effect depends on
# something outside these commits and the bisect is invalid.
#
# WHY A IS THE PRIMARY SUSPECT RATHER THAN THE FIX ITSELF
#
# The regressed benchmark measures `source.select(['num1=f(num1, num2)'])` on a static 10M row
# parquet-backed table. There are no incremental cycles and no row set inserts, so the DH-23630
# insert-pre-pass gate has no obvious path to it. DH-23579 rewrites ArrayContainer and touches all
# four Rsp*Iterator classes, and chunk-wise row set iteration is exactly what a static select does.
#
# WHY ONLY THE VECTORIZED UDFs MOVED
#
# The vectorized path runs at ~4.4M rows/s with one Python call per 2048-row chunk. The No Vectorize
# path runs at ~0.46M rows/s with one call per row. A small absolute per-row engine cost is a large
# percentage of the fast path and is completely masked by Python call overhead on the slow path. That
# makes these UDF benchmarks the suite's most sensitive detector of small engine-side costs -- it is
# not evidence that UDFs use the insert path.
#
# FALSIFICATION: if the No Vectorize variants also drop, the "masked by Python overhead" reading is
# wrong and the cause is something specific to the vectorized/jpy path instead.
#
# JDK. The bench server has JDK 17, but that only launches Gradle (BUILD_JAVA in
# build-server-distribution-remote.sh). deephaven-core defaults compilerVersion=21 and settings.gradle
# applies org.gradle.toolchains.foojay-resolver-convention, so Gradle provisions a 21 toolchain;
# bytecode targets languageLevel=17 with runtimeVersion=21. The container's JDK comes from the base
# image, not the build host: contexts/server/Dockerfile does
# `FROM docker.io/eclipse-temurin:${OPENJDK_VERSION}` and server-base.hcl defaults OPENJDK_VERSION=21,
# PYTHON_VERSION=3.12, UBUNTU_VERSION=24.04. So these builds run Temurin 21 on Ubuntu 24.04 with
# Python 3.12, matching the nightly images. Confirmed working: commit-hash builds have been run on
# this server before.
#
# The <owner>:<ref> form is what triggers a source build (build-server-distribution-remote.sh splits
# on the colon and does `git checkout <ref>` in a full clone, which is why commit hashes work). A
# bare hash would NOT work: with no colon it is treated as a tag and becomes
# ghcr.io/deephaven/server:<hash>, and no such tags exist -- both endpoints return HTTP 404.
#
# RollingGroupTick is the positive control, not a comparison: the fix's +43.7% must appear at
# src_dh23630 and persist through src_top. If it does not, the wrong code is being measured. This is
# what exposed the first attempt at this matrix, where all five rows silently ran one build and the
# UDF numbers looked like a clean "no regression anywhere" result. RollingMaxTick and RollingSumTick
# were dropped as redundant for that purpose -- they cost ~300 s/iter for no extra assurance.
#
# COST. Five source builds. clear-test-server-remote wipes ${GIT_DIR} and every docker image between
# combinations, so each row does its own full clone, gradle assemble and py-server wheel build -- the
# skip guards in build-server-distribution-remote.sh and build-docker-image-remote.sh never fire
# across rows. That isolation is what makes the rows comparable, and it means five builds plus
# ~41 min of benchmarking per row. Budget hours.
#
# TSV columns: run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=5

PKG=io.deephaven.benchmark.tests.standard
DIST=random
ROWSM=10

# 3 iterations. The UDF effect is -16 to -18% with run-to-run CV under 1% across 7 runs, and the
# updateBy gain is +37 to +44%, so both are far outside noise and do not need 5. build-matrix.sh
# forces this to an odd number, so 1, 3 and 5 are the real choices.
ITERS=3

# Cumulative source builds, in commit order. Full hashes so the record is unambiguous.
SRC_BASE=deephaven:9fa9e83c55ccb2e9ddedd61d1ba59d2d8da68c85
SRC_DH23579=deephaven:8cbd379b3e81c3fe316b78f20722a01bf977d3b6
SRC_DH23630=deephaven:5e417cbd1bb3d8f9e3fbd9233f2034027c5319f3
SRC_DH23502=deephaven:3ed1d774b5ca7488700fa7e51377f28d9455be74
SRC_TOP=deephaven:c50cd7f2fc02b819376a80e8767dff76c0624aa2

# For the record, the nightly images those endpoints correspond to, not used as rows here:
#   09-11  sha256:2672b50205bd7ecef5d9fcc7016bd4345f75ff5333da3d77b9f05ebc50d2638b  (pushed 03:48Z)
#   09-12  sha256:fc92a762040dd422ad711f18393d728edc5c0cb5e53de59171a67679ae3c0eef  (pushed 03:50Z)
# 09-11 had three edge pushes; the 06:49Z one is tagged 42.5/latest and the 18:26Z one is hours after
# the run, so neither is what that nightly measured.

# 21 benchmarks: UserFormula 13 (~359 s/iter, measured) plus RollingGroupTick 8 (~160 s/iter).
# ~8.7 min per iteration -> ~26 min of benchmarking per row at ITERS=3, on top of a build per row.
#
# Do NOT write the "Test" suffix. ConsoleLauncherUtil.formatConsoleWildcards rewrites this list into
# ^.*[.](entry1|entry2|...)Test.*$ -- it appends Test itself, so "UserFormulaTest" would compile to
# (UserFormulaTest.*)Test.*$ and match nothing, silently. Verified: these two entries resolve to
# exactly UserFormulaTest and RollingGroupTickTest.
CLASSES='UserFormula,RollingGroupTick'

# JFR per engine JVM. The engine restarts once per benchmark, so each benchmark gets its own
# recording; %t makes the names unique (%p is always 1 in the container). Distinct prefix per row
# because they all land in the same directory. Requires docker.compose.stop.timeout>0 in the adhoc
# properties, or the JVM is SIGKILLed before it can write the dump and every file is 0 bytes.
jfr_opts() {
    echo "-Xmx24g -XX:StartFlightRecording=name=bench,filename=/data/$1-%t-%p.jfr,settings=profile,maxsize=200m"
}

# emit <run_label> <docker_image> <jfr_filename_prefix>
row() { echo -e "$1\t$2\t$PKG\t$CLASSES\t$ITERS\t$ROWSM\t$DIST\t$(jfr_opts "$3")"; }

row src_base "$SRC_BASE" src-base
row src_dh23579 "$SRC_DH23579" src-dh23579
row src_dh23630 "$SRC_DH23630" src-dh23630
row src_dh23502 "$SRC_DH23502" src-dh23502
row src_top "$SRC_TOP" src-top
