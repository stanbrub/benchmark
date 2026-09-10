# A/B the commit window that regressed bucketed incremental updateBy on 2026-08-27
#
# The 08-26 nightly ran image f3c1b0bd and was healthy; the 08-27 nightly ran ab3e36a4 and lost
# 5-27% across the updateBy family. Four commits separate those two images, all dated 08-26:
#   e5205e7f91  chore(gradle): bump jetty 12.1.11 -> 12.1.12          (#8347)
#   e48d95d6fc  perf: DH-23407: Improve super-linear rowset ops       (#8413)   <-- suspect
#   893188eaf5  fix: DH-23199: manifest.json loader details           (#8299)
#   ab3e36a467  chore: Update web version 1.27.5                      (#8418)
#
# Both digests are the images the nightlies actually ran, not merely images dated that day.
# 08-27 had a second push at 15:18Z, hours after the 06:00 run, which is NOT the one used.
#
# Class list derived from nightly history: benchmarks whose op_rate dropped >5% sustained
# (mean 08-20..08-26 vs 08-27..09-02) AND whose worst single day across 2026-06-25..2026-09-09
# was exactly 08-27. Tier A is the >=15% subset, which is unambiguous against the +-5-9%
# day-to-day noise. Tier B is the 5-15% remainder, left off by default on cost.
#
# TSV columns: run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=2

PKG=io.deephaven.benchmark.tests.standard
DIST=random
ROWSM=10

# One iteration. Each row already yields 144 independent measurements (18 classes x 8 benchmarks),
# so a family-wide 15-27% shift is unmissable without repeats, and every extra iteration multiplies
# the JFR file count. Raise this for a timing-only rerun with the recording turned off.
ITERS=1

GOOD=ghcr.io/deephaven/server@sha256:c81da3595be567b552f6d97984334511161233ac56243bf6ae98a7dacb5a2074
BAD=ghcr.io/deephaven/server@sha256:f27d2369223aff16ab43115fde226b3714a9e7916712638009d0417ba7dde4da

# 18 classes that lost >=15%, worst first. ~52 min per iteration per image.
TIER_A='RollingGroupTick*,RollingMaxTick*,RollingSumTick*,RollingAvgTick*,RollingProdTick*,RollingWAvgTick*,RollingMinTick*,RollingStdTick*,RollingMinTime*,RollingMaxTime*,RollingSumTime*,RollingProdTime*,RollingCountTick*,RollingCountTime*,RollingAvgTime*,RollingStdTime*,RollingWAvgTime*,RollingGroupTime*'

# 18 classes that lost 5-15%. Adding these costs ~60 min per iteration per image; to include
# them, append ",$TIER_B" to the CLASSES line below and set EXPECTED_COMBOS to match.
TIER_B='MixedCombo*,RollingFormulaTick,RollingCombo*,EmMinTime*,EmsTime*,EmaTick*,EmaTime*,EmMaxTime*,CumProd*,EmMinTick*,CumMax*,RollingCountWhereTick*,EmStdTick*,EmMaxTick*,CumCombo*,RollingCountWhereTime*,EmsTick*,EmStdTime*'

CLASSES="$TIER_A"

# JFR per engine JVM. The engine restarts once per benchmark, so each benchmark gets its own
# recording. %t (UTC, second resolution) is what makes the names unique; %p is always 1 because
# java is pid 1 in the container. Both rows write to the same directory, so the third argument
# gives each image its own prefix - without it, good and bad are only separable by knowing the
# cutover time. Requires docker.compose.stop.timeout>0 in the adhoc properties, or the JVM is
# killed before it can write the dump and every file lands at 0 bytes.
jfr_opts() {
    echo "-Xmx24g -XX:StartFlightRecording=name=bench,filename=/data/$1-%t-%p.jfr,settings=profile,maxsize=200m"
}

# emit <run_label> <docker_image> <jfr_filename_prefix>
row() { echo -e "$1\t$2\t$PKG\t$CLASSES\t$ITERS\t$ROWSM\t$DIST\t$(jfr_opts "$3")"; }

row dh23407_good "$GOOD" good
row dh23407_bad "$BAD" bad
