# Release benchmarks: one full pass per row, so every benchmark gets the same sample count
#
# Each row is a complete pass over the standard suite and publishes its results before the next row
# starts. That matters for a run this long: iterations inside a single row would upload nothing until
# the end, and a failure part way through would lose the whole run. Rows are independent, so a failed
# row costs one pass instead of all of them.
#
# Row count is the sample count. With test_iterations at 1, a release row runs as two jobs, !Iterate
# then Iterate, which together cover the suite exactly once. So 7 rows means n=7 for all 666
# benchmarks, where the nightly scheme gives n=1 to the 562 untagged ones and n=5 to the 104 tagged.
#
# Keep the row count odd. Queries report the middle run by rate and carry that run's id, so an even
# count has no real middle and the reported numbers stop matching a run that happened.
#
# All rows share the run_label, so they land in one set and accumulate. <version> takes the label from
# the version the engine reports, so edge becomes a 99 in the last position and cannot collide with
# the release of the same major.minor.
#
# Budget roughly 4 hours per row of test time, plus setup, engine restarts and data generation.
#
# TSV columns: run_type, run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=7

RTYP='release'
LABEL='<version>'
IMG=${IMG:-edge}
PKG=io.deephaven.benchmark.tests.standard
CLS='*'
ITERS=1
ROWSM=10
DIST=random
OPTS='<default>'

PASSES=7

for i in $(seq ${PASSES}); do
  echo -e "$RTYP\t$LABEL\t$IMG\t$PKG\t$CLS\t$ITERS\t$ROWSM\t$DIST\t$OPTS"
done
