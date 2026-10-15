# Smoke test for parquet-cache-48g.sh: the parts no earlier study has run
# TSV columns: run_type, run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
#
# The default cap at 48G is already proven for all training tests with G1 and ZGC on this host (GC study,
# Final-gc-report, n=7). Not yet run: the min cap (2 slots), and the training tests other than FilterTrain on the
# jvm25 image. One failing test loses its whole row, so check both before committing a weekend to the full matrix.
# Labels are separate from the weekend sets, so these runs don't mix into them.
EXPECTED_COMBOS=2

RTYP='adhoc'
IMG=ghcr.io/stanbrub/server:jvm25
PKG=io.deephaven.benchmark.tests.train
CLS='*Train'
ITERS=1
ROWSM=10
DIST=random
HEAP=48
MIN_CAP=2

declare -A GCS=(
  [g1gc]="-XX:+UseG1GC"
  [zgc]="-XX:-UseG1GC -XX:+UseZGC"
)

BASE="-XX:+AlwaysPreTouch -XX:+UseTransparentHugePages -XX:+UseStringDeduplication -XX:+UseCompactObjectHeaders -DServerStateTracker.reportIntervalMillis=1000"
CYCLE="-DPeriodicUpdateGraph.targetCycleDurationMillis=1000 -Dbench.incLoadTarget=1.00"
# The cache rejects an initial capacity above its max (default initial 128)
CACHE="-DParquetColumnLocation.maxPageCacheSize=${MIN_CAP} -DParquetColumnLocation.initialPageCacheSize=${MIN_CAP}"

for gc in g1gc zgc; do
  opts="${GCS[$gc]} -Xms${HEAP}g -Xmx${HEAP}g $CACHE $BASE $CYCLE"
  echo -e "$RTYP\tpcache48smoke_${gc}_min_c${MIN_CAP}\t$IMG\t$PKG\t$CLS\t$ITERS\t$ROWSM\t$DIST\t$opts"
done
