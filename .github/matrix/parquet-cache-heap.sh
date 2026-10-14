# Sweep heap size with the Parquet page cache capped to a fixed fraction of the heap (worst case)
# TSV columns: run_type, run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
#
# Worst-case cache bytes = (columns read x files) x maxPageCacheSize x values per page x bytes per value
# FilterTrain at 10M scale: 4 columns x 8 files x 20,000 values x 8 bytes = 5,120,000 bytes per cache slot
# (default 8192 slots = 39.06 GiB). Each row sets maxPageCacheSize so the worst case is PCT% of the heap.

RTYP='adhoc'
IMG=ghcr.io/stanbrub/server:jvm25
PKG=io.deephaven.benchmark.tests.train
CLS=FilterTrain
ITERS=1
ROWSM=10
DIST=random

COLS=4; FILES=8; VALS=20000; BYTES=8
SLOT_BYTES=$((COLS * FILES * VALS * BYTES))
MAX_HEAP=24; MIN_HEAP=4; STEP=2
PCTS=(70 50)

BASE="-XX:+UseG1GC -XX:+AlwaysPreTouch -XX:+UseTransparentHugePages -XX:+UseStringDeduplication -XX:+UseCompactObjectHeaders -DServerStateTracker.reportIntervalMillis=1000"
CYCLE="-DPeriodicUpdateGraph.targetCycleDurationMillis=1000 -Dbench.incLoadTarget=1.00"

for pct in "${PCTS[@]}"; do
  for h in $(seq ${MAX_HEAP} -${STEP} ${MIN_HEAP}); do
    cache=$((pct * h * 1073741824 / (100 * SLOT_BYTES)))
    opts="-Xms${h}g -Xmx${h}g -DParquetColumnLocation.maxPageCacheSize=${cache} $BASE $CYCLE"
    echo -e "$RTYP\tpcache_p${pct}_h${h}_c${cache}\t$IMG\t$PKG\t*${CLS}\t$ITERS\t$ROWSM\t$DIST\t$opts"
  done
done
