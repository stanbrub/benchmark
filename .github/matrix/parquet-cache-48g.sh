# Sweep the Parquet page cache cap at a fixed 48G heap for every training benchmark, with G1 and ZGC
# TSV columns: run_type, run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
#
# Worst-case cache bytes = (columns read x files) x maxPageCacheSize x values per page x bytes per value
# Percentages are FilterTrain's worst case, the largest of the training tests: 4 columns x 8 files x 20,000 values
# x 8 bytes = 5,120,000 bytes per cache slot. The default 8192 slots (39.06 GiB) is 81% of 48G, so the default row
# anchors the top of the sweep. The other training tests read fewer columns, and most of them only one or two of the
# 8 files, so the same cap is a smaller share of the heap for them.
#
# The min row is the smallest cap the cache accepts (2), the nearest thing to off. It also lowers the initial
# capacity, because the cache rejects an initial capacity above its max.
#
# Rows sharing a label accumulate into one set. PASSES runs every cap once per pass, alternating direction,
# so slow drift over the run hits every cap about equally instead of favoring whichever ran first.
# 7 passes give n=7 per set, enough for a rank test to separate caps (3 vs 3 can't get below p=0.1).
#
# 84 rows at roughly 32-37 minutes each (all seven training tests) is about 45-52 hours: a weekend run.
EXPECTED_COMBOS=84

RTYP='adhoc'
IMG=ghcr.io/stanbrub/server:jvm25
PKG=io.deephaven.benchmark.tests.train
CLS='*Train'
ITERS=1
ROWSM=10
DIST=random
HEAP=48
PASSES=7

COLS=4; FILES=8; VALS=20000; BYTES=8
SLOT_BYTES=$((COLS * FILES * VALS * BYTES))
DEFAULT_CAP=8192
INITIAL_CAP=128
MIN_CAP=2
PCTS=(50 20 10 5)

declare -A GCS=(
  [g1gc]="-XX:+UseG1GC"
  [zgc]="-XX:-UseG1GC -XX:+UseZGC"
)
GC_ORDER=(g1gc zgc)

BASE="-XX:+AlwaysPreTouch -XX:+UseTransparentHugePages -XX:+UseStringDeduplication -XX:+UseCompactObjectHeaders -DServerStateTracker.reportIntervalMillis=1000"
CYCLE="-DPeriodicUpdateGraph.targetCycleDurationMillis=1000 -Dbench.incLoadTarget=1.00"

row() {
  local gc=$1 name=$2 cache=$3 opts="${GCS[$1]} -Xms${HEAP}g -Xmx${HEAP}g"
  if [[ -n "${cache}" ]]; then
    opts+=" -DParquetColumnLocation.maxPageCacheSize=${cache}"
    if (( cache < INITIAL_CAP )); then opts+=" -DParquetColumnLocation.initialPageCacheSize=${cache}"; fi
  fi
  echo -e "$RTYP\tpcache48_${gc}_${name}\t$IMG\t$PKG\t${CLS}\t$ITERS\t$ROWSM\t$DIST\t$opts $BASE $CYCLE"
}

# One line per cap, largest first: <name> <cache, empty for the engine default>
caps() {
  echo "default_c${DEFAULT_CAP} -"
  for pct in "${PCTS[@]}"; do
    cache=$((pct * HEAP * 1073741824 / (100 * SLOT_BYTES)))
    echo "p${pct}_c${cache} ${cache}"
  done
  echo "min_c${MIN_CAP} ${MIN_CAP}"
}

for pass in $(seq ${PASSES}); do
  if (( pass % 2 )); then order=cat; else order=tac; fi
  caps | ${order} | while read -r name cache; do
    [[ "${cache}" == "-" ]] && cache=""
    for gc in "${GC_ORDER[@]}"; do
      row "${gc}" "${name}" "${cache}"
    done
  done
done
