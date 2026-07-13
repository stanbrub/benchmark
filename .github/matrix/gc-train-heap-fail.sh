# Sweep heap size from 48g to 4g to find failure points per GC and test class
# TSV columns: run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=132

IMG=ghcr.io/stanbrub/server:jvm25
PKG=io.deephaven.benchmark.tests.train
CLS='*Train'
ITERS=1
ROWSM=1
DIST=random
MAX_HEAP=48
MIN_HEAP=16

BASE="-XX:+UseStringDeduplication -XX:+UseCompactObjectHeaders -DServerStateTracker.reportIntervalMillis=1000"
CYCLE="-DPeriodicUpdateGraph.targetCycleDurationMillis=1000 -Dtrain.incLoadTarget=1.00 -Dtrain.staticInc=Static+Inc"

declare -A GCS=(
  [g1gc]="-XX:+UseG1GC"
  [zgc]="-XX:-UseG1GC -XX:+UseZGC"
  [shen]="-XX:-UseG1GC -XX:+UseShenandoahGC -XX:ShenandoahGCMode=generational"
  [para]="-XX:-UseG1GC -XX:+UseParallelGC"
)

for gc in g1gc zgc shen para; do
  for h in $(seq ${MAX_HEAP} -2 ${MIN_HEAP}); do
    opts="${GCS[$gc]} -Xms${h}g -Xmx${h}g $BASE $CYCLE"
    echo -e "gc_${gc}_1000_h${h}_j25\t$IMG\t$PKG\t$CLS\t$ITERS\t$ROWSM\t$DIST\t$opts"
  done
done

