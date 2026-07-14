# Sweep heap size to find failure points per GC and test class
# TSV columns: run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options

IMG=ghcr.io/stanbrub/server:jvm25
PKG=io.deephaven.benchmark.tests.train
ITERS=1
ROWSM=10
DIST=random
STEP=4
MODE=Static  # Static+Inc

BASE="-XX:+AlwaysPreTouch -XX:+UseTransparentHugePages -XX:+UseStringDeduplication -XX:+UseCompactObjectHeaders -DServerStateTracker.reportIntervalMillis=1000"
CYCLE="-DPeriodicUpdateGraph.targetCycleDurationMillis=1000 -Dtrain.incLoadTarget=1.00 -Dtrain.staticInc=${MODE}"

declare -A GCS=(
  [g1gc]="-XX:+UseG1GC"
  [zgc]="-XX:-UseG1GC -XX:+UseZGC"
  [shen]="-XX:-UseG1GC -XX:+UseShenandoahGC -XX:ShenandoahGCMode=generational"
  [para]="-XX:-UseG1GC -XX:+UseParallelGC"
)

# Per-class, per-GC heap ranges: "class  gc  min  max"
COMBOS=(
  "FilterTrain       g1gc  16  48"
  "FilterTrain       zgc   16  48"
  "FilterTrain       shen  16  48"
  "FilterTrain       para  16  48"
  "FormulaTrain      g1gc  16  48"
  "FormulaTrain      zgc   16  48"
  "FormulaTrain      shen  16  48"
  "FormulaTrain      para  16  48"
  "NaturalJoinTrain  g1gc  16  48"
  "NaturalJoinTrain  zgc   16  48"
  "NaturalJoinTrain  shen  16  48"
  "NaturalJoinTrain  para  16  48"
  "AggByTrain        g1gc  16  48"
  "AggByTrain        zgc   16  48"
  "AggByTrain        shen  16  48"
  "AggByTrain        para  16  48"
  "OrderedTrain      g1gc  16  48"
  "OrderedTrain      zgc   16  48"
  "OrderedTrain      shen  16  48"
  "OrderedTrain      para  16  48"
  "UpdateByTrain     g1gc  16  48"
  "UpdateByTrain     zgc   16  48"
  "UpdateByTrain     shen  16  48"
  "UpdateByTrain     para  16  48"
)

for combo in "${COMBOS[@]}"; do
  read -r cls gc min_heap max_heap <<< "$combo"
  for h in $(seq ${max_heap} -${STEP} ${min_heap}); do
    opts="${GCS[$gc]} -Xms${h}g -Xmx${h}g $BASE $CYCLE"
    echo -e "gc_${gc}_${cls}_h${h}_j25\t$IMG\t$PKG\t*${cls}\t$ITERS\t$ROWSM\t$DIST\t$opts"
  done
done

