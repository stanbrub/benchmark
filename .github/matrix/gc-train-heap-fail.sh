# Sweep heap size to find failure points per GC and test class
# TSV columns: run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options

IMG=ghcr.io/stanbrub/server:jvm25
PKG=io.deephaven.benchmark.tests.train
ITERS=1
ROWSM=10
DIST=random
STEP=1
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
COMBOS=(				# Failure Success
#  "FilterTrain       g1gc  20  21"	#     20G     21G
  "FilterTrain       zgc     1  3"	#              4G
#  "FilterTrain      shen   13  14"	#     13G     14G
  "FilterTrain       para    1  3"	#              4G
#  "FormulaTrain      g1gc    1  1"	#              1G
#  "FormulaTrain      zgc     1  1"	#              1G
#  "FormulaTrain      shen     3  4"	#      3G      4G
#  "FormulaTrain      para    1  1"	#              1G
#  "NaturalJoinTrain  g1gc   16  17"	#     16G     17G
#  "NaturalJoinTrain  zgc    15  16"	#     15G     16G
#  "NaturalJoinTrain  shen    17  18"	#     17G     18G
#  "NaturalJoinTrain  para  19  20"	#     19G     20G
#  "AggByTrain        g1gc    1  1"	#              1G
#  "AggByTrain        zgc     1  1"	#              1G
#  "AggByTrain        shen    1  1"	#              1G
#  "AggByTrain        para  16  17"	#     16G     17G
#  "OrderedTrain      g1gc    2  3"	#      2G      3G
#  "OrderedTrain      zgc     2  3"	#      2G      3G
#  "OrderedTrain      shen    2  3"	#      2G      4G
#  "OrderedTrain      para    2  3"	#      2G      4G
#  "UpdateByTrain     g1gc   14  15"	#     14G     15G
#  "UpdateByTrain     zgc    15  16"	#     15G     16G
#  "UpdateByTrain     shen  16  17"	#     16G     17G
#  "UpdateByTrain     para  17  19"	#     18G     19G
)

for combo in "${COMBOS[@]}"; do
  read -r cls gc min_heap max_heap <<< "$combo"
  for h in $(seq ${max_heap} -${STEP} ${min_heap}); do
    opts="${GCS[$gc]} -Xms${h}g -Xmx${h}g $BASE $CYCLE"
    echo -e "gc_${gc}_${cls}_h${h}_j25\t$IMG\t$PKG\t*${cls}\t$ITERS\t$ROWSM\t$DIST\t$opts"
  done
done

