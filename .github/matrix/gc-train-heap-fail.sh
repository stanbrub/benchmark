# Sweep heap size to find failure points per GC and test class
# TSV columns: run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options

IMG=ghcr.io/stanbrub/server:jvm25
PKG=io.deephaven.benchmark.tests.train
ITERS=1
ROWSM=10
DIST=random
STEP=1

BASE="-XX:+AlwaysPreTouch -XX:+UseTransparentHugePages -XX:+UseStringDeduplication -XX:+UseCompactObjectHeaders -DServerStateTracker.reportIntervalMillis=1000"
CYCLE="-DPeriodicUpdateGraph.targetCycleDurationMillis=1000 -Dtrain.incLoadTarget=1.00"

declare -A GCS=(
  [g1gc]="-XX:+UseG1GC"
  [zgc]="-XX:-UseG1GC -XX:+UseZGC"
  [shen]="-XX:-UseG1GC -XX:+UseShenandoahGC -XX:ShenandoahGCMode=generational"
  [para]="-XX:-UseG1GC -XX:+UseParallelGC"
)

# Per-class, per-GC heap ranges: "class  gc  min  max"
COMBOS=(				# Failure Success
#  "FilterTrain       g1gc  21  21"	#     20G     21G
#  "FilterTrain       zgc     2  2"	#      1G      2G
  "FilterTrain      shen   15  16"	#     14G     15G
#  "FilterTrain       para    4  4"	#      3G      4G
#  "FormulaTrain      g1gc    1  1"	#              1G
#  "FormulaTrain      zgc     1  1"	#              1G
#  "FormulaTrain      shen    4  4"	#      3G      4G
#  "FormulaTrain      para    1  1"	#              1G
#  "NaturalJoinTrain  g1gc  17  17"	#     16G     17G
#  "NaturalJoinTrain  zgc   16  16"	#     15G     16G
#  "NaturalJoinTrain  shen  18  18"	#     17G     18G
#  "NaturalJoinTrain  para  20  20"	#     19G     20G
#  "AggByTrain        g1gc    1  1"	#              1G
#  "AggByTrain        zgc     1  1"	#              1G
#  "AggByTrain        shen    4  4"	#      3G      4G
#  "AggByTrain        para  17  17"	#     16G     17G
#  "OrderedTrain      g1gc    3  3"	#      2G      3G
#  "OrderedTrain      zgc     3  3"	#      2G      3G
#  "OrderedTrain      shen    3  3"	#      2G      3G
#  "OrderedTrain      para    3  3"	#      2G      3G
#  "UpdateByTrain     g1gc  15  15"	#     14G     15G
#  "UpdateByTrain     zgc   16  16"	#     15G     16G
#  "UpdateByTrain     shen  17  17"	#     16G     17G
#  "UpdateByTrain     para  20  20"	#     19G     20G
)

for combo in "${COMBOS[@]}"; do
  read -r cls gc min_heap max_heap <<< "$combo"
  for h in $(seq ${max_heap} -${STEP} ${min_heap}); do
    opts="${GCS[$gc]} -Xms${h}g -Xmx${h}g $BASE $CYCLE"
    echo -e "gc_${gc}_${cls}_h${h}_j25\t$IMG\t$PKG\t*${cls}\t$ITERS\t$ROWSM\t$DIST\t$opts"
  done
done

