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
COMBOS=(				# Failure Success
#  "FilterTrain       g1gc  21  23"	# 16G,20G     24G
  "FilterTrain       zgc    4  12"	#             16G
  "FilterTrain       shen   4  12"	#             16G
  "FilterTrain       para   4  12"	#     32G     16G
  "FormulaTrain      g1gc   4  12"	#             16G
  "FormulaTrain      zgc    4  12"      #             16G
  "FormulaTrain      shen   4  12"      #             16G
  "FormulaTrain      para   4  12"	#             16G
#  "NaturalJoinTrain  g1gc  17  19"	#     16G     20G
  "NaturalJoinTrain  zgc    4  12"	#             16G
#  "NaturalJoinTrain  shen  17  19"	#     16G     20G
#  "NaturalJoinTrain  para  17  19"	#     16G     20G
  "AggByTrain        g1gc   4  12"	#             16G
  "AggByTrain        zgc    4  12"	#             16G
  "AggByTrain        shen   4  12"	#             16G
#  "AggByTrain        para  17  19"	#     16G     20G
  "OrderedTrain      g1gc   4  12"	#             16G
  "OrderedTrain      zgc    4  12"	#             16G
  "OrderedTrain      shen   4  12"      #             16G
  "OrderedTrain      para   4  12"      #             16G
  "UpdateByTrain     g1gc   4  12"	#             16G
  "UpdateByTrain     zgc    4  12"	#             16G
#  "UpdateByTrain     shen  17  19"	#     16G     20G
#  "UpdateByTrain     para  17  19"	#     16G     20G
)

for combo in "${COMBOS[@]}"; do
  read -r cls gc min_heap max_heap <<< "$combo"
  for h in $(seq ${max_heap} -${STEP} ${min_heap}); do
    opts="${GCS[$gc]} -Xms${h}g -Xmx${h}g $BASE $CYCLE"
    echo -e "gc_${gc}_${cls}_h${h}_j25\t$IMG\t$PKG\t*${cls}\t$ITERS\t$ROWSM\t$DIST\t$opts"
  done
done

