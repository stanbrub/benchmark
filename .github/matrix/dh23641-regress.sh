# DH-23641 (PR #8878, robbcamera:rjc_DH-23641): regression and memory check of sparse Parquet reads against its merge base
#
# The PR decodes only the requested rows when a read covers a small fraction of a page, and caches them as a partial
# page; dense reads still decode and cache whole pages. So the benchmarks that matter are the ones whose measured
# operation reads Parquet:
# - heap: FilterTrain alone, swept down from 32G, base and fix alternating at each heap. Filter is the heaviest Parquet
#   memory user, and the lowest heap that passes is the memory measure; a failure only loses its own row. Run first, so
#   the answer comes early and both source builds happen up front.
# - train: every training test reads its source through a Parquet view, so sparse reads happen inside the operation
#   (e.g. the .where() chained after the where_in). 48G G1 with the default page cache cap, as in the 48G cap sweep.
# - pq: the standard Parquet file tests (write, then read back with select(), including S3 via minio). Dense reads, so
#   they check the rewritten page store's full-page path. Whole classes, since each read test reads its write test's
#   file. Nightly heap.
# The other standard tests select() their source before timing, so their measured operation never reads Parquet.
#
# base: 8b1fc776be, the PR's merge base on main
# fix:  b09520d3f5, head of rjc_DH-23641 (PR #8878)
# Both are built from source on the test server (JDK 21 images, so no -XX:+UseCompactObjectHeaders) and reused by
# every row. Rows sharing a label accumulate into one set; each pass runs every train and pq set once, and odd and even
# passes swap which build goes first, so drift over the run hits base and fix alike.
#
# Memory evidence stays on the test server: every row writes a GC log, and heap rows also a JFR recording, to /data
# named after the row (one file per engine start). Failed rows upload no artifact, and JFR upload to GCS isn't merged,
# so copy them off before the next matrix run clears the server:
#   scp 'gc-benchmark:/home/ubuntu/deephaven/data/dh23641_*' <study>/
#
# Budget about 1-1.5 h for the heap rows, then roughly 31 min per train row and 21 min per pq row (105 min per pass).
#
# TSV columns: run_type, run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=32

RTYP='adhoc'
ITERS=1
ROWSM=10
DIST=random
PASSES=5
HEAPS=(32 28 24 20 16 12)

BASE=deephaven:8b1fc776be84bc200161258f15533c7f6129a6f0
FIX=robbcamera:b09520d3f5977b8b16c346a8f94e70689193c2d0

TRAIN_PKG=io.deephaven.benchmark.tests.train
G1="-XX:+UseG1GC -XX:+AlwaysPreTouch -XX:+UseTransparentHugePages -XX:+UseStringDeduplication -DServerStateTracker.reportIntervalMillis=1000 -DPeriodicUpdateGraph.targetCycleDurationMillis=1000 -Dbench.incLoadTarget=1.00"
TRAIN_OPTS="-Xms48g -Xmx48g ${G1}"

PQ_PKG=io.deephaven.benchmark.tests.standard
PQ_CLS='ParquetColType,ParquetMultiCol,S3ParquetMultiCol'
PQ_OPTS="-Xmx24g"

# GC log per engine start: heap before and after every collection, including full GCs
gclog() { echo "-Xlog:gc,gc+heap+exit:file=/data/$1-gc-%t.log:time,uptimemillis:filecount=0"; }
# JFR with default settings (low overhead), dumped when the engine exits, including the shutdown after an OOM
jfr() { echo "-XX:StartFlightRecording=name=$1,filename=/data/$1-%t.jfr,settings=default,dumponexit=true,maxsize=500m"; }

# label, image, package, classes, config_options
row() { echo -e "$RTYP\tdh23641_$1\t$2\t$3\t$4\t$ITERS\t$ROWSM\t$DIST\t$5"; }

for h in "${HEAPS[@]}"; do
  for b in "base $BASE" "fix $FIX"; do
    read -r name img <<< "$b"
    label="heap_${name}_h${h}"
    row "$label" "$img" "$TRAIN_PKG" '*FilterTrain' "-Xms${h}g -Xmx${h}g ${G1} $(gclog dh23641_${label}) $(jfr dh23641_${label})"
  done
done

for pass in $(seq ${PASSES}); do
  if (( pass % 2 )); then builds=("base $BASE" "fix $FIX"); else builds=("fix $FIX" "base $BASE"); fi
  for b in "${builds[@]}"; do
    read -r name img <<< "$b"
    row "train_${name}" "$img" "$TRAIN_PKG" '*Train' "${TRAIN_OPTS} $(gclog dh23641_train_${name})"
  done
  for b in "${builds[@]}"; do
    read -r name img <<< "$b"
    row "pq_${name}" "$img" "$PQ_PKG" "$PQ_CLS" "${PQ_OPTS} $(gclog dh23641_pq_${name})"
  done
done
