# DH-23641 (PR #8878, sparse Parquet reads): current main vs Robb's branch, all training and Parquet benchmarks
#
# main: deephaven:main, resolved when the first main row builds it (recorded in docker.image.revision)
# fix:  robbcamera:rjc_DH-23641, the PR branch (243a7f5b8e when written, merged with main at c7646208c5)
# main is 9 commits ahead of the branch's merge base, including DH-24085 (Parquet deferred region invalidation,
# pushdown eligibility, EncodedStringDecoder offset), DH-22146 (merge), DH-24178 (RowSet containers) and DH-24175
# (aggregation rehash), so the difference is Robb's change plus that drift, not his change alone (see dh23641-sweep2.sh
# for the isolated comparison). Both are built from source on the test server and reused by every row.
#
# 1. train (10 rows): every training test, 48G G1, main vs fix, 5 runs each
# 2. pq (10 rows): every standard Parquet file test (ParquetColType, ParquetMultiCol, S3ParquetMultiCol via minio),
#    24G, main vs fix, 5 runs each. Whole classes, since each read test reads its write test's file.
# Rows sharing a label accumulate into one set; passes alternate which build goes first. Each row uploads to GCS as it
# finishes (fetch with fetch_gcs.py dh23641_main_).
#
# Budget: about 34 min per train row and 17 per pq row: roughly 9 h including the two source builds.
#
# TSV columns: run_type, run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=20

RTYP='adhoc'
ITERS=1
ROWSM=10
DIST=random
PASSES=5

MAIN=deephaven:main
FIX=robbcamera:rjc_DH-23641

TRAIN=io.deephaven.benchmark.tests.train
STANDARD=io.deephaven.benchmark.tests.standard
TRAIN_CLS='*Train'
PQ_CLS='ParquetColType,ParquetMultiCol,S3ParquetMultiCol'

G1="-XX:+UseG1GC -XX:+AlwaysPreTouch -XX:+UseTransparentHugePages -XX:+UseStringDeduplication -DServerStateTracker.reportIntervalMillis=1000 -DPeriodicUpdateGraph.targetCycleDurationMillis=1000 -Dbench.incLoadTarget=1.00"
TRAIN_OPTS="-Xms48g -Xmx48g ${G1}"
PQ_OPTS="-Xmx24g"

# label, image, package, classes, config_options
row() { echo -e "$RTYP\tdh23641_main_$1\t$2\t$3\t$4\t$ITERS\t$ROWSM\t$DIST\t$5"; }

for pass in $(seq ${PASSES}); do
  if (( pass % 2 )); then builds=("main $MAIN" "fix $FIX"); else builds=("fix $FIX" "main $MAIN"); fi
  for b in "${builds[@]}"; do
    read -r name img <<< "$b"
    row "train_${name}" "$img" "$TRAIN" "$TRAIN_CLS" "$TRAIN_OPTS"
  done
  for b in "${builds[@]}"; do
    read -r name img <<< "$b"
    row "pq_${name}" "$img" "$STANDARD" "$PQ_CLS" "$PQ_OPTS"
  done
done
