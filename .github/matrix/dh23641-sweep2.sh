# DH-23641 (PR #8878, sparse Parquet reads), second sweep after the PR merged main and took review changes
#
# The PR head moved from b09520d3f5 to 243a7f5b8e: a merge of main (now including DH-24112, which changed RowSet.invert
# and InverseWrappedRowSetRowRedirection, both in UpdateBy's profile) and a review cleanup. ColumnChunkPageStore's
# fillChunk is unchanged: its cached-page shortcut still only covers requests within one page, which is what made
# UpdateBy Inc 8% slower in dh23641-weekend.sh, so that regression is expected to remain. Base is the new merge base.
#
# Each row uploads to GCS as it finishes (fetch with fetch_gcs.py dh23641_v2_):
# 1. train (10 rows): Filter (the training test that reads sparsely), UpdateBy (the known regression), and AggBy and
#    NaturalJoin as controls that read Parquet through a view in the measured operation. 48G G1, default page cache
#    cap, base vs fix, 5 runs each. Left out: InlineFormula and UserFormula (dense reads; UserFormula is UDF-bound) and
#    Ordered (dense reads, dominated by its own operators); all were flat in both earlier runs.
# 2. pq (10 rows): standard Parquet file tests (full reads), 24G, base vs fix, 5 runs each: ParquetColType (each column
#    type, local files) and S3ParquetMultiCol (multi-column file through S3 via minio). Left out: ParquetMultiCol, the same
#    file under seven codecs, since the PR doesn't touch decompression. Whole classes, since each read test reads its
#    write test's file.
#
# base: c7646208c5, the PR's merge base on main (Oct 24), the main commit merged into the branch
# fix:  243a7f5b8e, head of rjc_DH-23641 (PR #8878)
# Both are built from source on the test server (JDK 21 images, so no -XX:+UseCompactObjectHeaders) and reused by every
# row. Rows sharing a label accumulate into one set; passes alternate which build goes first.
#
# Budget: about 17.5 min per train row and 12 per pq row: roughly 5.5 h including the two source builds and data
# generation.
#
# TSV columns: run_type, run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=20

RTYP='adhoc'
ITERS=1
ROWSM=10
DIST=random
PASSES=5

BASE=deephaven:c7646208c55fae6b06961964857b7ee390f25f2b
FIX=robbcamera:243a7f5b8e6649ff9c8bf6dc17850d161165d96f

TRAIN=io.deephaven.benchmark.tests.train
STANDARD=io.deephaven.benchmark.tests.standard
TRAIN_CLS='AggByTrain,FilterTrain,NaturalJoinTrain,UpdateByTrain'
PQ_CLS='ParquetColType,S3ParquetMultiCol'

G1="-XX:+UseG1GC -XX:+AlwaysPreTouch -XX:+UseTransparentHugePages -XX:+UseStringDeduplication -DServerStateTracker.reportIntervalMillis=1000 -DPeriodicUpdateGraph.targetCycleDurationMillis=1000 -Dbench.incLoadTarget=1.00"
TRAIN_OPTS="-Xms48g -Xmx48g ${G1}"
PQ_OPTS="-Xmx24g"

# label, image, package, classes, config_options
row() { echo -e "$RTYP\tdh23641_v2_$1\t$2\t$3\t$4\t$ITERS\t$ROWSM\t$DIST\t$5"; }

for pass in $(seq ${PASSES}); do
  if (( pass % 2 )); then builds=("base $BASE" "fix $FIX"); else builds=("fix $FIX" "base $BASE"); fi
  for b in "${builds[@]}"; do
    read -r name img <<< "$b"
    row "train_${name}" "$img" "$TRAIN" "$TRAIN_CLS" "$TRAIN_OPTS"
  done
  for b in "${builds[@]}"; do
    read -r name img <<< "$b"
    row "pq_${name}" "$img" "$STANDARD" "$PQ_CLS" "$PQ_OPTS"
  done
done
