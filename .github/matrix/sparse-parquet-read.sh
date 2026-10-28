# Sparse vs dense Parquet read benchmarks against DH-23641 (PR #8878, sparse Parquet reads)
#
# Checks that ParquetRead- Sparse Multi Col takes the sparse read path: it should be faster with the PR, while
# ParquetRead- Dense Multi Col, the control, should not change. Both filter on long10K and then read str10K, int10K,
# short10K and bigDec10K for the remaining rows: 1 in 100 rows for sparse and 1 in 2 for dense, against the PR's
# sparse cutoff of 12.5% of a page. The file's pages hold at least 5184 rows for those columns.
#
# The whole ParquetMultiCol class runs, since the filtered reads read the file of its last write test. Its other read
# and write tests are full reads, which the PR leaves on the old path, so they should not change either.
#
# main: b7ed9cb7c8, head of deephaven-core main (Oct 28)
# fix:  243a7f5b8e, head of rjc_DH-23641 (PR #8878)
# The branch's merge base is c7646208c5, 11 commits behind main, so main vs fix is the PR plus that drift, not the PR
# alone. The drift that could touch these reads: DH-24085 (deferred Parquet regions, EncodedStringDecoder), DH-22146
# (single-table merge) and DH-24178 (RowSet bitmap containers).
# Both are built from source on the test server and reused by every row. Rows sharing a label accumulate into one
# set; passes alternate which build goes first.
#
# TSV columns: run_type, run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=10

RTYP='adhoc'
ITERS=1
ROWSM=10
DIST=random
PASSES=5

MAIN=deephaven:b7ed9cb7c8ee8e94f248c50a21f6856098780717
FIX=robbcamera:243a7f5b8e6649ff9c8bf6dc17850d161165d96f

PKG=io.deephaven.benchmark.tests.standard
CLS='ParquetMultiCol'
OPTS="-Xmx24g"

# label, image
row() { echo -e "$RTYP\tsparse_pq_$1\t$2\t$PKG\t$CLS\t$ITERS\t$ROWSM\t$DIST\t$OPTS"; }

for pass in $(seq ${PASSES}); do
  if (( pass % 2 )); then builds=("main $MAIN" "fix $FIX"); else builds=("fix $FIX" "main $MAIN"); fi
  for b in "${builds[@]}"; do
    read -r name img <<< "$b"
    row "$name" "$img"
  done
done
