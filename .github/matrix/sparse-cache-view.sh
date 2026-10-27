# Sparse Parquet page cache (DH-23641, PR #8878) across the standard benchmarks, with the source read through a view
#
# -Dbench.cachedSource=false makes StandardTestRunner read its tables through view() rather than select(), so each
# measured operation reads Parquet pages itself, through the page cache, instead of from in-memory columns. Joins read
# the right table that way too. The derived timestamp in the scaled timed tables is still stored, with update(), since
# ii in a view would look up each row's position on every read. Tests that need select() keep it: the file tests (full
# reads), the flat-left NaturalJoin, and operations that call select() themselves. Without the property (nightly,
# release) the runner selects as before; a runner's useCachedSource(...) overrides the property either way.
#
# Base vs fix, each over two groups that split the standard package roughly in half by runtime:
# 1. ub (updateby): Cum*, Em*, Rolling*, MixedCombo
# 2. ops: aggby, by, formula, index, join, select, sort, vectors, where, client
# Left out: file (unchanged by the view switch; full reads, already measured in dh23641-sweep2.sh) and kafka (no Parquet).
#
# base: c7646208c5, the PR's merge base on main (Oct 24), the main commit merged into the branch
# fix:  243a7f5b8e, head of rjc_DH-23641 (PR #8878)
# Both are built from source on the test server and reused by every row. Rows sharing a label accumulate into one set;
# passes alternate which build goes first.
#
# Budget: the nightly ran these groups in about 2 h each with select() reads; reading inside the operation will add to
# that. Roughly 24 h or more for 3 passes. PASSES=1 (with EXPECTED_COMBOS=4) is an overnight survey.
#
# Fetch results with the study's fetch script using the spc_ label prefix (../2026-10-27_sparse-parquet-cache).
#
# TSV columns: run_type, run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=12

RTYP='adhoc'
ITERS=1
ROWSM=10
DIST=random
PASSES=3
OPTS='-Xmx24g -Dbench.cachedSource=false'

BASE=deephaven:c7646208c55fae6b06961964857b7ee390f25f2b
FIX=robbcamera:243a7f5b8e6649ff9c8bf6dc17850d161165d96f

PKG=io.deephaven.benchmark.tests.standard
UB_CLS='Cum*,Em*,Rolling*,MixedCombo'
OPS_CLS='BasicMathCombo,CountWhere,PositionalCombo,SortedCombo,WeightedCombo,*By,Ungroup,DateTimeFormula,InlineFormula,UserFormula,RowIterator,DataIndex*,*Join,SelectDistinct,Sort*,Vector,Where*,BarrageClient'

# label, image, classes
row() { echo -e "$RTYP\tspc_$1\t$2\t$PKG\t$3\t$ITERS\t$ROWSM\t$DIST\t$OPTS"; }

for pass in $(seq ${PASSES}); do
  if (( pass % 2 )); then builds=("base $BASE" "fix $FIX"); else builds=("fix $FIX" "base $BASE"); fi
  for b in "${builds[@]}"; do
    read -r name img <<< "$b"
    row "ub_${name}" "$img" "$UB_CLS"
  done
  for b in "${builds[@]}"; do
    read -r name img <<< "$b"
    row "ops_${name}" "$img" "$OPS_CLS"
  done
done
