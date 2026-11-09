# DH-24246 (PR #9075, adaptive sparse array sources), base vs fix over the nightly benchmarks
#
# One dispatch: every SUBSET pass is listed before any REST pass, and rows run one at a time in list order, so the
# subset results are complete about 5 h in. Together the two lists are the nightly suite (standard package, '*'), each
# benchmark with the same number of runs per build. Keep the two lists disjoint: a class in both would get twice the
# runs, and a class in neither would be missing from the full set.
#
# The phase is in each row's label (dh24246_<phase>_<build>), so every job name in GitHub shows it. The phases need
# separate labels anyway: one label per build would get 10 rows, and build-matrix.sh requires an odd count per set.
# The full set for a build is the union of its subset and rest sets, which hold disjoint benchmarks.
#
# The PR changes no query. It changes the source type that some engine paths create, defaulting to adaptive sources:
#   select/update formula columns (newDestInstance): adaptive with compact leaves, and leaf-aligned parallel population.
#     Inc always; Static only for update over a merged (scale factor > 1) parent. Static select and flat parents get
#     array sources, which are unchanged.
#   UpdateBy outputs: adaptive with full block leaves, Static and Inc (UpdateByControl.useRedirection defaults to false)
#   join row redirections: adaptive full block, Inc only (a flat static left gets a contiguous redirection)
#   sort row redirection: adaptive full block, Inc only
#
# SUBSET (8 classes, about 25 min of nightly test time per row, roughly 5 h), one class per changed path:
#   formula: InlineFormula (Inc select and update), Vector (Inc update)
#   join: NaturalJoin (Inc redirection; Strided Left is the only existing benchmark whose result has sparse row keys),
#     ReverseAsOfJoin (Inc redirection through the as-of join)
#   sort: SortCombo (Inc redirection)
#   updateby: MixedCombo (cumulative, rolling and ema operator outputs, Static and Inc)
#   controls the PR doesn't reach: Where, BasicMathCombo
# REST (81 classes, about 210 min per row, roughly 37 h; each row stays inside the 6 h job limit): the other classes
#   the PR reaches (UserFormula, DateTimeFormula, ExactJoin, AsOfJoin, Join, SortAscending, SortDescending, and every
#   other UpdateBy class), and every class it doesn't reach (aggregations, where, partition_by, ungroup, data indexes,
#   RowIterator, Kafka, the Barrage client, and file reads, which select into flat arrays).
#
# base: 3fe4d1ece2, the PR's merge base on main (the branch has merged no main since)
# fix:  733df1750b, head of cpwright:engine/cpw/DH-24246-adaptive-sparse-source (PR #9075)
# Both built from source on the test server at their first row and reused by all 20 rows (only row 1 clears the
# server). Same scale, distribution and heap as nightly. Passes alternate which build goes first.
#
# TSV columns: run_type, run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=20

RTYP='adhoc'
PKG=io.deephaven.benchmark.tests.standard
ITERS=1
ROWSM=10
DIST=random
OPTS='<default>'
PASSES=5

BASE=deephaven:3fe4d1ece2099513924d3178263abebea82610c1
FIX=cpwright:733df1750be9f5d12ee18012d7b7ba1c0865afc9

SUBSET='InlineFormula,Vector'
SUBSET+=',NaturalJoin,ReverseAsOfJoin'
SUBSET+=',SortCombo'
SUBSET+=',MixedCombo'
SUBSET+=',Where,BasicMathCombo'

REST='UserFormula,DateTimeFormula'
REST+=',ExactJoin,AsOfJoin,Join'
REST+=',SortAscending,SortDescending'
REST+=',CumCombo,RollingCombo,CumSum,RollingGroupTick,RollingFormulaTick,EmStdTick'
REST+=',CountWhere,PositionalCombo,SortedCombo,WeightedCombo'
REST+=',AvgBy,CountBy,FirstBy,GroupBy,HeadBy,LastBy,MaxBy,MedianBy,MinBy,PartitionBy,StdBy,SumBy,TailBy,Ungroup,VarBy'
REST+=',BarrageClient'
REST+=',CsvColType,CsvMultiCol,ParquetColType,ParquetMultiCol,S3ParquetMultiCol'
REST+=',RowIterator'
REST+=',DataIndexBenefit,DataIndexLoad'
REST+=',KafkaBlinkHeap,KafkaBlinkWidth,KafkaDataType'
REST+=',SelectDistinct'
REST+=',CumCountWhere,CumMax,CumMin,CumProd'
REST+=',EmMaxTick,EmMaxTime,EmMinTick,EmMinTime,EmStdTime,EmaTick,EmaTime,EmsTick,EmsTime'
REST+=',RollingAvgTick,RollingAvgTime,RollingCountTick,RollingCountTime,RollingCountWhereTick,RollingCountWhereTime'
REST+=',RollingFormulaTime,RollingGroupTime,RollingMaxTick,RollingMaxTime,RollingMinTick,RollingMinTime'
REST+=',RollingProdTick,RollingProdTime,RollingStdTick,RollingStdTime,RollingSumTick,RollingSumTime'
REST+=',RollingWAvgTick,RollingWAvgTime'
REST+=',WhereIn,WhereNotIn,WhereOneOf'

count() { tr ',' '\n' <<< "$1" | wc -l; }
PLAN="DH-24246: rows 1-$((PASSES * 2)) subset ($(count "${SUBSET}") classes), rows $((PASSES * 2 + 1))-$((PASSES * 4)) rest ($(count "${REST}") classes), ${PASSES} passes each"
echo "${PLAN}" >&2
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  echo "### ${PLAN}" >> "${GITHUB_STEP_SUMMARY}"
fi

# phase, build, image, classes
row() { echo -e "$RTYP\tdh24246_$1_$2\t$3\t$PKG\t$4\t$ITERS\t$ROWSM\t$DIST\t$OPTS"; }

for phase in subset rest; do
  if [[ ${phase} == subset ]]; then cls=${SUBSET}; else cls=${REST}; fi
  for pass in $(seq ${PASSES}); do
    if (( pass % 2 )); then builds=("base $BASE" "fix $FIX"); else builds=("fix $FIX" "base $BASE"); fi
    for b in "${builds[@]}"; do
      read -r name img <<< "$b"
      row "${phase}" "${name}" "${img}" "${cls}"
    done
  done
done
