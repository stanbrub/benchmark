# DH-23641 (PR #8878, sparse Parquet reads) follow-up, plus a GC workaround check for DH-24098. A weekend run.
#
# Sections run in order of importance; each row uploads as it finishes, so a cancelled run keeps what's done.
# 1. UpdateBy (42 rows). In dh23641-regress.sh, UpdateBy Inc ran 0.911x base (p 0.151, 5 vs 5): every fix run in the
#    benchmark's slower mode (about 10% more CPU per row, identical GC and RSS), against 1 of 5 base runs. Its windowed
#    operators re-read earlier rows of every bucket each cycle, a few rows per page, which the PR reads sparsely.
#    - base, fix: 9 runs each, to confirm or rule out the difference
#    - fix_nosparse: -DColumnChunkPageStore.sparseReadMaxDensity=0 disables sparse reads; matching base means the
#      sparse path is the cause
#    - fix_miss0: -DColumnChunkPageStore.sparseMissesBeforeFullCaching=0 caches a page whole on its first sparse miss
#      (default 2); recovering means the threshold is the knob to tune
#    - base_jfr, fix_jfr: 3 runs each with -Dbench.jfr=profile, CPU profiles uploaded in test-logs.tgz. JFR changes
#      rates, so these only locate the extra work; the plain rows measure it.
# 2. Heap floor (20 rows). The regression run's FilterTrain sweep had base failing at 20G and below and the fix passing
#    down to 16G, one run per heap. Repeat 20G and 16G 5 times each. A failed row uploads nothing, so pass/fail comes
#    from the job list; GC logs go to the server's /data (see below).
# 3. ZGC (10 rows). All training tests at 48G, base vs fix, 5 runs each; only G1 was tested so far.
# 4. SoftRefLRUPolicyMSPerMB (20 rows), for DH-24098: can a GC flag stand in for a byte cap until one ships? The base
#    build (no sparse reads), all training tests at 48G G1, default page cache cap, 1000 (the default, set explicitly),
#    100, 10 and 0, 5 runs each. 100 was never chosen by a sweep; UpdateBy keeps its own input caches in soft
#    references, so watch it as well as Filter.
#
# base: 8b1fc776be, the PR's merge base on main
# fix:  b09520d3f5, head of rjc_DH-23641 (PR #8878)
# Both are built from source on the test server (JDK 21 images, so no -XX:+UseCompactObjectHeaders) and reused by every
# row. Rows sharing a label accumulate into one set; within each section, passes rotate which set goes first.
# Needs the JFR upload from main (DH-23789) merged into this branch.
#
# Section 2's GC logs are written to the server's /data and aren't uploaded; copy them before the next matrix run:
#   scp 'gc-benchmark:/home/ubuntu/deephaven/data/dh23641_wk_*' <study>/dh23641/server-weekend/
#
# Budget: about 8 min per UpdateBy row, 5-8 min per heap row, 36 min per ZGC row and 31 min per G1 training row,
# so 25 h or so including the two source builds and data generation.
#
# TSV columns: run_type, run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=92

RTYP='adhoc'
PKG=io.deephaven.benchmark.tests.train
ITERS=1
ROWSM=10
DIST=random

BASE=deephaven:8b1fc776be84bc200161258f15533c7f6129a6f0
FIX=robbcamera:b09520d3f5977b8b16c346a8f94e70689193c2d0

COMMON="-XX:+AlwaysPreTouch -XX:+UseTransparentHugePages -XX:+UseStringDeduplication -DServerStateTracker.reportIntervalMillis=1000 -DPeriodicUpdateGraph.targetCycleDurationMillis=1000 -Dbench.incLoadTarget=1.00"
G1="-XX:+UseG1GC ${COMMON}"
ZGC="-XX:-UseG1GC -XX:+UseZGC ${COMMON}"
H48="-Xms48g -Xmx48g"

gclog() { echo "-Xlog:gc,gc+heap+exit:file=/data/$1-gc-%t.log:time,uptimemillis:filecount=0"; }

# label, image, classes, config_options
row() { echo -e "$RTYP\tdh23641_wk_$1\t$2\t$PKG\t$3\t$ITERS\t$ROWSM\t$DIST\t$4"; }

# Rotate the sets of one section, one of each per pass: <passes> <class> <name|image|opts>...
rotate() {
  local passes=$1 cls=$2; shift 2
  local sets=("$@") n=$#
  for pass in $(seq ${passes}); do
    for i in $(seq 0 $(( n - 1 ))); do
      IFS='|' read -r name img opts <<< "${sets[$(( (pass + i) % n ))]}"
      row "$name" "$img" "$cls" "$opts"
    done
  done
}

# 1. UpdateBy
rotate 9 '*UpdateByTrain' \
  "ub_base|$BASE|$H48 $G1" \
  "ub_fix|$FIX|$H48 $G1" \
  "ub_fix_nosparse|$FIX|$H48 $G1 -DColumnChunkPageStore.sparseReadMaxDensity=0" \
  "ub_fix_miss0|$FIX|$H48 $G1 -DColumnChunkPageStore.sparseMissesBeforeFullCaching=0"
rotate 3 '*UpdateByTrain' \
  "ub_base_jfr|$BASE|$H48 $G1 -Dbench.jfr=profile" \
  "ub_fix_jfr|$FIX|$H48 $G1 -Dbench.jfr=profile"

# 2. Heap floor
sets=()
for h in 20 16; do
  for b in "base|$BASE" "fix|$FIX"; do
    IFS='|' read -r name img <<< "$b"
    sets+=("heap_${name}_h${h}|$img|-Xms${h}g -Xmx${h}g $G1 $(gclog dh23641_wk_heap_${name}_h${h})")
  done
done
rotate 5 '*FilterTrain' "${sets[@]}"

# 3. ZGC
rotate 5 '*Train' \
  "zgc_base|$BASE|$H48 $ZGC" \
  "zgc_fix|$FIX|$H48 $ZGC"

# 4. SoftRefLRUPolicyMSPerMB on the base build
rotate 5 '*Train' \
  "softref1000|$BASE|$H48 $G1 -XX:SoftRefLRUPolicyMSPerMB=1000" \
  "softref100|$BASE|$H48 $G1 -XX:SoftRefLRUPolicyMSPerMB=100" \
  "softref10|$BASE|$H48 $G1 -XX:SoftRefLRUPolicyMSPerMB=10" \
  "softref0|$BASE|$H48 $G1 -XX:SoftRefLRUPolicyMSPerMB=0"
