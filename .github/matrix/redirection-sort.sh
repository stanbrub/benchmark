# DH-23829 (redirection hash maps): before vs after, plain and with JFR
#
# before: parent of part 1.5, the first DH-23829 change to production code (part 1 is JMH only)
# after:  part 21-25, the last DH-23829 commit
# Plain rows give the rates. JFR rows reuse the same images and are for profiling and memory only,
# since JFR can shift rates. Details: studies/2026-10-05_redirection-hashmaps/FINDINGS.md
#
# TSV columns: run_type, run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=4

RTYP='adhoc'
PKG=io.deephaven.benchmark.tests.standard
# Explicit: 'Sort*' would also match SortedComboTest. 'Join' matches only JoinTest.
CLASSES='SortAscending,SortDescending,SortCombo,Join,NaturalJoin'
DIST=random
ROWSM=10
PLAIN='<default>'

BEFORE=deephaven:78483756db40f1f6cbc6e4547e2c288ea57cdced
AFTER=deephaven:ed8213c46aaf0d6714d22da26876e482db1cfab8

# Needs docker.compose.stop.timeout > 0 (adhoc has 30) so the recording flushes
jfr_opts() {
    echo "-Xmx24g -XX:StartFlightRecording=name=bench,filename=/data/$1-%t-%p.jfr,settings=profile,maxsize=200m"
}

# $3 is test_iterations, $4 is config_options
row() { echo -e "$RTYP\t$1\t$2\t$PKG\t$CLASSES\t$3\t$ROWSM\t$DIST\t$4"; }

row before     "$BEFORE" 5 "$PLAIN"
row after      "$AFTER"  5 "$PLAIN"
row before_jfr "$BEFORE" 1 "$(jfr_opts before)"
row after_jfr  "$AFTER"  1 "$(jfr_opts after)"
