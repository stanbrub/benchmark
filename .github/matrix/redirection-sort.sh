# DH-23829 (redirection hash maps): before vs after, plain and with JFR
#
# before: 09-30 nightly edge image, GIT_REVISION 9939e3495e (before all DH-23829)
# after:  10-04 nightly edge image, GIT_REVISION ed8213c46a (part 21-25, the last DH-23829 commit)
# Published images, since commits before the mypy 2.4.0 fix (8fb7f0d475) no longer build. Other
# commits between them include DH-23853 and DH-23886 (broad ticking paths).
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

BEFORE=ghcr.io/deephaven/server@sha256:42be1a6ea2325339878b258b3d0313f5188fcd98233ba8dd8fb2c30b1c64c024
AFTER=ghcr.io/deephaven/server@sha256:a06a5ee774b8eb0b9c9db29fd338d1d1ae0de27b9c4d274070a29ccb3331068b

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
