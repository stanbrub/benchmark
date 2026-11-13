# Run every standard benchmark three times (one pass per row) with the rescaled factors, for comparison against
# the nightly that ran the same image (2026-11-12, run-VajlNWV). Uses the nightly's heap so the results are
# comparable. Each row is a separate job, so each pass stays under the 6 hour job limit, and the shared label puts
# all runs in one set.
# TSV columns: run_type, run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=3

RTYP='adhoc'
IMG=ghcr.io/deephaven/server@sha256:618755645bf6d9503eb224401c4262be9f6260efcaaf91bf35c42660da07470a
PKG=io.deephaven.benchmark.tests.standard
CLS='*'
ITERS=1
ROWSM=10
DIST=random

row() { echo -e "$RTYP\t$1\t$IMG\t$PKG\t$CLS\t$ITERS\t$ROWSM\t$DIST\t$2"; }

row scale3 "-Xmx24g"
row scale3 "-Xmx24g"
row scale3 "-Xmx24g"
