# Release study: 42.0 vs 43.0, both built from their deephaven-core release branches
#
# Both refs build on the same deephaven-server-docker main (currently Python 3.14, JVM 25, Ubuntu 26.04),
# so the comparison is between engine versions rather than between the OS, Python and JVM each shipped on.
#
# Runs as adhoc so nothing touches release/ until the sets are complete. The labels are already in the
# release format, so publishing is a straight copy from adhoc/<actor>/<label> to release/deephaven/<label>.
# 42.000.01 keeps it from colliding with the existing 42.000.00 set made from the published 42.0 image.
#
# Rows alternate 42, 43 so both images are built in the first two rows, where a bake failure on either
# side shows up early, and any drift on the host over the long run falls on both versions alike.
#
# <default> config_options becomes -Xmx24g, the same heap release-benchmark-docker-compose.yml uses.
#
# TSV columns: run_type, run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=14

RTYP='adhoc'
PKG=io.deephaven.benchmark.tests.standard
CLS='*'
ITERS=1
ROWSM=10
DIST=random
OPTS='<default>'

PASSES=7

# emit <label> <owner:ref> as one full pass
row() { echo -e "$RTYP\t$1\t$2\t$PKG\t$CLS\t$ITERS\t$ROWSM\t$DIST\t$OPTS"; }

for i in $(seq ${PASSES}); do
  row 42.000.01 deephaven:release/v42.0
  row 43.000.00 deephaven:release/v43.0
done
