# A/B the two nightly IMAGES across the 2026-09-12 boundary, not the commits
#
# The commit bisect (udf-regression-rerun.sh) cleared all seven commits: five source builds, each
# verified to contain its own code, showed the UDF benchmarks flat within noise while reproducing the
# updateBy fix at +42.9%. So the regression is not in deephaven-core's java code.
#
#   nightly           bisect (src_base -> src_top)
#   -16.8%  -17.5%    -1.6%  +0.8%      UDF- 2 Doubles to Double Python Hints [Serial] -Static
#   +43.7%            +42.9%            RollingGroupTick- 3 Groups 100K Unique Combos -Inc
#
# This run uses the officially built images those two nightlies actually ran, so it separates the
# last two possibilities:
#   reproduces here  -> the cause is inside the image, not the java code and not the host
#   flat here        -> the cause is the nightly host or its environment
#
# Hardware is the same on both servers; the nightly host runs kernel 6.8.0-90 and benchmark-gc runs
# 6.8.0-110.
#
# Configured to match the nightly run rather than our earlier matrices: 5 iterations and NO JFR.
# JFR would add overhead to the fastest benchmarks in the suite (~4.4M rows/sec) and cannot see the
# python side anyway, where these benchmarks spend most of their time. Nightly passes no config
# options, which manage-deephaven-remote.sh turns into -Xmx24g.
#
# RollingGroupTick is the control: the 09-12 image contains DH-23630, so it must show the large gain.
#
# TSV columns: run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=2

PKG=io.deephaven.benchmark.tests.standard
CLASSES='UserFormula,RollingGroupTick'
DIST=random
ROWSM=10
ITERS=5
OPTS='<default>'

# 09-11 nightly, pushed 03:48:56Z ahead of the ~07:00Z run start. Revision 9fa9e83c55.
IMG_0911=ghcr.io/deephaven/server@sha256:2672b50205bd7ecef5d9fcc7016bd4345f75ff5333da3d77b9f05ebc50d2638b
# 09-12 nightly, pushed 03:50:10Z, the only edge push that day. Revision c50cd7f2fc.
IMG_0912=ghcr.io/deephaven/server@sha256:fc92a762040dd422ad711f18393d728edc5c0cb5e53de59171a67679ae3c0eef

row() { echo -e "$1\t$2\t$PKG\t$CLASSES\t$ITERS\t$ROWSM\t$DIST\t$OPTS"; }

row img0911_good "$IMG_0911"
row img0912_bad "$IMG_0912"
