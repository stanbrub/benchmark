# Redo the UDF commit bisect WITHOUT JFR, because JFR masks the regression
#
# udf3 established two things. The regression is already present in the 09-11 18:26Z image, a build
# earlier than the nightly data suggests, and -XX:StartFlightRecording recovers ~17% of it on the
# same image:
#
#   os_old_java_old       4,396 / 4,434     old OS, old java, no JFR
#   os_new_java_mid       3,655 / 3,746     new OS, +3 engine commits, no JFR   -16.8% / -15.5%
#   os_new_java_new       3,690 / 3,690     new OS, +all 7 commits, no JFR      -16.0% / -16.8%
#   os_new_java_new_jfr   4,329 / 4,373     SAME IMAGE as above, JFR on          -1.5% /  -1.4%
#
# The original bisect (udf-regression-rerun.sh) ran every row with JFR, so its "no java cause"
# result proved nothing. This rerun is that bisect with config_options=<default>.
#
# The 18:26Z image carries both the OS/cpython flip and the three engine commits, so those are the
# two candidate causes:
#   libc6 2.39-0ubuntu8.8 -> 8.9,  python3.12 3.12.3-1ubuntu0.16 -> 0.17 (cpython rebuilt)
#   8cbd379b3 DH-23579,  5e417cbd1 DH-23630,  3ed1d774b DH-23502
#
# A rebuild today resolves libc6 8.9 and python3.12 0.17, matching the post-flip stack, and the base
# image pre-pull in build-docker-image-remote.sh guarantees every row shares one OS. So this run
# holds the OS fixed at the new stack and varies only java:
#
#   all five source rows slow   -> the OS/cpython flip is the cause, java is clear
#   the rows split              -> the responsible commit is identified
#   all five source rows fast   -> our local builds differ from the official images in some way we
#                                  have not found, since src_top is byte identical to the 09-12
#                                  image across 110 deephaven jars, 119 os packages and 29 wheels
#
# Row 1 is an in-run anchor: the 09-11 03:48Z image, old OS and old java, which measured 4,396 and
# 4,434 in udf3. It provides a known-fast reference inside this matrix so nothing depends on
# comparing across runs.
#
# UserFormula only. No RollingGroupTick control: dependency.jar.size in the platform data
# distinguishes all five builds, as it did last time (5 distinct values, endpoints matching the
# nightly images exactly).
#
# TSV columns: run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=6

PKG=io.deephaven.benchmark.tests.standard
CLASSES='UserFormula'
DIST=random
ROWSM=10
ITERS=3
OPTS='<default>'

ANCHOR=ghcr.io/deephaven/server@sha256:2672b50205bd7ecef5d9fcc7016bd4345f75ff5333da3d77b9f05ebc50d2638b

row() { echo -e "$1\t$2\t$PKG\t$CLASSES\t$ITERS\t$ROWSM\t$DIST\t$OPTS"; }

row anchor_img_0911 "$ANCHOR"
row src_base deephaven:9fa9e83c55ccb2e9ddedd61d1ba59d2d8da68c85
row src_dh23579 deephaven:8cbd379b3e81c3fe316b78f20722a01bf977d3b6
row src_dh23630 deephaven:5e417cbd1bb3d8f9e3fbd9233f2034027c5319f3
row src_dh23502 deephaven:3ed1d774b5ca7488700fa7e51377f28d9455be74
row src_top deephaven:c50cd7f2fc02b819376a80e8767dff76c0624aa2
