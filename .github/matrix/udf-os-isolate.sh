# Isolate the 2026-09-12 UDF regression: OS/cpython layer vs java, and test the JFR confound
#
# What is established:
#   - The regression reproduces between the two nightly images under identical options:
#     UDF- 2 Doubles to Double Python Hints [Serial] -Static  -16.2% / -16.7%, p~0.012, CV <1%
#   - A five commit source bisect over the same window found no java cause, while reproducing the
#     DH-23630 updateBy gain at +42.9%.
#   - 106 of 110 deephaven jars are byte identical between the two nightly images. The four that
#     differ are Container, engine-rowset, engine-table and extensions-parquet-table, which is
#     exactly what DH-23579/DH-23630/DH-23502 touched.
#   - The OS layer flipped in the 09-11 18:26Z build, not the 09-12 build:
#       0911-0348  rev 9fa9e83c55  libc6 8.8  python3.12 0.16  (cpython built Jul 15)
#       0911-1826  rev 261121f7c5  libc6 8.9  python3.12 0.17  (cpython built Aug 31)
#       0912-0350  rev c50cd7f2fc  libc6 8.9  python3.12 0.17  (cpython built Aug 31)
#
# Row 2 is the new information: officially built, new OS, and it already carries all three engine
# commits. If it is slow, the regression tracks the OS/cpython flip and arrived a build earlier than
# the nightly data suggests. If it is fast, the cause is specific to the 09-12 image.
#
# Row 4 settles a confound. src_top, our local rebuild of row 3's exact java code (verified byte
# identical at the class level), measured 4,370 rather than 3,690 -- but that run carried
# -XX:StartFlightRecording while the image rows did not. Running row 3's image with JFR shows
# whether the flag is worth 18% on this benchmark.
#
# UserFormula only. RollingGroupTick is dropped as a control here because the jar comparison above
# already proves the images differ in java code, which is what the control existed to demonstrate.
#
# TSV columns: run_label, docker_image, test_package, test_class_list,
#              test_iterations, scale_row_count, distribution, config_options
EXPECTED_COMBOS=4

PKG=io.deephaven.benchmark.tests.standard
CLASSES='UserFormula'
DIST=random
ROWSM=10
ITERS=5
PLAIN='<default>'
JFR='-Xmx24g -XX:StartFlightRecording=name=bench,filename=/data/jfrprobe-%t-%p.jfr,settings=profile,maxsize=200m'

IMG_0348=ghcr.io/deephaven/server@sha256:2672b50205bd7ecef5d9fcc7016bd4345f75ff5333da3d77b9f05ebc50d2638b
IMG_1826=ghcr.io/deephaven/server@sha256:9ce15832870cf5ca231701da41176f8938dbb7b37aa94171fd794f899eb15971
IMG_0350=ghcr.io/deephaven/server@sha256:fc92a762040dd422ad711f18393d728edc5c0cb5e53de59171a67679ae3c0eef

row() { echo -e "$1\t$2\t$PKG\t$CLASSES\t$ITERS\t$ROWSM\t$DIST\t$3"; }

row os_old_java_old "$IMG_0348" "$PLAIN"
row os_new_java_mid "$IMG_1826" "$PLAIN"
row os_new_java_new "$IMG_0350" "$PLAIN"
row os_new_java_new_jfr "$IMG_0350" "$JFR"
