#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

# Copyright (c) 2026-2026 Deephaven Data Labs and Patent Pending

# Turn the group csv written by the nightly_vs_release Deephaven query into a matrix script
# for the Matrix Benchmarks workflow. Each regressed benchmark prefix gets two rows: one on the
# image the nightly ran and one on the release image, so the regression can be reproduced back
# to back on the same hardware.
#
# The nightly rows pin a digest rather than a tag. The nightly ran whatever 'edge' pointed at
# that day, and 'edge' has moved every day since, so only the digest reproduces the run.
#
# Requires ghcr-image-sha.sh (see IMAGE_SHA_SCRIPT) and a GitHub token with the read:packages
# scope, which that script takes from GH_TOKEN, ~/.config/ghcr-token, or `gh auth token`.
#
# ex. make-regression-matrix.sh /data/nightly-vs-release-groups.csv
# ex. make-regression-matrix.sh -o /tmp/rerun.sh nightly-vs-release-groups.csv

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
GITHUB_DIR=$(cd "${SCRIPT_DIR}/.." && pwd)

IMAGE_SHA_SCRIPT=${IMAGE_SHA_SCRIPT:-ghcr-image-sha.sh}
IMAGE_REPO=${IMAGE_REPO:-ghcr.io/deephaven/server}
LOOKBACK_DAYS=${LOOKBACK_DAYS:-7}
OUT_FILE=${GITHUB_DIR}/matrix/regression-rerun.sh

# Match the nightly workflow, since these runs are meant to reproduce it
LABEL_PREFIX=regress
TEST_PACKAGE=io.deephaven.benchmark.tests.standard
TEST_ITERATIONS=5
SCALE_ROW_COUNT=10   # Millions, scaled up by build-matrix.sh
DISTRIBUTION=random
CONFIG_OPTIONS='<default>'

usage() {
  echo "$0: Must supply the group csv written by the nightly_vs_release query"
  echo "  ex. $0 /data/nightly-vs-release-groups.csv"
  echo "  -o  matrix script to write (default ${OUT_FILE})"
  exit 1
}

while getopts ":o:" OPT; do
  case ${OPT} in
    o) OUT_FILE=$OPTARG ;;
    *) usage ;;
  esac
done
shift $((OPTIND - 1))

if [[ $# != 1 ]]; then
  usage
fi

CSV=$1

if [[ ! -r ${CSV} ]]; then
  echo "$0: Cannot read group csv: ${CSV}" >&2
  exit 2
fi

if ! command -v "${IMAGE_SHA_SCRIPT}" >/dev/null 2>&1; then
  echo "$0: Cannot run ${IMAGE_SHA_SCRIPT}, set IMAGE_SHA_SCRIPT to its path" >&2
  exit 2
fi

HEADER=$(head -1 "${CSV}")
for NAME in BenchmarkPrefix Worst_Benchmark Nightly_Date Release_Image; do
  if [[ ,${HEADER}, != *,${NAME},* ]]; then
    echo "$0: Missing the ${NAME} column in ${CSV}" >&2
    exit 2
  fi
done

# Pull the columns the matrix needs by header name, so added or reordered columns do not matter.
# Splitting on commas is safe here because benchmark names never contain one.
mapfile -t ROWS < <(awk -F, -v OFS='\t' '
  NR == 1 { for (i = 1; i <= NF; i++) col[$i] = i; next }
  { print $col["BenchmarkPrefix"], $col["Worst_Benchmark"], $col["Nightly_Date"], $col["Release_Image"] }
' "${CSV}")

if [[ ${#ROWS[@]} == 0 ]]; then
  echo "$0: No regressions in ${CSV}, nothing to rerun" >&2
  exit 2
fi

# Resolve the digest 'edge' pointed at on a date, walking back when a date published no image.
# Benchmarks still ran on such days, on whatever image was published before them.
# Sets DATED_IMAGE rather than echoing it, so the cache survives. A command substitution runs
# in a subshell, which would throw the cache away and repeat every lookup.
declare -A IMAGE_CACHE
DATED_IMAGE=""
LOOKUP_ERR=$(mktemp)
trap 'rm -f "${LOOKUP_ERR}"' EXIT
dated_image() {
  local DAY=$1 BACK IMAGE PROBE
  if [[ -n ${IMAGE_CACHE[${DAY}]:-} ]]; then
    DATED_IMAGE=${IMAGE_CACHE[${DAY}]}
    return
  fi
  for BACK in $(seq 0 "${LOOKBACK_DAYS}"); do
    PROBE=$(date -d "${DAY} -${BACK} days" +%F)
    if IMAGE=$("${IMAGE_SHA_SCRIPT}" "${IMAGE_REPO}:edge" "${PROBE}" 2>"${LOOKUP_ERR}"); then
      if [[ ${BACK} -gt 0 ]]; then
        echo "$0: No edge image published ${DAY}, using ${PROBE}" >&2
      fi
      IMAGE_CACHE[${DAY}]=${IMAGE%%$'\n'*}  # Newest first when a date has several
      DATED_IMAGE=${IMAGE_CACHE[${DAY}]}
      return
    fi
  done
  echo "$0: Cannot resolve ${IMAGE_REPO}:edge for ${DAY} within ${LOOKBACK_DAYS} days" >&2
  cat "${LOOKUP_ERR}" >&2
  return 1
}

# Sets CLASS_ENTRY to a test_class_list entry for a benchmark name prefix. The class filter
# expands 'Sort*' to ^.*[.](Sort.*)Test.*$, so a bare prefix only ever hits <Prefix>Test.
# Prefer the wildcard, which also picks up siblings like SortAscendingTest and SortDescendingTest.
# Some prefixes name no class at all though (RollingFormulaParamTick lives in
# RollingFormulaTickTest), so fall back to the class whose source declares the benchmark.
TEST_SRC_DIR=${GITHUB_DIR}/../src/it/java/${TEST_PACKAGE//./\/}
class_entry() {
  local PREFIX=$1 BENCH=$2 NAME FILE
  if [[ -n $(find "${TEST_SRC_DIR}" -name "${PREFIX}*Test.java" -print -quit) ]]; then
    CLASS_ENTRY="${PREFIX}*"
    return
  fi
  NAME=${BENCH% -Static}
  NAME=${NAME% -Inc}
  FILE=$(grep -rl -F "\"${NAME}" "${TEST_SRC_DIR}" | head -1)
  if [[ -z ${FILE} ]]; then
    echo "$0: No test class declares '${BENCH}', and none is named ${PREFIX}*Test" >&2
    return 1
  fi
  CLASS_ENTRY=$(basename "${FILE}" .java)
  CLASS_ENTRY=${CLASS_ENTRY%Test}
  echo "$0: ${PREFIX} benchmarks live in ${CLASS_ENTRY}Test" >&2
}

if [[ ! -d ${TEST_SRC_DIR} ]]; then
  echo "$0: Cannot find test sources at ${TEST_SRC_DIR}" >&2
  exit 2
fi

# Two prefixes can share a class, and duplicate rows would collide on run_label and blend into
# one result set. The csv is sorted worst first, so the first prefix to claim a class wins.
declare -A CLAIMED_BY
TSV_ROWS=()
for ROW in "${ROWS[@]}"; do
  IFS=$'\t' read -r PREFIX WORST_BENCHMARK NIGHTLY_DATE RELEASE_IMAGE <<<"${ROW}"
  class_entry "${PREFIX}" "${WORST_BENCHMARK}"
  if [[ -n ${CLAIMED_BY[${CLASS_ENTRY}]:-} ]]; then
    echo "$0: ${PREFIX} shares ${CLASS_ENTRY} with ${CLAIMED_BY[${CLASS_ENTRY}]}, already covered" >&2
    continue
  fi
  CLAIMED_BY[${CLASS_ENTRY}]=${PREFIX}
  dated_image "${NIGHTLY_DATE}"
  NIGHTLY_IMAGE=${DATED_IMAGE}
  LABEL=${CLASS_ENTRY%\*}
  RELEASE_TAG=${RELEASE_IMAGE##*:}
  TSV_ROWS+=("${LABEL_PREFIX}_${LABEL}_edge_${NIGHTLY_DATE}"$'\t'"${NIGHTLY_IMAGE}"$'\t'"${CLASS_ENTRY}")
  TSV_ROWS+=("${LABEL_PREFIX}_${LABEL}_rel_${RELEASE_TAG}"$'\t'"${RELEASE_IMAGE}"$'\t'"${CLASS_ENTRY}")
done

mkdir -p "$(dirname "${OUT_FILE}")"
{
  echo "# Rerun benchmark classes that regressed between the nightlies and the release"
  echo "# Generated $(date +%F) by $(basename "$0") from $(basename "${CSV}")"
  echo "# TSV columns: run_label, docker_image, test_package, test_class_list,"
  echo "#              test_iterations, scale_row_count, distribution, config_options"
  echo "EXPECTED_COMBOS=${#TSV_ROWS[@]}"
  echo
  echo "PKG=${TEST_PACKAGE}"
  echo "ITERS=${TEST_ITERATIONS}"
  echo "ROWSM=${SCALE_ROW_COUNT}"
  echo "DIST=${DISTRIBUTION}"
  echo "OPTS='${CONFIG_OPTIONS}'"
  echo
  echo '# emit <run_label> <docker_image> <test_class_list>'
  echo 'row() { echo -e "$1\t$2\t$PKG\t$3\t$ITERS\t$ROWSM\t$DIST\t$OPTS"; }'
  echo
  for ROW in "${TSV_ROWS[@]}"; do
    IFS=$'\t' read -r LABEL IMAGE CLASSES <<<"${ROW}"
    echo "row ${LABEL} ${IMAGE} '${CLASSES}'"
  done
} >"${OUT_FILE}"

chmod +x "${OUT_FILE}"
echo "Wrote ${#TSV_ROWS[@]} combinations to ${OUT_FILE}"

# The workflow input is a hardcoded choice list, so a new file has to be offered there once
MATRIX_NAME=$(basename "${OUT_FILE}")
WORKFLOW=${GITHUB_DIR}/workflows/matrix-exist-remote-benchmarks.yml
if [[ -r ${WORKFLOW} ]] && ! grep -q -- "- ${MATRIX_NAME}" "${WORKFLOW}"; then
  echo "Add '- ${MATRIX_NAME}' to the matrix_file options in $(basename "${WORKFLOW}")"
fi
