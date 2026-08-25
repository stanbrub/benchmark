#!/usr/bin/env bash

set -o errexit
set -o pipefail

# Copyright (c) 2026-2026 Deephaven Data Labs and Patent Pending

# Runs a matrix script and converts its TSV output to a JSON matrix for GitHub Actions.
# The matrix script outputs tab-separated rows with fields:
#   run_label, docker_image, test_package, test_class_list,
#   test_iterations, scale_row_count, distribution, config_options
# Scale_row_count is in millions and auto-scaled to actual.
# Test_iterations is auto-forced to odd.
#
# Usage: build-matrix.sh <matrix-script>

FILE="$1"

if [[ -z "$FILE" ]]; then
  echo "Usage: build-matrix.sh <matrix-script>"
  exit 1
fi

if [[ ! -f "$FILE" ]]; then
  echo "::error::Matrix script not found: ${FILE}"
  exit 1
fi

TSV=$(bash "$FILE")

RESULT=$(echo "$TSV" | jq -Rsc '
  ["run_label","docker_image","test_package","test_class_list",
   "test_iterations","scale_row_count","distribution","config_options"] as $h |
  split("\n") | map(select(length > 0)) |
  [.[] | split("\t") | [range(length) as $i | {($h[$i]): .[$i]}] | add |
    if .scale_row_count then
      .scale_row_count = ((.scale_row_count | tonumber) * 1000000 | tostring)
    else . end |
    if .test_iterations then
      .test_iterations = ((.test_iterations | tonumber) as $n |
        if ($n % 2) == 0 then ($n + 1) else $n end | tostring)
    else . end
  ]
')

COUNT=$(echo "$RESULT" | jq 'length')
EXPECTED=$(grep -m1 '^EXPECTED_COMBOS=' "$FILE" | cut -d= -f2 || true)

if [[ -n "$EXPECTED" && "$COUNT" -ne "$EXPECTED" ]]; then
  echo "::error::Expected ${EXPECTED} combinations but got ${COUNT}. Update EXPECTED_COMBOS in ${FILE}."; exit 1
fi
echo "Matrix combinations: ${COUNT}"

MATRIX_OUT=$(echo "$RESULT" | jq -c .)
if [[ -n "$GITHUB_OUTPUT" ]]; then
  echo "matrix=${MATRIX_OUT}" >> "$GITHUB_OUTPUT"
  echo "combo_count=${COUNT}" >> "$GITHUB_OUTPUT"
  echo "### Matrix: ${COUNT} combinations" >> "$GITHUB_STEP_SUMMARY"
else
  echo "matrix=${MATRIX_OUT}"
fi

