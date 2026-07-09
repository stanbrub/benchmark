#!/usr/bin/env bash

set -o errexit
set -o pipefail

# Copyright (c) 2026-2026 Deephaven Data Labs and Patent Pending

# Reads a matrix config file and outputs a JSON matrix for GitHub Actions.
# Config format: key=value lines. Pipe-separated values define matrix axes.
#   Lines starting with # are comments. Blank lines are ignored.
#   ex. docker_image=edge                              (common value)
#   ex. config_options=-Xmx24g|-Xmx24g -XX:+UseZGC    (matrix axis)
# Scale_row_count is in millions and auto-scaled to actual.
# Test_iterations is auto-forced to odd.
#
# Usage: build-matrix.sh <config-file> <max-combinations>

FILE="$1"
MAX_COMBOS="${2:-10}"

if [[ -z "$FILE" ]]; then
  echo "Usage: build-matrix.sh <config-file> [max-combinations]"
  exit 1
fi

if [[ ! -f "$FILE" ]]; then
  echo "::error::Matrix file not found: ${FILE}"
  exit 1
fi

COMMON_JSON='{}'
MATRIX_JSON='{}'

while IFS= read -r line; do
  [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue

  key="${line%%=*}"
  value="${line#*=}"
  key=$(echo "$key" | xargs)

  if [[ "$value" == *"|"* ]]; then
    arr=$(echo "$value" | tr '|' '\n' | jq -R . | jq -s .)
    MATRIX_JSON=$(echo "$MATRIX_JSON" | jq --arg k "$key" --argjson v "$arr" '. + {($k): $v}')
  else
    COMMON_JSON=$(echo "$COMMON_JSON" | jq --arg k "$key" --arg v "$value" '. + {($k): $v}')
  fi
done < "$FILE"

RESULT=$(jq -n \
  --argjson common "$COMMON_JSON" \
  --argjson matrix "$MATRIX_JSON" \
  '
  def cartesian:
    keys_unsorted as $keys |
    if ($keys | length) == 0 then [{}]
    else
      $keys[0] as $k |
      .[$k] as $vals |
      (del(.[$k]) | cartesian) as $rest |
      [$vals[] as $v | $rest[] | . + {($k): ($v | tostring)}]
    end;

  ($matrix | cartesian) as $combos |
  [range($combos | length) as $i |
    $common + $combos[$i] |
    if .scale_row_count then
      .scale_row_count = ((.scale_row_count | tonumber) * 1000000 | tostring)
    else . end |
    if .test_iterations then
      .test_iterations = (
        (.test_iterations | tonumber) as $n |
        if ($n % 2) == 0 then ($n + 1) else $n end | tostring
      )
    else . end |
    if (.run_label == null or .run_label == "") then
      .run_label = "matrix_\($i + 1)"
    else . end
  ]
  ')

COUNT=$(echo "$RESULT" | jq 'length')
echo "Matrix combinations: ${COUNT} (limit: ${MAX_COMBOS})"

if [[ "$COUNT" -eq 0 ]]; then
  echo "::error::No combinations generated. Check config file."; exit 1
fi
if [[ "$COUNT" -gt "$MAX_COMBOS" ]]; then
  echo "::error::Combination count ${COUNT} exceeds limit ${MAX_COMBOS}."; exit 1
fi

MATRIX_OUT=$(echo "$RESULT" | jq -c .)
if [[ -n "$GITHUB_OUTPUT" ]]; then
  echo "matrix=${MATRIX_OUT}" >> "$GITHUB_OUTPUT"
else
  echo "matrix=${MATRIX_OUT}"
fi

echo "$RESULT" | jq -r 'to_entries[] | "  [\(.key + 1)] \(.value.run_label): \(.value | del(.run_label) | tojson)"'
