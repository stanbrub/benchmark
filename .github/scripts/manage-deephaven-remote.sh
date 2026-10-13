#!/usr/bin/env bash

set -o errexit
set -o pipefail

# Copyright (c) 2023-2026 Deephaven Data Labs and Patent Pending

# Start or Stop a Deephaven image based on the given directive and image/branch name
# The directives argument can be start or stop
# The supplied image argument can be an image name or <owner>::<branch>

if [[ $# -lt 3 ]]; then
  echo "$0: Missing docker directive, image/branch, config options argument"
  exit 1
fi

DIRECTIVE=$1
DOCKER_IMG=$2
CONFIG_OPTS="${@:3}"
HOST=`hostname`
GIT_DIR=${HOME}/git
DEEPHAVEN_DIR=${HOME}/deephaven

if [ ! -d "${DEEPHAVEN_DIR}" ]; then
  echo "$0: Missing one or more Benchmark setup directories"
  exit 1
fi

title () { echo; echo $1; }

title "- Setting up Remote Docker Image on ${HOST} -"

cd ${DEEPHAVEN_DIR}

if [[ ${CONFIG_OPTS} == "<default>" ]]; then
  CONFIG_OPTS="-Xmx24g"
fi
echo "CONFIG_OPTS=${CONFIG_OPTS}" > .env
echo "ENV_DEEPHAVEN_HOST_OS_DIR=${DEEPHAVEN_DIR}" >> .env

# Reuse a pulled image the same way a built one is reused. A clear wipes images, so the first run
# after it pulls, and later rows of a long matrix cannot drift onto a newly published tag
pull_if_absent () {
  if docker image inspect "$1" &>/dev/null 2>&1; then
    echo "Image $1 already pulled. Skipping pull."
  else
    docker compose pull
  fi
}

if [[ ${DOCKER_IMG} == ghcr.io/* ]]; then
  echo "DOCKER_IMG=${DOCKER_IMG}" >> .env
  pull_if_absent "${DOCKER_IMG}"
elif [[ ${DOCKER_IMG} == *":"* ]]; then
  # Locally built from <owner>:<ref>, under the per-ref tag recorded by the distribution build.
  LOCAL_TAG=$(cat ${GIT_DIR}/benchmark-tag)
  echo "DOCKER_IMG=deephaven/server:${LOCAL_TAG}" >> .env
else
  echo "DOCKER_IMG=ghcr.io/deephaven/server:${DOCKER_IMG}" >> .env
  pull_if_absent "ghcr.io/deephaven/server:${DOCKER_IMG}"
fi

if [[ ${DIRECTIVE} == 'start' ]]; then
  docker compose up -d
fi

if [[ ${DIRECTIVE} == 'stop' ]]; then
  docker compose down
fi

