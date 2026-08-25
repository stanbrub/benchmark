#!/usr/bin/env bash

set -o errexit
set -o pipefail

# Copyright (c) 2023-2026 Deephaven Data Labs and Patent Pending

# Assemble the Deephaven server artifacts on the remote side if needed
# The supplied argument can be an image name or <owner>::<branch>
# Ensure that the artifacts and Deephaven version are available in standard directories

if [[ $# != 1 ]]; then
  echo "$0: Missing docker image/branch argument"
  exit 1
fi

HOST=`hostname`
GIT_DIR=${HOME}/git
DEEPHAVEN_DIR=${HOME}/deephaven
DOCKER_IMG=$1
BRANCH_DELIM=":"
BUILD_JAVA=temurin-17-jdk-amd64

if [ ! -d "${DEEPHAVEN_DIR}" ]; then
  echo "$0: Missing one or more Benchmark setup directories"
  exit 1
fi

title () { echo; echo $1; }

DEEPHAVEN_VERSION_FILE=${GIT_DIR}/deephaven-core/build/version
if [ -f "${DEEPHAVEN_VERSION_FILE}" ]; then
  echo "Server distribution already built. Skipping."
  exit 0
fi

OWNER=$(sed 's/'"${BRANCH_DELIM}"'.*//g' <<< "${DOCKER_IMG}")
BRANCH_NAME=$(sed 's/.*'"${BRANCH_DELIM}"'//g' <<< "${DOCKER_IMG}")
echo "OWNER: ${OWNER}"
echo "BRANCH: ${BRANCH_NAME}"

title "-- Cloning deephaven-core --"
cd ${GIT_DIR}
if [ ! -d "deephaven-core" ]; then
  # Do not use --single-branch here, because it does not allow checkout by commit hash 
  git clone https://github.com/${OWNER}/deephaven-core.git
fi
cd deephaven-core
git fetch origin
git checkout ${BRANCH_NAME}

title "-- Cloning deephaven-server-docker --"
cd ${GIT_DIR}
if [ ! -d "deephaven-server-docker" ]; then
  git clone -b main --single-branch https://github.com/deephaven/deephaven-server-docker.git
fi

title "-- Assembling Python Deephaven Core Server --"
cd ${GIT_DIR}/deephaven-core
export JAVA_HOME=/usr/lib/jvm/${BUILD_JAVA}

echo "org.gradle.daemon=false" >> gradle.properties
./gradlew outputVersion server-jetty-app:assemble py-server:assemble



