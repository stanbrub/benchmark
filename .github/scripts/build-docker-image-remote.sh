#!/usr/bin/env bash

set -o errexit
set -o pipefail

# Copyright (c) 2023-2026 Deephaven Data Labs and Patent Pending

# Build a local docker image on the remote side
# Ensure the docker image is running in the Deephaven directory

HOST=`hostname`
GIT_DIR=${HOME}/git
DEEPHAVEN_DIR=${HOME}/deephaven
DEEPHAVEN_VERSION_FILE=${GIT_DIR}/deephaven-core/build/version

if [ ! -d "${DEEPHAVEN_DIR}" ]; then
  echo "$0: Missing one or more Benchmark setup directories"
  exit 1
fi

title () { echo; echo $1; }

# Per-ref tag written by build-server-distribution-remote.sh.
DEEPHAVEN_TAG_FILE=${GIT_DIR}/benchmark-tag
DOCKER_TAG=$(cat ${DEEPHAVEN_TAG_FILE})
echo "DOCKER TAG: ${DOCKER_TAG}"

# Before the version file check: a skipped assemble legitimately leaves no version file.
if docker image inspect deephaven/server:${DOCKER_TAG} &>/dev/null 2>&1; then
  echo "Docker image deephaven/server:${DOCKER_TAG} already built. Skipping."
  exit 0
fi

if [ ! -f "${DEEPHAVEN_VERSION_FILE}" ]; then
  echo "$0: Missing Deephaven version file. Was the project built first?"
  exit 1
fi

title "- Setting up Remote Docker Image on ${HOST} -"

title "-- Building Deephaven Docker Image --"
export DEEPHAVEN_VERSION=$(cat ${DEEPHAVEN_VERSION_FILE})
cd ${GIT_DIR}/deephaven-server-docker
# Clear the previous ref's copies, since a differing version leaves a second wheel behind
rm -f contexts/server/server-jetty-*.tar contexts/server-slim/server-jetty-*.tar contexts/server/deephaven_core-*.whl
cp ${GIT_DIR}/deephaven-core/server/jetty-app/build/distributions/server-jetty-*.tar contexts/server/
cp ${GIT_DIR}/deephaven-core/server/jetty-app/build/distributions/server-jetty-*.tar contexts/server-slim/
cp ${GIT_DIR}/deephaven-core/py/server/build/wheel/deephaven_core-*-py3-none-any.whl contexts/server/

export DEEPHAVEN_SOURCES=custom
export DEEPHAVEN_CORE_WHEEL=$(find contexts/server -type f -name "*.whl" | xargs -n 1 basename)
export TAG=${DOCKER_TAG}

# Let server.hcl label the image with the commit, since local images have no digest to identify them
export GIT_REVISION=$(git -C ${GIT_DIR}/deephaven-core rev-parse HEAD)

echo "DEEPHAVEN_VERSION: ${DEEPHAVEN_VERSION}"
echo "DEEPHAVEN_CORE_WHEEL: ${DEEPHAVEN_CORE_WHEEL}"
echo "GIT_REVISION: ${GIT_REVISION}"

# Pull once so bakes resolve locally instead of from Docker Hub, which intermittently 502s
# Bases are every FROM image in the Dockerfile, with ARGs filled in from the args bake resolves for this
# branch and environment. The syntax frontend is left out: BuildKit checks the registry for it anyway.
BAKE_TARGET=$(docker buildx bake -f server.hcl --print | jq -c '.target.server')
DOCKERFILE=$(jq -r '"\(.context)/\(.dockerfile // "Dockerfile")"' <<< "${BAKE_TARGET}")
STAGES=$(awk 'toupper($1) == "FROM" && toupper($(NF-1)) == "AS" {print $NF}' ${DOCKERFILE})
declare -A BASE_IMAGES=()
while read -r ref; do
  while [[ ${ref} =~ \$\{([A-Za-z_][A-Za-z0-9_]*)\} ]]; do
    name=${BASH_REMATCH[1]}
    value=$(jq -r --arg n "${name}" '.args[$n] // empty' <<< "${BAKE_TARGET}")
    if [[ -z "${value}" ]]; then
      echo "$0: No bake arg for \${${name}} in ${DOCKERFILE}"
      exit 1
    fi
    ref=${ref//\$\{${name}\}/${value}}
  done
  if [[ ${ref} == *'$'* ]]; then
    echo "$0: Cannot resolve FROM ${ref} in ${DOCKERFILE}"
    exit 1
  fi
  # Stage references like os-bits or deephaven-custom are built here, not pulled
  if ! grep -qxF "${ref}" <<< "${STAGES}"; then
    BASE_IMAGES[${ref}]=1
  fi
done < <(awk 'toupper($1) == "FROM" {for (i = 2; i <= NF; i++) if ($i !~ /^--/) {print $i; break}}' ${DOCKERFILE})
if [[ ${#BASE_IMAGES[@]} -eq 0 ]]; then
  echo "$0: No base images found in ${DOCKERFILE}"
  exit 1
fi
echo "BASE IMAGES: ${!BASE_IMAGES[*]}"
for image in "${!BASE_IMAGES[@]}"; do
  if docker image inspect ${image} &>/dev/null; then
    continue
  fi
  for attempt in 1 2 3; do
    if docker pull ${image}; then
      break
    fi
    if [ ${attempt} -eq 3 ]; then
      echo "$0: Failed to pull ${image} after ${attempt} attempts"
      exit 1
    fi
    echo "Pull of ${image} failed (attempt ${attempt}); retrying in 30s"
    sleep 30
  done
done

docker buildx bake -f server.hcl

