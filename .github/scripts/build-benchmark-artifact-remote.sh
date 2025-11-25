#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset

# Build benchmark artifact on the remote side
# Assumes git branch is available and docker is running

HOST=`hostname`
GIT_DIR=/root/git/benchmark
RUN_DIR=/root/run
DEEPHAVEN_DIR=/root/deephaven

if [ ! -d "${GIT_DIR}" ]; then
  echo "$0: Missing one or more Benchmark setup directories"
  exit 1
fi

title () { echo; echo $1; }

title "- Building Remote Benchmark Artifact on ${HOST} -"

title "-- Building and Verifying --"
cd ${GIT_DIR}
mvn verify

title "-- Cleanup After Build  --"
cd ${DEEPHAVEN_DIR};
docker compose down

title "-- Create AOT Stuff --"  # Presumes the AOTMode=record has been turned on previous to verify
sed -i '/AOT_OPTS/c\
AOT_OPTS=-XX:AOTMode=create -XX:AOTConfiguration=/data/app.aotconf -XX:AOTCache=/data/app.aot' .env
docker compose up 
docker compose down

# Set up all successive docker runs to use the cached AOT if the compose file has the AOT_OPTS var
sed -i '/AOT_OPTS/c\
AOT_OPTS=-XX:AOTCache=/data/app.aot -XX:AOTMode=on' .env


rm -f data/*.*

title "-- Copying Artifact and Tests to Run Directory --"
rm -rf ${RUN_DIR}
mkdir -p ${RUN_DIR}/
cp ${GIT_DIR}/target/deephaven-benchmark-*.jar ${RUN_DIR}/
mv ${RUN_DIR}/deephaven-benchmark-*-tests.jar ${RUN_DIR}/standard-tests.jar
cp ${GIT_DIR}/.github/resources/*.properties ${RUN_DIR}/

