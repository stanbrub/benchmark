#!/usr/bin/env bash

set -o errexit
set -o pipefail

# Copyright (c) 2026-2026 Deephaven Data Labs and Patent Pending

# Clear all build artifacts and data on the remote server to prepare for a fresh setup

GIT_DIR=${HOME}/git
RUN_DIR=${HOME}/run
DEEPHAVEN_DIR=${HOME}/deephaven

title () { echo; echo $1; }

title "- Clearing Remote Server on $(hostname) -"

title "-- Removing Git Repositories --"
sudo rm -rf ${GIT_DIR}

if command -v docker &>/dev/null; then
  title "-- Stopping Docker Containers --"
  sudo docker ps -q | xargs --no-run-if-empty -n 1 sudo docker kill
  title "-- Removing Docker Containers --"
  sudo docker ps -a -q | xargs --no-run-if-empty -n 1 sudo docker rm --force
  title "-- Removing Docker Images --"
  sudo docker images -a -q | xargs --no-run-if-empty -n 1 sudo docker rmi --force
  title "-- Pruning Docker Volumes --"
  sudo docker system prune --volumes --force
fi

title "-- Removing Run Directory --"
sudo rm -rf ${RUN_DIR}

title "-- Removing Deephaven Directory --"
sudo rm -rf ${DEEPHAVEN_DIR}
