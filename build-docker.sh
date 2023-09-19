#!/usr/bin/env bash

set -e

git status
if git diff-index --quiet HEAD --; then
  poetry export --with=test >build-requirements.txt
  POETRY_APP_VERSION=$(poetry version | awk '{print $2}')
  DOCKER_APP_VERSION=${POETRY_APP_VERSION/\+/_plus_}
  poetry build
  echo "Building Docker image for ${POETRY_APP_VERSION} with Docker tag ${DOCKER_APP_VERSION}"
  docker build --build-arg "ARG_APP_VERSION=${POETRY_APP_VERSION}" -t "ctberthiaume/seaflowpy:${DOCKER_APP_VERSION}" .
  echo "Docker build complete, running seaflowpy tests in side container"
  docker run -it --rm "ctberthiaume/seaflowpy:${DOCKER_APP_VERSION}" bash -c 'cd /seaflowpy-src/* && pytest'
else
  echo ""
  echo "Error: build will not begin until uncommitted changes have been committed" >&2
  exit 1
fi