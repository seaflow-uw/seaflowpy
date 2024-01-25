#!/usr/bin/env bash

set -e

poetry export --with=test >build-requirements.txt
POETRY_APP_VERSION=$(poetry version | awk '{print $2}')
poetry build
echo "Building Docker image for ${POETRY_APP_VERSION} with Docker tag ${POETRY_APP_VERSION}"
docker build --build-arg "ARG_APP_VERSION=${POETRY_APP_VERSION}" -t "ctberthiaume/seaflowpy:${POETRY_APP_VERSION}" .
echo "Docker build complete, running seaflowpy tests in side container"
docker run -it --rm "ctberthiaume/seaflowpy:${POETRY_APP_VERSION}" bash -c 'cd /seaflowpy-src/* && pytest'
