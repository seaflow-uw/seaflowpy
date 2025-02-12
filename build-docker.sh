#!/usr/bin/env bash

set -e

uv export --no-dev --all-extras --group test --no-emit-project >build-requirements.txt
APP_VERSION=$(uv run seaflowpy version)
if [[ -z "$APP_VERSION" ]]; then
    echo "could not get seaflowpy version string" >&2
    exit 1
fi
uv build
echo "Building Docker image for ${APP_VERSION} with Docker tag ${APP_VERSION}"
docker build --build-arg "ARG_APP_VERSION=${APP_VERSION}" -t "ctberthiaume/seaflowpy:${APP_VERSION}" .
echo "Docker build complete, running seaflowpy tests in side container"
docker run -it --rm "ctberthiaume/seaflowpy:${APP_VERSION}" bash -c 'cd /seaflowpy-src/* && pytest'
