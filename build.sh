#!/bin/bash
# Build Docker image, Python wheel and source tarball
function print_help {
    echo "usage: build.sh [-h] [-s]"
    echo "  -h: display this help"
    echo "  -s: skip tests"
    echo "  -v: custom version string"
}

skiptests=0
verstr=""
while getopts ":hsv:" opt; do
    case ${opt} in
        h ) # help
            print_help
            exit
            ;;
        s ) # skip tests
            skiptests=1
            ;;
        v ) # custom version string
            verstr=$OPTARG
            ;;
        : )
            echo "error: $OPTARG requires an argument" 1>&2
            exit 1
            ;;
        \? )
            print_help
            exit
            ;;
    esac
done
shift $((OPTIND - 1))
if [[ -z "$verstr" ]]; then
    verstr=$(git describe --tags --dirty --always)
fi
echo "using version string $verstr"

# --------------------------------------------------------------------------- #
# Build a Docker image
# --------------------------------------------------------------------------- #
docker build -t seaflowpy:"$verstr" .
buildrc=$?
if [ $buildrc -ne 0 ]; then
    echo "error building Docker image" >&2
    exit $?
fi

# --------------------------------------------------------------------------- #
# Test the new Docker image
# --------------------------------------------------------------------------- #
if [[ "$skiptests" -eq 0 ]]; then
    docker run -it --rm seaflowpy:"$verstr" pytest
    dockertestrc=$?
    if [ $dockertestrc -ne 0 ]; then
        echo "docker image failed tests" >&2
        exit $?
    fi
fi

# --------------------------------------------------------------------------- #
# Grab the universal wheel and source tarball from the new Docker image
# Will create directory seaflowpy-dist
# --------------------------------------------------------------------------- #
[[ -d seaflowpy-dist ]] || mkdir seaflowpy-dist
docker run -it --rm -v "$(pwd)"/seaflowpy-dist:/dist seaflowpy:"$verstr" bash -c 'cp /seaflowpy/dist/* /dist/'

# --------------------------------------------------------------------------- #
# Misc docker tasks
# --------------------------------------------------------------------------- #
# Find docker images created with this script
# docker image ls --filter=reference='seaflowpy:*'

# Remove all iamges created with this script
# docker rmi $(docker image ls -q --filter=reference='seaflowpy:*')

# Tag the image created with this script and push to docker hub
# docker image tag seaflowpy:<version> account/seaflowpy:<version>
# docker push account/seaflowpy:<version>

# --------------------------------------------------------------------------- #
# Optional, upload wheel and source tarball to PyPI
# --------------------------------------------------------------------------- #
# Source tarball and wheel should be seaflowpy-dist after Docker build
# If not, copy from docker image
# mkdir dist
# docker run -it --rm -v $(pwd)/seaflowpy-dist:/app seaflowpy:$verstr bash -c 'cp /seaflowpy-dist/* /app'
# Test against test PyPI repo
# twine upload --repository-url https://test.pypi.org/legacy/ seaflowpy-dist/seaflowpy-<version>*

# Create a virtualenv and test install from test.pypi.org
# python -m venv pypi-test
# pypi-test/bin/pip install -r requirements.txt
# pypi-test/bin/pip install -i https://testpypi.python.org/pypi seaflowpy
# pypi-test/bin/seaflowpy version

# Then upload to the real PyPI
# twine upload seaflowpy-dist/seaflowpy-<version>*
