#!/bin/bash
# Build PyPI pure Python wheel, Docker image, pyinstaller execs

# --------------------------------------------------------------------------- #
# Pre-build checks
# --------------------------------------------------------------------------- #
# Make sure we're not in a Python virtual environment
if python3 -c 'import sys; sys.exit((hasattr(sys, "real_prefix") and sys.prefix != sys.real_prefix) or (hasattr(sys, "base_prefix") and sys.prefix != sys.base_prefix))'; then
    echo "Not in Python 3 virtual environment, continuing..." >&2
else
    echo "Please exit Python 3 virtual environment before building" >&2
    exit
fi

# --------------------------------------------------------------------------- #
# Step 1
# Create pure python wheel and source tarball. Will be saved in ./dist
# --------------------------------------------------------------------------- #
python3 setup.py -q sdist
python3 setup.py -q bdist_wheel
[[ -d build ]] && rm -rf build

# --------------------------------------------------------------------------- #
# Step 2
# Test new wheel in a temporary virtual environment
# --------------------------------------------------------------------------- #
venvdir=$(mktemp -dt seaflowpy)
if [ -z "$venvdir" ]; then
    echo "Could not create virtualenv temp directory name" >&2
    exit 1
fi
echo "Creating virtualenv $venvdir" >&2
python3 -m venv "$venvdir"
# shellcheck source=/dev/null
source "$venvdir/bin/activate"
echo "Installing requirements-dev.txt; seaflowpy from wheel" >&2
pip3 install -q -r requirements-dev.txt
# --no-index to prevent pulling from pypi in case pypi version is higher
pip3 install -q --no-index -f ./dist seaflowpy
git clean -fdx tests  # clean up test caches
pytest
pytestrc=$?
deactivate

if [ $pytestrc -ne 0 ]; then
    exit $pytestrc
fi

# --------------------------------------------------------------------------- #
# Step 3
# Build a docker image with wheel, tagged with current version string
# --------------------------------------------------------------------------- #
verstr=$(git describe --tags --dirty --always)
docker build -t seaflowpy:"$verstr" .
if [ $? -ne 0 ]; then
    echo "Error building Docker image" >&2
    exit $?
fi

# --------------------------------------------------------------------------- #
# Step 4
# Test the new docker image
# --------------------------------------------------------------------------- #
git clean -fdx tests  # remove test cache
docker run --rm -v "$(pwd):/mnt" seaflowpy:"$verstr" bash -c 'cd /mnt && pip3 install -q pytest pytest-benchmark && pytest --cache-clear'
git clean -fdx tests  # remove test cache from linux
dockertestrc=$?
if [ $dockertestrc -ne 0 ]; then
    echo "Docker image failed tests" >&2
    exit $?
fi

# --------------------------------------------------------------------------- #
# Step 5
# Build pyinstaller executables. Linux target will be built in a temp docker
# container using wheel from step 1. MacOS target will be built in the temp
# virtual environment created in step 2.
# --------------------------------------------------------------------------- #
# shellcheck source=/dev/null
source "$venvdir/bin/activate"
pip3 install pyinstaller
cd pyinstaller || exit 1
./build_all.sh
deactivate

# --------------------------------------------------------------------------- #
# Cleanup tasks. Find build files to remove, and remove them.
# --------------------------------------------------------------------------- #
# git clean -fdn  # remove build files (dry run)
# git clean -fd   # remove build files

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
# Test against test PyPI repo
# twine upload --repository-url https://test.pypi.org/legacy/ dist/seaflowpy-*

# Create a virtualenv and test install from test.pypi.org
# python -m venv pypi-test
# pypi-test/bin/pip install -r requirements.txt
# pypi-test/bin/pip install -i https://testpypi.python.org/pypi seaflowpy
# pypi-test/bin/seaflowpy version

# Then upload to the real PyPI
# twine upload dist/seaflowpy-*
