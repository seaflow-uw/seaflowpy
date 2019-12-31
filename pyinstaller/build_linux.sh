#!/bin/bash
git clean -fdx hooks linux64

# Move to repo root for docker bind mounts
cd .. || exit

# Common commands to run in docker image to build pyinstaller exe
docker_cmd='apt-get -q update && apt-get -q install -y binutils libzmq3-dev && pip3 install -q pyinstaller && cd /mnt/linux64 && ./build.sh'

if [ $# -eq 0 ]; then
    # Build temp image, run pyinstaller
    docker build -t seaflowpy-pyinstaller-temp . \
    && docker run --rm -v $(pwd)/pyinstaller:/mnt seaflowpy-pyinstaller-temp \
        bash -c "$docker_cmd" \
    && docker rmi seaflowpy-pyinstaller-temp
else
    # Build pyinstaller exe in docker image passed as first cli arg
    docker run --rm -v $(pwd)/pyinstaller:/mnt "$1" \
        bash -c "$docker_cmd"
fi
# Zip pyinstaller executable to keep executable permissions
cd pyinstaller/linux64/dist || exit
mkdir seaflowpy-$(git describe)-linux64 && cp seaflowpy seaflowpy-$(git describe)-linux64/seaflowpy-linux64
zip -0 seaflowpy-$(git describe)-linux64.zip -r seaflowpy-$(git describe)-linux64
