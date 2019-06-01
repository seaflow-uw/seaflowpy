#!/bin/bash
git clean -fdx hooks linux64

# Move to repo root, build temp image, run pyinstaller
cd .. || exit
docker build -t seaflowpy-pyinstaller-temp . \
&& docker run --rm -v $(pwd)/pyinstaller:/mnt seaflowpy-pyinstaller-temp \
    bash -c 'apt-get -q update && apt-get -q install -y binutils libzmq3-dev && pip3 install -q pyinstaller && cd /mnt/linux64 && ./build.sh' \
&& docker rmi seaflowpy-pyinstaller-temp
