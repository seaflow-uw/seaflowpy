#!/bin/bash
# Do some cleanup removing untracked files
git clean -fdx hooks
git clean -fdx linux64

# Move to repo root, build temp image, run pyinstaller
cd .. || exit
docker build -t seaflowpy-pyinstaller-temp . \
&& docker run --rm -v $(pwd)/pyinstaller:/mnt seaflowpy-pyinstaller-temp \
    bash -c 'apt-get update && apt-get install -y binutils && pip3 install pyinstaller && cd /mnt/linux64 && ./build.sh' \
&& docker rmi seaflowpy-pyinstaller-temp
