#!/bin/bash
# Build MacOS and Linux pyinstaller executables
./build_macos.sh
./build_linux.sh "$@" # pass any docker image name here
