#!/bin/bash
cd hooks || exit
git clean -fdx  # remove untracked files from this directory
cd ../macos || exit
git clean -fdx  # remove untracked files from this directory
./build.sh
