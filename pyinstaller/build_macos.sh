#!/bin/bash
git clean -fdx hooks macos
cd macos || exit
./build.sh
