#!/bin/bash
git clean -fdx hooks macos
cd macos || exit
./build.sh

# Zip pyinstaller executable to keep executable permissions
cd dist || exit
mkdir seaflowpy-$(git describe)-darwin && cp seaflowpy seaflowpy-$(git describe)-darwin/seaflowpy-darwin
zip -0 seaflowpy-$(git describe)-darwin.zip -r seaflowpy-$(git describe)-darwin
