#!/bin/sh
# Script to build a single file executable of seaflowpy with pyinstaller
# based on the seaflowpy package installed in the current environment and
# data files in this source tree. Executable will be at dist/seaflowpy.

pyinstaller --onefile --additional-hooks-dir '../hooks' \
  --add-data '../../src/seaflowpy/data/popcycle.sql:seaflowpy/data' \
  "$(which seaflowpy)"
