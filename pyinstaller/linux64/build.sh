#!/bin/sh
# Script to build a single file executable of seaflowpy with pyinstaller
# based on the seaflowpy package installed in the current environment and
# data files in this source tree. Executable will be at dist/seaflowpy.

# Based on use of --user when installing packages in Docker image, manually
# set site-packages location here.
sitepackages=/usr/local/seaflowpy/lib/python3.7/site-packages
pyinstaller --onefile --additional-hooks-dir '../hooks' \
  --add-data "$sitepackages/seaflowpy/data/popcycle.sql:seaflowpy/data" \
  "$(which seaflowpy)"
