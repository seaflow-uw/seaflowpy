#!/usr/bin/env bash
pipenv run python -m ipykernel install --user --name $(basename $(pipenv --venv))
