# Seaflowpy

A Python package for SeaFlow flow cytometer data.

## Table of Contents

1. [Install](#install)
1. [Read EVT/OPP/VCT Files](#evtoppvct)
1. [Command-line Interface](#cli)
1. [Configuration](#configuration)
1. [Integration with R](#rintegration)
1. [Testing](#testing)
1. [Development](#development)

<a name="install"></a>

## Install

This package is compatible with Python 3.7 and 3.8.

### Source

This will clone the repo and create a new virtual environment `seaflowpy`.
`venv` can be replaced with `virtualenv`, `conda`, etc.

```sh
git clone https://github.com/armbrustlab/seaflowpy
cd seaflowpy
[[ -d ~/venvs ]] || mkdir ~/venvs
python3 -m venv ~/venvs/seaflowpy
source ~/venvs/seaflowpy/bin/activate
pip3 install -U pip setuptools wheel
pip3 install -r requirements-test.txt
pip3 install .
# Confirm the seaflowpy command-line tool is accessible
seaflowpy version
# Make sure basic tests pass
pytest
# Leave the new virtual environment
deactivate
```

### PyPI

```sh
pip3 install seaflowpy
```

### Docker

Docker images are available from Docker Hub at `ctberthiaume/seaflowpy`.

```sh
docker pull ctberthiaume/seaflowpy
docker run -it ctberthiaume/seaflowpy seaflowpy version
```

The Docker build file is in this repo at `/Dockerfile`. The build process for the Docker image is detailed in `/build.sh`.

<a name="evtoppvct"></a>

## Read EVT/OPP/VCT Files

All file reading functions will return a `pandas.DataFrame` of particle data.
Gzipped EVT, OPP, or VCT files can be read if they end with a ".gz" extension.
For these code examples assume `seaflowpy` has been imported as `sfp`
and `pandas` has been imported as `pd`, e.g.

```python
import pandas as pd
import seaflowpy as sfp
```

and `*_filepath` has been set to the correct data file.

Read an EVT file

```python
evt = sfp.fileio.read_evt_labview(evt_filepath)
```

Read an OPP file as an Apache Arrow Parquet file, select the 50% quantile, and subset columns.
VCT files created with `popcycle` are also standard Parquet files and can be read in a similar fashion.

```python
opp = pd.read_parquet(opp_filepath)
opp50 = opp[opp["q50"]]
opp50 = opp50[['fsc_small', 'chl_small', 'pe']]
```

<a name="cli"></a>

## Command-line interface

All `seaflowpy` CLI tools are accessible from the `seaflowpy` executable.
Run `seaflowpy --help` to begin exploring the CLI usage documentation.

### SFL validation workflow

SFL validation sub-commands are available under the `seaflowpy sfl` command.
The usage details for each command can be accessed as `seaflowpy sfl <cmd> -h`.

The basic worfkflow should be

1) If starting with an SDS file, first convert to SFL with `seaflowpy sds2sfl`

2) If the SFL file is output from `sds2sfl` or is a raw SeaFlow SFL file,
convert it to a normalized format with `seaflowpy sfl print`.
This command can be used to concatenate multiple SFL files,
e.g. merge all SFL files in day-of-year directories.

3) Check for potential errors or warnings with `seaflowpy sfl validate`.

4) Fix errors and warnings. Duplicate file errors can be fixed with `seaflowpy sfl dedup`.
Bad lat/lon errors may be fixed with`seaflowpy sfl convert-gga`,
assuming the bad coordinates are GGA to begin with.
This can be checked with with `seaflowpy sfl detect-gga`.
Other errors or missing values may need to be fixed manually.

5) (Optional) Update event rates based on true event counts and file duration
with `seaflowpy sfl fix-event-rate`.
True event counts for raw EVT files can be determined with `seaflowpy evt count`.
If filtering has already been performed then event counts can be pulled from
the `all_count` column of the opp table in the SQLITE3 database.
e.g. `sqlite3 -separator $'\t' SCOPE_14.db 'SELECT file, all_count ORDER BY file'`

6) (Optional) As a check for dataset completeness,
the list of files in an SFL file can be compared to the actual EVT files present
with `seaflowpy sfl manifest`. It's normal for a few files to differ,
especially near midnight. If a large number of files are missing it may be a
sign that the data transfer was incomplete or the SFL file is missing some days.

7) Once all errors or warnings have been fixed, do a final `seaflowpy validate`
before adding the SFL file to the appropriate repository.


<a name="configuration"></a>

## Configuration

To use `seaflowpy sfl manifest` AWS credentials need to be configured.
The easiest way to do this is to install the `awscli` Python package
and go through configuration.

```sh
pip3 install awscli
aws configure
```

This will store AWS configuration in `~/.aws` which `seaflowpy` will use to
access Seaflow data in S3 storage.

<a name="rintegration"></a>

## Integration with R

To call `seaflowpy` from R, update the PATH environment variable in
`~/.Renviron`. For example:

```sh
PATH=${PATH}:${HOME}/venvs/seaflowpy/bin
```

<a name="testing"></a>

## Testing

Seaflowpy uses `pytest` for testing. Tests can be run from this directory as
`pytest` to test the installed version of the package, or run `tox` to install
the source into a temporary virtual environment for testing.

<a name="development"></a>

## Development

### Source code structure

This project follows the [Git feature branch workflow](https://www.atlassian.com/git/tutorials/comparing-workflows/feature-branch-workflow).
Active development happens on the `develop` branch and on feature branches which are eventually merged into `develop`.

### Build

To build source tarball, wheel, and Docker image, run `./build.sh`. This will

* create `seaflowpy-dist` with source tarball and wheel file (created during Docker build)

* Docker image named `seaflowpy:<version>`

To remove all build files, run `rm -rf ./seaflowpy-dist`.

### Updating requirements files

Create a new virtual environment

```sh
python3 -m venv newenv
source newenv/bin/activate
```

Update pip, wheel, setuptools

```sh
pip3 install -U pip wheel setuptools
```

And install `seaflowpy`

```sh
pip3 install .
```

Then freeze the requirements

```sh
pip3 freeze | grep -v seaflowpy >requirements.txt
```

Then install test dependencies, test, and freeze

```sh
pip3 install pytest pytest-benchmark
pytest
pip3 freeze | grep -v seaflowpy >requirements-test.txt
```

Then install dev dependencies, test, and freeze

```sh
pip3 install pylint twine
pytest
pip3 freeze | grep -v seaflowpy >requirements-dev.txt
```

Leave the virtual environment

```sh
deactivate
```
