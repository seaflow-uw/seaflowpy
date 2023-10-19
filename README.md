# Seaflowpy

A Python package for SeaFlow flow cytometer data.

## Table of Contents

1. [Install](#install)
1. [Command-line Interface](#cli)
1. [Configuration](#configuration)
1. [Testing](#testing)

<a name="install"></a>

## Install

This package is compatible with Python 3.10 and 3.11

### Source

To install this project from a clone of the repo use poetry with `poetry install`.

### PyPI

```sh
pip install seaflowpy
```

### Docker

Docker images are available from Docker Hub at `ctberthiaume/seaflowpy`.

```sh
docker pull ctberthiaume/seaflowpy
docker run -it ctberthiaume/seaflowpy seaflowpy version
```

The Docker build file is in this repo at `/Dockerfile`. The build process for the Docker image is detailed in `/build-docker.sh`.

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

<a name="testing"></a>

## Testing

Seaflowpy uses `pytest` for testing. Tests can be run from this directory as
`pytest` to test the installed version of the package.
