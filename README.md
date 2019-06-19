# Seaflowpy

A Python package for SeaFlow flow cytometer data.

## Install

This package is compatible with Python 3.7.

### Command-line tool as single-file download

Single file executables of the `seaflowpy` command-line tool
for MacOS and Linux can be downloaded from the project's github
[releases page](https://github.com/armbrustlab/seaflowpy/releases).
This is the recommended method if only the command-line tool is required.

### Docker

Docker image are available from Docker Hub at `ctberthiaume/seaflowpy`.

```
docker run -it ctberthiaume/seaflowpy seaflowpy version
```

The Docker build file is in this repo at `/Dockerfile`.

### PyPI

```
pip3 install seaflowpy
```

### Source

This will clone the repo and create a new virtual environment `seaflowpy`.
`venv` can be replaced with `virtualenv`, `conda`, etc.

```sh
git clone https://github.com/armbrustlab/seaflowpy
cd seaflowpy
python3 -m venv seaflowpy
source seaflowpy/bin/activate
pip3 install -r requirements.txt
pip3 install .
# Confirm the seaflowpy command-line tool is accessible
seaflowpy version
deactivate
```

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

## Integration with R

To call `seaflowpy` from R, update the PATH environment variable in
`~/.Renviron`. For example:

```sh
PATH=${PATH}:${HOME}/venvs/seaflowpy/bin
```

## Testing

Seaflowpy uses `pytest` for testing. Tests can be run from this directory as
`pytest` to test the installed version of the package, or run `tox` to install
the source into a temporary virtual environment for testing.

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

## Development

### Source code structure

This project follows the [Git feature branch workflow](https://www.atlassian.com/git/tutorials/comparing-workflows/feature-branch-workflow).
Active development happens on the `develop` branch and on feature branches which are eventually merged into `develop`.
Commits on the `master` branch represent stable release snapshots with version tags and build products,
merged from `develop` with `--no-ff` to create a single commit in `master`
while keeping the complete commit history in develop.

### Build

To build source tarball, wheel, PyInstaller files, and Docker image, run `./build.sh`.
This will

* create `dist` with source tarball and wheel file

* executable files in `./pyinstaller/macos/dist/seaflowpy` and `./pyinstaller/linux64/dist/seaflowpy`

* Docker image named `seaflowpy:<version>`

To remove all build files, run `git clean -fd`.

PyInstaller files and Docker image create depend on the wheel file located in `dist`.

### Updating requirements files

Create a new virtual environment

```sh
python3 -m venv newenv
source newenv/bin/actviate
```

And install `seaflowpy`

```sh
pip3 install .
```

Then freeze the requirements

```sh
pip3 freeze | grep -v seaflowpy >requirements.txt
```

Then install dev dependencies and freeze

```sh
pip3 install pylint pytest tox twine
pip3 freeze | grep -v seaflowpy >requirements-dev.txt
```

Do some testing, then leave this temporary virtual environment

```sh
deactivate
```
