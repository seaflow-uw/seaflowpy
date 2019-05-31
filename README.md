# Seaflowpy

A Python package for SeaFlow flow cytometer data.

## Install

This package is compatible with Python 3.7.

### Command-line tool as single-file download

Single file executables of the `seaflowpy` command-line tool
for MacOS and Linux can be downloaded from the project's github
[release page](https://github.com/armbrustlab/seaflowpy/releases).

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

#### `seaflowpy sfl convert-gga`

Converts GGA coordinate values to decimal degree. Otherwise the file is
unchanged.

#### `seaflowpy sfl dedup`

Remove lines in an SFL file with duplicate "FILE" values.
Because it's impossible to know which of the duplicated SFL entries
corresponds to which EVT file, all duplicate rows are removed.
A unique list of removed files is printed to STDERR.

#### `seaflowpy sfl manifest`

Compare EVT files listed in an SFL file with EVT files on-disk
or in cloud object storage.
This can serve as a quick sanity check for the internal consistency of a
SeaFlow cruise data folder.
NB, it's normal for one file to be missing from the SFL file
or EVT day of year folder around midnight.

#### `seaflowpy sfl print`

Print a standard version of an SFL file with only the necessary columns.
The correct day of year folder will be added to "FILE" column values if not
present. "DATE" column will be created if not present from "FILE" column values
(only applies to new-style datestamped file names).
Any other required columns which are missing will be created with "NA" values.

#### `seaflowpy sfl validate`

Validate key values in an SFL file. The following checks are performed:

* all required columns are present
* "FILE" column values have day of year folders, are in the proper format,
in chronological order, and are unique
* "DATE" column values are in the proper format, represent valid date and times,
and are UTC
* "LAT" and "LON" coordinate column values are valid decimal degree values

Because some of these errors can affect every row of the file
(e.g. out of order files), only the first error of each type is printed.
To get a full printout of all errors run the command with `--verbose`.

## Development

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
